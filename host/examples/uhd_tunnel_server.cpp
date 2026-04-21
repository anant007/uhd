//
// Copyright 2026 Ettus Research, a National Instruments Brand
//
// SPDX-License-Identifier: GPL-3.0-or-later
//

// uhd_tunnel_server — Runs on the machine physically connected to the USRP.
// Bridges UDP traffic between the USRP and remote uhd_tunnel_client via TCP.
//
// For each configured UHD UDP port (49152, 49153, 49600):
//   - Listens on a TCP port for the tunnel client to connect
//   - Maintains a real UDP socket to the USRP device
//   - Bidirectionally relays datagrams using length-prefixed TCP framing
//
// For MPM RPC (TCP 49601):
//   - Listens for tunnel client, then connects to USRP's RPC port
//   - Raw bidirectional TCP byte relay
//
// Usage:
//   uhd_tunnel_server --device-addr 192.168.10.2 [--tunnel-port 5800]

#include "uhd_tunnel/uhd_tunnel_common.h"

#include <boost/program_options.hpp>

#include <csignal>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace po = boost::program_options;
using namespace uhd_tunnel;

// ============================================================================
// Global State
// ============================================================================
static std::atomic<bool> stop_signal(false);

static void sig_handler(int)
{
    stop_signal.store(true);
}

// Per-channel runtime state
struct ChannelState {
    socket_t tcp_listen_sock = INVALID_SOCK;  // TCP listen socket
    socket_t tcp_client_sock = INVALID_SOCK;  // Accepted TCP client
    socket_t udp_sock        = INVALID_SOCK;  // UDP socket to USRP
    ChannelStats stats;
    std::thread tcp_to_udp_thread;
    std::thread udp_to_tcp_thread;
};

// ============================================================================
// UDP-over-TCP Channel: TCP → UDP direction
// Reads framed datagrams from the tunnel client, sends them to USRP via UDP.
// ============================================================================
static void thread_tcp_to_udp(ChannelState& ch, size_t ch_idx, const std::string& device_addr)
{
    const auto& def = CHANNELS[ch_idx];
    std::vector<uint8_t> buf(MAX_UDP_DGRAM);
    bool udp_connected = false;

    std::cout << "[server] " << def.name << " TCP→UDP thread started\n";

    while (!stop_signal.load()) {
        uint32_t len = 0;
        if (!tcp_recv_frame(ch.tcp_client_sock, buf.data(), len)) {
            if (!stop_signal.load())
                std::cerr << "[server] " << def.name << " TCP recv failed\n";
            break;
        }

        if (len == 0) continue; // keepalive

        // Lazy-create the connected UDP socket to USRP on first packet
        // (we use a connected socket so the kernel remembers the destination)
        if (!udp_connected) {
            ch.udp_sock = create_udp_sender(device_addr, def.uhd_port);
            if (ch.udp_sock == INVALID_SOCK) {
                std::cerr << "[server] " << def.name
                          << " failed to create UDP socket to " << device_addr
                          << ":" << def.uhd_port << "\n";
                break;
            }
            // Set a recv timeout so the UDP→TCP thread can check stop_signal
            set_recv_timeout(ch.udp_sock, 200);
            udp_connected = true;
        }

        int sent = ::send(ch.udp_sock, reinterpret_cast<const char*>(buf.data()),
            static_cast<int>(len), 0);
        if (sent <= 0) {
            ch.stats.errors.fetch_add(1);
        } else {
            ch.stats.packets_to_device.fetch_add(1);
            ch.stats.bytes_to_device.fetch_add(static_cast<uint64_t>(sent));
        }
    }

    std::cout << "[server] " << def.name << " TCP→UDP thread exiting\n";
}

// ============================================================================
// UDP-over-TCP Channel: UDP → TCP direction
// Receives UDP datagrams from the USRP, frames them, sends to tunnel client.
// ============================================================================
static void thread_udp_to_tcp(ChannelState& ch, size_t ch_idx)
{
    const auto& def = CHANNELS[ch_idx];
    std::vector<uint8_t> buf(MAX_UDP_DGRAM);

    std::cout << "[server] " << def.name << " UDP→TCP thread started\n";

    // Wait for the UDP socket to be created by the TCP→UDP thread
    while (!stop_signal.load() && ch.udp_sock == INVALID_SOCK) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    while (!stop_signal.load()) {
        if (ch.udp_sock == INVALID_SOCK) break;

        int n = ::recv(ch.udp_sock, reinterpret_cast<char*>(buf.data()),
            static_cast<int>(MAX_UDP_DGRAM), 0);
        if (n <= 0) {
            // Timeout or error — just retry if not stopping
            continue;
        }

        if (!tcp_send_frame(ch.tcp_client_sock, buf.data(), static_cast<uint32_t>(n))) {
            if (!stop_signal.load())
                std::cerr << "[server] " << def.name << " TCP send failed\n";
            break;
        }

        ch.stats.packets_from_device.fetch_add(1);
        ch.stats.bytes_from_device.fetch_add(static_cast<uint64_t>(n));
    }

    std::cout << "[server] " << def.name << " UDP→TCP thread exiting\n";
}

// ============================================================================
// TCP Relay Channel (for MPM RPC port 49601)
// Bidirectional raw TCP byte relay between tunnel client and USRP.
// ============================================================================
static void thread_tcp_relay_half(
    socket_t from, socket_t to, ChannelStats& stats, bool to_device, const char* label)
{
    std::vector<char> buf(65536);
    while (!stop_signal.load()) {
        int n = ::recv(from, buf.data(), static_cast<int>(buf.size()), 0);
        if (n <= 0) break;

        int sent_total = 0;
        while (sent_total < n) {
            int s = ::send(to, buf.data() + sent_total, n - sent_total, 0);
            if (s <= 0) goto done;
            sent_total += s;
        }

        if (to_device) {
            stats.packets_to_device.fetch_add(1);
            stats.bytes_to_device.fetch_add(static_cast<uint64_t>(n));
        } else {
            stats.packets_from_device.fetch_add(1);
            stats.bytes_from_device.fetch_add(static_cast<uint64_t>(n));
        }
    }
done:
    std::cout << "[server] TCP relay " << label << " exiting\n";
}

// ============================================================================
// Main
// ============================================================================
int main(int argc, char* argv[])
{
    try {
        return [&]() -> int {
    std::string device_addr, bind_addr;
    uint16_t tunnel_port;
    size_t buf_size;
    int stats_interval;

    // clang-format off
    po::options_description desc("UHD Tunnel Server — Options");
    desc.add_options()
        ("help,h",          "Show help message")
        ("device-addr",     po::value<std::string>(&device_addr)->required(),
                            "USRP device IP address (e.g. 192.168.10.2)")
        ("tunnel-port",     po::value<uint16_t>(&tunnel_port)->default_value(TUNNEL_BASE_PORT),
                            "Base TCP port for tunnel channels")
        ("bind-addr",       po::value<std::string>(&bind_addr)->default_value("0.0.0.0"),
                            "Address to bind tunnel TCP listeners")
        ("buf-size",        po::value<size_t>(&buf_size)->default_value(TCP_BUF_SIZE),
                            "TCP socket buffer size (bytes)")
        ("stats-interval",  po::value<int>(&stats_interval)->default_value(5),
                            "Stats print interval (seconds, 0=off)")
    ;
    // clang-format on

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 0;
    }
    po::notify(vm);

    std::signal(SIGINT, &sig_handler);
#ifndef _WIN32
    std::signal(SIGPIPE, SIG_IGN);
#endif

    init_sockets();

    std::cout << "============================================\n"
              << "  UHD Tunnel Server\n"
              << "  Device:    " << device_addr << "\n"
              << "  Bind:      " << bind_addr << "\n"
              << "  Base port: " << tunnel_port << "\n"
              << "============================================\n\n";

    // Create per-channel state
    std::array<ChannelState, NUM_CHANNELS> channels;

    // Create TCP listen sockets for all channels
    for (size_t i = 0; i < NUM_CHANNELS; i++) {
        uint16_t port         = tunnel_port + CHANNELS[i].tunnel_offset;
        channels[i].tcp_listen_sock = create_tcp_listener(bind_addr, port);
        if (channels[i].tcp_listen_sock == INVALID_SOCK) {
            std::cerr << "[server] FATAL: cannot listen on " << bind_addr
                      << ":" << port << " for " << CHANNELS[i].name << "\n";
            cleanup_sockets();
            return 1;
        }
        std::cout << "[server] Listening on " << bind_addr << ":" << port
                  << " for " << CHANNELS[i].name
                  << " (UHD port " << CHANNELS[i].uhd_port << ")\n";
    }

    std::cout << "\n[server] Waiting for tunnel client to connect...\n";

    // Accept one client per channel (blocking)
    for (size_t i = 0; i < NUM_CHANNELS && !stop_signal.load(); i++) {
        struct sockaddr_in client_addr;
        socklen_t_ addr_len = sizeof(client_addr);

        // Use a timeout on accept so we can check stop_signal
        set_recv_timeout(channels[i].tcp_listen_sock, 1000);

        while (!stop_signal.load()) {
            socket_t client = ::accept(channels[i].tcp_listen_sock,
                reinterpret_cast<struct sockaddr*>(&client_addr), &addr_len);
            if (client == INVALID_SOCK) {
                continue; // timeout, retry
            }

            channels[i].tcp_client_sock = client;
            set_tcp_nodelay(client);
            set_socket_buffers(client, buf_size, buf_size);

            std::cout << "[server] " << CHANNELS[i].name
                      << " client connected from "
                      << inet_ntoa(client_addr.sin_addr)
                      << ":" << ntohs(client_addr.sin_port) << "\n";
            break;
        }
    }

    if (stop_signal.load()) {
        std::cout << "[server] Interrupted during accept\n";
        for (auto& ch : channels) {
            if (ch.tcp_listen_sock != INVALID_SOCK) close_socket(ch.tcp_listen_sock);
            if (ch.tcp_client_sock != INVALID_SOCK) close_socket(ch.tcp_client_sock);
        }
        cleanup_sockets();
        return 0;
    }

    std::cout << "\n[server] All channels connected. Starting relay threads...\n\n";

    // Start relay threads per channel
    for (size_t i = 0; i < NUM_CHANNELS; i++) {
        if (CHANNELS[i].is_tcp_relay) {
            // TCP-to-TCP relay for MPM RPC
            // Connect to USRP's RPC port
            socket_t rpc_sock = tcp_connect(device_addr, CHANNELS[i].uhd_port);
            if (rpc_sock == INVALID_SOCK) {
                std::cerr << "[server] WARNING: Cannot connect to " << device_addr
                          << ":" << CHANNELS[i].uhd_port
                          << " — " << CHANNELS[i].name << " relay disabled\n";
                continue;
            }
            channels[i].udp_sock = rpc_sock; // reuse field for the device-side TCP socket

            channels[i].tcp_to_udp_thread = std::thread(
                thread_tcp_relay_half,
                channels[i].tcp_client_sock, rpc_sock,
                std::ref(channels[i].stats), true, CHANNELS[i].name);

            channels[i].udp_to_tcp_thread = std::thread(
                thread_tcp_relay_half,
                rpc_sock, channels[i].tcp_client_sock,
                std::ref(channels[i].stats), false, CHANNELS[i].name);
        } else {
            // UDP-over-TCP relay
            channels[i].tcp_to_udp_thread = std::thread(
                thread_tcp_to_udp, std::ref(channels[i]), i, device_addr);

            channels[i].udp_to_tcp_thread = std::thread(
                thread_udp_to_tcp, std::ref(channels[i]), i);
        }
    }

    // Stats loop
    std::cout << "[server] Tunnel active. Press Ctrl+C to stop.\n";
    while (!stop_signal.load()) {
        if (stats_interval > 0) {
            for (int s = 0; s < stats_interval && !stop_signal.load(); s++) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
            if (!stop_signal.load()) {
                ChannelStats flat[NUM_CHANNELS];
                for (size_t i = 0; i < NUM_CHANNELS; i++) {
                    flat[i].packets_to_device.store(
                        channels[i].stats.packets_to_device.load());
                    flat[i].packets_from_device.store(
                        channels[i].stats.packets_from_device.load());
                    flat[i].bytes_to_device.store(
                        channels[i].stats.bytes_to_device.load());
                    flat[i].bytes_from_device.store(
                        channels[i].stats.bytes_from_device.load());
                    flat[i].errors.store(channels[i].stats.errors.load());
                }
                print_stats(flat, NUM_CHANNELS);
            }
        } else {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    // Shutdown
    std::cout << "\n[server] Shutting down...\n";

    // Close all sockets to unblock threads
    for (auto& ch : channels) {
        if (ch.tcp_client_sock != INVALID_SOCK) close_socket(ch.tcp_client_sock);
        if (ch.udp_sock != INVALID_SOCK) close_socket(ch.udp_sock);
        if (ch.tcp_listen_sock != INVALID_SOCK) close_socket(ch.tcp_listen_sock);
    }

    // Join threads
    for (auto& ch : channels) {
        if (ch.tcp_to_udp_thread.joinable()) ch.tcp_to_udp_thread.join();
        if (ch.udp_to_tcp_thread.joinable()) ch.udp_to_tcp_thread.join();
    }

    // Final stats
    {
        ChannelStats flat[NUM_CHANNELS];
        for (size_t i = 0; i < NUM_CHANNELS; i++) {
            flat[i].packets_to_device.store(
                channels[i].stats.packets_to_device.load());
            flat[i].packets_from_device.store(
                channels[i].stats.packets_from_device.load());
            flat[i].bytes_to_device.store(
                channels[i].stats.bytes_to_device.load());
            flat[i].bytes_from_device.store(
                channels[i].stats.bytes_from_device.load());
            flat[i].errors.store(channels[i].stats.errors.load());
        }
        print_stats(flat, NUM_CHANNELS);
    }

    cleanup_sockets();
    std::cout << "[server] Done.\n";
    return 0;
        }();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
