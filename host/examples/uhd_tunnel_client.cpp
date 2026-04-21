//
// Copyright 2026 Ettus Research, a National Instruments Brand
//
// SPDX-License-Identifier: GPL-3.0-or-later
//

// uhd_tunnel_client — Runs on the remote machine where UHD software operates.
// Presents virtual UDP endpoints on 127.0.0.1 that mirror the real USRP ports,
// relaying all traffic to/from uhd_tunnel_server over reliable TCP.
//
// For each UDP channel:
//   - Binds a local UDP socket on 127.0.0.1:<uhd_port>
//   - Connects a TCP socket to the tunnel server
//   - Relays bidirectionally: local UHD app ↔ TCP tunnel ↔ remote USRP
//
// For MPM RPC (TCP 49601):
//   - Listens on 127.0.0.1:49601 for the UHD app to connect
//   - Relays to the tunnel server, which connects to the real USRP RPC
//
// Discovery interception:
//   - Listens for UHD broadcast discovery on the appropriate ports
//   - Forwards discovery requests through the tunnel
//   - Returns responses to the UHD app as if the device were local
//
// Usage:
//   uhd_tunnel_client --server-addr 1.2.3.4 [--tunnel-port 5800]
//   Then: uhd_find_devices --args="addr=127.0.0.1"

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
    socket_t tcp_sock = INVALID_SOCK;    // TCP connection to tunnel server
    socket_t udp_sock = INVALID_SOCK;    // Local UDP socket (bound to 127.0.0.1:port)
    ChannelStats stats;
    std::thread local_to_tcp_thread;
    std::thread tcp_to_local_thread;

    // For UDP channels: track the last source address so we can send responses
    // back to the correct UHD application socket.
    struct sockaddr_in last_client_addr;
    socklen_t_ last_client_addr_len = sizeof(struct sockaddr_in);
    std::atomic<bool> has_client{false};
    std::mutex client_addr_mutex;
};

// ============================================================================
// UDP-over-TCP Channel: Local UDP → TCP direction
// Receives UDP datagrams from the local UHD app, frames them, sends to server.
// ============================================================================
static void thread_local_udp_to_tcp(ChannelState& ch, size_t ch_idx)
{
    const auto& def = CHANNELS[ch_idx];
    std::vector<uint8_t> buf(MAX_UDP_DGRAM);

    std::cout << "[client] " << def.name << " LocalUDP→TCP thread started\n";

    while (!stop_signal.load()) {
        struct sockaddr_in from_addr;
        socklen_t_ from_len = sizeof(from_addr);

        int n = ::recvfrom(ch.udp_sock, reinterpret_cast<char*>(buf.data()),
            static_cast<int>(MAX_UDP_DGRAM), 0,
            reinterpret_cast<struct sockaddr*>(&from_addr), &from_len);

        if (n <= 0) {
            // timeout or error
            continue;
        }

        // Remember the client address so we can send responses back
        {
            std::lock_guard<std::mutex> lock(ch.client_addr_mutex);
            ch.last_client_addr     = from_addr;
            ch.last_client_addr_len = from_len;
            ch.has_client.store(true);
        }

        // Forward to tunnel server via TCP
        if (!tcp_send_frame(ch.tcp_sock, buf.data(), static_cast<uint32_t>(n))) {
            if (!stop_signal.load())
                std::cerr << "[client] " << def.name << " TCP send failed\n";
            break;
        }

        ch.stats.packets_to_device.fetch_add(1);
        ch.stats.bytes_to_device.fetch_add(static_cast<uint64_t>(n));
    }

    std::cout << "[client] " << def.name << " LocalUDP→TCP thread exiting\n";
}

// ============================================================================
// UDP-over-TCP Channel: TCP → Local UDP direction
// Receives framed datagrams from server, sends them to the local UHD app.
// ============================================================================
static void thread_tcp_to_local_udp(ChannelState& ch, size_t ch_idx)
{
    const auto& def = CHANNELS[ch_idx];
    std::vector<uint8_t> buf(MAX_UDP_DGRAM);

    std::cout << "[client] " << def.name << " TCP→LocalUDP thread started\n";

    while (!stop_signal.load()) {
        uint32_t len = 0;
        if (!tcp_recv_frame(ch.tcp_sock, buf.data(), len)) {
            if (!stop_signal.load())
                std::cerr << "[client] " << def.name << " TCP recv failed\n";
            break;
        }

        if (len == 0) continue; // keepalive

        // Send to the last known UHD client address
        if (!ch.has_client.load()) {
            // No client has sent us anything yet — buffer the response
            // For discovery, the client sends first, so this shouldn't happen often.
            // We'll queue it for a short time.
            for (int wait = 0; wait < 20 && !ch.has_client.load() && !stop_signal.load();
                 wait++) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            if (!ch.has_client.load()) {
                ch.stats.errors.fetch_add(1);
                continue;
            }
        }

        struct sockaddr_in dest_addr;
        socklen_t_ dest_len;
        {
            std::lock_guard<std::mutex> lock(ch.client_addr_mutex);
            dest_addr = ch.last_client_addr;
            dest_len  = ch.last_client_addr_len;
        }

        int sent = ::sendto(ch.udp_sock, reinterpret_cast<const char*>(buf.data()),
            static_cast<int>(len), 0,
            reinterpret_cast<struct sockaddr*>(&dest_addr), dest_len);

        if (sent <= 0) {
            ch.stats.errors.fetch_add(1);
        } else {
            ch.stats.packets_from_device.fetch_add(1);
            ch.stats.bytes_from_device.fetch_add(static_cast<uint64_t>(sent));
        }
    }

    std::cout << "[client] " << def.name << " TCP→LocalUDP thread exiting\n";
}

// ============================================================================
// TCP Relay Channel (for MPM RPC port 49601)
// Listens locally on 127.0.0.1:49601. When UHD connects, bridges to server.
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
    std::cout << "[client] TCP relay " << label << " exiting\n";
}

static void thread_rpc_listener(ChannelState& ch, size_t ch_idx,
    const std::string& local_addr)
{
    const auto& def = CHANNELS[ch_idx];

    // Create a local TCP listener on 127.0.0.1:<MPM_RPC_PORT>
    socket_t listen_sock = create_tcp_listener(local_addr, def.uhd_port);
    if (listen_sock == INVALID_SOCK) {
        std::cerr << "[client] WARNING: Cannot listen on " << local_addr
                  << ":" << def.uhd_port << " for " << def.name << "\n";
        return;
    }
    set_recv_timeout(listen_sock, 1000);

    std::cout << "[client] " << def.name << " listening on "
              << local_addr << ":" << def.uhd_port << "\n";

    while (!stop_signal.load()) {
        struct sockaddr_in client_addr;
        socklen_t_ addr_len = sizeof(client_addr);
        socket_t local_client = ::accept(listen_sock,
            reinterpret_cast<struct sockaddr*>(&client_addr), &addr_len);
        if (local_client == INVALID_SOCK) continue;

        std::cout << "[client] " << def.name << " local UHD app connected\n";
        // Clear inherited SO_RCVTIMEO from listen socket (Windows behavior)
        set_recv_timeout(local_client, 0);
        set_tcp_nodelay(local_client);

        // Relay in both directions
        std::thread t1(thread_tcp_relay_half,
            local_client, ch.tcp_sock,
            std::ref(ch.stats), true, def.name);
        std::thread t2(thread_tcp_relay_half,
            ch.tcp_sock, local_client,
            std::ref(ch.stats), false, def.name);

        t1.join();
        t2.join();

        close_socket(local_client);
        std::cout << "[client] " << def.name << " local UHD app disconnected\n";
    }

    close_socket(listen_sock);
}

// ============================================================================
// Main
// ============================================================================
int main(int argc, char* argv[])
{
    try {
        return [&]() -> int {
    std::string server_addr, local_addr;
    uint16_t tunnel_port;
    size_t buf_size;
    int stats_interval;

    // clang-format off
    po::options_description desc("UHD Tunnel Client — Options");
    desc.add_options()
        ("help,h",          "Show help message")
        ("server-addr",     po::value<std::string>(&server_addr)->required(),
                            "Tunnel server address (IP of machine with USRP)")
        ("tunnel-port",     po::value<uint16_t>(&tunnel_port)->default_value(TUNNEL_BASE_PORT),
                            "Base TCP port of tunnel server")
        ("local-addr",      po::value<std::string>(&local_addr)->default_value("127.0.0.1"),
                            "Local address to present UHD ports on")
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
              << "  UHD Tunnel Client\n"
              << "  Server:     " << server_addr << "\n"
              << "  Local addr: " << local_addr << "\n"
              << "  Base port:  " << tunnel_port << "\n"
              << "============================================\n\n";

    // Create per-channel state
    std::array<ChannelState, NUM_CHANNELS> channels;

    // Connect TCP to server for all channels
    std::cout << "[client] Connecting to tunnel server...\n";
    for (size_t i = 0; i < NUM_CHANNELS; i++) {
        uint16_t port   = tunnel_port + CHANNELS[i].tunnel_offset;
        channels[i].tcp_sock = tcp_connect(server_addr, port);
        if (channels[i].tcp_sock == INVALID_SOCK) {
            std::cerr << "[client] FATAL: cannot connect to " << server_addr
                      << ":" << port << " for " << CHANNELS[i].name << "\n";
            // Close already-connected channels
            for (size_t j = 0; j < i; j++) {
                if (channels[j].tcp_sock != INVALID_SOCK)
                    close_socket(channels[j].tcp_sock);
            }
            cleanup_sockets();
            return 1;
        }
        std::cout << "[client] Connected: " << CHANNELS[i].name
                  << " → " << server_addr << ":" << port << "\n";
    }

    std::cout << "\n[client] All tunnel channels connected. Setting up local ports...\n";

    // Bind local UDP sockets for non-TCP-relay channels
    for (size_t i = 0; i < NUM_CHANNELS; i++) {
        if (CHANNELS[i].is_tcp_relay) continue;

        channels[i].udp_sock = create_udp_socket(local_addr, CHANNELS[i].uhd_port);
        if (channels[i].udp_sock == INVALID_SOCK) {
            std::cerr << "[client] FATAL: cannot bind UDP " << local_addr
                      << ":" << CHANNELS[i].uhd_port << " for " << CHANNELS[i].name << "\n";
            for (auto& ch : channels) {
                if (ch.tcp_sock != INVALID_SOCK) close_socket(ch.tcp_sock);
                if (ch.udp_sock != INVALID_SOCK) close_socket(ch.udp_sock);
            }
            cleanup_sockets();
            return 1;
        }
        set_recv_timeout(channels[i].udp_sock, 200);
        std::cout << "[client] " << CHANNELS[i].name << " bound to "
                  << local_addr << ":" << CHANNELS[i].uhd_port << "\n";
    }

    // Start relay threads per channel
    for (size_t i = 0; i < NUM_CHANNELS; i++) {
        if (CHANNELS[i].is_tcp_relay) {
            // MPM RPC: start a local TCP listener thread
            channels[i].local_to_tcp_thread = std::thread(
                thread_rpc_listener, std::ref(channels[i]), i, local_addr);
        } else {
            // UDP channels: bidirectional relay
            channels[i].local_to_tcp_thread = std::thread(
                thread_local_udp_to_tcp, std::ref(channels[i]), i);

            channels[i].tcp_to_local_thread = std::thread(
                thread_tcp_to_local_udp, std::ref(channels[i]), i);
        }
    }

    std::cout << "\n[client] Tunnel active. UHD applications can now use:\n"
              << "  uhd_find_devices --args=\"addr=" << local_addr << "\"\n"
              << "  or specify addr=" << local_addr << " in your UHD application.\n"
              << "  Press Ctrl+C to stop.\n\n";

    // Stats loop
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
    std::cout << "\n[client] Shutting down...\n";

    for (auto& ch : channels) {
        if (ch.tcp_sock != INVALID_SOCK) close_socket(ch.tcp_sock);
        if (ch.udp_sock != INVALID_SOCK) close_socket(ch.udp_sock);
    }

    for (auto& ch : channels) {
        if (ch.local_to_tcp_thread.joinable()) ch.local_to_tcp_thread.join();
        if (ch.tcp_to_local_thread.joinable()) ch.tcp_to_local_thread.join();
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
    std::cout << "[client] Done.\n";
    return 0;
        }();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
