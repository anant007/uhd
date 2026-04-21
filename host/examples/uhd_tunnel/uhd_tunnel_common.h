//
// Copyright 2026 Ettus Research, a National Instruments Brand
//
// SPDX-License-Identifier: GPL-3.0-or-later
//

#pragma once

// UHD Tunnel Common — Shared constants, framing, and utilities for
// uhd_tunnel_server and uhd_tunnel_client.

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#ifdef _WIN32
#    ifndef _WIN32_WINNT
#        define _WIN32_WINNT 0x0600
#    endif
#    include <winsock2.h>
#    include <ws2tcpip.h>
#    include <mstcpip.h>
#    pragma comment(lib, "ws2_32.lib")
using socket_t    = SOCKET;
using socklen_t_  = int;
constexpr socket_t INVALID_SOCK = INVALID_SOCKET;
inline int close_socket(socket_t s) { return closesocket(s); }
inline int get_last_error() { return WSAGetLastError(); }
inline void init_sockets()
{
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
}
inline void cleanup_sockets() { WSACleanup(); }
#else
#    include <arpa/inet.h>
#    include <errno.h>
#    include <fcntl.h>
#    include <netdb.h>
#    include <netinet/in.h>
#    include <netinet/tcp.h>
#    include <sys/socket.h>
#    include <unistd.h>
using socket_t    = int;
using socklen_t_  = socklen_t;
constexpr socket_t INVALID_SOCK = -1;
inline int close_socket(socket_t s) { return close(s); }
inline int get_last_error() { return errno; }
inline void init_sockets() {}
inline void cleanup_sockets() {}
#endif

namespace uhd_tunnel {

// ============================================================================
// UHD Port Constants
// ============================================================================
constexpr uint16_t X300_FW_CTRL_PORT    = 49152;
constexpr uint16_t CHDR_DATA_PORT       = 49153;
constexpr uint16_t X300_GPSDO_PORT      = 49156;
constexpr uint16_t X300_FPGA_PROG_PORT  = 49157;
constexpr uint16_t X300_MTU_DETECT_PORT = 49158;
constexpr uint16_t X300_FPGA_READ_PORT  = 49159;
constexpr uint16_t MPM_DISCOVERY_PORT   = 49600;
constexpr uint16_t MPM_RPC_PORT         = 49601;

// ============================================================================
// Tunnel Constants
// ============================================================================
constexpr uint16_t TUNNEL_BASE_PORT  = 5800;
constexpr size_t MAX_UDP_DGRAM       = 9600;  // MAX_ETHERNET_MTU
constexpr size_t FRAME_HDR_SIZE      = 4;     // uint32_t length prefix
constexpr size_t MAX_FRAME_SIZE      = FRAME_HDR_SIZE + MAX_UDP_DGRAM;
constexpr size_t TCP_BUF_SIZE        = 10 * 1024 * 1024; // 10 MB
constexpr size_t UDP_BUF_SIZE        = 2500000;           // Match UHD default

// ============================================================================
// Channel Definitions
// ============================================================================
// Each "channel" is a separate TCP connection forwarding one UHD UDP port.
struct ChannelDef {
    const char* name;
    uint16_t uhd_port;         // The real UDP port on the USRP side
    uint16_t tunnel_offset;    // Offset from tunnel_base_port
    bool is_tcp_relay;         // true = raw TCP relay, false = UDP-over-TCP
};

// Default channel table. Indices are used throughout.
// X300 uses 49152-49159 for various services. MPM uses 49600 (UDP) + 49601 (TCP).
// All ports must be tunneled to prevent Windows ICMP-port-unreachable triggering
// WSAECONNRESET on the UHD-side discovery sockets.
constexpr size_t NUM_CHANNELS = 8;
constexpr ChannelDef CHANNELS[NUM_CHANNELS] = {
    {"x300_ctrl",    X300_FW_CTRL_PORT,    0, false},  // 49152: discovery + control
    {"chdr_data",    CHDR_DATA_PORT,       1, false},  // 49153: CHDR/VITA data
    {"x300_gpsdo",   X300_GPSDO_PORT,      2, false},  // 49156: GPSDO
    {"x300_fpga",    X300_FPGA_PROG_PORT,  3, false},  // 49157: FPGA programming
    {"x300_mtu",     X300_MTU_DETECT_PORT, 4, false},  // 49158: MTU detection
    {"x300_fpga_rd", X300_FPGA_READ_PORT,  5, false},  // 49159: FPGA read
    {"mpm_disc",     MPM_DISCOVERY_PORT,   6, false},  // 49600: MPM discovery
    {"mpm_rpc",      MPM_RPC_PORT,         7, true},   // 49601: MPM RPC (TCP)
};

// ============================================================================
// Statistics Counters (per-channel)
// ============================================================================
struct ChannelStats {
    std::atomic<uint64_t> packets_to_device{0};
    std::atomic<uint64_t> packets_from_device{0};
    std::atomic<uint64_t> bytes_to_device{0};
    std::atomic<uint64_t> bytes_from_device{0};
    std::atomic<uint64_t> errors{0};
};

// ============================================================================
// TCP Framed I/O — Length-prefixed datagrams over TCP
// ============================================================================

// Send a single framed UDP datagram over a TCP socket.
// Frame format: [4-byte LE length][payload]
// Returns true on success, false on connection error.
inline bool tcp_send_frame(socket_t sock, const uint8_t* data, uint32_t len)
{
    // Prepare header + data in a single buffer to reduce syscalls
    uint8_t hdr[FRAME_HDR_SIZE];
    std::memcpy(hdr, &len, sizeof(uint32_t)); // LE on x86

    // Send header
    size_t sent = 0;
    while (sent < FRAME_HDR_SIZE) {
        int n = ::send(sock, reinterpret_cast<const char*>(hdr + sent),
            static_cast<int>(FRAME_HDR_SIZE - sent), 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }

    // Send payload
    sent = 0;
    while (sent < len) {
        int n = ::send(sock, reinterpret_cast<const char*>(data + sent),
            static_cast<int>(len - sent), 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// Receive exactly `count` bytes from a TCP socket.
// Returns true if all bytes received, false on disconnect/error.
inline bool tcp_recv_exact(socket_t sock, uint8_t* buf, size_t count)
{
    size_t received = 0;
    while (received < count) {
        int n = ::recv(sock, reinterpret_cast<char*>(buf + received),
            static_cast<int>(count - received), 0);
        if (n <= 0) return false;
        received += static_cast<size_t>(n);
    }
    return true;
}

// Receive a single framed UDP datagram from a TCP socket.
// Writes payload into `buf` and sets `len` to the payload length.
// `buf` must be at least MAX_UDP_DGRAM bytes.
// Returns true on success, false on disconnect/error.
inline bool tcp_recv_frame(socket_t sock, uint8_t* buf, uint32_t& len)
{
    uint8_t hdr[FRAME_HDR_SIZE];
    if (!tcp_recv_exact(sock, hdr, FRAME_HDR_SIZE))
        return false;

    std::memcpy(&len, hdr, sizeof(uint32_t)); // LE on x86

    if (len > MAX_UDP_DGRAM) {
        std::cerr << "[tunnel] ERROR: frame too large: " << len << " bytes\n";
        return false;
    }

    if (len == 0) return true; // keepalive

    return tcp_recv_exact(sock, buf, len);
}

// ============================================================================
// Socket Helpers
// ============================================================================

// Set TCP_NODELAY on a TCP socket
inline bool set_tcp_nodelay(socket_t sock)
{
    int flag = 1;
    return setsockopt(sock, IPPROTO_TCP, TCP_NODELAY,
               reinterpret_cast<const char*>(&flag), sizeof(flag))
        == 0;
}

// Set socket send/recv buffer sizes
inline void set_socket_buffers(socket_t sock, size_t recv_size, size_t send_size)
{
    int rbuf = static_cast<int>(recv_size);
    int sbuf = static_cast<int>(send_size);
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF,
        reinterpret_cast<const char*>(&rbuf), sizeof(rbuf));
    setsockopt(sock, SOL_SOCKET, SO_SNDBUF,
        reinterpret_cast<const char*>(&sbuf), sizeof(sbuf));
}

// Set SO_REUSEADDR on a socket
inline void set_reuse_addr(socket_t sock)
{
    int flag = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR,
        reinterpret_cast<const char*>(&flag), sizeof(flag));
}

// Disable Windows ICMP-port-unreachable propagation on UDP sockets.
// By default on Windows, when a UDP socket sends to a port and gets back an
// ICMP "port unreachable", the next recvfrom() returns WSAECONNRESET (10054).
// This breaks UDP servers that just want to keep listening. SIO_UDP_CONNRESET
// disables this behavior. No-op on non-Windows platforms.
inline void disable_udp_conn_reset(socket_t sock)
{
#ifdef _WIN32
    // SIO_UDP_CONNRESET = _WSAIOW(IOC_VENDOR, 12) — define it inline if the
    // SDK header didn't export it (varies between Windows SDK versions).
#    ifndef SIO_UDP_CONNRESET
#        define SIO_UDP_CONNRESET _WSAIOW(IOC_VENDOR, 12)
#    endif
    BOOL behavior = FALSE;
    DWORD bytes_returned = 0;
    WSAIoctl(sock, SIO_UDP_CONNRESET, &behavior, sizeof(behavior),
        nullptr, 0, &bytes_returned, nullptr, nullptr);
#else
    (void)sock;
#endif
}

// Set socket to non-blocking mode
inline bool set_nonblocking(socket_t sock)
{
#ifdef _WIN32
    u_long mode = 1;
    return ioctlsocket(sock, FIONBIO, &mode) == 0;
#else
    int flags = fcntl(sock, F_GETFL, 0);
    return fcntl(sock, F_SETFL, flags | O_NONBLOCK) == 0;
#endif
}

// Set socket to blocking mode
inline bool set_blocking(socket_t sock)
{
#ifdef _WIN32
    u_long mode = 0;
    return ioctlsocket(sock, FIONBIO, &mode) == 0;
#else
    int flags = fcntl(sock, F_GETFL, 0);
    return fcntl(sock, F_SETFL, flags & ~O_NONBLOCK) == 0;
#endif
}

// Set a recv timeout on a socket (milliseconds)
inline void set_recv_timeout(socket_t sock, int timeout_ms)
{
#ifdef _WIN32
    DWORD tv = static_cast<DWORD>(timeout_ms);
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,
        reinterpret_cast<const char*>(&tv), sizeof(tv));
#else
    struct timeval tv;
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO,
        reinterpret_cast<const char*>(&tv), sizeof(tv));
#endif
}

// Create a UDP socket bound to a specific address and port
inline socket_t create_udp_socket(const std::string& bind_addr, uint16_t port,
    bool broadcast = false)
{
    socket_t sock = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock == INVALID_SOCK) return INVALID_SOCK;

    set_reuse_addr(sock);

    if (broadcast) {
        int flag = 1;
        setsockopt(sock, SOL_SOCKET, SO_BROADCAST,
            reinterpret_cast<const char*>(&flag), sizeof(flag));
    }

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = (bind_addr == "0.0.0.0")
                               ? INADDR_ANY
                               : inet_addr(bind_addr.c_str());

    if (::bind(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "[tunnel] Failed to bind UDP " << bind_addr << ":" << port
                  << " (error " << get_last_error() << ")\n";
        close_socket(sock);
        return INVALID_SOCK;
    }

    set_socket_buffers(sock, UDP_BUF_SIZE, UDP_BUF_SIZE);
    disable_udp_conn_reset(sock);
    return sock;
}

// Create a connected UDP socket to a remote address and port (for sending)
inline socket_t create_udp_sender(const std::string& remote_addr, uint16_t port)
{
    socket_t sock = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock == INVALID_SOCK) return INVALID_SOCK;

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = inet_addr(remote_addr.c_str());

    if (::connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "[tunnel] Failed to connect UDP to " << remote_addr << ":" << port
                  << " (error " << get_last_error() << ")\n";
        close_socket(sock);
        return INVALID_SOCK;
    }

    set_socket_buffers(sock, UDP_BUF_SIZE, UDP_BUF_SIZE);
    disable_udp_conn_reset(sock);
    return sock;
}

// Create a TCP server socket (listen)
inline socket_t create_tcp_listener(const std::string& bind_addr, uint16_t port)
{
    socket_t sock = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCK) return INVALID_SOCK;

    set_reuse_addr(sock);

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = (bind_addr == "0.0.0.0")
                               ? INADDR_ANY
                               : inet_addr(bind_addr.c_str());

    if (::bind(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "[tunnel] Failed to bind TCP " << bind_addr << ":" << port
                  << " (error " << get_last_error() << ")\n";
        close_socket(sock);
        return INVALID_SOCK;
    }

    if (::listen(sock, 4) != 0) {
        close_socket(sock);
        return INVALID_SOCK;
    }

    return sock;
}

// Connect to a TCP server
inline socket_t tcp_connect(const std::string& remote_addr, uint16_t port)
{
    socket_t sock = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCK) return INVALID_SOCK;

    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = inet_addr(remote_addr.c_str());

    if (::connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "[tunnel] Failed to connect TCP to " << remote_addr << ":" << port
                  << " (error " << get_last_error() << ")\n";
        close_socket(sock);
        return INVALID_SOCK;
    }

    set_tcp_nodelay(sock);
    set_socket_buffers(sock, TCP_BUF_SIZE, TCP_BUF_SIZE);
    return sock;
}

// ============================================================================
// Timestamp Utility
// ============================================================================
inline std::string timestamp_str()
{
    auto now  = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    char buf[32];
    struct tm tm_buf;
#ifdef _WIN32
    localtime_s(&tm_buf, &time);
#else
    localtime_r(&time, &tm_buf);
#endif
    std::strftime(buf, sizeof(buf), "%H:%M:%S", &tm_buf);
    return std::string(buf);
}

// ============================================================================
// Print Helpers
// ============================================================================
inline void print_stats(const ChannelStats stats[], size_t n)
{
    std::cout << "\n[" << timestamp_str() << "] === Tunnel Statistics ===\n";
    for (size_t i = 0; i < n; i++) {
        if (i < NUM_CHANNELS) {
            std::cout << "  " << CHANNELS[i].name << " (:" << CHANNELS[i].uhd_port << ")"
                      << "  to_dev=" << stats[i].packets_to_device.load()
                      << " pkts (" << stats[i].bytes_to_device.load() << " B)"
                      << "  from_dev=" << stats[i].packets_from_device.load()
                      << " pkts (" << stats[i].bytes_from_device.load() << " B)"
                      << "  errors=" << stats[i].errors.load() << "\n";
        }
    }
    std::cout << std::flush;
}

} // namespace uhd_tunnel
