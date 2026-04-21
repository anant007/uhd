# UHD Transport Tunnel — Architecture & Dev Log

**Project**: UDP-over-TCP bidirectional tunnel for remote UHD/USRP access  
**Started**: 2026-04-20  
**Status**: Phase 1 — Lightweight MVP  

---

## 1. Problem Statement

A USRP SDR device is physically connected to **Host A** via Ethernet. A UHD application
on **Host B** (remote, across the internet or LAN) needs to interact with the device
as if it were locally attached. Standard UHD device discovery and CHDR streaming both
use UDP, which cannot traverse NAT/firewalls reliably and does not guarantee delivery.

**Constraints**:
- Packet loss is **not tolerated** (TCP transport required)
- Packet delay is **tolerated** (WAN-scale latency OK)
- No modifications to UHD library code (`lib/`, `include/uhd/`)
- Must support both MPM devices (N3xx, X4xx, E3xx) and X300-class devices

---

## 2. Architecture Overview

```
  HOST B (Remote)                              HOST A (USRP connected)
 ┌──────────────────────┐                     ┌──────────────────────────┐
 │ UHD Application       │                     │  USRP Device             │
 │  addr=127.0.0.1       │                     │  192.168.10.2            │
 │         │              │                     │         │                │
 │  ┌──────▼──────┐       │                     │  ┌──────▼──────┐        │
 │  │ uhd_tunnel   │       │   TCP Tunnel(s)    │  │ uhd_tunnel   │        │
 │  │ _client      │◄─────┼───────────────────►│  │ _server      │        │
 │  └──────┬──────┘       │  (one per channel) │  └──────┬──────┘        │
 │         │              │                     │         │                │
 │  Virtual UDP sockets   │                     │  Real UDP sockets to    │
 │  on 127.0.0.1:         │                     │  USRP at device_addr:   │
 │    49152 (X300 ctrl)   │                     │    49152                 │
 │    49153 (CHDR data)   │                     │    49153                 │
 │    49600 (MPM disc)    │                     │    49600                 │
 │  TCP relay:            │                     │  TCP relay:              │
 │    49601 (MPM RPC)     │                     │    49601                 │
 └──────────────────────┘                     └──────────────────────────┘
```

### Component Roles

| Component | Role |
|-----------|------|
| **uhd_tunnel_server** | Runs on Host A (with USRP). Binds TCP listen ports. For each UHD UDP port, maintains a real UDP socket to the USRP and relays traffic bidirectionally through TCP. Also proxies MPM RPC (TCP 49601). |
| **uhd_tunnel_client** | Runs on Host B (remote). Connects to server's TCP ports. Presents virtual UDP sockets on `127.0.0.1` that UHD applications connect to. Intercepts discovery broadcasts and responds with the tunneled device info. |

---

## 3. Protocol Design

### 3.1 TCP Framing for UDP Datagrams

Since TCP is a stream protocol and UDP has message boundaries, each UDP datagram
is sent over TCP with a **length-prefix frame**:

```
┌──────────────┬──────────────────────────┐
│ Length (4B)   │ UDP Datagram Payload      │
│ uint32_t LE  │ (up to 9600 bytes)        │
└──────────────┴──────────────────────────┘
```

- `Length`: Little-endian uint32_t, size of the following payload in bytes
- Max single datagram: 9600 bytes (`MAX_ETHERNET_MTU`)
- Zero-length frame = keepalive/heartbeat

### 3.2 Tunnel Channels

Each UHD port is tunneled over a separate TCP connection ("channel"):

| Channel ID | Purpose | USRP UDP Port | TCP Tunnel Port (configurable) |
|-----------|---------|---------------|-------------------------------|
| 0 | X300 FW Control / Discovery | 49152 | 5800 |
| 1 | CHDR Data (primary) | 49153 | 5801 |
| 2 | MPM Discovery + MTU Echo | 49600 | 5802 |
| 3 | MPM RPC (TCP-to-TCP relay) | 49601 | 5803 |

For MVP, channels 0-2 use UDP-over-TCP framing. Channel 3 is raw TCP relay.

### 3.3 Discovery Interception

**MPM Discovery** (port 49600):
1. Client listens for UDP broadcasts on `0.0.0.0:49600`
2. On receiving `"MPM-DISC"`, forwards to server via TCP channel 2
3. Server sends `"MPM-DISC"` as real UDP broadcast to USRP on port 49600
4. Server receives response, forwards back via TCP channel 2
5. Client rewrites the source address to `127.0.0.1` and sends response back to UHD

**X300 Discovery** (port 49152):
1. Same pattern via TCP channel 0
2. 16-byte `x300_fw_comms_t` struct forwarded verbatim
3. When UHD is pointed at `127.0.0.1`, X300 Ethernet init now logs a remote-mode warning instead of failing the EEPROM IP match check.

### 3.4 MTU Handling

MTU discovery (`MPM-ECHO` on 49600, or port 49158 for X300) will report
the **tunnel client's local loopback MTU** (65535) rather than the real link MTU.
The server should cap reported MTU to the real link's MTU. For MVP, we can
hardcode the MTU response or let it pass through (loopback MTU is always larger).

---

## 4. Implementation Plan

### Phase 1: Lightweight MVP ✅ (Current)

**Goal**: End-to-end tunnel that allows `uhd_find_devices` and basic streaming
from a remote machine. Hardcoded/CLI-configured ports.

- [x] Architecture document (this file)
- [ ] `uhd_tunnel/uhd_tunnel_common.h` — Shared structures, framing, constants
- [ ] `uhd_tunnel_server.cpp` — Server-side implementation  
- [ ] `uhd_tunnel_client.cpp` — Client-side implementation
- [ ] CMakeLists.txt updates for both targets
- [ ] Build verification

**MVP Feature Set**:
- Single USRP device tunnel (one device address)
- UDP-over-TCP for ports 49152, 49153, 49600
- TCP relay for port 49601
- Length-prefixed framing
- CLI args: `--device-addr`, `--tunnel-port`, `--remote-addr`
- `TCP_NODELAY` enabled on all TCP connections
- Configurable socket buffer sizes
- Ctrl-C graceful shutdown
- Basic stats printing (bytes forwarded, packets relayed)

### Phase 2: Robustness (Future)

- [ ] Reconnection logic (auto-reconnect on TCP drop)
- [ ] Heartbeat/keepalive detection
- [ ] Multiple device support (tunnel N devices)
- [ ] MTU rewriting (cap to real link MTU)
- [ ] Bandwidth throttling / QoS
- [ ] Compression option (LZ4 for IQ data)
- [ ] Encryption (TLS wrapping)
- [ ] Logging to file

### Phase 3: Deep UHD Integration (Future)

- [ ] CHDR packet inspection (rewrite EPIDs if needed)
- [ ] Flow control adaptation (adjust STRS/STRC for WAN latency)
- [ ] Dynamic port discovery (sniff RPC to learn CHDR ports)
- [ ] Custom UHD transport plugin (`tunnel_link_if`)
- [ ] Device filter/firewall (restrict which blocks are accessible)

---

## 5. Threading Model

### Server (uhd_tunnel_server)

```
Main Thread
 ├─ Accept TCP connections on tunnel ports (5800-5803)
 │
 ├─ Per-channel pair:
 │   ├─ [TCP→UDP Thread] Read TCP frames → send UDP to USRP
 │   └─ [UDP→TCP Thread] Recv UDP from USRP → write TCP frames
 │
 └─ [Stats Thread] Periodic throughput/latency reporting
```

### Client (uhd_tunnel_client)

```
Main Thread
 ├─ Connect TCP to server tunnel ports
 │
 ├─ Per UDP channel:
 │   ├─ [Local UDP listener] on 127.0.0.1:<port>
 │   ├─ [UDP→TCP Thread] Recv local UDP → write TCP frames  
 │   └─ [TCP→UDP Thread] Read TCP frames → send local UDP response
 │
 ├─ [Discovery Listener Thread] Intercept broadcasts, relay via tunnel
 │
 └─ [TCP Relay] For MPM RPC (49601): bidirectional TCP splice
```

---

## 6. Key Constants

```cpp
// UHD Ports
constexpr uint16_t X300_FW_CTRL_PORT       = 49152;
constexpr uint16_t CHDR_DATA_PORT          = 49153;
constexpr uint16_t MPM_DISCOVERY_PORT      = 49600;
constexpr uint16_t MPM_RPC_PORT            = 49601;
constexpr uint16_t X300_GPSDO_PORT         = 49156;
constexpr uint16_t X300_MTU_DETECT_PORT    = 49158;

// Tunnel Defaults
constexpr uint16_t TUNNEL_BASE_PORT        = 5800;
constexpr size_t   MAX_UDP_DGRAM           = 9600;   // MAX_ETHERNET_MTU
constexpr size_t   FRAME_HDR_SIZE          = 4;       // uint32_t length prefix
constexpr size_t   TCP_RECV_BUF_SIZE       = 10485760; // 10 MB
constexpr size_t   TCP_SEND_BUF_SIZE       = 10485760;
constexpr size_t   UDP_RECV_BUF_SIZE       = 2500000;  // Match UHD defaults
constexpr size_t   UDP_SEND_BUF_SIZE       = 2500000;

// Discovery
constexpr char     MPM_DISC_CMD[]          = "MPM-DISC";
constexpr char     MPM_ECHO_CMD[]          = "MPM-ECHO";
constexpr char     MPM_DISC_PREAMBLE[]     = "USRP-MPM";
```

---

## 7. Build Integration

Both executables are added to `examples/CMakeLists.txt` alongside existing examples.
They link only to `uhd` and `${Boost_LIBRARIES}` (Boost.Asio, Boost.ProgramOptions).
No extra dependencies required — Boost.Asio provides the TCP/UDP socket layer.

```cmake
# In example_sources list:
uhd_tunnel_server.cpp
uhd_tunnel_client.cpp
```

---

## 8. CLI Interface

### uhd_tunnel_server

```
Usage: uhd_tunnel_server [options]

Options:
  --help                  Show help message
  --device-addr arg       USRP device IP address (required, e.g. 192.168.10.2)
  --tunnel-port arg       Base TCP port for tunnel (default: 5800)
  --bind-addr arg         Address to bind tunnel TCP sockets (default: 0.0.0.0)
  --device-type arg       Device type: "x300" or "mpm" (default: auto-detect)
  --buf-size arg          TCP socket buffer size in bytes (default: 10485760)
  --stats-interval arg    Stats print interval in seconds (default: 5)
```

### uhd_tunnel_client

```
Usage: uhd_tunnel_client [options]

Options:
  --help                  Show help message
  --server-addr arg       Tunnel server address (required, e.g. 1.2.3.4)
  --tunnel-port arg       Base TCP port of tunnel server (default: 5800)
  --local-addr arg        Local address to present (default: 127.0.0.1)
  --buf-size arg          TCP socket buffer size in bytes (default: 10485760)
  --stats-interval arg    Stats print interval in seconds (default: 5)
```

---

## 9. Risk Assessment

| Risk | Mitigation |
|------|------------|
| High latency breaks flow control | Phase 2: Adjust flow control params; Phase 1: use large buffers |
| MTU discovery fails | Transparent on loopback (64KB MTU); cap in Phase 2 |
| TCP head-of-line blocking stalls streams | One TCP connection per channel isolates streams |
| NAT/firewall blocks tunnel ports | User configures port forwarding; future: single-port multiplexed mode |
| MPM RPC timeout during setup | Default timeouts are 30s, sufficient for WAN |
| UDP broadcast interception tricky on Windows | Client binds to 0.0.0.0, sets SO_REUSEADDR |

---

## 10. Dev Log

### 2026-04-20 — Initial Implementation

- Created architecture document
- Implemented `uhd_tunnel_common.h` (shared framing, constants, utilities)
- Implemented `uhd_tunnel_server.cpp` (MVP)
- Implemented `uhd_tunnel_client.cpp` (MVP)
- Updated `CMakeLists.txt` with new targets
- Build verification pending

---
