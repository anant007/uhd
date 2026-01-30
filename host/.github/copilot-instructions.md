# rfnoc_stream_tool - AI Agent Development Guide

## Project Focus

You are developing **`rfnoc_stream_tool.cpp`** - a monolithic multi-stream CHDR packet capture and streaming tool in `examples/`. The rest of the UHD codebase is **stable dependencies** - do not modify `lib/`, `include/uhd/`, or `tests/` unless absolutely critical.

**Tool Location**: `examples/rfnoc_stream_tool.cpp` (~6500 lines)  
**Header**: `examples/rfnoc_stream_tool/rfnoc_stream_tool.h` (~1700 lines)  
**Config**: YAML files (e.g., `local_testing_config.yaml`)  
**Build**: CMake automatically links `yaml-cpp` for this target (see `examples/CMakeLists.txt:73-77`)

## Tool Architecture

### Core Purpose
Capture synchronized IQ samples from multiple RFNoC streaming endpoints (DDC, FIR, Radio) with:
- **Multi-stream support**: Parallel capture from 2+ blocks simultaneously
- **PPS time synchronization**: Hardware timestamp reset at PPS edge
- **YAML-driven configuration**: Declarative graph setup, block properties, stream endpoints
- **File formats**: TSI proprietary format with `Packet_header.h` structure
- **Network streaming**: Optional TCP socket output per stream
- **Real-time processing**: Per-stream decimation modes (`fgb`, `sgb`) for SARSAT

### File Structure
```
examples/
├── rfnoc_stream_tool.cpp           # Main implementation (6500 lines)
├── rfnoc_stream_tool/
│   ├── rfnoc_stream_tool.h         # Headers, structs, globals (1700 lines)
│   ├── Packet_header.h             # TSI file format header
│   ├── daughterboard_caps.h        # Hardware capability detection
│   ├── timeconverter.h             # Filename time-slotting
│   ├── runtime_reload_helpers.h    # Hot-reload YAML config
│   ├── plot_spectrum.py            # Post-processing visualization
│   └── rfnoc_stream_tool_README    # Usage examples
├── local_testing_config.yaml       # Development config
└── sdrworkstation2_testing_config.yaml
```

### Key Data Structures (in `.h`)

**`PacketBuffer`**: Ring buffer element for captured packets
```cpp
struct PacketBuffer {
    std::vector<uint8_t> data;      // CHDR packet (header + timestamp + payload)
    size_t stream_id;               // Multi-stream identifier
    uint64_t packet_number;         // Sequence counter
    uhd::time_spec_t timestamp;     // UHD timestamp
    bool has_timestamp;
};
```

**`SPSCRingBuffer<T>`**: Lock-free single-producer-single-consumer ring buffer
- Power-of-2 capacity (default 32MB per stream)
- Cache-aligned atomics to avoid false sharing
- Used between receiver thread → writer thread per stream

**`StreamEndpointConfig`**: YAML-defined stream configuration
```cpp
struct StreamEndpointConfig {
    std::string block_id;           // "0/FIR#0", "0/DDC#1"
    size_t port;
    std::string direction;          // "rx" or "tx"
    std::map<std::string, std::string> stream_args; // {"spp": "1024"}
    TsiConfig tsi_config;           // TSI format + decimation mode
    SocketConfig socket;            // Optional TCP streaming
};
```

**`TsiConfig`**: TSI-specific capture settings
```cpp
struct TsiConfig {
    bool enabled;
    uint16_t sat_id;                // Satellite ID (430 for SARSAT)
    uint64_t tuning_freq_hz;        // Center frequency
    std::string sample_processing_mode; // "fgb", "sgb", or "" (raw)
    size_t csv_max_packets;
    size_t csv_samples_per_packet;
};
```

## Build & Run Workflow

### Building (Windows PowerShell)
```powershell
# From host/ directory
cmake --build build --config Release --target rfnoc_stream_tool

# Output: build/examples/Release/rfnoc_stream_tool.exe
```

**CMake Note**: `yaml-cpp` is auto-fetched via FetchContent (see `examples/CMakeLists.txt:16-21`). If you add new dependencies, update this section.

### Running
```powershell
# Basic multi-stream capture with PPS sync
.\build\examples\Release\rfnoc_stream_tool.exe --yaml local_testing_config.yaml --pps-reset

# Continuous capture (num-packets=0)
.\build\examples\Release\rfnoc_stream_tool.exe --yaml config.yaml --num-packets 0

# Generate YAML template from connected device
.\build\examples\Release\rfnoc_stream_tool.exe --args "addr=192.168.10.2" --create-yaml-template 1
```

### Environment Variables
- **`TEMPSTR_DEFINE`**: Default output directory (default: `C:/Users/sdrworkstation2`)
- Set before running: `$env:TEMPSTR_DEFINE = "C:\custom\path"`

## YAML Configuration Deep-Dive

### Essential Sections
```yaml
device_args: "addr=192.168.10.2"
clock_source: "internal"  # or "external", "gpsdo"
time_source: "internal"   # or "external", "gpsdo"

multi_stream:
  enable: true
  sync_streams: true      # Start all streams simultaneously
  separate_files: true    # One file per stream vs. combined
  max_streams: 2
  stream_blocks: ["0/FIR#0", "0/FIR#1"]

connections:             # FPGA graph connections (optional if using defaults)
  - src: "0/Radio#0:0"
    dst: "0/FIR#0:0"

stream_endpoints:        # Host streaming configuration
  - block_id: "0/FIR#0"
    port: 0
    direction: "rx"
    stream_args:
      spp: "1024"        # Samples per packet (affects latency/throughput)
    tsi_config:
      enabled: true
      sat_id: 430
      sample_processing_mode: "sgb"  # 2x decimation + averaging
    socket:              # Optional TCP streaming
      enabled: true
      mode: "server"
      host: "127.0.0.1"
      port: 5001
      nonblocking: true

block_properties:        # Block-specific settings
  "0/Radio#0":
    freq: "406000000"    # 406 MHz
    rate: "10000000"     # 10 MSPS
    gain: "30"
```

### Sample Processing Modes
- **`fgb`** (First Gen Beacon): Polyphase quadrature demod, fs/4 shift, 2x decimation  
  - Input: 4 samples → Output: 2 samples
  - Use case: 406 MHz SARSAT FGB signals
- **`sgb`** (Second Gen Beacon): Averaging filter, 2x decimation  
  - Input: 2 samples → Output: 1 sample  
  - Improves SNR for narrow-band signals
- **(empty/omitted)**: Pass-through, no processing

## Development Patterns

### Adding a New Feature
1. **Update structures** in `rfnoc_stream_tool.h` (e.g., new config struct)
2. **Add YAML parsing** in `parse_yaml_config()` function (~line 1800)
3. **Implement logic** in appropriate section (tool has clear section markers like `// SECTION 3: Capture Functions`)
4. **Test with YAML**: Create test config in `examples/`, avoid hardcoding values
5. **Update README**: Add example to `rfnoc_stream_tool/rfnoc_stream_tool_README`

### Critical Functions to Know

**`capture_multi_stream()`** (~line 2215): Main capture orchestration
- Creates streamers per endpoint
- Spawns receiver threads (one per stream)
- Spawns writer threads (separate file I/O)
- Handles PPS synchronization
- Monitors ring buffer health

**`parse_yaml_config()`** (~line 1800): YAML → `GraphConfig` struct
- Parses all config sections
- Validates block IDs against discovered graph
- Sets defaults for missing fields

**`apply_block_properties()`** (~line 1499): Configure RFNoC blocks
- Dynamic casting to specific block types (`radio_control`, `ddc_block_control`, etc.)
- Sets properties like `freq`, `rate`, `gain`, `enable`
- Handles per-channel vs. global properties

**`discover_blocks_enhanced()`** (~line 310): Graph introspection
- Finds all blocks via `graph->find_blocks("")`
- Detects SEP capability per block
- Returns `std::vector<BlockInfo>` with port counts

### Threading Model
```
Main Thread
 ├─> [Receiver Thread 0] → RingBuffer[0] → [Writer Thread 0] → File[0]
 ├─> [Receiver Thread 1] → RingBuffer[1] → [Writer Thread 1] → File[1]
 └─> [Monitor Thread] → Checks buffer fullness, prints stats
```

**Synchronization**:
- **`std::atomic<bool> stop_signal_called`**: Global stop flag (Ctrl+C handler)
- **Per-stream stop flags**: Individual thread control
- **PPS sync**: All streamers start at same `uhd::time_spec_t` after PPS reset

### Error Handling
- **UHD exceptions**: Catch `uhd::exception` for graph/streamer errors
- **YAML errors**: Catch `YAML::Exception` during parsing
- **File I/O**: Check `std::ofstream::good()` before writing
- **Logging**: Use `std::cout`/`std::cerr` (tool is user-facing, not library)

## UHD Dependency Reference

### RFNoC Graph API (Read-Only Knowledge)
You'll use these, but **don't modify UHD library code**:

**Creating graph**:
```cpp
auto graph = uhd::rfnoc::rfnoc_graph::make(device_addr);
graph->find_blocks<uhd::rfnoc::radio_control>("Radio");
graph->connect(src_blk, src_port, dst_blk, dst_port);
graph->commit();  // MUST call after connections/property changes
```

**Streamers**:
```cpp
auto stream_args = uhd::stream_args_t("fc32", "sc16");  // host_format, wire_format
auto rx_stream = graph->create_rx_streamer(1, stream_args);
graph->connect(block_id, port, rx_stream, 0);
```

**Receiving packets**:
```cpp
uhd::rx_metadata_t md;
size_t num_rx = rx_stream->recv(buffs, samps_per_buff, md, timeout);
if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
    // Handle overflow, timeout, etc.
}
```

**Block-specific control** (cast from `noc_block_base`):
```cpp
auto radio = std::dynamic_pointer_cast<uhd::rfnoc::radio_control>(block);
if (radio) {
    radio->set_rx_frequency(406e6, 0);  // channel 0
    radio->set_rx_gain(30, 0);
}
```

### CHDR Packet Structure (for analysis features)
```
[0-7]   Header (64-bit LE)
        [0-15]   dst_epid
        [16-31]  length (bytes)
        [32-47]  seq_num
        [48-52]  num_mdata
        [53-55]  pkt_type (6=no_ts, 7=with_ts)
        [56]     eov, [57] eob, [58-59] vc
[8-15]  Timestamp (64-bit, optional if pkt_type==7)
[16+]   Payload (IQ samples)
```

Parsing handled by `chdr_packet_data` struct (see `rfnoc_stream_tool.h:294`).

## CMake Changes (When Needed)

### Adding New External Library
Edit `examples/CMakeLists.txt` around line 73:
```cmake
if(${example_name} STREQUAL "rfnoc_stream_tool")
    target_link_libraries(${example_name} yaml-cpp::yaml-cpp)
    # Add new dependency here, e.g.:
    # target_link_libraries(${example_name} new_library::new_library)
endif()
```

### Adding New Source Files
If splitting `.cpp` into modules, update line 44:
```cmake
list(APPEND example_sources 
    rfnoc_stream_tool.cpp
    rfnoc_stream_tool_helper.cpp  # New file
)
```

## Debugging Tips

1. **Graph topology**: Use `--show-graph 1` to print all blocks/connections, then exit
2. **PPS issues**: Set `--verify-pps-reset 0` if PPS signal not connected
3. **Buffer overruns**: Increase `--ring-buffer-mb` or reduce `--rate`
4. **YAML syntax**: Tool prints line numbers on parse errors
5. **Block not found**: Run `uhd_usrp_probe --args="addr=..."` to see available blocks
6. **Timestamp discontinuities**: Check `csv_analysis.csv` for sequence gaps after `--csv` run

## Common Tasks

### Add New YAML Option
1. Add field to config struct in `.h` (e.g., `StreamEndpointConfig`)
2. Parse in `parse_yaml_config()`: `config.new_field = node["new_field"].as<Type>()`
3. Use in `capture_multi_stream()` or relevant function

### Support New Block Type
1. Add `#include <uhd/rfnoc/new_block_control.hpp>` to `.h`
2. Update `apply_block_properties()` with new `dynamic_pointer_cast<new_block_control>()`
3. Set block-specific properties via `block->set_property()` or dedicated methods

### Change File Format
1. Modify `write_tsi_packet_to_file()` function (~line 2100)
2. Update `Packet_header.h` structure if changing header
3. Ensure backward compatibility or version bump in file header

### Add Network Protocol
1. Extend `SocketConfig` struct with new mode/options
2. Implement in `network_sender_thread()` function
3. Parse in YAML under `stream_endpoints[].socket`

## Quick Reference

```powershell
# Rebuild after changes
cmake --build build --config Release --target rfnoc_stream_tool

# Find device
uhd_find_devices

# Inspect FPGA image blocks
uhd_usrp_probe --args="addr=192.168.10.2"

# Generate fresh YAML template
.\rfnoc_stream_tool.exe --args="addr=192.168.10.2" --create-yaml-template 1 > template.yaml

# Test with verbose output
.\rfnoc_stream_tool.exe --yaml config.yaml 2>&1 | Tee-Object capture.log
```
