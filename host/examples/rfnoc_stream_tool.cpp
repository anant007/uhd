//
// Copyright 2025 Techno-Sciences Inc.
//
// SPDX-License-Identifier: GPL-3.0-or-later
//
// CHDR Packet Capture Tool - Enhanced with Multi-Stream Support
//
#include "rfnoc_stream_tool/rfnoc_stream_tool.h"

// Global Signal Handler
void sig_int_handler(int)
{
    stop_signal_called.store(true, std::memory_order_relaxed);
    for (auto* f : g_per_stream_stop_flags) {
        if (f)
            f->store(true, std::memory_order_relaxed);
    }
}

// Template Helper Functions
template <typename T>
void write_le(std::vector<uint8_t>& buffer, T value)
{
    for (size_t i = 0; i < sizeof(T); i++) {
        buffer.push_back((value >> (i * 8)) & 0xFF);
    }
}

template <typename T>
T read_le(const uint8_t* data)
{
    T value = 0;
    for (size_t i = 0; i < sizeof(T); i++) {
        value |= static_cast<T>(data[i]) << (i * 8);
    }
    return value;
}

// Block Discovery and Information
std::vector<BlockInfo> discover_blocks_enhanced(uhd::rfnoc::rfnoc_graph::sptr graph)
{
    std::vector<BlockInfo> blocks;
    auto block_ids = graph->find_blocks("");

    for (const auto& id : block_ids) {
        BlockInfo info;
        info.block_id = id.to_string();

        auto block = graph->get_block(id);
        if (!block)
            continue;

        info.block_type          = id.get_block_name();
        info.num_input_ports     = block->get_num_input_ports();
        info.num_output_ports    = block->get_num_output_ports();
        info.has_stream_endpoint = false;

        // Check for streaming capability
        static const std::set<std::string> stream_capable = {
            "Radio", "DDC", "DUC", "Replay", "DmaFIFO", "SigGen", "NullSrcSink"};

        if (stream_capable.count(info.block_type)
            || id.to_string().find("SEP") != std::string::npos) {
            info.has_stream_endpoint = true;
        }

        // Get properties
        try {
            info.properties = block->get_property_ids();
            for (const auto& prop : info.properties) {
                if (prop.find("freq") != std::string::npos
                    || prop.find("rate") != std::string::npos
                    || prop.find("gain") != std::string::npos
                    || prop.find("bandwidth") != std::string::npos) {
                    info.property_types[prop] = "double";
                } else if (prop.find("enable") != std::string::npos) {
                    info.property_types[prop] = "bool";
                } else if (prop.find("antenna") != std::string::npos
                           || prop.find("waveform") != std::string::npos) {
                    info.property_types[prop] = "string";
                }
            }
        } catch (...) {
        }

        blocks.push_back(info);
    }

    return blocks;
}

// Graph Topology Discovery
GraphTopology discover_graph_topology(uhd::rfnoc::rfnoc_graph::sptr graph) {
    GraphTopology topology;
    topology.blocks             = discover_blocks_enhanced(graph);
    topology.static_connections = graph->enumerate_static_connections();
    topology.active_connections = graph->enumerate_active_connections();

    auto all_connections = topology.static_connections;
    all_connections.insert(all_connections.end(),
        topology.active_connections.begin(),
        topology.active_connections.end());

    for (const auto& edge : all_connections) {
        if (edge.dst_blockid.find("SEP") != std::string::npos) {
            topology.block_stream_ports[edge.src_blockid].push_back(edge.src_port);
        }
    }

    for (const auto& block : topology.blocks) {
        if (block.has_stream_endpoint
            && topology.block_stream_ports.find(block.block_id)
                   == topology.block_stream_ports.end()) {
            for (size_t port = 0; port < block.num_output_ports; ++port) {
                topology.block_stream_ports[block.block_id].push_back(port);
            }
        }
    }

    return topology;
}

// =============================================================================
// TSI Constants
// =============================================================================

/// Default receiver type identifier
static constexpr char TSI_RECEIVER_TYPE[4] = {'T', 'S', 'I', '1'};

/// TSI file magic number
static constexpr char TSI_FILE_MAGIC[8] = {'T', 'S', 'I', 'P', 'K', 'T', '0', '1'};

// =============================================================================
// TSI Time Conversion Utilities
// =============================================================================

/**
 * @brief Time components extracted from timestamp
 */
struct TsiTimeComponents
{
    uint16_t year;
    uint8_t month;
    uint8_t day;
    uint8_t hour;
    uint8_t minute;
    uint8_t second;
    uint32_t frac_5ns; ///< 5-nanosecond counter
};

/**
 * @brief Convert UHD timestamp to TSI time components
 *
 * @param timestamp UHD time specification
 * @param tick_rate Device tick rate (typically 200MHz)
 * @return TsiTimeComponents with all fields populated
 */
inline TsiTimeComponents timestamp_to_tsi_time(
    const uhd::time_spec_t& timestamp, double tick_rate)
{
    TsiTimeComponents tc;

    // Get wall-clock time for date components
    auto now        = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm* tm_now = std::gmtime(&time_t_now);

    tc.year  = static_cast<uint16_t>(tm_now->tm_year + 1900);
    tc.month = static_cast<uint8_t>(tm_now->tm_mon + 1);
    tc.day   = static_cast<uint8_t>(tm_now->tm_mday);

    // Time of day from hardware timestamp
    double full_secs     = timestamp.get_full_secs();
    uint32_t secs_of_day = static_cast<uint32_t>(full_secs) % 86400;

    tc.hour   = static_cast<uint8_t>((secs_of_day / 3600) % 24);
    tc.minute = static_cast<uint8_t>((secs_of_day / 60) % 60);
    tc.second = static_cast<uint8_t>(secs_of_day % 60);

    // Fractional seconds as 5ns counter
    // At 200MHz: 1 tick = 5ns
    uint64_t ticks         = timestamp.to_ticks(tick_rate);
    uint64_t ticks_per_sec = static_cast<uint64_t>(tick_rate);
    tc.frac_5ns            = static_cast<uint32_t>(ticks % ticks_per_sec);

    return tc;
}

/**
 * @brief Build TSI packet header from PacketBuffer metadata
 *
 * @param pkt Source PacketBuffer with timestamp and metadata
 * @param tick_rate Device tick rate
 * @param stream_id Stream/channel ID (0-7)
 * @param sat_id Satellite ID
 * @param tuning_freq_hz Tuning frequency in Hz
 * @return Populated packetheader structure
 */
inline packetheader build_tsi_header_from_packet(const PacketBuffer& pkt,
    double tick_rate,
    size_t stream_id,
    uint16_t sat_id,
    uint32_t tuning_freq_hz)
{
    packetheader header;

    // Receiver type
    std::memcpy(header.ReceiverType, TSI_RECEIVER_TYPE, 4);

    // Packet sequence number
    header.PacketNumber = static_cast<unsigned int>(pkt.packet_number);

    // Satellite ID
    header.SATID = sat_id;

    // Time components
    TsiTimeComponents tc;
    if (pkt.has_timestamp) {
        tc = timestamp_to_tsi_time(pkt.timestamp, tick_rate);
    } else {
        // Fallback to wall clock
        auto now        = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        std::tm* tm_now = std::gmtime(&time_t_now);
        tc.year         = static_cast<uint16_t>(tm_now->tm_year + 1900);
        tc.month        = static_cast<uint8_t>(tm_now->tm_mon + 1);
        tc.day          = static_cast<uint8_t>(tm_now->tm_mday);
        tc.hour         = static_cast<uint8_t>(tm_now->tm_hour);
        tc.minute       = static_cast<uint8_t>(tm_now->tm_min);
        tc.second       = static_cast<uint8_t>(tm_now->tm_sec);
        tc.frac_5ns     = 0;
    }

    // YearMonth: 12 bits year + 4 bits month
    header.YearMonth = ((tc.year & 0x0FFF) << 4) | (tc.month & 0x0F);

    header.Hour             = tc.hour;
    header.Day              = tc.day;
    header.Sec              = tc.second;
    header.Minute           = tc.minute;
    header.FiveNanoSecCount = tc.frac_5ns;

    // Tuning frequency
    header.TuningFreq = tuning_freq_hz;

    // Receiver flags - channel number
    header.ReceiverFlag.all               = 0;
    header.ReceiverFlag.bit.channelNumber = static_cast<unsigned short>(stream_id & 0x07);

    // Reserved
    header.Reserved = 0;

    return header;
}

/**
 * @brief Extract raw IQ payload from PacketBuffer (strip CHDR header)
 *
 * PacketBuffer.data contains: [CHDR header (8 bytes)] [timestamp (0 or 8 bytes)]
 * [payload] This function returns only the payload portion.
 *
 * @param pkt Source PacketBuffer
 * @return Pair of (payload pointer, payload size)
 */
inline std::pair<const uint8_t*, size_t> extract_payload_from_packet(
    const PacketBuffer& pkt)
{
    constexpr size_t CHDR_HEADER_SIZE = 8;
    constexpr size_t TIMESTAMP_SIZE   = 8;

    size_t header_offset = CHDR_HEADER_SIZE;
    if (pkt.has_timestamp) {
        header_offset += TIMESTAMP_SIZE;
    }

    if (pkt.data.size() <= header_offset) {
        return {nullptr, 0};
    }

    return {pkt.data.data() + header_offset, pkt.data.size() - header_offset};
}

// =============================================================================
// SECTION 4: New TSI CSV Generation Functions
// Add these functions after the TSI time utilities, before tsi_file_writer_thread
// =============================================================================

/**
 * @brief Write TSI packet headers to CSV file for verification
 *
 * This creates a CSV file that mirrors what's written in the binary TSI file,
 * allowing verification that headers are correctly formatted.
 *
 * @param tsi_filename The TSI binary file to read
 * @param csv_filename Output CSV filename
 * @param csv_config Configuration for CSV output
 * @param bytes_per_sample Bytes per IQ sample (4 for sc16)
 * @param samples_per_packet Expected samples per packet
 */
void generate_tsi_verification_csv(const std::string& tsi_filename,
    const std::string& csv_filename,
    const TsiCsvConfig& csv_config,
    size_t bytes_per_sample,
    size_t samples_per_packet)
{
    std::ifstream tsi_file(tsi_filename, std::ios::binary);
    if (!tsi_file.is_open()) {
        std::cerr << "Failed to open TSI file for CSV generation: " << tsi_filename
                  << std::endl;
        return;
    }

    std::ofstream csv_file(csv_filename);
    if (!csv_file.is_open()) {
        std::cerr << "Failed to create TSI CSV file: " << csv_filename << std::endl;
        return;
    }

    // Write CSV header
    csv_file << "packet_num,"
             << "receiver_type,"
             << "sat_id,"
             << "year,"
             << "month,"
             << "day,"
             << "hour,"
             << "minute,"
             << "second,"
             << "frac_5ns_count,"
             << "tuning_freq_hz,"
             << "channel_num,"
             << "receiver_flags_raw,"
             << "payload_size_bytes,"
             << "num_samples";

    if (csv_config.include_sample_values) {
        csv_file << ",first_samples_hex";
    }
    csv_file << std::endl;

    // Calculate payload size if not provided
    size_t payload_bytes = samples_per_packet * bytes_per_sample;

    size_t packet_count   = 0;
    const size_t max_pkts = (csv_config.max_packets > 0) ? csv_config.max_packets
                                                         : SIZE_MAX;

    while (!tsi_file.eof() && packet_count < max_pkts) {
        // Read TSI header
        packetheader header;
        tsi_file.read(reinterpret_cast<char*>(&header), sizeof(packetheader));

        if (tsi_file.gcount() != sizeof(packetheader)) {
            break; // End of file or incomplete header
        }

        // Extract year and month from YearMonth field
        uint16_t year = (header.YearMonth >> 4) & 0x0FFF;
        uint8_t month = header.YearMonth & 0x0F;

        // Write header fields to CSV
        csv_file << packet_count << ","
                 << "\"" << std::string(*header.ReceiverType, 4) << "\"," << header.SATID
                 << "," << year << "," << static_cast<int>(month) << ","
                 << static_cast<int>(header.Day) << "," << static_cast<int>(header.Hour)
                 << "," << static_cast<int>(header.Minute) << ","
                 << static_cast<int>(header.Sec) << "," << header.FiveNanoSecCount << ","
                 << header.TuningFreq << "," << header.ReceiverFlag.bit.channelNumber
                 << ","
                 << "0x" << std::hex << std::setw(4) << std::setfill('0')
                 << header.ReceiverFlag.all << std::dec << "," << payload_bytes << ","
                 << (payload_bytes / bytes_per_sample);

        // Read and optionally display sample values
        if (payload_bytes > 0) {
            std::vector<uint8_t> payload(payload_bytes);
            tsi_file.read(reinterpret_cast<char*>(payload.data()), payload_bytes);

            if (csv_config.include_sample_values) {
                csv_file << ",0x";
                size_t bytes_to_show = std::min(
                    payload.size(), csv_config.max_samples_per_packet * bytes_per_sample);
                for (size_t i = 0; i < bytes_to_show; ++i) {
                    csv_file << std::hex << std::setw(2) << std::setfill('0')
                             << static_cast<int>(payload[i]);
                }
                csv_file << std::dec;
            }
        }

        csv_file << std::endl;
        packet_count++;
    }

    tsi_file.close();
    csv_file.close();

    std::cout << "TSI verification CSV written: " << csv_filename << " (" << packet_count
              << " packets)" << std::endl;
}

/**
 * @brief Real-time TSI CSV writer that writes as packets are captured
 *
 * This version writes to CSV in real-time as packets are being written
 * to the binary file, useful for monitoring captures.
 */
// class TsiCsvWriter {
// public:
TsiCsvWriter::TsiCsvWriter(const std::string& csv_filename, const TsiCsvConfig& config)
    : config_(config), packets_written_(0)
{
    csv_file_.open(csv_filename);
    if (csv_file_.is_open()) {
        write_header();
    }
}

TsiCsvWriter::~TsiCsvWriter()
{
    if (csv_file_.is_open()) {
        csv_file_.close();
    }
}

bool TsiCsvWriter::is_open() const
{
    return csv_file_.is_open();
}

/**
 * @brief Write a TSI packet header to CSV
 *
 * @param header The TSI packet header
 * @param payload_ptr Pointer to payload data (optional)
 * @param payload_size Size of payload in bytes
 * @param bytes_per_sample Bytes per IQ sample
 */
void TsiCsvWriter::write_packet(const packetheader& header,
    const uint8_t* payload_ptr,
    size_t payload_size,
    size_t bytes_per_sample)
{
    if (!csv_file_.is_open())
        return;
    if (config_.max_packets > 0 && packets_written_ >= config_.max_packets)
        return;

    // Extract year and month
    uint16_t year = (header.YearMonth >> 4) & 0x0FFF;
    uint8_t month = header.YearMonth & 0x0F;

    csv_file_ << packets_written_ << ","
              << "\"" << std::string(*header.ReceiverType, 4) << "\"," << header.SATID
              << "," << year << "," << static_cast<int>(month) << ","
              << static_cast<int>(header.Day) << "," << static_cast<int>(header.Hour)
              << "," << static_cast<int>(header.Minute) << ","
              << static_cast<int>(header.Sec) << "," << header.FiveNanoSecCount << ","
              << header.TuningFreq << "," << header.ReceiverFlag.bit.channelNumber << ","
              << "0x" << std::hex << std::setw(4) << std::setfill('0')
              << header.ReceiverFlag.all << std::dec << "," << payload_size << ","
              << (bytes_per_sample > 0 ? payload_size / bytes_per_sample : 0);

    if (config_.include_sample_values && payload_ptr && payload_size > 0) {
        csv_file_ << ",0x";
        size_t bytes_to_show =
            std::min(payload_size, config_.max_samples_per_packet * bytes_per_sample);
        for (size_t i = 0; i < bytes_to_show; ++i) {
            csv_file_ << std::hex << std::setw(2) << std::setfill('0')
                      << static_cast<int>(payload_ptr[i]);
        }
        csv_file_ << std::dec;
    }

    csv_file_ << std::endl;
    packets_written_++;
}

size_t TsiCsvWriter::packets_written() const
{
    return packets_written_;
}

// private:
void TsiCsvWriter::write_header()
{
    csv_file_ << "packet_num,"
              << "receiver_type,"
              << "sat_id,"
              << "year,"
              << "month,"
              << "day,"
              << "hour,"
              << "minute,"
              << "second,"
              << "frac_5ns_count,"
              << "tuning_freq_hz,"
              << "channel_num,"
              << "receiver_flags_raw,"
              << "payload_size_bytes,"
              << "num_samples";
    if (config_.include_sample_values) {
        csv_file_ << ",first_samples_hex";
    }
    csv_file_ << std::endl;
}

std::ofstream csv_file_;
TsiCsvConfig config_;
size_t packets_written_;
// };

// =============================================================================
// SECTION 5: Modified tsi_file_writer_thread
// Replace the existing function (~lines 302-449)
// =============================================================================

/**
 * @brief TSI format file writer thread
 *
 * This function follows the EXACT same pattern as file_writer_thread() but:
 * 1. Writes TSI headers instead of raw CHDR packets
 * 2. Strips CHDR headers from payloads
 * 3. Does NOT write magic numbers or file headers
 * 4. Optionally generates verification CSV
 *
 * @param ctx StreamContext with ring buffer
 * @param stop_writing Atomic flag to signal shutdown
 * @param writer_stats Statistics tracking
 * @param tsi_config TSI-specific configuration
 */
void tsi_file_writer_thread(StreamContext& ctx,
    std::atomic<bool>& stop_writing,
    FileWriterStats& writer_stats,
    const TsiOutputConfig& tsi_config)
{
    writer_stats.start_time = std::chrono::steady_clock::now();
    uhd::set_thread_priority_safe(0.5, true);

    // Generate TSI output filename
    std::string tsi_filename = ctx.output_filename;
    size_t dot_pos           = tsi_filename.rfind('.');
    if (dot_pos != std::string::npos) {
        tsi_filename.insert(dot_pos, "_tsi");
    } else {
        tsi_filename += "_tsi.dat";
    }

    // Open output file
    std::ofstream output_file(tsi_filename, std::ios::binary);
    if (!output_file.is_open()) {
        std::cerr << "[TSI Writer " << ctx.stream_id
                  << "] Failed to open file: " << tsi_filename << std::endl;
        return;
    }

    // NOTE: No file header/magic number - raw TSI packets only

    std::cout << "[TSI Writer " << ctx.stream_id
              << "] Started writing to: " << tsi_filename << std::endl;

    // Optional: Create CSV writer for verification
    std::unique_ptr<TsiCsvWriter> csv_writer;
    if (tsi_config.csv_max_packets > 0) {
        std::cout << "[TSI Writer " << ctx.stream_id
                  << "] Initializing CSV verification output..." << std::endl;
        std::string csv_filename = tsi_filename;
        size_t dot               = csv_filename.rfind('.');
        if (dot != std::string::npos) {
            csv_filename.replace(dot, std::string::npos, "_verification.csv");
        } else {
            csv_filename += "_verification.csv";
        }

        TsiCsvConfig csv_cfg;
        csv_cfg.max_packets            = tsi_config.csv_max_packets;
        csv_cfg.max_samples_per_packet = tsi_config.csv_samples_per_packet;
        csv_cfg.include_sample_values  = true;

        csv_writer = std::make_unique<TsiCsvWriter>(csv_filename, csv_cfg);
        std::cout << "[TSI Writer " << ctx.stream_id << "] CSV verification enabled for "
                  << tsi_config.csv_max_packets << " packets" << std::endl;
    }

    // Batch buffer for efficient writes
    std::vector<PacketBuffer> write_batch;
    write_batch.reserve(ctx.buffer_config.batch_write_size);

    // Main write loop
    while (!stop_writing.load() || !ctx.ring_buffer->empty()) {
        PacketBuffer packet;

        // Collect batch of packets
        while (write_batch.size() < ctx.buffer_config.batch_write_size
               && ctx.ring_buffer->pop(packet)) {
            write_batch.push_back(std::move(packet));
        }

        if (!write_batch.empty()) {
            try {
                for (const auto& pkt : write_batch) {
                    // Build TSI header
                    packetheader header = build_tsi_header_from_packet(pkt,
                        ctx.tick_rate,
                        ctx.stream_id,
                        tsi_config.sat_id,
                        tsi_config.tuning_freq_hz);

                    // Write TSI header (32 bytes)
                    output_file.write(
                        reinterpret_cast<const char*>(&header), sizeof(packetheader));

                    // Extract and write raw payload (strip CHDR header)
                    auto [payload_ptr, payload_size] = extract_payload_from_packet(pkt);
                    if (payload_ptr && payload_size > 0) {
                        output_file.write(
                            reinterpret_cast<const char*>(payload_ptr), payload_size);
                    }

                    // Write to CSV if enabled
                    if (csv_writer && csv_writer->is_open()) {
                        csv_writer->write_packet(header, payload_ptr, payload_size);
                    }

                    writer_stats.packets_written++;
                    writer_stats.bytes_written += sizeof(packetheader) + payload_size;
                }
                write_batch.clear();
            } catch (const std::exception& e) {
                std::cerr << "[TSI Writer " << ctx.stream_id
                          << "] Write error: " << e.what() << std::endl;
                writer_stats.write_errors++;
            }
        } else if (stop_writing.load() && ctx.ring_buffer->empty()) {
            break;
        } else {
            std::this_thread::sleep_for(1ms);
        }

        // Track buffer usage
        size_t current_usage = ctx.ring_buffer->size();
        if (current_usage > ctx.stats.max_buffer_usage) {
            ctx.stats.max_buffer_usage = current_usage;
        }
    }

    // Drain remaining packets
    while (!ctx.ring_buffer->empty()) {
        PacketBuffer packet;
        if (ctx.ring_buffer->pop(packet)) {
            packetheader header = build_tsi_header_from_packet(packet,
                ctx.tick_rate,
                ctx.stream_id,
                tsi_config.sat_id,
                tsi_config.tuning_freq_hz);

            output_file.write(
                reinterpret_cast<const char*>(&header), sizeof(packetheader));

            auto [payload_ptr, payload_size] = extract_payload_from_packet(packet);
            if (payload_ptr && payload_size > 0) {
                output_file.write(
                    reinterpret_cast<const char*>(payload_ptr), payload_size);
            }

            if (csv_writer && csv_writer->is_open()) {
                csv_writer->write_packet(header, payload_ptr, payload_size);
            }

            writer_stats.packets_written++;
            writer_stats.bytes_written += sizeof(packetheader) + payload_size;
        }
    }

    output_file.close();
    writer_stats.end_time = std::chrono::steady_clock::now();

    double duration =
        std::chrono::duration<double>(writer_stats.end_time - writer_stats.start_time)
            .count();

    std::cout << "[TSI Writer " << ctx.stream_id << "] Complete."
              << " Packets: " << writer_stats.packets_written
              << ", Bytes: " << writer_stats.bytes_written << ", Duration: " << std::fixed
              << std::setprecision(2) << duration << "s"
              << ", Rate: " << (writer_stats.bytes_written / duration / 1e6) << " MB/s"
              << ", Max buffer: " << ctx.stats.max_buffer_usage;

    if (csv_writer) {
        std::cout << ", CSV packets: " << csv_writer->packets_written();
    }
    std::cout << std::endl;
}

// =============================================================================
// SECTION 6: Simplified apply_block_properties
// Replace the existing function (~lines 1465-1564)
// =============================================================================

/**
 * @brief Parse a property string that may include channel suffix (e.g., "freq/0")
 *
 * @param prop Property string
 * @return Pair of (property_name, channel_index)
 */
std::pair<std::string, size_t> parse_property_with_channel(const std::string& prop)
{
    size_t slash_pos = prop.rfind('/');
    if (slash_pos != std::string::npos && slash_pos < prop.length() - 1) {
        std::string prop_name = prop.substr(0, slash_pos);
        size_t chan           = std::stoul(prop.substr(slash_pos + 1));
        return {prop_name, chan};
    }
    return {prop, 0};
}

/**
 * @brief Apply block properties from YAML configuration
 *
 * Simplified and straightforward implementation that directly parses
 * YAML values and applies them to the appropriate block controllers.
 *
 * Supports: DDC, FIR, Radio, SigGen, DUC, FFT, Window, KeepOneInN,
 *           MovingAverage, VectorIIR blocks
 *
 * @param graph RFNoC graph
 * @param properties Map of block_id -> (property_name -> value_string)
 * @param default_rate Default rate for blocks that need it
 * @return true if all properties were applied successfully
 */
bool apply_block_properties(uhd::rfnoc::rfnoc_graph::sptr& graph,
    const std::map<std::string, std::map<std::string, std::string>>& properties,
    double default_rate)
{
    bool success = true;

    for (const auto& [block_id_str, props] : properties) {
        try {
            // ================================================================
            // DDC Block
            // ================================================================
            if (block_id_str.find("DDC") != std::string::npos) {
                std::cout << " About to apply DDC configuration settings with values: "
                          << std::endl;
                std::cout << "  ----------------------------------------" << std::endl;
                std::cout << "  | Property         | Value             |" << std::endl;
                std::cout << "  ----------------------------------------" << std::endl;
                std::cout << std::fixed << std::setprecision(6);
                for (const auto& [prop, value] : props) {
                    std::cout << "  | " << std::left << std::setw(16) << prop << " | "
                              << std::right << std::setw(16) << value << " |"
                              << std::endl;
                }
                std::cout << "  ----------------------------------------" << std::endl;
                if (props.empty()) {
                    std::cout << "  No properties to set for DDC block." << std::endl;
                    continue;
                }
                auto ddc = graph->get_block<uhd::rfnoc::ddc_block_control>(block_id_str);
                if (!ddc) {
                    std::cerr << "  Failed to cast to DDC block control" << std::endl;
                    success = false;
                    continue;
                }

                for (const auto& [prop, value] : props) {
                    auto [prop_name, chan] = parse_property_with_channel(prop);

                    try {
                        if (prop_name == "freq") {
                            double freq = std::stod(value);
                            ddc->set_freq(freq, chan);
                            std::cout << "  Set freq[" << chan << "] = " << freq << " Hz"
                                      << std::endl;
                        } else if (prop_name == "output_rate") {
                            std::cout
                                << " Output rate read from config and now being set..."
                                << std::endl;
                            double rate = std::stod(value);
                            ddc->set_output_rate(rate, chan);
                            std::cout << "  Set output_rate[" << chan << "] = " << rate
                                      << " sps" << std::endl;
                        } else if (prop_name == "input_rate") {
                            double rate = std::stod(value);
                            ddc->set_input_rate(rate, chan);
                            std::cout << "  Set input_rate[" << chan << "] = " << rate
                                      << " sps" << std::endl;
                        } else if (prop_name == "decim") {
                            int decim            = std::stoi(value);
                            double current_input = ddc->get_input_rate(chan);
                            ddc->set_output_rate(current_input / decim, chan);
                            std::cout << "  Set decimation[" << chan << "] = " << decim
                                      << std::endl;
                        } else {
                            std::cout << "  Unknown DDC property: " << prop_name
                                      << std::endl;
                        }
                    } catch (const uhd::resolve_error& e) {
                        std::cerr << "  Property not found: " << prop_name << ": "
                                  << e.what() << std::endl;
                        success = false;
                    } catch (const uhd::rfnoc_error& e) {
                        std::cerr << "  RFNoC error setting " << prop_name << ": "
                                  << e.what() << std::endl;
                        success = false;
                    } catch (const std::exception& e) {
                        std::cerr << "  Failed to set " << prop_name << ": " << e.what()
                                  << std::endl;
                        success = false;
                    }
                }
            }
            // ================================================================
            // FIR Filter Block
            // ================================================================
            else if (block_id_str.find("FIR") != std::string::npos) {
                auto fir =
                    graph->get_block<uhd::rfnoc::fir_filter_block_control>(block_id_str);
                if (!fir) {
                    std::cerr << "  Failed to cast to FIR block control" << std::endl;
                    success = false;
                    continue;
                }

                for (const auto& [prop, value] : props) {
                    auto [prop_name, chan] = parse_property_with_channel(prop);

                    try {
                        if (prop_name == "coefficients" || prop_name == "coeffs") {
                            // Parse coefficient string: comma or space separated
                            std::vector<int16_t> coeffs;
                            std::stringstream ss(value);
                            std::string token;
                            char delim = (value.find(',') != std::string::npos) ? ','
                                                                                : ' ';

                            while (std::getline(ss, token, delim)) {
                                // Trim whitespace
                                token.erase(0, token.find_first_not_of(" \t"));
                                token.erase(token.find_last_not_of(" \t") + 1);
                                if (!token.empty()) {
                                    coeffs.push_back(
                                        static_cast<int16_t>(std::stoi(token)));
                                }
                            }

                            if (!coeffs.empty()) {
                                fir->set_coefficients(coeffs, chan);
                                std::cout << "  Set coefficients[" << chan
                                          << "] = " << coeffs.size() << " taps"
                                          << std::endl;
                            }
                        } else if (prop_name == "coefficients_file"
                                   || prop_name == "coeffs_file") {
                            // Load coefficients from file
                            std::ifstream coeff_file(value);
                            if (!coeff_file.is_open()) {
                                std::cerr
                                    << "  Failed to open coefficients file: " << value
                                    << std::endl;
                                success = false;
                                continue;
                            }

                            std::vector<int16_t> coeffs;
                            std::string line;
                            while (std::getline(coeff_file, line)) {
                                // Skip comments and empty lines
                                if (line.empty() || line[0] == '#' || line[0] == '%')
                                    continue;

                                std::stringstream ss(line);
                                double coeff_val;
                                while (ss >> coeff_val) {
                                    // Scale floating point [-1,1] to int16 if needed
                                    if (coeff_val >= -1.0 && coeff_val <= 1.0) {
                                        coeffs.push_back(
                                            static_cast<int16_t>(coeff_val * 32767));
                                    } else {
                                        coeffs.push_back(static_cast<int16_t>(coeff_val));
                                    }
                                }
                            }

                            if (!coeffs.empty()) {
                                fir->set_coefficients(coeffs, chan);
                                std::cout << "  Loaded " << coeffs.size()
                                          << " coefficients from " << value << std::endl;
                            }
                        } else {
                            std::cout << "  Unknown FIR property: " << prop_name
                                      << std::endl;
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "  Failed to set " << prop_name << ": " << e.what()
                                  << std::endl;
                        success = false;
                    }
                }
            }
            // ================================================================
            // Unknown Block Type
            // ================================================================
            else {
                std::cout << "  Block type '" << block_id_str
                          << "' not explicitly handled, "
                          << "skipping property configuration" << std::endl;
            }

        } catch (const uhd::lookup_error& e) {
            std::cerr << "Block not found: " << block_id_str << ": " << e.what()
                      << std::endl;
            success = false;
        } catch (const uhd::rfnoc_error& e) {
            std::cerr << "RFNoC error configuring block " << block_id_str << ": "
                      << e.what() << std::endl;
            success = false;
        } catch (const std::exception& e) {
            std::cerr << "Failed to configure block " << block_id_str << ": " << e.what()
                      << std::endl;
            success = false;
        }
    }

    return success;
}
// =============================================================================
// Capture Function with TSI Output (uses same capture, different writer)
// =============================================================================

/**
 * @brief Ring buffer capture with TSI format output
 *
 * This function uses the SAME capture code as capture_stream_ringbuffer() but
 * launches tsi_file_writer_thread() instead of file_writer_thread().
 *
 * The capture path is identical - only the output format differs.
 */
template <typename samp_type>
void capture_stream_ringbuffer_tsi(StreamContext& ctx,
    std::atomic<bool>& start_capture,
    std::atomic<bool>& stop_writing,
    size_t num_packets,
    FileWriterStats& writer_stats,
    const TsiOutputConfig& tsi_config)
{
    uhd::set_thread_priority_safe(1.0, true);

    // Wait for synchronized start
    while (!start_capture.load()) {
        std::this_thread::sleep_for(1ms);
    }

    ctx.stats.start_time = std::chrono::steady_clock::now();

    std::vector<samp_type> buff(ctx.samps_per_buff);
    std::vector<void*> buff_ptrs = {&buff.front()};
    uhd::rx_metadata_t md;

    // Issue stream command
    uhd::stream_cmd_t stream_cmd(num_packets == 0
                                     ? uhd::stream_cmd_t::STREAM_MODE_START_CONTINUOUS
                                     : uhd::stream_cmd_t::STREAM_MODE_NUM_SAMPS_AND_DONE);

    if (num_packets > 0) {
        stream_cmd.num_samps = num_packets * ctx.samps_per_buff;
    }

    stream_cmd.stream_now = true;
    ctx.rx_streamer->issue_stream_cmd(stream_cmd);

    std::cout << "[Stream " << ctx.stream_id << "] Started TSI capture from "
              << ctx.block_id << ":" << ctx.port << std::endl;

    size_t consecutive_timeouts           = 0;
    const size_t max_consecutive_timeouts = 5;

    while (!stop_signal_called.load()
           && (num_packets == 0 || ctx.stats.packets_captured < num_packets)) {
        size_t num_rx_samps =
            ctx.rx_streamer->recv(buff_ptrs, ctx.samps_per_buff, md, 3.0);

        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_TIMEOUT) {
            if (++consecutive_timeouts >= max_consecutive_timeouts) {
                std::cout << "[Stream " << ctx.stream_id
                          << "] Multiple timeouts, stopping" << std::endl;
                break;
            }
            continue;
        } else {
            consecutive_timeouts = 0;
        }

        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
            ctx.stats.overflow_count++;
            continue;
        }

        if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
            std::cerr << "[Stream " << ctx.stream_id << "] Error: " << md.strerror()
                      << std::endl;
            ctx.stats.error_count++;
            break;
        }

        if (num_rx_samps == 0)
            continue;

        // Build PacketBuffer (same as capture_stream_ringbuffer)
        PacketBuffer packet_buffer;
        packet_buffer.stream_id     = ctx.stream_id;
        packet_buffer.packet_number = ctx.stats.packets_captured;

        size_t payload_bytes    = num_rx_samps * sizeof(samp_type);
        size_t header_bytes     = 8;
        size_t timestamp_bytes  = md.has_time_spec ? 8 : 0;
        size_t total_chdr_bytes = header_bytes + timestamp_bytes + payload_bytes;

        packet_buffer.data.reserve(total_chdr_bytes);

        // Build CHDR header (kept for consistency with existing code)
        uint64_t header = 0;
        header |= (uint64_t)ctx.stream_id & 0xFFFF;
        header |= ((uint64_t)total_chdr_bytes & 0xFFFF) << 16;
        header |= ((uint64_t)ctx.stats.packets_captured & 0xFFFF) << 32;
        header |= ((uint64_t)0 & 0x1F) << 48;
        header |=
            ((uint64_t)(md.has_time_spec ? PKT_TYPE_DATA_WITH_TS : PKT_TYPE_DATA_NO_TS)
                & 0x7)
            << 53;
        header |= ((uint64_t)(md.end_of_burst ? 1 : 0) & 0x1) << 57;

        // Write header to buffer
        for (size_t i = 0; i < sizeof(uint64_t); i++) {
            packet_buffer.data.push_back((header >> (i * 8)) & 0xFF);
        }

        // Write timestamp if present
        if (md.has_time_spec) {
            uint64_t timestamp_ticks = md.time_spec.to_ticks(ctx.tick_rate);
            for (size_t i = 0; i < sizeof(uint64_t); i++) {
                packet_buffer.data.push_back((timestamp_ticks >> (i * 8)) & 0xFF);
            }
            packet_buffer.has_timestamp = true;
            packet_buffer.timestamp     = md.time_spec;

            double timestamp_sec = md.time_spec.get_real_secs();
            if (ctx.stats.first_timestamp == 0.0) {
                ctx.stats.first_timestamp = timestamp_sec;
            }
            ctx.stats.last_timestamp = timestamp_sec;
        }

        // Write payload
        const uint8_t* sample_bytes = reinterpret_cast<const uint8_t*>(buff.data());
        packet_buffer.data.insert(
            packet_buffer.data.end(), sample_bytes, sample_bytes + payload_bytes);

        // Push to ring buffer
        if (!ctx.ring_buffer->push(std::move(packet_buffer))) {
            ctx.stats.buffer_overflows++;
        }

        // Store for analysis if needed
        if (ctx.analysis_packets && ctx.analysis_packets->size() < MAX_ANALYSIS_PACKETS) {
            chdr_packet_data pkt;
            pkt.stream_id    = ctx.stream_id;
            pkt.stream_block = ctx.block_id;
            pkt.stream_port  = ctx.port;
            pkt.header_raw   = header;
            pkt.parse_header();
            if (md.has_time_spec) {
                pkt.timestamp = md.time_spec.to_ticks(ctx.tick_rate);
            }
            pkt.payload.assign(sample_bytes, sample_bytes + payload_bytes);

            std::lock_guard<std::mutex> lock(*ctx.analysis_mutex);
            ctx.analysis_packets->push_back(pkt);
        }

        ctx.stats.packets_captured++;
        ctx.stats.total_samples += num_rx_samps;

        if (ctx.stats.packets_captured % 1000 == 0) {
            std::cout << "[Stream " << ctx.stream_id
                      << "] TSI Packets: " << ctx.stats.packets_captured;
            if (ctx.stats.overflow_count > 0) {
                std::cout << " (O: " << ctx.stats.overflow_count << ")";
            }
            if (ctx.stats.buffer_overflows > 0) {
                std::cout << " (BufOF: " << ctx.stats.buffer_overflows << ")";
            }
            std::cout << std::endl;
        }
    }

    stream_cmd.stream_mode = uhd::stream_cmd_t::STREAM_MODE_STOP_CONTINUOUS;
    ctx.rx_streamer->issue_stream_cmd(stream_cmd);

    ctx.stats.end_time = std::chrono::steady_clock::now();

    // Signal writer to stop
    stop_writing.store(true);
}

// =============================================================================
// Multi-Stream TSI Capture (follows capture_multi_stream_unified pattern)
// =============================================================================

/**
 * @brief Multi-stream capture with TSI packet format output
 *
 * Uses the same architecture as capture_multi_stream_unified but launches
 * tsi_file_writer_thread for each stream instead of file_writer_thread.
 */
template <typename samp_type>
void capture_multi_stream_tsi(uhd::rfnoc::rfnoc_graph::sptr graph,
    const GraphConfig& config,
    const std::string& file,
    size_t num_packets,
    bool enable_analysis,
    const std::string& csv_file,
    double rate,
    size_t samps_per_buff,
    uhd::time_spec_t pps_reset_time,
    bool pps_reset_used,
    const TsiOutputConfig& tsi_config)
{
    print_graph_info(graph);

    // Initialize blocks
    for (const auto& block_id : config.block_init_order) {
        try {
            auto block = graph->get_block(uhd::rfnoc::block_id_t(block_id));
            if (block)
                std::cout << "  Initialized: " << block_id << std::endl;
        } catch (...) {
        }
    }

    // Configure graph
    if (!config.switchboard_configs.empty()) {
        configure_switchboards(graph, config.switchboard_configs);
    }

    if (!config.dynamic_connections.empty()) {
        apply_dynamic_connections(
            graph, config.dynamic_connections, config.commit_after_each_connection);
    }

    if (!config.signal_paths.empty()) {
        apply_signal_paths(graph, config.signal_paths);
    }

    if (config.auto_connect_radio_to_ddc) {
        auto_connect_radio_to_ddc(graph);
    }

    // Find endpoints
    auto endpoints = find_all_stream_endpoints_enhanced(
        graph, config.stream_endpoints, config.multi_stream);

    if (endpoints.empty()) {
        throw std::runtime_error("No suitable streaming endpoints found");
    }

    std::cout << "\nFound " << endpoints.size() << " streaming endpoints" << std::endl;

    // Get tick rate and tuning frequency
    double tick_rate            = DEFAULT_TICKRATE;
    uint32_t actual_tuning_freq = tsi_config.tuning_freq_hz;

    auto radio_blocks = graph->find_blocks("Radio");
    if (!radio_blocks.empty()) {
        auto radio = graph->get_block<uhd::rfnoc::radio_control>(radio_blocks[0]);
        tick_rate  = radio->get_tick_rate();

        if (actual_tuning_freq == 0) {
            try {
                actual_tuning_freq = static_cast<uint32_t>(radio->get_rx_frequency(0));
            } catch (...) {
            }
        }
    }

    // Update TSI config with actual frequency
    TsiOutputConfig actual_tsi_config = tsi_config;
    actual_tsi_config.tuning_freq_hz  = actual_tuning_freq;

    // Create contexts (same as capture_multi_stream_unified)
    std::vector<StreamContext> contexts;
    std::vector<std::atomic<bool>> stop_writing_flags(endpoints.size());
    std::vector<FileWriterStats> writer_stats(endpoints.size());
    std::vector<chdr_packet_data> all_analysis_packets;
    std::mutex analysis_mutex;

    std::vector<uhd::rfnoc::ddc_block_control::sptr> ddc_controls;
    std::vector<size_t> ddc_channels;

    for (size_t i = 0; i < endpoints.size(); ++i) {
        const auto& [block_id, port] = endpoints[i];

        try {
            uhd::rfnoc::block_id_t endpoint_id(block_id);
            auto endpoint_block = graph->get_block(endpoint_id);

            if (!endpoint_block || port >= endpoint_block->get_num_output_ports()) {
                continue;
            }

            uhd::stream_args_t stream_args("sc16", "sc16");
            stream_args.channels = {0};

            for (const auto& sep : config.stream_endpoints) {
                if (sep.block_id == block_id && sep.port == port) {
                    for (const auto& [key, value] : sep.stream_args) {
                        stream_args.args[key] = value;
                    }
                    break;
                }
            }

            auto rx_streamer = graph->create_rx_streamer(1, stream_args);
            graph->connect(block_id, port, rx_streamer, 0, true);

            if (endpoint_id.get_block_name() == "DDC") {
                auto ddc_ctrl =
                    graph->get_block<uhd::rfnoc::ddc_block_control>(endpoint_id);
                if (ddc_ctrl) {
                    ddc_controls.push_back(ddc_ctrl);
                    ddc_channels.push_back(port);
                }
            }

            // Create StreamContext (same as existing code)
            StreamContext ctx;
            ctx.stream_id        = i;
            ctx.block_id         = block_id;
            ctx.port             = port;
            ctx.rx_streamer      = rx_streamer;
            ctx.stats.stream_id  = i;
            ctx.stats.block_id   = block_id;
            ctx.stats.port       = port;
            ctx.analysis_packets = enable_analysis ? &all_analysis_packets : nullptr;
            ctx.analysis_mutex   = &analysis_mutex;
            ctx.tick_rate        = tick_rate;
            ctx.samps_per_buff   = samps_per_buff;
            ctx.pps_reset_time   = pps_reset_time;
            ctx.pps_reset_used   = pps_reset_used;
            ctx.buffer_config    = config.multi_stream.buffer_config;
            ctx.output_filename =
                config.multi_stream.file_prefix + "_" + std::to_string(i) + ".dat";

            // Calculate ring buffer size (same as existing code)
            const size_t bytes_per_samp    = sizeof(samp_type);
            const size_t est_payload_bytes = samps_per_buff * bytes_per_samp;
            const size_t est_pkt_bytes = est_payload_bytes + sizeof(PacketBuffer) + 16;
            size_t est_pkts            = std::max<size_t>(
                1, config.multi_stream.buffer_config.ring_buffer_size / est_pkt_bytes);

            // Round to power of 2
            size_t power_of_2 = 1;
            while (power_of_2 < est_pkts)
                power_of_2 <<= 1;
            if (power_of_2 < 2)
                power_of_2 = 2;

            ctx.ring_buffer = std::make_shared<SPSCRingBuffer<PacketBuffer>>(power_of_2);

            contexts.push_back(std::move(ctx));

        } catch (const std::exception& e) {
            std::cerr << "Failed to setup stream " << i << ": " << e.what() << std::endl;
        }
    }

    if (contexts.empty()) {
        throw std::runtime_error("No streams could be configured");
    }

    // Setup global stop flags
    g_per_stream_stop_flags.clear();
    g_per_stream_stop_flags.reserve(contexts.size());
    for (size_t i = 0; i < contexts.size(); ++i) {
        g_per_stream_stop_flags.push_back(&stop_writing_flags[i]);
    }

    // Commit graph
    graph->commit();

    if (!config.block_properties.empty()) {
        apply_block_properties(graph, config.block_properties, rate);
    }

    for (size_t i = 0; i < ddc_controls.size(); ++i) {
        ddc_controls[i]->set_output_rate(rate, ddc_channels[i]);
    }

    // Wait for LO lock
    for (const auto& radio_id : radio_blocks) {
        auto radio = graph->get_block<uhd::rfnoc::radio_control>(radio_id);
        if (radio) {
            for (size_t chan = 0; chan < radio->get_num_output_ports(); ++chan) {
                std::cout << "Waiting for LO lock on " << radio_id.to_string()
                          << " channel " << chan << ": ";
                auto sensors = radio->get_rx_sensor_names(chan);
                bool has_lo  = std::find(sensors.begin(), sensors.end(), "lo_locked")
                              != sensors.end();

                if (!has_lo) {
                    std::cout << " No LO sensor" << std::endl;
                    break;
                }

                auto start = std::chrono::steady_clock::now();
                while (!radio->get_rx_sensor("lo_locked", chan).to_bool()) {
                    std::this_thread::sleep_for(50ms);
                    if (std::chrono::steady_clock::now() - start > 10s) {
                        throw std::runtime_error("LO failed to lock");
                    }
                }
                std::cout << " locked." << std::endl;
            }
        }
    }

    // Start TSI writer threads (key difference: use tsi_file_writer_thread)
    std::vector<std::unique_ptr<std::thread>> writer_threads;
    for (size_t i = 0; i < contexts.size(); ++i) {
        writer_threads.push_back(std::make_unique<std::thread>(tsi_file_writer_thread,
            std::ref(contexts[i]),
            std::ref(stop_writing_flags[i]),
            std::ref(writer_stats[i]),
            std::cref(actual_tsi_config)));
    }

    // Start capture threads
    std::vector<std::thread> capture_threads;
    std::atomic<bool> start_capture(false);

    for (size_t i = 0; i < contexts.size(); ++i) {
        capture_threads.emplace_back(capture_stream_ringbuffer_tsi<samp_type>,
            std::ref(contexts[i]),
            std::ref(start_capture),
            std::ref(stop_writing_flags[i]),
            num_packets,
            std::ref(writer_stats[i]),
            std::cref(actual_tsi_config));
    }

    // Synchronize start
    if (config.multi_stream.sync_streams) {
        std::this_thread::sleep_for(
            std::chrono::duration<double>(config.multi_stream.sync_delay));
    }

    auto overall_start = std::chrono::steady_clock::now();
    start_capture.store(true);
    std::cout << "\nStarting multi-stream TSI capture (ring buffer mode)..." << std::endl;

    // Wait for capture threads
    for (auto& thread : capture_threads) {
        thread.join();
    }

    // Wait for writer threads
    std::cout << "\nWaiting for TSI writer threads to finish..." << std::endl;
    for (auto& thread : writer_threads) {
        if (thread && thread->joinable()) {
            thread->join();
        }
    }

    auto overall_end = std::chrono::steady_clock::now();
    double overall_duration =
        std::chrono::duration<double>(overall_end - overall_start).count();

    // Collect and print statistics
    std::vector<StreamStats> all_stats;
    size_t total_packets = 0, total_samples = 0, total_overflows = 0;
    size_t total_bytes = 0;

    for (size_t i = 0; i < contexts.size(); ++i) {
        all_stats.push_back(contexts[i].stats);
        total_packets += contexts[i].stats.packets_captured;
        total_samples += contexts[i].stats.total_samples;
        total_overflows += contexts[i].stats.overflow_count;
        total_bytes += writer_stats[i].bytes_written;
    }

    std::cout << "\n=== TSI Capture Statistics (Ring Buffer Mode) ===" << std::endl;
    std::cout << "Total streams: " << contexts.size() << std::endl;
    std::cout << "Total packets: " << total_packets << std::endl;
    std::cout << "Total samples: " << total_samples << std::endl;
    std::cout << "Total bytes written: " << total_bytes << std::endl;
    std::cout << "Duration: " << std::fixed << std::setprecision(2) << overall_duration
              << " seconds" << std::endl;
    std::cout << "Aggregate sample rate: " << (total_samples / overall_duration) / 1e6
              << " Msps" << std::endl;
    std::cout << "Aggregate write rate: " << (total_bytes / overall_duration) / 1e6
              << " MB/s" << std::endl;

    if (total_overflows > 0) {
        std::cout << "Total overflows: " << total_overflows << std::endl;
    }

    std::cout << "\nPer-stream statistics:" << std::endl;
    for (size_t i = 0; i < contexts.size(); ++i) {
        std::string tsi_fn = contexts[i].output_filename;
        size_t dot         = tsi_fn.rfind('.');
        if (dot != std::string::npos)
            tsi_fn.insert(dot, "_tsi");
        else
            tsi_fn += "_tsi.dat";

        std::cout << "  Stream " << i << " (" << contexts[i].block_id << ":"
                  << contexts[i].port << "):" << std::endl;
        std::cout << "    Packets: " << writer_stats[i].packets_written << std::endl;
        std::cout << "    Bytes: " << writer_stats[i].bytes_written << std::endl;
        std::cout << "    Max buffer: " << contexts[i].stats.max_buffer_usage << " / "
                  << contexts[i].ring_buffer->capacity() << std::endl;
        std::cout << "    Output: " << tsi_fn << std::endl;
    }

    // Analysis
    if (enable_analysis && !csv_file.empty() && !all_analysis_packets.empty()) {
        std::cout << "\nAnalyzing packets..." << std::endl;


        std::sort(all_analysis_packets.begin(),
            all_analysis_packets.end(),
            [](const chdr_packet_data& a, const chdr_packet_data& b) {
                if (a.stream_id != b.stream_id)
                    return a.stream_id < b.stream_id;
                return a.seq_num < b.seq_num;
            });

        analyze_packets_unified(all_analysis_packets,
            csv_file,
            tick_rate,
            all_stats,
            pps_reset_time,
            pps_reset_used,
            samps_per_buff,
            rate);
    }
}

// =============================================================================
// Explicit Template Instantiations
// =============================================================================

template void capture_stream_ringbuffer_tsi<std::complex<short>>(StreamContext&,
    std::atomic<bool>&,
    std::atomic<bool>&,
    size_t,
    FileWriterStats&,
    const TsiOutputConfig&);
template void capture_stream_ringbuffer_tsi<std::complex<float>>(StreamContext&,
    std::atomic<bool>&,
    std::atomic<bool>&,
    size_t,
    FileWriterStats&,
    const TsiOutputConfig&);
template void capture_stream_ringbuffer_tsi<std::complex<double>>(StreamContext&,
    std::atomic<bool>&,
    std::atomic<bool>&,
    size_t,
    FileWriterStats&,
    const TsiOutputConfig&);

template void capture_multi_stream_tsi<std::complex<short>>(uhd::rfnoc::rfnoc_graph::sptr,
    const GraphConfig&,
    const std::string&,
    size_t,
    bool,
    const std::string&,
    double,
    size_t,
    uhd::time_spec_t,
    bool,
    const TsiOutputConfig&);
template void capture_multi_stream_tsi<std::complex<float>>(uhd::rfnoc::rfnoc_graph::sptr,
    const GraphConfig&,
    const std::string&,
    size_t,
    bool,
    const std::string&,
    double,
    size_t,
    uhd::time_spec_t,
    bool,
    const TsiOutputConfig&);
template void capture_multi_stream_tsi<std::complex<double>>(
    uhd::rfnoc::rfnoc_graph::sptr,
    const GraphConfig&,
    const std::string&,
    size_t,
    bool,
    const std::string&,
    double,
    size_t,
    uhd::time_spec_t,
    bool,
    const TsiOutputConfig&);

// File I/O Functions
void write_file_header(std::ofstream& file,
    double tick_rate,
    bool pps_reset_used,
    uhd::time_spec_t pps_reset_time,
    size_t num_streams)
{
    ChdrFileHeader header;
    header.tick_rate          = tick_rate;
    header.pps_reset_used     = pps_reset_used ? 1 : 0;
    header.pps_reset_time_sec = pps_reset_time.get_real_secs();
    header.num_streams        = static_cast<uint32_t>(num_streams);
    file.write(reinterpret_cast<const char*>(&header), sizeof(header));
}

void write_stream_header(
    std::ofstream& file, size_t stream_id, const std::string& block_id, size_t port)
{
    StreamHeader header;
    header.stream_id = static_cast<uint32_t>(stream_id);
    std::strncpy(header.block_id, block_id.c_str(), sizeof(header.block_id) - 1);
    header.block_id[sizeof(header.block_id) - 1] = '\0';
    header.port                                  = static_cast<uint32_t>(port);
    file.write(reinterpret_cast<const char*>(&header), sizeof(header));
}

// File Writer Thread (for ring buffer mode)
void file_writer_thread(
    StreamContext& ctx, std::atomic<bool>& stop_writing, FileWriterStats& writer_stats)
{
    writer_stats.start_time = std::chrono::steady_clock::now();
    uhd::set_thread_priority_safe(0.5, true);

    std::ofstream output_file(ctx.output_filename, std::ios::binary);
    if (!output_file.is_open()) {
        std::cerr << "[Writer " << ctx.stream_id
                  << "] Failed to open file: " << ctx.output_filename << std::endl;
        return;
    }

    ChdrFileHeader header;
    header.tick_rate          = ctx.tick_rate;
    header.pps_reset_used     = ctx.pps_reset_used ? 1 : 0;
    header.pps_reset_time_sec = ctx.pps_reset_time.get_real_secs();
    header.num_streams        = 1;
    header.ring_buffer_used   = 1;
    output_file.write(reinterpret_cast<const char*>(&header), sizeof(header));

    StreamHeader stream_header;
    stream_header.stream_id = static_cast<uint32_t>(ctx.stream_id);
    std::strncpy(
        stream_header.block_id, ctx.block_id.c_str(), sizeof(stream_header.block_id) - 1);
    stream_header.port             = static_cast<uint32_t>(ctx.port);
    stream_header.ring_buffer_size = static_cast<uint32_t>(ctx.ring_buffer->capacity());
    output_file.write(
        reinterpret_cast<const char*>(&stream_header), sizeof(stream_header));

    std::vector<PacketBuffer> write_batch;
    write_batch.reserve(ctx.buffer_config.batch_write_size);

    while (!stop_writing.load() || !ctx.ring_buffer->empty()) {
        PacketBuffer packet;

        while (write_batch.size() < ctx.buffer_config.batch_write_size
               && ctx.ring_buffer->pop(packet)) {
            write_batch.push_back(packet);
        }

        if (!write_batch.empty()) {
            try {
                for (const auto& pkt : write_batch) {
                    uint32_t pkt_size = static_cast<uint32_t>(pkt.data.size());
                    output_file.write(
                        reinterpret_cast<const char*>(&pkt_size), sizeof(pkt_size));
                    output_file.write(
                        reinterpret_cast<const char*>(pkt.data.data()), pkt.data.size());
                    writer_stats.packets_written++;
                    writer_stats.bytes_written += sizeof(pkt_size) + pkt.data.size();
                }
                write_batch.clear();
            } catch (const std::exception& e) {
                std::cerr << "[Writer " << ctx.stream_id << "] Write error: " << e.what()
                          << std::endl;
                writer_stats.write_errors++;
            }
        } else if (stop_writing.load() && ctx.ring_buffer->empty()) {
            break;
        } else {
            std::this_thread::sleep_for(1ms);
        }

        size_t current_usage = ctx.ring_buffer->size();
        if (current_usage > ctx.stats.max_buffer_usage) {
            ctx.stats.max_buffer_usage = current_usage;
        }
    }

    while (!ctx.ring_buffer->empty()) {
        PacketBuffer packet;
        if (ctx.ring_buffer->pop(packet)) {
            uint32_t pkt_size = static_cast<uint32_t>(packet.data.size());
            output_file.write(reinterpret_cast<const char*>(&pkt_size), sizeof(pkt_size));
            output_file.write(
                reinterpret_cast<const char*>(packet.data.data()), packet.data.size());
            writer_stats.packets_written++;
            writer_stats.bytes_written += sizeof(pkt_size) + packet.data.size();
        }
    }

    output_file.close();
    writer_stats.end_time = std::chrono::steady_clock::now();
}

// Unified Capture Stream Function
template <typename samp_type>
void capture_stream_unified(StreamContext& ctx,
    std::atomic<bool>& start_capture,
    std::atomic<bool>* stop_writing,
    size_t num_packets,
    FileWriterStats* writer_stats = nullptr)
{
    uhd::set_thread_priority_safe(1.0, true);

    while (!start_capture.load()) {
        std::this_thread::sleep_for(1ms);
    }

    ctx.stats.start_time = std::chrono::steady_clock::now();

    std::vector<samp_type> buff(ctx.samps_per_buff);
    std::vector<void*> buff_ptrs = {&buff.front()};
    uhd::rx_metadata_t md;

    uhd::stream_cmd_t stream_cmd(num_packets == 0
                                     ? uhd::stream_cmd_t::STREAM_MODE_START_CONTINUOUS
                                     : uhd::stream_cmd_t::STREAM_MODE_NUM_SAMPS_AND_DONE);

    if (num_packets > 0) {
        stream_cmd.num_samps = num_packets * ctx.samps_per_buff;
    }

    stream_cmd.stream_now = true;
    ctx.rx_streamer->issue_stream_cmd(stream_cmd);

    std::cout << "[Stream " << ctx.stream_id << "] Started capture from " << ctx.block_id
              << ":" << ctx.port << std::endl;

    size_t consecutive_timeouts           = 0;
    const size_t max_consecutive_timeouts = 5;

    while (!stop_signal_called.load()
           && (num_packets == 0 || ctx.stats.packets_captured < num_packets)) {
        size_t num_rx_samps =
            ctx.rx_streamer->recv(buff_ptrs, ctx.samps_per_buff, md, 3.0);

        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_TIMEOUT) {
            if (++consecutive_timeouts >= max_consecutive_timeouts) {
                std::cout << "[Stream " << ctx.stream_id
                          << "] Multiple timeouts, stopping" << std::endl;
                break;
            }
            continue;
        } else {
            consecutive_timeouts = 0;
        }

        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
            ctx.stats.overflow_count++;
            continue;
        }

        if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
            std::cerr << "[Stream " << ctx.stream_id << "] Error: " << md.strerror()
                      << std::endl;
            ctx.stats.error_count++;
            break;
        }

        if (num_rx_samps == 0)
            continue;

        // Build CHDR packet
        PacketBuffer packet_buffer;
        packet_buffer.stream_id     = ctx.stream_id;
        packet_buffer.packet_number = ctx.stats.packets_captured;

        size_t payload_bytes    = num_rx_samps * sizeof(samp_type);
        size_t header_bytes     = 8;
        size_t timestamp_bytes  = md.has_time_spec ? 8 : 0;
        size_t total_chdr_bytes = header_bytes + timestamp_bytes + payload_bytes;

        packet_buffer.data.reserve(total_chdr_bytes);

        // Build and write header
        uint64_t header = 0;
        header |= (uint64_t)ctx.stream_id & 0xFFFF;
        header |= ((uint64_t)total_chdr_bytes & 0xFFFF) << 16;
        header |= ((uint64_t)ctx.stats.packets_captured & 0xFFFF) << 32;
        header |= ((uint64_t)0 & 0x1F) << 48;
        header |=
            ((uint64_t)(md.has_time_spec ? PKT_TYPE_DATA_WITH_TS : PKT_TYPE_DATA_NO_TS)
                & 0x7)
            << 53;
        header |= ((uint64_t)(md.end_of_burst ? 1 : 0) & 0x1) << 57;

        write_le(packet_buffer.data, header);

        // Write timestamp if present
        if (md.has_time_spec) {
            uint64_t timestamp_ticks = md.time_spec.to_ticks(ctx.tick_rate);
            write_le(packet_buffer.data, timestamp_ticks);
            packet_buffer.has_timestamp = true;
            packet_buffer.timestamp     = md.time_spec;

            double timestamp_sec = md.time_spec.get_real_secs();
            if (ctx.stats.first_timestamp == 0.0) {
                ctx.stats.first_timestamp = timestamp_sec;
            }
            ctx.stats.last_timestamp = timestamp_sec;
        }

        // Write payload
        const uint8_t* sample_bytes = reinterpret_cast<const uint8_t*>(buff.data());
        packet_buffer.data.insert(
            packet_buffer.data.end(), sample_bytes, sample_bytes + payload_bytes);

        // Handle ring buffer or direct file write
        if (ctx.ring_buffer) {
            if (!ctx.ring_buffer->push(std::move(packet_buffer))) {
                ctx.stats.buffer_overflows++;
            }
        } else if (ctx.output_file) {
            if (ctx.file_mutex) {
                std::lock_guard<std::mutex> lock(*ctx.file_mutex);
            }
            uint32_t pkt_size = static_cast<uint32_t>(packet_buffer.data.size());
            ctx.output_file->write(
                reinterpret_cast<const char*>(&pkt_size), sizeof(pkt_size));
            ctx.output_file->write(
                reinterpret_cast<const char*>(packet_buffer.data.data()),
                packet_buffer.data.size());
            ctx.stats.total_bytes_written += sizeof(pkt_size) + packet_buffer.data.size();
        }

        // Store for analysis if needed
        if (ctx.analysis_packets && ctx.analysis_packets->size() < MAX_ANALYSIS_PACKETS) {
            chdr_packet_data pkt;
            pkt.stream_id    = ctx.stream_id;
            pkt.stream_block = ctx.block_id;
            pkt.stream_port  = ctx.port;
            pkt.header_raw   = header;
            pkt.parse_header();
            if (md.has_time_spec) {
                pkt.timestamp = md.time_spec.to_ticks(ctx.tick_rate);
            }
            pkt.payload.assign(sample_bytes, sample_bytes + payload_bytes);

            std::lock_guard<std::mutex> lock(*ctx.analysis_mutex);
            ctx.analysis_packets->push_back(pkt);
        }

        ctx.stats.packets_captured++;
        ctx.stats.total_samples += num_rx_samps;

        if (ctx.stats.packets_captured % 1000 == 0) {
            std::cout << "[Stream " << ctx.stream_id
                      << "] Packets: " << ctx.stats.packets_captured;
            if (ctx.stats.overflow_count > 0) {
                std::cout << " (O: " << ctx.stats.overflow_count << ")";
            }
            std::cout << std::endl;
        }
    }

    stream_cmd.stream_mode = uhd::stream_cmd_t::STREAM_MODE_STOP_CONTINUOUS;
    ctx.rx_streamer->issue_stream_cmd(stream_cmd);

    ctx.stats.end_time = std::chrono::steady_clock::now();

    if (stop_writing) {
        stop_writing->store(true);
    }
}

// PPS Reset Function
uhd::time_spec_t perform_pps_reset(
    uhd::rfnoc::rfnoc_graph::sptr graph, const PpsResetConfig& config)
{
    if (!config.enable_pps_reset) {
        return uhd::time_spec_t(0.0);
    }

    std::cout << "\n=== PPS Reset Sequence ===" << std::endl;

    try {
        auto mb_controller = graph->get_mb_controller(0);
        auto timekeeper    = mb_controller->get_timekeeper(0);

        uhd::time_spec_t time_before = timekeeper->get_time_now();
        std::cout << "Device time before PPS reset: " << time_before.get_real_secs()
                  << " seconds" << std::endl;

        timekeeper->set_ticks_next_pps(0);

        std::cout << "Waiting " << config.wait_time_sec << " seconds for PPS..."
                  << std::endl;
        std::this_thread::sleep_for(std::chrono::duration<double>(config.wait_time_sec));

        uhd::time_spec_t current_time = timekeeper->get_time_now();
        std::cout << "Device time after PPS reset: " << current_time.get_real_secs()
                  << " seconds" << std::endl;

        if (config.verify_reset
            && current_time.get_real_secs() > config.max_time_after_reset) {
            std::cerr << "Warning: PPS reset may have failed" << std::endl;
        }

        return uhd::time_spec_t(0.0);
    } catch (const std::exception& e) {
        std::cerr << "Error during PPS reset: " << e.what() << std::endl;
        return uhd::time_spec_t(0.0);
    }
}

// Configuration Loading
GraphConfig load_graph_config(const std::string& yaml_file)
{
    GraphConfig config;

    try {
        YAML::Node root = YAML::LoadFile(yaml_file);

        // Load all configuration sections
        auto load_section = [&root](const std::string& name, auto& target, auto loader) {
            if (root[name]) {
                loader(root[name], target);
            }
        };

        // PPS reset
        load_section(
            "pps_reset", config.pps_reset, [](const YAML::Node& n, PpsResetConfig& c) {
                c.enable_pps_reset     = n["enable"].as<bool>(false);
                c.wait_time_sec        = n["wait_time_sec"].as<double>(1.5);
                c.verify_reset         = n["verify_reset"].as<bool>(true);
                c.max_time_after_reset = n["max_time_after_reset"].as<double>(1.0);
            });

        // Multi-stream
        load_section("multi_stream",
            config.multi_stream,
            [](const YAML::Node& n, MultiStreamConfig& c) {
                c.enable_multi_stream = n["enable"].as<bool>(false);
                c.sync_streams        = n["sync_streams"].as<bool>(true);
                c.separate_files      = n["separate_files"].as<bool>(false);
                c.file_prefix         = n["file_prefix"].as<std::string>("stream");
                c.max_streams         = n["max_streams"].as<size_t>(0);
                c.sync_delay          = n["sync_delay"].as<double>(0.1);
                if (n["stream_blocks"]) {
                    for (const auto& block : n["stream_blocks"]) {
                        c.stream_blocks.push_back(block.as<std::string>());
                    }
                }
            });

        // Connections
        if (root["connections"]) {
            for (const auto& conn : root["connections"]) {
                ConnectionConfig cc;
                cc.src_block    = conn["src_block"].as<std::string>();
                cc.src_port     = conn["src_port"].as<size_t>(0);
                cc.dst_block    = conn["dst_block"].as<std::string>();
                cc.dst_port     = conn["dst_port"].as<size_t>(0);
                cc.is_back_edge = conn["is_back_edge"].as<bool>(false);
                config.dynamic_connections.push_back(cc);
            }
        }

        // Signal paths
        if (root["signal_paths"]) {
            for (const auto& path : root["signal_paths"]) {
                SignalPathConfig spc;
                spc.name = path["name"].as<std::string>("");
                if (path["connections"]) {
                    for (const auto& conn : path["connections"]) {
                        ConnectionConfig cc;
                        std::string src = conn["src"].as<std::string>();
                        std::string dst = conn["dst"].as<std::string>();

                        auto parse_block_port =
                            [](const std::string& s, std::string& block, size_t& port) {
                                size_t colon = s.find(':');
                                if (colon != std::string::npos) {
                                    block = s.substr(0, colon);
                                    port  = std::stoul(s.substr(colon + 1));
                                } else {
                                    block = s;
                                    port  = 0;
                                }
                            };

                        parse_block_port(src, cc.src_block, cc.src_port);
                        parse_block_port(dst, cc.dst_block, cc.dst_port);
                        cc.is_back_edge = conn["is_back_edge"].as<bool>(false);
                        spc.connections.push_back(cc);
                    }
                }
                config.signal_paths.push_back(spc);
            }
        }

        // Switchboards
        if (root["switchboards"]) {
            for (const auto& sb : root["switchboards"]) {
                SwitchboardConfig sbc;
                sbc.block_id = sb["block_id"].as<std::string>();
                if (sb["connections"]) {
                    for (const auto& conn : sb["connections"]) {
                        sbc.connections[conn["input"].as<size_t>()] =
                            conn["output"].as<size_t>();
                    }
                }
                config.switchboard_configs.push_back(sbc);
            }
        }

        // Stream endpoints
        if (root["stream_endpoints"]) {
            for (const auto& sep : root["stream_endpoints"]) {
                StreamEndpointConfig sec;
                sec.block_id    = sep["block_id"].as<std::string>();
                sec.port        = sep["port"].as<size_t>(0);
                sec.direction   = sep["direction"].as<std::string>("rx");
                sec.enabled     = sep["enabled"].as<bool>(true);
                sec.stream_name = sep["name"].as<std::string>("");
                if (sep["stream_args"]) {
                    for (const auto& arg : sep["stream_args"]) {
                        sec.stream_args[arg.first.as<std::string>()] =
                            arg.second.as<std::string>();
                    }
                }
                config.stream_endpoints.push_back(sec);
            }
        }

        // Block properties
        if (root["block_properties"]) {
            for (const auto& block : root["block_properties"]) {
                std::string block_id = block.first.as<std::string>();
                for (const auto& prop : block.second) {
                    config.block_properties[block_id][prop.first.as<std::string>()] =
                        prop.second.as<std::string>();
                }
            }
        }

        // Auto-connect settings
        if (root["auto_connect"]) {
            config.auto_connect_radio_to_ddc =
                root["auto_connect"]["radio_to_ddc"].as<bool>(false);
            config.auto_find_stream_endpoint =
                root["auto_connect"]["find_stream_endpoint"].as<bool>(false);
        }

        // Advanced options
        if (root["advanced"]) {
            config.commit_after_each_connection =
                root["advanced"]["commit_after_each_connection"].as<bool>(false);
            config.discover_static_connections =
                root["advanced"]["discover_static_connections"].as<bool>(true);
            config.preserve_static_routes =
                root["advanced"]["preserve_static_routes"].as<bool>(true);
            if (root["advanced"]["block_init_order"]) {
                for (const auto& block : root["advanced"]["block_init_order"]) {
                    config.block_init_order.push_back(block.as<std::string>());
                }
            }
        }

    } catch (const std::exception& e) {
        std::cerr << "Error loading YAML config: " << e.what() << std::endl;
    }

    return config;
}


bool configure_switchboards(
    uhd::rfnoc::rfnoc_graph::sptr graph, const std::vector<SwitchboardConfig>& configs)
{
    for (const auto& config : configs) {
        try {
            uhd::rfnoc::block_id_t block_id(config.block_id);
            auto switchboard =
                graph->get_block<uhd::rfnoc::switchboard_block_control>(block_id);
            if (!switchboard)
                continue;

            std::cout << "Configuring switchboard " << config.block_id << std::endl;
            for (const auto& [input, output] : config.connections) {
                switchboard->connect(input, output);
                std::cout << "  Connected input " << input << " to output " << output
                          << std::endl;
            }
        } catch (const uhd::exception& e) {
            std::cerr << "Failed to configure switchboard: " << e.what() << std::endl;
            return false;
        }
    }
    return true;
}

bool apply_dynamic_connections(uhd::rfnoc::rfnoc_graph::sptr graph,
    const std::vector<ConnectionConfig>& connections,
    bool commit_after_each)
{
    for (const auto& conn : connections) {
        try {
            std::cout << "Connecting " << conn.src_block << ":" << conn.src_port << " -> "
                      << conn.dst_block << ":" << conn.dst_port;
            if (conn.is_back_edge)
                std::cout << " (back edge)";
            std::cout << std::endl;

            graph->connect(conn.src_block,
                conn.src_port,
                conn.dst_block,
                conn.dst_port,
                conn.is_back_edge);

            if (commit_after_each) {
                graph->commit();
            }
        } catch (const uhd::exception& e) {
            std::cerr << "Failed to connect: " << e.what() << std::endl;
            return false;
        }
    }
    return true;
}

bool apply_signal_paths(uhd::rfnoc::rfnoc_graph::sptr graph,
    const std::vector<SignalPathConfig>& signal_paths)
{
    std::cout << "\nApplying signal paths from configuration..." << std::endl;

    for (const auto& path : signal_paths) {
        if (!path.name.empty()) {
            std::cout << "  Establishing path: " << path.name << std::endl;
        }

        for (const auto& conn : path.connections) {
            auto edges  = graph->enumerate_active_connections();
            bool exists = false;
            for (const auto& edge : edges) {
                if (edge.src_blockid == conn.src_block && edge.src_port == conn.src_port
                    && edge.dst_blockid == conn.dst_block
                    && edge.dst_port == conn.dst_port) {
                    exists = true;
                    break;
                }
            }

            if (!exists) {
                try {
                    std::cout << "    Connecting: " << conn.src_block << ":"
                              << conn.src_port << " -> " << conn.dst_block << ":"
                              << conn.dst_port;
                    if (conn.is_back_edge)
                        std::cout << " (back edge)";
                    std::cout << std::endl;

                    graph->connect(conn.src_block,
                        conn.src_port,
                        conn.dst_block,
                        conn.dst_port,
                        conn.is_back_edge);
                } catch (const std::exception& e) {
                    std::cerr << "      Failed: " << e.what() << std::endl;
                    return false;
                }
            }
        }
    }
    return true;
}

bool auto_connect_radio_to_ddc(uhd::rfnoc::rfnoc_graph::sptr graph)
{
    using uhd::rfnoc::block_id_t;
    bool made_connection = false;

    const auto radio_blocks = graph->find_blocks("Radio");
    const auto ddc_blocks   = graph->find_blocks("DDC");

    if (radio_blocks.empty() || ddc_blocks.empty()) {
        return false;
    }

    const std::vector<std::string> optional_chain = {"Switchboard",
        "FIR",
        "Window",
        "FFT",
        "MovingAverage",
        "VectorIIR",
        "KeepOneInN"};

    for (const auto& radio_id : radio_blocks) {
        const size_t dev  = radio_id.get_device_no();
        const size_t chan = radio_id.get_block_count();

        block_id_t ddc_match;
        for (const auto& ddc_id : ddc_blocks) {
            if (ddc_id.get_device_no() == dev && ddc_id.get_block_count() == chan) {
                ddc_match = ddc_id;
                break;
            }
        }

        if (!graph->has_block(ddc_match))
            continue;

        block_id_t prev_blk = radio_id;
        size_t prev_p       = 0;

        for (const auto& blk_name : optional_chain) {
            block_id_t candidate(dev, blk_name, chan);
            if (!graph->has_block(candidate))
                continue;

            auto edges       = graph->enumerate_active_connections();
            bool edge_exists = false;
            for (const auto& e : edges) {
                if (e.src_blockid == prev_blk.to_string() && e.src_port == prev_p
                    && e.dst_blockid == candidate.to_string() && e.dst_port == 0) {
                    edge_exists = true;
                    break;
                }
            }

            if (edge_exists) {
                prev_blk = candidate;
                continue;
            }

            if (!graph->is_connectable(prev_blk, prev_p, candidate, 0))
                break;

            graph->connect(prev_blk, prev_p, candidate, 0);
            prev_blk        = candidate;
            made_connection = true;
        }

        auto edges             = graph->enumerate_active_connections();
        bool final_edge_exists = false;
        for (const auto& e : edges) {
            if (e.src_blockid == prev_blk.to_string() && e.src_port == prev_p
                && e.dst_blockid == ddc_match.to_string() && e.dst_port == 0) {
                final_edge_exists = true;
                break;
            }
        }

        if (!final_edge_exists && graph->is_connectable(prev_blk, prev_p, ddc_match, 0)) {
            graph->connect(prev_blk, prev_p, ddc_match, 0);
            made_connection = true;
        }
    }

    return made_connection;
}

std::vector<std::pair<std::string, size_t>> find_all_stream_endpoints_enhanced(
    uhd::rfnoc::rfnoc_graph::sptr graph,
    const std::vector<StreamEndpointConfig>& configured_endpoints,
    const MultiStreamConfig& multi_config)
{
    std::vector<std::pair<std::string, size_t>> endpoints;

    // Add configured endpoints
    for (const auto& ep : configured_endpoints) {
        if (ep.enabled && ep.direction == "rx") {
            try {
                uhd::rfnoc::block_id_t block_id(ep.block_id);
                auto block = graph->get_block(block_id);
                if (block && ep.port < block->get_num_output_ports()) {
                    endpoints.push_back({ep.block_id, ep.port});
                }
            } catch (...) {
            }
        }
    }

    // Add stream blocks from config
    if (!multi_config.stream_blocks.empty()) {
        for (const auto& block_id : multi_config.stream_blocks) {
            try {
                uhd::rfnoc::block_id_t id(block_id);
                auto block = graph->get_block(id);
                if (!block)
                    continue;

                if (id.get_block_name() == "DDC") {
                    for (size_t port = 0; port < block->get_num_output_ports(); ++port) {
                        bool already_added = false;
                        for (const auto& [bid, p] : endpoints) {
                            if (bid == block_id && p == port) {
                                already_added = true;
                                break;
                            }
                        }

                        if (!already_added) {
                            auto edges          = graph->enumerate_active_connections();
                            bool has_downstream = false;
                            for (const auto& edge : edges) {
                                if (edge.src_blockid == block_id
                                    && edge.src_port == port) {
                                    if (edge.dst_blockid.find("SEP") == std::string::npos
                                        && edge.dst_blockid.find("NullSrcSink")
                                               == std::string::npos) {
                                        has_downstream = true;
                                    }
                                    break;
                                }
                            }

                            if (!has_downstream) {
                                endpoints.push_back({block_id, port});
                            }
                        }
                    }
                } else {
                    bool already_added = false;
                    for (const auto& [bid, port] : endpoints) {
                        if (bid == block_id) {
                            already_added = true;
                            break;
                        }
                    }
                    if (!already_added) {
                        endpoints.push_back({block_id, 0});
                    }
                }
            } catch (...) {
            }
        }
    }

    // Auto-discover if needed
    if (multi_config.enable_multi_stream
        && (endpoints.empty()
            || (multi_config.max_streams > 0
                && endpoints.size() < multi_config.max_streams))) {
        auto blocks                             = discover_blocks_enhanced(graph);
        std::vector<std::string> priority_order = {
            "DDC", "Radio", "Replay", "DmaFIFO", "SigGen", "DUC"};

        for (const auto& block_type : priority_order) {
            for (const auto& block : blocks) {
                if (block.block_type == block_type && block.has_stream_endpoint) {
                    if (block_type == "DDC") {
                        for (size_t port = 0; port < block.num_output_ports; ++port) {
                            bool already_added = false;
                            for (const auto& [bid, p] : endpoints) {
                                if (bid == block.block_id && p == port) {
                                    already_added = true;
                                    break;
                                }
                            }

                            if (!already_added) {
                                endpoints.push_back({block.block_id, port});
                                if (multi_config.max_streams > 0
                                    && endpoints.size() >= multi_config.max_streams) {
                                    goto done_discovering;
                                }
                            }
                        }
                    } else {
                        bool already_added = false;
                        for (const auto& [bid, port] : endpoints) {
                            if (bid == block.block_id) {
                                already_added = true;
                                break;
                            }
                        }

                        if (!already_added) {
                            endpoints.push_back({block.block_id, 0});
                            if (multi_config.max_streams > 0
                                && endpoints.size() >= multi_config.max_streams) {
                                goto done_discovering;
                            }
                        }
                    }
                }
            }
        }
    }

done_discovering:
    if (multi_config.max_streams > 0 && endpoints.size() > multi_config.max_streams) {
        endpoints.resize(multi_config.max_streams);
    }

    return endpoints;
}

void print_graph_info(const uhd::rfnoc::rfnoc_graph::sptr& graph)
{
    std::cout << "\n=== RFNoC Graph Information ===" << std::endl;

    auto blocks = discover_blocks_enhanced(graph);
    std::cout << "\nRFNoC blocks:" << std::endl;
    for (const auto& block : blocks) {
        std::cout << "  * " << block.block_id << " (Type: " << block.block_type
                  << ", In: " << block.num_input_ports
                  << ", Out: " << block.num_output_ports;
        if (block.has_stream_endpoint)
            std::cout << ", SEP";
        std::cout << ")" << std::endl;
    }

    std::cout << "\nStatic connections:" << std::endl;
    for (const auto& edge : graph->enumerate_static_connections()) {
        std::cout << "  * " << edge.to_string() << std::endl;
    }

    std::cout << "\nActive connections:" << std::endl;
    for (const auto& edge : graph->enumerate_active_connections()) {
        std::cout << "  * " << edge.to_string() << std::endl;
    }
}

// Unified Analysis Function
void analyze_packets_unified(const std::vector<chdr_packet_data>& packets,
    const std::string& csv_file,
    double tick_rate,
    const std::vector<StreamStats>& stream_stats,
    uhd::time_spec_t pps_reset_time = uhd::time_spec_t(0.0),
    bool pps_reset_used             = false,
    size_t samps_per_buff           = 0,
    double rate                     = 0)
{
    std::ofstream csv(csv_file);
    if (!csv.is_open()) {
        throw std::runtime_error("Failed to open CSV file: " + csv_file);
    }

    // Write CSV header with PPS reset info
    csv << "packet_num,         stream_id,          stream_block,   stream_port,        "
           "vc,"
        << "eob,                eov,                pkt_type,       pkt_type_str,       "
           "num_mdata,"
        << "seq_num,            length,             dst_epid,       has_timestamp,      "
           "timestamp_ticks,"
        << "timestamp_sec,      time_since_pps_sec, payload_size,   num_samples,        "
           "first_4_bytes_hex"
        << std::endl;

    // Calculate first packet offset for PPS alignment (if needed)
    uint64_t first_pkt_offset    = 0;
    uint64_t first_pkt_sec_ticks = 0;
    std::cout << "Here's the unmodified tick values for packet 0: "
              << packets[0].timestamp << "\n\n"
              << std::endl;
    if (pps_reset_used && !packets.empty() && packets[0].has_timestamp
        && packets[0].timestamp > DEFAULT_TICKRATE) {
        first_pkt_offset    = ((packets[0].timestamp) % DEFAULT_TICKRATE);
        first_pkt_sec_ticks = packets[0].timestamp / DEFAULT_TICKRATE;
    }

    double samps_per_sec_num = samps_per_buff * (DEFAULT_TICKRATE / rate);

    // Analyze each packet
    for (size_t i = 0; i < packets.size(); ++i) {
        const auto& pkt = packets[i];
        if ((pkt.timestamp % DEFAULT_TICKRATE) < ((
                samps_per_sec_num
                * ((static_cast<double>(pkt.timestamp - first_pkt_offset)) / tick_rate)))
            && ((pkt.timestamp % DEFAULT_TICKRATE)
                < ((packets[i == 0 ? 0 : (i - 1)]).timestamp) % DEFAULT_TICKRATE)) {
            std::cout << "first_packet_offset rollover detected" << std::endl;
            first_pkt_offset = ((pkt.timestamp - DEFAULT_TICKRATE) % DEFAULT_TICKRATE);
        }
        csv << i << "," << pkt.stream_id << ","
            << "\"" << pkt.stream_block << "\"," << pkt.stream_port << "," << std::hex
            << "0x" << std::setw(2) << std::setfill('0') << (int)pkt.vc << "," << std::dec
            << (pkt.eob ? "1" : "0") << "," << (pkt.eov ? "1" : "0") << "," << std::hex
            << "0x" << (int)pkt.pkt_type << "," << pkt.pkt_type_str() << "," << std::dec
            << (int)pkt.num_mdata << "," << pkt.seq_num << "," << pkt.length << ","
            << std::hex << "0x" << std::setw(4) << std::setfill('0') << pkt.dst_epid
            << "," << std::dec << (pkt.has_timestamp ? "1" : "0") << ",";
        uint64_t temp_timestamp = 0;
        if (pkt.has_timestamp) {
            // Adjust timestamp relative to PPS reset if used
            uint64_t adjusted_timestamp = pkt.timestamp;
            if (pps_reset_used) {
                // Remove the initial offset to align to PPS edge
                adjusted_timestamp =
                    (pkt.timestamp - first_pkt_offset) % static_cast<uint64_t>(tick_rate);
            }
            uint64_t timestamp_sec = 0;
            double time_since_pps  = 0;
            if ((pkt.timestamp / DEFAULT_TICKRATE) < 2) {
                temp_timestamp = (pkt.timestamp % DEFAULT_TICKRATE /*) - first_pkt_offset +  ( first_pkt_offset % (samps_per_buff*(DEFAULT_TICKRATE/static_cast<uint64_t>(rate)))*/ );
                timestamp_sec = ((static_cast<double>(pkt.timestamp)) / tick_rate);
                // Calculate time since PPS reset
                time_since_pps = (static_cast<double>(pkt.timestamp /*+  ( first_pkt_offset % (samps_per_buff*(DEFAULT_TICKRATE/static_cast<uint64_t>(rate))) )*/ ) / tick_rate) -  timestamp_sec;
            } else {
                temp_timestamp =
                    (pkt.timestamp % DEFAULT_TICKRATE) - first_pkt_offset
                    + (first_pkt_offset
                        % (samps_per_buff
                            * (DEFAULT_TICKRATE / static_cast<uint64_t>(rate))));
                timestamp_sec =
                    ((static_cast<double>(pkt.timestamp - first_pkt_offset)) / tick_rate);
                // Calculate time since PPS reset
                time_since_pps =
                    (static_cast<double>(
                         pkt.timestamp - first_pkt_offset
                         + (first_pkt_offset
                             % (samps_per_buff
                                 * (DEFAULT_TICKRATE / static_cast<uint64_t>(rate)))))
                        / tick_rate)
                    - timestamp_sec;
            }
            csv << std::dec << temp_timestamp << "," << std::fixed
                << std::setprecision(12) << timestamp_sec << "," << std::setprecision(12)
                << time_since_pps;
        } else {
            csv << "N/A,N/A,N/A";
        }

        size_t num_samples = pkt.payload.size() / 4;
        csv << "," << std::dec << pkt.payload.size() << "," << num_samples << ",0x";

        for (size_t j = 0; j < std::min((size_t)4, pkt.payload.size()); j++) {
            csv << std::hex << std::setw(2) << std::setfill('0')
                << (unsigned int)pkt.payload[j];
        }

        csv << std::endl;
    }

    csv.close();

    // Write statistics
    std::string stats_file =
        csv_file.substr(0, csv_file.find_last_of('.')) + "_stats.csv";
    std::ofstream stats_csv(stats_file);
    if (stats_csv.is_open()) {
        stats_csv << "stream_id,block_id,port,packets_captured,total_samples,"
                  << "overflow_count,error_count,duration_sec,avg_sample_rate_msps,"
                  << "first_timestamp,last_timestamp";
        if (pps_reset_used) {
            stats_csv << ",pps_reset_used,pps_reset_time";
        }
        stats_csv << std::endl;

        for (const auto& stat : stream_stats) {
            double duration =
                std::chrono::duration<double>(stat.end_time - stat.start_time).count();
            double avg_rate = (stat.total_samples / duration) / 1e6;

            stats_csv << stat.stream_id << ","
                      << "\"" << stat.block_id << "\"," << stat.port << ","
                      << stat.packets_captured << "," << stat.total_samples << ","
                      << stat.overflow_count << "," << stat.error_count << ","
                      << std::fixed << std::setprecision(6) << duration << "," << avg_rate
                      << "," << std::setprecision(12) << stat.first_timestamp << ","
                      << stat.last_timestamp;

            if (pps_reset_used) {
                stats_csv << "," << (pps_reset_used ? "1" : "0") << ","
                          << pps_reset_time.get_real_secs();
            }
            stats_csv << std::endl;
        }
        stats_csv.close();
    }

    std::cout << "Analysis complete. Results written to: " << csv_file << std::endl;
}

// Wrapper functions for backward compatibility
void analyze_packets(const std::vector<chdr_packet_data>& packets,
    const std::string& csv_file,
    double tick_rate,
    const std::vector<StreamStats>& stream_stats)
{
    analyze_packets_unified(packets, csv_file, tick_rate, stream_stats);
}

void analyze_packets_with_pps_reset(const std::vector<chdr_packet_data>& packets,
    const std::string& csv_file,
    double tick_rate,
    const std::vector<StreamStats>& stream_stats,
    uhd::time_spec_t pps_reset_time,
    bool pps_reset_used,
    size_t samps_per_buff,
    double rate)
{
    analyze_packets_unified(packets,
        csv_file,
        tick_rate,
        stream_stats,
        pps_reset_time,
        pps_reset_used,
        samps_per_buff,
        rate);
}

// Unified Multi-Stream Capture Function
template <typename samp_type>
void capture_multi_stream_unified(uhd::rfnoc::rfnoc_graph::sptr graph,
    const GraphConfig& config,
    const std::string& file,
    size_t num_packets,
    bool enable_analysis,
    const std::string& csv_file,
    double rate,
    size_t samps_per_buff,
    uhd::time_spec_t pps_reset_time,
    bool pps_reset_used,
    bool use_ring_buffer)
{
    print_graph_info(graph);

    // Initialize blocks
    for (const auto& block_id : config.block_init_order) {
        try {
            auto block = graph->get_block(uhd::rfnoc::block_id_t(block_id));
            if (block)
                std::cout << "  Initialized: " << block_id << std::endl;
        } catch (...) {
        }
    }

    // Configure graph
    if (!config.switchboard_configs.empty()) {
        configure_switchboards(graph, config.switchboard_configs);
    }

    if (!config.dynamic_connections.empty()) {
        apply_dynamic_connections(
            graph, config.dynamic_connections, config.commit_after_each_connection);
    }

    if (!config.signal_paths.empty()) {
        apply_signal_paths(graph, config.signal_paths);
    }

    if (config.auto_connect_radio_to_ddc) {
        auto_connect_radio_to_ddc(graph);
    }

    // Find endpoints
    auto endpoints = find_all_stream_endpoints_enhanced(
        graph, config.stream_endpoints, config.multi_stream);

    if (endpoints.empty()) {
        throw std::runtime_error("No suitable streaming endpoints found");
    }

    std::cout << "\nFound " << endpoints.size() << " streaming endpoints" << std::endl;

    // Get tick rate
    double tick_rate  = DEFAULT_TICKRATE;
    auto radio_blocks = graph->find_blocks("Radio");
    if (!radio_blocks.empty()) {
        auto radio = graph->get_block<uhd::rfnoc::radio_control>(radio_blocks[0]);
        tick_rate  = radio->get_tick_rate();
    }

    // Create contexts and files
    std::vector<StreamContext> contexts;
    std::vector<std::unique_ptr<std::ofstream>> output_files;
    std::unique_ptr<std::mutex> shared_file_mutex;
    std::vector<chdr_packet_data> all_analysis_packets;
    std::mutex analysis_mutex;
    std::vector<std::atomic<bool>> stop_writing_flags(endpoints.size());
    std::vector<FileWriterStats> writer_stats(endpoints.size());

    // Create output files
    if (config.multi_stream.separate_files || use_ring_buffer) {
        for (size_t i = 0; i < endpoints.size(); ++i) {
            std::string fname =
                config.multi_stream.file_prefix + "_" + std::to_string(i) + ".dat";
            if (!use_ring_buffer) {
                output_files.emplace_back(
                    std::make_unique<std::ofstream>(fname, std::ios::binary));
                if (!output_files.back()->is_open()) {
                    throw std::runtime_error("Failed to open output file: " + fname);
                }
                write_file_header(
                    *output_files.back(), tick_rate, pps_reset_used, pps_reset_time, 1);
                write_stream_header(
                    *output_files.back(), i, endpoints[i].first, endpoints[i].second);
            }
        }
    } else {
        output_files.emplace_back(
            std::make_unique<std::ofstream>(file, std::ios::binary));
        if (!output_files.back()->is_open()) {
            throw std::runtime_error("Failed to open output file: " + file);
        }
        shared_file_mutex = std::make_unique<std::mutex>();
        write_file_header(*output_files.back(),
            tick_rate,
            pps_reset_used,
            pps_reset_time,
            endpoints.size());
        for (size_t i = 0; i < endpoints.size(); ++i) {
            write_stream_header(
                *output_files.back(), i, endpoints[i].first, endpoints[i].second);
        }
    }

    // Create streamers and contexts
    std::vector<uhd::rfnoc::ddc_block_control::sptr> ddc_controls;
    std::vector<size_t> ddc_channels;

    for (size_t i = 0; i < endpoints.size(); ++i) {
        const auto& [block_id, port] = endpoints[i];

        try {
            uhd::rfnoc::block_id_t endpoint_id(block_id);
            auto endpoint_block = graph->get_block(endpoint_id);

            if (!endpoint_block || port >= endpoint_block->get_num_output_ports()) {
                continue;
            }

            uhd::stream_args_t stream_args("sc16", "sc16");
            stream_args.channels = {0};

            for (const auto& sep : config.stream_endpoints) {
                if (sep.block_id == block_id && sep.port == port) {
                    for (const auto& [key, value] : sep.stream_args) {
                        stream_args.args[key] = value;
                    }
                    break;
                }
            }

            auto rx_streamer = graph->create_rx_streamer(1, stream_args);
            graph->connect(block_id, port, rx_streamer, 0, true);

            if (endpoint_id.get_block_name() == "DDC") {
                auto ddc_ctrl =
                    graph->get_block<uhd::rfnoc::ddc_block_control>(endpoint_id);
                if (ddc_ctrl) {
                    ddc_controls.push_back(ddc_ctrl);
                    ddc_channels.push_back(port);
                }
            }

            StreamContext ctx;
            ctx.stream_id        = i;
            ctx.block_id         = block_id;
            ctx.port             = port;
            ctx.rx_streamer      = rx_streamer;
            ctx.stats.stream_id  = i;
            ctx.stats.block_id   = block_id;
            ctx.stats.port       = port;
            ctx.analysis_packets = enable_analysis ? &all_analysis_packets : nullptr;
            ctx.analysis_mutex   = &analysis_mutex;
            ctx.tick_rate        = tick_rate;
            ctx.samps_per_buff   = samps_per_buff;
            ctx.pps_reset_time   = pps_reset_time;
            ctx.pps_reset_used   = pps_reset_used;
            ctx.buffer_config    = config.multi_stream.buffer_config;

            if (use_ring_buffer) {
                const size_t bytes_per_samp    = sizeof(samp_type);
                const size_t est_payload_bytes = samps_per_buff * bytes_per_samp;
                const size_t est_pkt_bytes     = est_payload_bytes + 16;
                size_t est_pkts                = std::max<size_t>(1,
                    config.multi_stream.buffer_config.ring_buffer_size / est_pkt_bytes);
                size_t power_of_2              = 1;
                while (power_of_2 < est_pkts)
                    power_of_2 <<= 1;
                if (power_of_2 < 2)
                    power_of_2 = 2;
                ctx.ring_buffer =
                    std::make_shared<SPSCRingBuffer<PacketBuffer>>(power_of_2);
                ctx.output_filename =
                    config.multi_stream.file_prefix + "_" + std::to_string(i) + ".dat";
            } else {
                ctx.output_file = config.multi_stream.separate_files
                                      ? output_files[i].get()
                                      : output_files[0].get();
                ctx.file_mutex  = config.multi_stream.separate_files
                                      ? nullptr
                                      : shared_file_mutex.get();
            }

            contexts.push_back(std::move(ctx));

        } catch (const std::exception& e) {
            std::cerr << "Failed to setup stream " << i << ": " << e.what() << std::endl;
        }
    }

    if (use_ring_buffer) {
        g_per_stream_stop_flags.clear();
        g_per_stream_stop_flags.reserve(contexts.size());
        for (size_t i = 0; i < contexts.size(); ++i) {
            g_per_stream_stop_flags.push_back(&stop_writing_flags[i]);
        }
    }

    // Commit and set properties
    graph->commit();

    if (!config.block_properties.empty()) {
        apply_block_properties(graph, config.block_properties, rate);
    }

    for (size_t i = 0; i < ddc_controls.size(); ++i) {
        ddc_controls[i]->set_output_rate(rate, ddc_channels[i]);
    }

    // Wait for LO lock
    for (const auto& radio_id : radio_blocks) {
        auto radio = graph->get_block<uhd::rfnoc::radio_control>(radio_id);
        if (radio) {
            for (size_t chan = 0; chan < radio->get_num_output_ports(); ++chan) {
                std::cout << "Waiting for LO lock on " << radio_id.to_string()
                          << " channel " << chan << ": ";
                auto sensors = radio->get_rx_sensor_names(chan);
                bool has_lo_locked =
                    std::find(sensors.begin(), sensors.end(), "lo_locked")
                    != sensors.end();

                if (!has_lo_locked) {
                    std::cout << " No LO sensor (baseband/passive frontend)" << std::endl;
                    break;
                } else {
                    std::cout << "LO sensor detected, waiting for lock";
                    auto start_time = std::chrono::steady_clock::now();
                    while (!radio->get_rx_sensor("lo_locked", chan).to_bool()) {
                        std::this_thread::sleep_for(50ms);
                        if (std::chrono::steady_clock::now() - start_time > 10s) {
                            throw std::runtime_error("LO failed to lock for channel "
                                                     + std::to_string(chan)
                                                     + " after 10 seconds");
                        }
                    }
                }
                std::cout << " locked." << std::endl;
            }
        }
    }

    // Start file writer threads if using ring buffer
    if (use_ring_buffer) {
        for (size_t i = 0; i < contexts.size(); ++i) {
            contexts[i].writer_thread.reset(new std::thread(file_writer_thread,
                std::ref(contexts[i]),
                std::ref(stop_writing_flags[i]),
                std::ref(writer_stats[i])));
        }
    }

    // Create and start capture threads
    std::vector<std::thread> capture_threads;
    std::atomic<bool> start_capture(false);

    for (size_t i = 0; i < contexts.size(); ++i) {
        std::atomic<bool>* stop_ptr = use_ring_buffer ? &stop_writing_flags[i] : nullptr;
        FileWriterStats* stats_ptr  = use_ring_buffer ? &writer_stats[i] : nullptr;

        capture_threads.emplace_back(capture_stream_unified<samp_type>,
            std::ref(contexts[i]),
            std::ref(start_capture),
            stop_ptr,
            num_packets,
            stats_ptr);
    }

    // Synchronize start
    if (config.multi_stream.sync_streams) {
        std::this_thread::sleep_for(
            std::chrono::duration<double>(config.multi_stream.sync_delay));
    }

    auto overall_start = std::chrono::steady_clock::now();
    start_capture.store(true);
    std::cout << "\nStarting multi-stream capture..." << std::endl;

    // Wait for capture threads
    for (auto& thread : capture_threads) {
        thread.join();
    }

    // Wait for writer threads if using ring buffer
    if (use_ring_buffer) {
        std::cout << "\nWaiting for file writers to finish..." << std::endl;
        for (auto& ctx : contexts) {
            if (ctx.writer_thread && ctx.writer_thread->joinable()) {
                ctx.writer_thread->join();
            }
        }
    }

    auto overall_end = std::chrono::steady_clock::now();
    double overall_duration =
        std::chrono::duration<double>(overall_end - overall_start).count();

    // Collect statistics
    std::vector<StreamStats> all_stats;
    size_t total_packets = 0, total_samples = 0, total_overflows = 0;

    for (const auto& ctx : contexts) {
        all_stats.push_back(ctx.stats);
        total_packets += ctx.stats.packets_captured;
        total_samples += ctx.stats.total_samples;
        total_overflows += ctx.stats.overflow_count;
    }

    // Close files
    for (auto& file : output_files) {
        if (file && file->is_open()) {
            file->close();
        }
    }

    // Print statistics
    std::cout << "\n=== Capture Statistics ===" << std::endl;
    std::cout << "Total streams: " << contexts.size() << std::endl;
    std::cout << "Total packets: " << total_packets << std::endl;
    std::cout << "Total samples: " << total_samples << std::endl;
    std::cout << "Duration: " << overall_duration << " seconds" << std::endl;
    std::cout << "Aggregate rate: " << (total_samples / overall_duration) / 1e6 << " Msps"
              << std::endl;
    if (total_overflows > 0) {
        std::cout << "Total overflows: " << total_overflows << std::endl;
    }

    // Perform analysis
    if (enable_analysis && !csv_file.empty() && !all_analysis_packets.empty()) {
        std::cout << "\nAnalyzing packets..." << std::endl;
        std::sort(all_analysis_packets.begin(),
            all_analysis_packets.end(),
            [](const chdr_packet_data& a, const chdr_packet_data& b) {
                if (a.stream_id != b.stream_id)
                    return a.stream_id < b.stream_id;
                return a.seq_num < b.seq_num;
            });

        analyze_packets_unified(all_analysis_packets,
            csv_file,
            tick_rate,
            all_stats,
            pps_reset_time,
            pps_reset_used,
            samps_per_buff,
            rate);
    }
}

// Template wrapper functions for backward compatibility
template <typename samp_type>
void capture_stream_ringbuffer(StreamContext& ctx,
    std::atomic<bool>& start_capture,
    std::atomic<bool>& stop_writing,
    size_t num_packets,
    FileWriterStats& writer_stats)
{
    capture_stream_unified<samp_type>(
        ctx, start_capture, &stop_writing, num_packets, &writer_stats);
}

template <typename samp_type>
void capture_stream(
    StreamContext& ctx, std::atomic<bool>& start_capture, size_t num_packets)
{
    capture_stream_unified<samp_type>(ctx, start_capture, nullptr, num_packets, nullptr);
}

template <typename samp_type>
void capture_multi_stream(uhd::rfnoc::rfnoc_graph::sptr graph,
    const GraphConfig& config,
    const std::string& file,
    size_t num_packets,
    bool enable_analysis,
    const std::string& csv_file,
    double rate,
    size_t samps_per_buff)
{
    capture_multi_stream_unified<samp_type>(graph,
        config,
        file,
        num_packets,
        enable_analysis,
        csv_file,
        rate,
        samps_per_buff,
        uhd::time_spec_t(0.0),
        false,
        false);
}

template <typename samp_type>
void capture_multi_stream_with_pps_reset(uhd::rfnoc::rfnoc_graph::sptr graph,
    const GraphConfig& config,
    const std::string& file,
    size_t num_packets,
    bool enable_analysis,
    const std::string& csv_file,
    double rate,
    size_t samps_per_buff,
    uhd::time_spec_t pps_reset_time,
    bool pps_reset_used)
{
    capture_multi_stream_unified<samp_type>(graph,
        config,
        file,
        num_packets,
        enable_analysis,
        csv_file,
        rate,
        samps_per_buff,
        pps_reset_time,
        pps_reset_used,
        false);
}

template <typename samp_type>
void capture_multi_stream_ringbuffer(uhd::rfnoc::rfnoc_graph::sptr graph,
    const GraphConfig& config,
    const std::string& file,
    size_t num_packets,
    bool enable_analysis,
    const std::string& csv_file,
    double rate,
    size_t samps_per_buff,
    uhd::time_spec_t pps_reset_time,
    bool pps_reset_used)
{
    capture_multi_stream_unified<samp_type>(graph,
        config,
        file,
        num_packets,
        enable_analysis,
        csv_file,
        rate,
        samps_per_buff,
        pps_reset_time,
        pps_reset_used,
        true);
}

// YAML Template Generation
void write_dynamic_yaml_template(
    uhd::rfnoc::rfnoc_graph::sptr graph, const std::string& filename)
{
    std::ofstream file(filename);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to create YAML template file");
    }

    GraphTopology topology = discover_graph_topology(graph);

    file << "# RFNoC Graph Configuration Template\n";
    file << "# Auto-generated based on FPGA image\n";
    file << "# Device: " << graph->get_tree()->access<std::string>("/name").get() << "\n";

    time_t temp_time =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    file << "# Generated: "
         << std::put_time(std::gmtime(&temp_time), "%Y-%m-%d %H:%M:%S UTC") << "\n\n";

    // Multi-stream config
    if (topology.block_stream_ports.size() > 1) {
        file << "multi_stream:\n";
        file << "  enable: true\n";
        file << "  sync_streams: true\n";
        file << "  separate_files: true\n";
        file << "  file_prefix: \"stream\"\n";
        file << "  max_streams: 0\n";
        file << "  sync_delay: 0.1\n";
        file << "  stream_blocks:\n";
        for (const auto& [block_id, ports] : topology.block_stream_ports) {
            for (const auto& port : ports) {
                file << "    - \"" << block_id << "\"  # Port " << port << "\n";
            }
        }
        file << "\n";
    }

    // Static connections
    if (!topology.static_connections.empty()) {
        file << "static_connections:\n";
        for (const auto& edge : topology.static_connections) {
            file << "  # " << edge.src_blockid << ":" << edge.src_port << " -> "
                 << edge.dst_blockid << ":" << edge.dst_port << "\n";
        }
        file << "\n";
    }

    // Dynamic connections
    file << "connections:\n";
    for (const auto& edge : topology.active_connections) {
        bool is_static = false;
        for (const auto& static_edge : topology.static_connections) {
            if (edge == static_edge) {
                is_static = true;
                break;
            }
        }
        if (!is_static) {
            file << "  - src_block: \"" << edge.src_blockid << "\"\n";
            file << "    src_port: " << edge.src_port << "\n";
            file << "    dst_block: \"" << edge.dst_blockid << "\"\n";
            file << "    dst_port: " << edge.dst_port << "\n";
        }
    }

    // Switchboards
    file << "\nswitchboards:\n";
    for (const auto& block : topology.blocks) {
        if (block.block_type == "Switchboard") {
            file << "  - block_id: \"" << block.block_id << "\"\n";
            file << "    connections:\n";
            size_t num_ports = std::min(block.num_input_ports, block.num_output_ports);
            for (size_t i = 0; i < num_ports; ++i) {
                file << "      - input: " << i << "\n";
                file << "        output: " << i << "\n";
            }
        }
    }

    // Stream endpoints
    file << "\nstream_endpoints:\n";
    int stream_idx = 0;
    for (const auto& [block_id, ports] : topology.block_stream_ports) {
        for (const auto& port : ports) {
            file << "  - block_id: \"" << block_id << "\"\n";
            file << "    port: " << port << "\n";
            file << "    direction: \"rx\"\n";
            file << "    enabled: true\n";
            file << "    name: \"Stream_" << stream_idx++ << "\"\n";
            file << "    stream_args:\n";
            file << "      spp: \"200\"\n";
        }
    }

    // Block properties - simplified template
    file << "\nblock_properties:\n";
    std::map<std::string, std::vector<BlockInfo>> blocks_by_type;
    for (const auto& block : topology.blocks) {
        blocks_by_type[block.block_type].push_back(block);
    }

    for (const auto& [block_type, blocks] : blocks_by_type) {
        for (const auto& block : blocks) {
            file << "  \"" << block.block_id << "\":\n";

            // Add type-specific default properties
            if (block_type == "Radio") {
                file << "    rate: \"200e6\"\n";
                file << "    freq/0: \"1e9\"\n";
                file << "    gain/0: \"30\"\n";
                file << "    antenna/0: \"RX2\"\n";
            } else if (block_type == "DDC") {
                file << "    freq/0: \"0\"\n";
                file << "    output_rate/0: \"1e6\"\n";
            } else if (block_type == "SigGen") {
                file << "    enable/0: \"true\"\n";
                file << "    waveform/0: \"sine\"\n";
                file << "    amplitude/0: \"0.5\"\n";
                file << "    frequency/0: \"1000\"\n";
            } else {
                file << "    # No configurable properties\n";
            }
        }
    }

    // Auto-connect settings
    file << "\nauto_connect:\n";
    file << "  radio_to_ddc: false\n";
    file << "  find_stream_endpoint: false\n";

    file.close();
    std::cout << "\nYAML template generated: " << filename << std::endl;
}

// Main Function
int UHD_SAFE_MAIN(int argc, char* argv[])
{
    // Variables
    std::string args, file, format, csv_file, yaml_config;
    size_t num_packets, spb, ring_buffer_mb, batch_write_size;
    double rate, freq, gain, bw;
    bool analyze_only           = false;
    bool show_graph             = false;
    bool create_yaml_template   = false;
    bool multi_stream           = false;
    bool use_pps_reset          = false;
    double pps_wait_time        = 1.5;
    bool verify_pps_reset       = true;
    double max_time_after_reset = 1.0;
    // Add command-line options
    bool use_tsi_format    = false;
    uint16_t sat_id        = 42;
    size_t tsi_csv_packets = 1000;
    size_t tsi_csv_samples = 4;

    po::options_description desc("Allowed options");
    desc.add_options()("help", "help message")("args",
        po::value<std::string>(&args)->default_value("addr=192.168.10.2"),
        "UHD device arguments")("file",
        po::value<std::string>(&file)->default_value("chdr_capture.dat"),
        "output filename")("yaml",
        po::value<std::string>(&yaml_config)->default_value(""),
        "YAML configuration file")("csv",
        po::value<std::string>(&csv_file)->default_value(""),
        "CSV output file for analysis")("num-packets",
        po::value<size_t>(&num_packets)->default_value(1000),
        "packets per stream (0 for continuous)")(
        "rate", po::value<double>(&rate)->default_value(10e6), "sample rate")(
        "freq", po::value<double>(&freq)->default_value(100e6), "center frequency")(
        "gain", po::value<double>(&gain)->default_value(30.0), "gain")(
        "bw", po::value<double>(&bw)->default_value(0.0), "analog bandwidth")("format",
        po::value<std::string>(&format)->default_value("sc16"),
        "sample format")("spb",
        po::value<size_t>(&spb)->default_value(2000),
        "samples per buffer")("ring-buffer-mb",
        po::value<size_t>(&ring_buffer_mb)->default_value(16),
        "ring buffer size in MB per stream")("batch-write-size",
        po::value<size_t>(&batch_write_size)->default_value(100),
        "packets to batch before writing")("show-graph",
        po::value<bool>(&show_graph)->default_value(false),
        "print RFNoC graph info and exit")("analyze-only",
        po::value<bool>(&analyze_only)->default_value(false),
        "only analyze existing file")("create-yaml-template",
        po::value<bool>(&create_yaml_template)->default_value(false),
        "create YAML template")("multi-stream",
        po::value<bool>(&multi_stream)->default_value(false),
        "enable multi-stream capture")("pps-reset",
        po::value<bool>(&use_pps_reset)->default_value(false),
        "reset timestamp to zero at next PPS before capture")("pps-wait-time",
        po::value<double>(&pps_wait_time)->default_value(1.5),
        "time to wait for PPS reset (seconds)")("verify-pps-reset",
        po::value<bool>(&verify_pps_reset)->default_value(true),
        "verify PPS reset was successful")("max-time-after-reset",
        po::value<double>(&max_time_after_reset)->default_value(1.0),
        "max acceptable time after PPS reset")("tsi-format",
        po::value<bool>(&use_tsi_format)->default_value(false),
        "output in TSI proprietary packet format")("sat-id",
        po::value<uint16_t>(&sat_id)->default_value(0),
        "Satellite ID for TSI headers")("tsi-csv-packets",
        po::value<size_t>(&tsi_csv_packets)->default_value(0),
        "Number of TSI packets to write to verification CSV (0 = disabled)")(
        "tsi-csv-samples",
        po::value<size_t>(&tsi_csv_samples)->default_value(4),
        "Number of samples per packet to include in TSI CSV");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << "CHDR Packet Capture Tool with Multi-Stream Support and PPS Reset"
                  << std::endl;
        std::cout << desc << std::endl;
        return EXIT_SUCCESS;
    }

    if (analyze_only) {
        std::cout << "File analysis not implemented in this version." << std::endl;
        return EXIT_SUCCESS;
    }

    if (create_yaml_template) {
        auto graph = uhd::rfnoc::rfnoc_graph::make(args);
        write_dynamic_yaml_template(graph, "rfnoc_config_discovered.yaml");
        return EXIT_SUCCESS;
    }

    std::signal(SIGINT, &sig_int_handler);

    auto graph = uhd::rfnoc::rfnoc_graph::make(args);

    if (show_graph) {
        print_graph_info(graph);
        return EXIT_SUCCESS;
    }

    if (vm.count("tsi-format") && use_tsi_format) {
        std::cout << "TSI proprietary packet format selected for output." << std::endl;
    }

    // Load configuration
    GraphConfig config;
    if (!yaml_config.empty()) {
        config = load_graph_config(yaml_config);
        // Override command line settings
        if (vm.count("pps-reset"))
            config.pps_reset.enable_pps_reset = use_pps_reset;
        if (vm.count("pps-wait-time"))
            config.pps_reset.wait_time_sec = pps_wait_time;
        if (vm.count("verify-pps-reset"))
            config.pps_reset.verify_reset = verify_pps_reset;
        if (vm.count("max-time-after-reset"))
            config.pps_reset.max_time_after_reset = max_time_after_reset;
    } else {
        config.auto_connect_radio_to_ddc                   = true;
        config.auto_find_stream_endpoint                   = true;
        config.multi_stream.enable_multi_stream            = multi_stream;
        config.pps_reset.enable_pps_reset                  = use_pps_reset;
        config.pps_reset.wait_time_sec                     = pps_wait_time;
        config.pps_reset.verify_reset                      = verify_pps_reset;
        config.pps_reset.max_time_after_reset              = max_time_after_reset;
        config.multi_stream.buffer_config.ring_buffer_size = ring_buffer_mb * 1024 * 1024;
        config.multi_stream.buffer_config.batch_write_size = batch_write_size;
        config.multi_stream.separate_files                 = true;

        // Add default Radio properties
        auto radio_blocks = graph->find_blocks("Radio");
        for (const auto& radio_id : radio_blocks) {
            std::string id_str                      = radio_id.to_string();
            config.block_properties[id_str]["freq"] = std::to_string(freq);
            config.block_properties[id_str]["gain"] = std::to_string(gain);
            config.block_properties[id_str]["rate"] = std::to_string(rate);
            if (bw > 0)
                config.block_properties[id_str]["bandwidth"] = std::to_string(bw);
        }
    }

    if (!config.multi_stream.enable_multi_stream) {
        config.multi_stream.enable_multi_stream = true;
    }

    // Perform PPS reset if requested
    uhd::time_spec_t pps_reset_time(0.0);
    bool pps_reset_used = false;
    if (config.pps_reset.enable_pps_reset) {
        pps_reset_time = perform_pps_reset(graph, config.pps_reset);
        pps_reset_used = true;
    }

    bool enable_analysis = !csv_file.empty();
    if (enable_analysis && csv_file.empty()) {
        csv_file = file + "_analysis.csv";
    }

    // Determine which capture method to use
    bool use_ring_buffer = config.multi_stream.separate_files && pps_reset_used;

    try {
        if (use_tsi_format) {
            TsiOutputConfig tsi_config;
            tsi_config.enabled                = true;
            tsi_config.sat_id                 = sat_id;
            tsi_config.tuning_freq_hz         = static_cast<uint32_t>(freq);
            tsi_config.include_file_header    = false; // No magic number header
            tsi_config.csv_max_packets        = tsi_csv_packets;
            tsi_config.csv_samples_per_packet = tsi_csv_samples;
            std::cout << "Using TSI proprietary packet format for output." << std::endl;


            // This by default assumes ringbuffer usage for TSI format
            capture_multi_stream_tsi<std::complex<short>>(graph,
                config,
                file,
                num_packets,
                enable_analysis,
                csv_file,
                rate,
                spb,
                pps_reset_time,
                pps_reset_used,
                tsi_config);
        } else {
            if (format == "sc16") {
                capture_multi_stream_unified<std::complex<short>>(graph,
                    config,
                    file,
                    num_packets,
                    enable_analysis,
                    csv_file,
                    rate,
                    spb,
                    pps_reset_time,
                    pps_reset_used,
                    use_ring_buffer);
            } else if (format == "fc32") {
                capture_multi_stream_unified<std::complex<float>>(graph,
                    config,
                    file,
                    num_packets,
                    enable_analysis,
                    csv_file,
                    rate,
                    spb,
                    pps_reset_time,
                    pps_reset_used,
                    use_ring_buffer);
            } else if (format == "fc64") {
                capture_multi_stream_unified<std::complex<double>>(graph,
                    config,
                    file,
                    num_packets,
                    enable_analysis,
                    csv_file,
                    rate,
                    spb,
                    pps_reset_time,
                    pps_reset_used,
                    use_ring_buffer);
            } else {
                throw std::runtime_error("Unsupported format: " + format);
            }
        }
    } catch (const uhd::exception& e) {
        std::cerr << "\nUHD Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::exception& e) {
        std::cerr << "\nError: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "\n=== Capture Completed Successfully ===" << std::endl;
    return EXIT_SUCCESS;
}