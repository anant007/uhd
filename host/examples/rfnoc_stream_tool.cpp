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

// Boost TCP definition
// Open as client; returns true if connect succeeded
bool BoostTcpSink::open_client(
    const std::string& host, uint16_t port, unsigned int timeout_ms, bool nonblocking)
{
    try {
        boost::asio::ip::tcp::resolver resolver(io_ctx_);
        boost::asio::ip::tcp::resolver::results_type endpoints =
            resolver.resolve(host, std::to_string(port));

        // create socket on heap to avoid copying issues
        socket_ = std::make_unique<boost::asio::ip::tcp::socket>(io_ctx_);

        // We'll run connect in a thread and wait up to timeout_ms.
        std::atomic_bool connect_done(false);
        boost::system::error_code connect_ec;

        std::thread connect_thread([&]() {
            try {
                boost::asio::connect(*socket_, endpoints, connect_ec);
            } catch (const std::exception& e) {
                // convert to error_code
                connect_ec = boost::asio::error::operation_aborted;
            }
            connect_done.store(true);
        });

        unsigned int waited = 0;
        while (!connect_done.load() && waited < timeout_ms) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            waited += 10;
        }

        if (!connect_done.load()) {
            // timeout -> close socket and abort thread
            boost::system::error_code ignore_ec;
            socket_->close(ignore_ec);
            connect_thread.join();
            socket_.reset();
            return false;
        }

        connect_thread.join();

        if (connect_ec) {
            socket_.reset();
            return false;
        }

        // set non-blocking mode (error_code overload)
        boost::system::error_code ec;
        socket_->non_blocking(nonblocking, ec);
        if (ec) {
            // still usable, but report failure if you want
        }

        // start a small io_context worker to keep asio happy (some operations require it)
        worker_thread_ = std::thread([this]() {
            // io_ctx_.run() blocks until stop called; restart() must be called
            io_ctx_.run();
        });

        is_open_.store(true);
        stop_worker_.store(false);
        return true;
    } catch (const std::exception& e) {
        socket_.reset();
        return false;
    }
}

// Open as server; returns true if a client connected within timeout
bool BoostTcpSink::open_server(
    uint16_t port, unsigned int accept_timeout_ms, bool nonblocking)
{
    try {
        is_server_mode_ = true;

        // Create acceptor with reuse_address option for fast reconnection
        acceptor_ = std::make_unique<boost::asio::ip::tcp::acceptor>(io_ctx_);
        acceptor_->open(boost::asio::ip::tcp::v4());
        acceptor_->set_option(boost::asio::socket_base::reuse_address(true));
        acceptor_->bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port));
        acceptor_->listen();

        // Create socket for the accepted connection
        socket_ = std::make_unique<boost::asio::ip::tcp::socket>(io_ctx_);

        std::cout << "[TCP Server] Listening on port " << port
                  << ", waiting for client connection..." << std::endl;

        // Accept in a thread with timeout
        std::atomic_bool accept_done(false);
        boost::system::error_code accept_ec;

        std::thread accept_thread([&]() {
            try {
                acceptor_->accept(*socket_, accept_ec);
            } catch (const std::exception& e) {
                accept_ec = boost::asio::error::operation_aborted;
            }
            accept_done.store(true);
        });

        unsigned int waited = 0;
        while (!accept_done.load() && waited < accept_timeout_ms) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            waited += 100;
        }

        if (!accept_done.load()) {
            // Timeout - close acceptor to abort the accept
            boost::system::error_code ignore_ec;
            acceptor_->close(ignore_ec);
            accept_thread.join();
            socket_.reset();
            acceptor_.reset();
            std::cerr << "[TCP Server] Accept timeout - no client connected" << std::endl;
            return false;
        }

        accept_thread.join();

        if (accept_ec) {
            socket_.reset();
            acceptor_.reset();
            std::cerr << "[TCP Server] Accept error: " << accept_ec.message()
                      << std::endl;
            return false;
        }

        // Set non-blocking mode
        boost::system::error_code ec;
        socket_->non_blocking(nonblocking, ec);

        // Get client endpoint for logging
        auto remote = socket_->remote_endpoint(ec);
        if (!ec) {
            std::cout << "[TCP Server] Client connected from "
                      << remote.address().to_string() << ":" << remote.port()
                      << std::endl;
        }

        // Start io_context worker
        worker_thread_ = std::thread([this]() { io_ctx_.run(); });

        is_open_.store(true);
        stop_worker_.store(false);
        return true;

    } catch (const std::exception& e) {
        std::cerr << "[TCP Server] Exception: " << e.what() << std::endl;
        socket_.reset();
        acceptor_.reset();
        return false;
    }
}

void BoostTcpSink::close()
{
    if (!is_open_.load())
        return;
    stop_worker_.store(true);

    boost::system::error_code ec;
    if (socket_ && socket_->is_open()) {
        socket_->shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
        socket_->close(ec);
    }

    // Close acceptor if in server mode
    if (acceptor_ && acceptor_->is_open()) {
        acceptor_->close(ec);
    }

    // Stop io_context and join worker thread
    io_ctx_.stop();
    if (worker_thread_.joinable())
        worker_thread_.join();

    // prepare io_ctx_ for reuse: use restart() instead of reset()
    io_ctx_.restart();
    socket_.reset();
    acceptor_.reset();
    is_open_.store(false);
}

bool BoostTcpSink::is_connected() const
{
    return is_open_.load() && socket_ && socket_->is_open();
}

// Sends up to len bytes. returns bytes_sent (>0), 0 if would-block, -1 on fatal error
ssize_t BoostTcpSink::send(
    const uint8_t* buf, size_t len, boost::system::error_code& out_ec)
{
    if (!is_connected()) {
        out_ec = boost::asio::error::not_connected;
        return -1;
    }
    try {
        // use socket_->send with error_code overload
        size_t sent = socket_->send(boost::asio::buffer(buf, len), 0, out_ec);
        if (out_ec) {
            // would_block indicated by try_again / would_block
            if (out_ec == boost::asio::error::try_again
                || out_ec == boost::asio::error::would_block) {
                return 0;
            }
            return -1;
        }
        return static_cast<ssize_t>(sent);
    } catch (const std::exception& e) {
        out_ec = boost::system::error_code(
            static_cast<int>(-1), boost::system::generic_category());
        return -1;
    }
}


// parse socket node for a single endpoint YAML::Node endpoint_node
static SocketConfig parse_socket_config(const YAML::Node& node)
{
    SocketConfig cfg;
    if (!node || !node.IsMap())
        return cfg;

    if (node["socket"]) {
        const auto& sock        = node["socket"];
        cfg.enabled             = sock["enabled"].as<bool>(false);
        cfg.mode                = sock["mode"].as<std::string>("client");
        cfg.host                = sock["host"].as<std::string>("127.0.0.1");
        cfg.port                = sock["port"].as<uint16_t>(5001);
        cfg.nonblocking         = sock["nonblocking"].as<bool>(true);
        cfg.drop_on_full        = sock["drop_on_full"].as<bool>(true);
        cfg.connect_timeout_ms  = sock["connect_timeout_ms"].as<unsigned int>(2000);
        cfg.accept_timeout_ms   = sock["accept_timeout_ms"].as<unsigned int>(5000);
        cfg.send_retry_delay_ms = sock["send_retry_delay_ms"].as<unsigned int>(1);
    }
    return cfg;
}

/**
 * @brief Parse per-stream TsiOutputConfig from YAML node
 *
 * This function parses the tsi_config section under each stream_endpoint.
 * The tsi_config now includes sample_processing_mode.
 */
TsiOutputConfig parse_tsi_config(const YAML::Node& node)
{
    TsiOutputConfig cfg;
    if (!node || !node.IsMap())
        return cfg;

    if (node["tsi_config"]) {
        const auto& tsi            = node["tsi_config"];
        cfg.enabled                = tsi["enabled"].as<bool>(false);
        cfg.sat_id                 = tsi["sat_id"].as<uint16_t>(0);
        cfg.tuning_freq_hz         = tsi["tuning_freq_hz"].as<uint32_t>(0);
        cfg.include_file_header    = tsi["include_file_header"].as<bool>(false);
        cfg.csv_max_packets        = tsi["csv_max_packets"].as<size_t>(0);
        cfg.csv_samples_per_packet = tsi["csv_samples_per_packet"].as<size_t>(4);

        // Parse sample_processing_mode (now part of TsiOutputConfig)
        if (tsi["sample_processing_mode"]) {
            std::string mode_str = tsi["sample_processing_mode"].as<std::string>("");
            cfg.sample_processing_mode = parse_sample_processing_mode(mode_str);
        }
    }
    return cfg;
}

// helper send with backpressure
static ssize_t send_with_backpressure(StreamContext& ctx, const uint8_t* buf, size_t len)
{
    if (!ctx.socket_sink || !ctx.socket_sink->is_connected())
        return -1;
    // prefix 4-byte network order length before the payload
    uint32_t plen   = static_cast<uint32_t>(len);
    uint32_t netlen = htonl(plen);
    boost::system::error_code ec;
    // send prefix
    ssize_t s = ctx.socket_sink->send(
        reinterpret_cast<const uint8_t*>(&netlen), sizeof(netlen), ec);
    if (s < 0)
        return -1;
    if (s == 0) { // would-block on prefix
        if (ctx.socket_cfg.drop_on_full)
            return 0;
        // else wait & retry
        std::this_thread::sleep_for(
            std::chrono::milliseconds(ctx.socket_cfg.send_retry_delay_ms));
        return send_with_backpressure(ctx, buf, len);
    }
    // send payload (handle partial sends)
    size_t sent = 0;
    while (sent < len) {
        ec.clear();
        ssize_t r = ctx.socket_sink->send(buf + sent, len - sent, ec);
        if (r > 0) {
            sent += static_cast<size_t>(r);
            continue;
        }
        if (r == 0) {
            if (ctx.socket_cfg.drop_on_full) {
                // drop remainder
                return static_cast<ssize_t>(sent);
            } else {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(ctx.socket_cfg.send_retry_delay_ms));
                continue;
            }
        }
        // r < 0 -> fatal error
        return -1;
    }
    return static_cast<ssize_t>(sent);
}

/**
 * @brief Parse sample processing mode from string
 */
SampleProcessingMode parse_sample_processing_mode(const std::string& mode_str)
{
    std::string lower_mode = mode_str;
    std::transform(lower_mode.begin(), lower_mode.end(), lower_mode.begin(), ::tolower);

    if (lower_mode == "fgb") {
        return SampleProcessingMode::FGB;
    } else if (lower_mode == "sgb") {
        return SampleProcessingMode::SGB;
    }
    return SampleProcessingMode::NONE;
}

/**
 * @brief Get string representation of sample processing mode
 */
std::string sample_processing_mode_to_string(SampleProcessingMode mode)
{
    switch (mode) {
        case SampleProcessingMode::FGB:
            return "FGB (Polyphase Quadrature Demodulation)";
        case SampleProcessingMode::SGB:
            return "SGB (Decimation with Averaging)";
        case SampleProcessingMode::NONE:
        default:
            return "NONE (Pass-through)";
    }
}

/**
 * @brief Get the decimation factor for a given processing mode
 */
size_t get_decimation_factor(SampleProcessingMode mode)
{
    switch (mode) {
        case SampleProcessingMode::FGB:
        case SampleProcessingMode::SGB:
            return 2;
        case SampleProcessingMode::NONE:
        default:
            return 1;
    }
}

/**
 * @brief Apply FGB (First Gen Beacon / SARSAT) processing to sc16 samples
 *
 * This implements polyphase component extraction for SARSAT beacon processing:
 * - Takes 4 input complex samples to produce 4 output REAL samples for legacy processing
 * - Takes 2 input complex samples to product 1 complex sample for current gen processing
 * - Output samples are NOT combined into complex pairs
 *
 * sc16 input format: Each complex sample is stored as [I16, Q16] (4 bytes)
 *
 * @param input_samples Pointer to input sc16 samples (I/Q interleaved as int16_t pairs)
 * @param num_input_samples Number of input complex samples
 * @param output_samples Output buffer for real samples (must be at least
 * num_input_samples)
 * @return Number of output samples produced
 */
size_t apply_fgb_processing(
    const int16_t* input_samples, size_t num_input_samples, int16_t* output_samples)
{
    // num_input_samples
    // Need at least 4 complex samples to produce 4 real output samples
    if (num_input_samples < 4) {
        return 0;
    }

    // Process groups of 4 input complex samples to produce 4 real output samples
    size_t num_groups = num_input_samples / 4;
    size_t output_idx = 0;

    for (size_t g = 0; g < num_groups; ++g) {
        // Input indices: each complex sample is 2 int16_t values (I, Q)
        size_t base_idx = g * 8; // 4 complex samples * 2 int16_t per sample

        // Multiplying by e^(j*(pi/2)*n):
        output_samples[output_idx++] = input_samples[base_idx + 0]; // I0
        output_samples[output_idx++] = -input_samples[base_idx + 3]; // -Q1
        output_samples[output_idx++] = -input_samples[base_idx + 4]; // -I2
        output_samples[output_idx++] = input_samples[base_idx + 7]; // Q3

        // // Multiplying by e^(-j*(pi/2)*n):s
        // output_samples[output_idx++] = input_samples[base_idx + 0]; // I0
        // output_samples[output_idx++] = input_samples[base_idx + 3]; // Q1
        // output_samples[output_idx++] = -input_samples[base_idx + 4]; // -I2
        // output_samples[output_idx++] = -input_samples[base_idx + 7]; // -Q3
    }

    // Return number of REAL output samples (not complex samples)
    return num_groups * 4;
}

/**
 * @brief Apply SGB (Decimation with averaging) processing to sc16 samples
 *
 * This implements decimation by 2 with a simple averaging filter:
 * - Takes 2 input samples to produce 1 output sample
 * - Averages adjacent samples: output[n] = (input[2n] + input[2n+1]) / 2
 * - Effectively halves the sample rate with improved SNR
 *
 * This is equivalent to a simple FIR lowpass with coefficients [0.5, 0.5]
 * followed by decimation by 2.
 *
 * sc16 format: Each complex sample is stored as [I16, Q16] (4 bytes total)
 */
size_t apply_sgb_processing(
    const int16_t* input_samples, size_t num_input_samples, int16_t* output_samples, bool invert_spectrum)
{
    // Need at least 2 samples to produce 1 output sample
    if (num_input_samples < 2) {
        return 0;
    }

    // Process pairs of input samples to produce one output sample
    size_t num_pairs  = num_input_samples / 2;
    size_t output_idx = 0;

    // Added earlier for software control, but no longer needed
    // const int16_t q_sign = invert_spectrum ? -1 : 1;

    for (size_t p = 0; p < num_pairs; ++p) {
        // Input indices: each complex sample is 2 int16_t values
        size_t base_idx = p * 4; // 2 complex samples * 2 int16_t per sample

        // Get I and Q components of both input samples
        int16_t i0 = input_samples[base_idx + 0];
        int16_t q0 = input_samples[base_idx + 1];

        output_samples[output_idx++] = i0;
        output_samples[output_idx++] = /*static_cast<int16_t>*/(q0)/* * q_sign)*/;
    }

    // Return number of complex output samples
    return num_pairs;
}

/**
 * @brief Process samples according to the specified mode
 *
 * Wrapper function that dispatches to the appropriate processing function
 * based on the mode. For NONE mode, data is copied as-is.
 */
size_t process_samples(SampleProcessingMode mode,
    const int16_t* input_samples,
    size_t num_input_samples,
    int16_t* output_samples,
    bool invert_spectrum)
{
    switch (mode) {
        case SampleProcessingMode::FGB:
            return apply_fgb_processing(input_samples, num_input_samples, output_samples);

        case SampleProcessingMode::SGB:
            return apply_sgb_processing(input_samples, num_input_samples, output_samples);

        case SampleProcessingMode::NONE:
        default:
            // Pass-through: copy input to output
            std::memcpy(
                output_samples, input_samples, num_input_samples * 2 * sizeof(int16_t));
            return num_input_samples;
    }
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
            "Radio", "DDC", "DUC", "FIR", "Replay", "DmaFIFO", "SigGen", "NullSrcSink"};

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
GraphTopology discover_graph_topology(uhd::rfnoc::rfnoc_graph::sptr graph)
{
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
    const uhd::time_spec_t& timestamp, double tick_rate, const TimeAnchor& anchor)
{
    TsiTimeComponents tc{};

    // CRITICAL FIX: Derive seconds from total ticks to ensure consistency
    // with fractional calculation. Using get_full_secs() directly can cause
    // misalignment if UHD's internal tick rate differs from our tick_rate.
    //
    // The key insight: both seconds AND fractional parts must be derived from
    // the same total_ticks calculation to ensure proper second rollover.

    const uint64_t ticks_per_sec = static_cast<uint64_t>(tick_rate);

    // Get total ticks using provided tick_rate for consistency
    // Note: to_ticks() converts time_spec_t to ticks using provided rate
    const uint64_t total_ticks = static_cast<uint64_t>(timestamp.to_ticks(tick_rate));

    // Derive integer seconds from total ticks (ensures rollover alignment)
    const uint64_t hw_full_secs = total_ticks / ticks_per_sec;

    // Calculate delta from anchor (handles both UTC and relative modes)
    const int64_t hw_delta_secs =
        static_cast<int64_t>(hw_full_secs) - anchor.hw_secs_at_anchor;

    // Absolute Unix time (derived, not accumulated)
    const std::time_t abs_unix_time = anchor.unix_time_at_anchor + hw_delta_secs;

    // Convert to calendar time (UTC, deterministic)
    std::tm tm_utc{};
#if defined(_WIN32)
    gmtime_s(&tm_utc, &abs_unix_time);
#else
    gmtime_r(&abs_unix_time, &tm_utc);
#endif

    tc.year   = static_cast<uint16_t>(tm_utc.tm_year + 1900);
    tc.month  = static_cast<uint8_t>(tm_utc.tm_mon + 1);
    tc.day    = static_cast<uint8_t>(tm_utc.tm_mday);
    tc.hour   = static_cast<uint8_t>(tm_utc.tm_hour);
    tc.minute = static_cast<uint8_t>(tm_utc.tm_min);
    tc.second = static_cast<uint8_t>(tm_utc.tm_sec);

    // Fractional seconds from ticks, converted to 5-nanosecond count
    // TSI format uses FiveNanoSecCount where each count = 5 nanoseconds
    // For tick_rate = 200MHz: 1 tick = 5ns (direct mapping)
    // For other tick rates: must convert ticks to 5ns units
    const uint64_t frac_ticks = total_ticks % ticks_per_sec;

    // Convert ticks to 5-nanosecond units:
    // time_in_ns = frac_ticks * (1e9 / tick_rate)
    // frac_5ns = time_in_ns / 5 = frac_ticks * (1e9 / tick_rate) / 5
    //          = frac_ticks * (2e8 / tick_rate)
    // For 200MHz: frac_ticks * (2e8 / 2e8) = frac_ticks * 1 = frac_ticks
    // For 100MHz: frac_ticks * (2e8 / 1e8) = frac_ticks * 2
    const double conversion_factor = 2e8 / tick_rate; // 200MHz/tick_rate
    tc.frac_5ns = static_cast<uint32_t>(frac_ticks * conversion_factor);

    return tc;
}

// =============================================================================
// Clock Source Management - Implementation (3-Tier Hierarchy)
// =============================================================================

// Convert ClockSourceTier to string for logging
std::string clock_tier_to_string(ClockSourceTier tier)
{
    switch (tier) {
        case ClockSourceTier::TIER_1_GPSDO:
            return "Tier 1 (GPSDO)";
        case ClockSourceTier::TIER_2_EXTERNAL:
            return "Tier 2 (External)";
        case ClockSourceTier::TIER_3_INTERNAL:
            return "Tier 3 (Internal)";
        default:
            return "Unknown";
    }
}

// Convert NetworkTimeSource to string for logging
std::string network_source_to_string(NetworkTimeSource src)
{
    switch (src) {
        case NetworkTimeSource::GPS_NETWORK:
            return "Network GPS";
        case NetworkTimeSource::NTP:
            return "NTP";
        case NetworkTimeSource::PTP:
            return "PTP";
        case NetworkTimeSource::HOST_SYSTEM:
            return "Host System Time";
        default:
            return "None";
    }
}

// ---------------------------------------------------------------------------
// Stub Interfaces for Network Time Sources (Future Implementation)
// ---------------------------------------------------------------------------

NetworkTimeResult try_network_gps_time(const ClockSourceConfig& config)
{
    if (!config.try_network_gps) {
        return NetworkTimeResult::make_failure("Network GPS disabled in config");
    }
    std::cout
        << "[Clock] Attempting network GPS time acquisition... STUB (not implemented)"
        << std::endl;
    return NetworkTimeResult::make_failure(
        "Network GPS not implemented - stub for future integration");
}

NetworkTimeResult try_ntp_time(const ClockSourceConfig& config)
{
    if (!config.try_ntp) {
        return NetworkTimeResult::make_failure("NTP disabled in config");
    }
    std::cout << "[Clock] Attempting NTP time acquisition... STUB (not implemented)"
              << std::endl;
    return NetworkTimeResult::make_failure(
        "NTP not implemented - stub for future integration");
}

NetworkTimeResult try_ptp_time(const ClockSourceConfig& config)
{
    if (!config.try_ptp) {
        return NetworkTimeResult::make_failure("PTP disabled in config");
    }
    std::cout << "[Clock] Attempting PTP time acquisition... STUB (not implemented)"
              << std::endl;
    return NetworkTimeResult::make_failure(
        "PTP not implemented - stub for future integration");
}

NetworkTimeResult get_host_system_time(const ClockSourceConfig& config)
{
    if (!config.use_host_time_fallback) {
        return NetworkTimeResult::make_failure("Host time fallback disabled in config");
    }
    std::cout << "[Clock] Using host system time as fallback... " << std::flush;
    try {
        auto now     = std::chrono::system_clock::now();
        auto epoch   = now.time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch);
        auto nanoseconds =
            std::chrono::duration_cast<std::chrono::nanoseconds>(epoch)
            - std::chrono::duration_cast<std::chrono::nanoseconds>(seconds);
        uhd::time_spec_t host_time(static_cast<int64_t>(seconds.count()),
            static_cast<double>(nanoseconds.count()) / 1e9);
        time_t time_t_val = std::chrono::system_clock::to_time_t(now);
        std::cout << "SUCCESS" << std::endl;
        std::cout << "[Clock] Host UTC time: "
                  << std::put_time(std::gmtime(&time_t_val), "%Y-%m-%d %H:%M:%S") << " ("
                  << host_time.get_real_secs() << " seconds since epoch)" << std::endl;
        return NetworkTimeResult::make_success(
            host_time, NetworkTimeSource::HOST_SYSTEM, 0.1, "Host system time (UTC)");
    } catch (const std::exception& e) {
        std::cout << "FAILED: " << e.what() << std::endl;
        return NetworkTimeResult::make_failure(
            std::string("Host time acquisition failed: ") + e.what());
    }
}

NetworkTimeResult acquire_best_network_time(const ClockSourceConfig& config)
{
    std::cout << "\n[Clock] === Acquiring Best Available Network Time ===" << std::endl;
    NetworkTimeResult result;
    result = try_network_gps_time(config);
    if (result.success)
        return result;
    result = try_ptp_time(config);
    if (result.success)
        return result;
    result = try_ntp_time(config);
    if (result.success)
        return result;
    result = get_host_system_time(config);
    if (result.success)
        return result;
    return NetworkTimeResult::make_failure("All network time sources unavailable");
}

// ---------------------------------------------------------------------------
// GPSDO Detection and Time Acquisition (Tier 1)
// ---------------------------------------------------------------------------

bool detect_gpsdo(uhd::rfnoc::rfnoc_graph::sptr graph, size_t mboard)
{
    try {
        auto mb_controller = graph->get_mb_controller(mboard);
        auto sensor_names  = mb_controller->get_sensor_names();
        bool has_gps_time =
            std::find(sensor_names.begin(), sensor_names.end(), "gps_time")
            != sensor_names.end();
        bool has_gps_locked =
            std::find(sensor_names.begin(), sensor_names.end(), "gps_locked")
            != sensor_names.end();
        return has_gps_time || has_gps_locked;
    } catch (const std::exception& e) {
        std::cerr << "[Clock] Error checking for GPSDO: " << e.what() << std::endl;
        return false;
    }
}

bool is_gpsdo_locked(uhd::rfnoc::rfnoc_graph::sptr graph, size_t mboard)
{
    try {
        auto mb_controller = graph->get_mb_controller(mboard);
        return mb_controller->get_sensor("gps_locked").to_bool();
    } catch (...) {
        return false;
    }
}

NetworkTimeResult get_gpsdo_time(uhd::rfnoc::rfnoc_graph::sptr graph, size_t mboard)
{
    try {
        auto mb_controller   = graph->get_mb_controller(mboard);
        auto gps_time_sensor = mb_controller->get_sensor("gps_time");
        int64_t gps_seconds  = gps_time_sensor.to_int();
        uhd::time_spec_t gps_time(gps_seconds);
        std::cout << "[Clock] GPSDO time acquired: " << gps_seconds
                  << " seconds since epoch" << std::endl;
        return NetworkTimeResult::make_success(
            gps_time, NetworkTimeSource::GPS_NETWORK, 1e-6, "GPSDO module time");
    } catch (const std::exception& e) {
        return NetworkTimeResult::make_failure(
            std::string("GPSDO time acquisition failed: ") + e.what());
    }
}

bool wait_for_gpsdo_lock(
    uhd::rfnoc::rfnoc_graph::sptr graph, double timeout_sec, size_t mboard)
{
    std::cout << "[Clock] Waiting for GPSDO lock..." << std::flush;
    auto start = std::chrono::steady_clock::now();
    while (true) {
        if (is_gpsdo_locked(graph, mboard)) {
            std::cout << " LOCKED" << std::endl;
            return true;
        }
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (std::chrono::duration<double>(elapsed).count() > timeout_sec) {
            std::cout << " TIMEOUT" << std::endl;
            return false;
        }
        std::cout << "." << std::flush;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

// ---------------------------------------------------------------------------
// External Reference Detection (Tier 2)
// ---------------------------------------------------------------------------

bool is_external_ref_locked(uhd::rfnoc::rfnoc_graph::sptr graph, size_t mboard)
{
    try {
        auto mb_controller = graph->get_mb_controller(mboard);
        auto sensor_names  = mb_controller->get_sensor_names();
        if (std::find(sensor_names.begin(), sensor_names.end(), "ref_locked")
            != sensor_names.end()) {
            return mb_controller->get_sensor("ref_locked").to_bool();
        }
        return false;
    } catch (...) {
        return false;
    }
}

std::vector<std::string> get_clock_sources(
    uhd::rfnoc::rfnoc_graph::sptr graph, size_t mboard)
{
    try {
        auto mb_controller = graph->get_mb_controller(mboard);
        return mb_controller->get_clock_sources();
    } catch (...) {
        return {};
    }
}

std::vector<std::string> get_time_sources(
    uhd::rfnoc::rfnoc_graph::sptr graph, size_t mboard)
{
    try {
        auto mb_controller = graph->get_mb_controller(mboard);
        return mb_controller->get_time_sources();
    } catch (...) {
        return {};
    }
}

// ---------------------------------------------------------------------------
// Clock Source Selection and Configuration
// ---------------------------------------------------------------------------

ClockSourceStatus probe_and_select_clock_source(
    uhd::rfnoc::rfnoc_graph::sptr graph, const ClockSourceConfig& config, size_t mboard)
{
    ClockSourceStatus status;
    status.last_check_time = std::chrono::steady_clock::now();

    std::cout << "\n[Clock] === Probing Clock Sources (3-Tier Hierarchy) ==="
              << std::endl;

    auto mb_controller = graph->get_mb_controller(mboard);
    auto clock_sources = get_clock_sources(graph, mboard);
    auto time_sources  = get_time_sources(graph, mboard);

    std::cout << "[Clock] Available clock sources: ";
    for (const auto& src : clock_sources)
        std::cout << src << " ";
    std::cout << std::endl;
    std::cout << "[Clock] Available time sources: ";
    for (const auto& src : time_sources)
        std::cout << src << " ";
    std::cout << std::endl;

    std::string target_clock = config.preferred_clock_source;


    // Tier 1: GPSDO
    bool try_gpsdo = config.use_gpsdo_if_available
                     && (target_clock.empty() || target_clock == "gpsdo");
    if (try_gpsdo) {
        std::cout << "\n[Clock] --- Checking Tier 1: GPSDO ---" << std::endl;
        status.gpsdo_present = detect_gpsdo(graph, mboard);
        if (status.gpsdo_present) {
            std::cout << "[Clock] GPSDO detected on device" << std::endl;
            bool clock_set = false, time_set = false;
            if (std::find(clock_sources.begin(), clock_sources.end(), "gpsdo")
                != clock_sources.end()) {
                try {
                    mb_controller->set_clock_source("gpsdo");
                    std::cout << "[Clock] Clock source set to GPSDO" << std::endl;
                    clock_set = true;
                } catch (const std::exception& e) {
                    std::cerr << "[Clock] Failed to set GPSDO clock source: " << e.what()
                              << std::endl;
                }
            }
            if (std::find(time_sources.begin(), time_sources.end(), "gpsdo")
                != time_sources.end()) {
                try {
                    mb_controller->set_time_source("gpsdo");
                    std::cout << "[Clock] Time source set to GPSDO" << std::endl;
                    time_set = true;
                } catch (const std::exception& e) {
                    std::cerr << "[Clock] Failed to set GPSDO time source: " << e.what()
                              << std::endl;
                }
            }
            if (clock_set && time_set) {
                status.gpsdo_locked =
                    wait_for_gpsdo_lock(graph, config.gpsdo_lock_timeout_sec, mboard);
                if (status.gpsdo_locked) {
                    status.current_tier       = ClockSourceTier::TIER_1_GPSDO;
                    status.active_time_source = NetworkTimeSource::GPS_NETWORK;
                    status.pps_present        = true;
                    status.status_message     = "Tier 1: GPSDO locked and operational";
                    std::cout << "[Clock] SUCCESS: " << status.status_message
                              << std::endl;
                    return status;
                }
            }
        } else {
            std::cout << "[Clock] GPSDO not detected on device" << std::endl;
        }
    }

    // Tier 2: External
    bool try_external = config.use_external_if_available
                        && (target_clock.empty() || target_clock == "external");
    if (try_external) {
        std::cout << "\n[Clock] --- Checking Tier 2: External Reference ---" << std::endl;
        bool has_external_clock =
            std::find(clock_sources.begin(), clock_sources.end(), "external")
            != clock_sources.end();
        bool has_external_time =
            std::find(time_sources.begin(), time_sources.end(), "external")
            != time_sources.end();
        if (has_external_clock || has_external_time) {
            std::cout << "[Clock] External reference available" << std::endl;
            try {
                if (has_external_clock) {
                    mb_controller->set_clock_source("external");
                    std::cout << "[Clock] Clock source set to external" << std::endl;
                }
                if (has_external_time) {
                    mb_controller->set_time_source("external");
                    std::cout << "[Clock] Time source set to external" << std::endl;
                }
                std::cout << "[Clock] Waiting for external reference lock..."
                          << std::flush;
                auto start = std::chrono::steady_clock::now();
                while (true) {
                    status.ref_locked = is_external_ref_locked(graph, mboard);
                    if (status.ref_locked) {
                        std::cout << " LOCKED" << std::endl;
                        break;
                    }
                    auto elapsed = std::chrono::steady_clock::now() - start;
                    if (std::chrono::duration<double>(elapsed).count()
                        > config.external_ref_lock_timeout_sec) {
                        std::cout << " TIMEOUT (proceeding anyway)" << std::endl;
                        break;
                    }
                    std::cout << "." << std::flush;
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
                status.current_tier   = ClockSourceTier::TIER_2_EXTERNAL;
                status.pps_present    = true;
                status.status_message = "Tier 2: External reference";
                std::cout << "[Clock] SUCCESS: " << status.status_message << std::endl;
                return status;
            } catch (const std::exception& e) {
                std::cerr << "[Clock] Failed to configure external reference: "
                          << e.what() << std::endl;
            }
        } else {
            std::cout << "[Clock] External reference not available" << std::endl;
        }
    }

    // Tier 3: Internal (fallback)
    std::cout << "\n[Clock] --- Using Tier 3: Internal Clock ---" << std::endl;
    try {
        if (std::find(clock_sources.begin(), clock_sources.end(), "internal")
            != clock_sources.end()) {
            mb_controller->set_clock_source("internal");
            std::cout << "[Clock] Clock source set to internal" << std::endl;
        }
        if (std::find(time_sources.begin(), time_sources.end(), "internal")
            != time_sources.end()) {
            mb_controller->set_time_source("internal");
            std::cout << "[Clock] Time source set to internal" << std::endl;
        }
        status.current_tier   = ClockSourceTier::TIER_3_INTERNAL;
        status.pps_present    = true;
        status.status_message = "Tier 3: Internal clock - periodic re-sync recommended";
        std::cout << "[Clock] " << status.status_message << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "[Clock] Failed to configure internal clock: " << e.what()
                  << std::endl;
        status.status_message = "Clock configuration failed: " + std::string(e.what());
    }
    return status;
}

// ---------------------------------------------------------------------------
// PPS-Aligned Time Synchronization with TimeAnchor
// ---------------------------------------------------------------------------

PpsAlignmentResult perform_pps_aligned_sync(uhd::rfnoc::rfnoc_graph::sptr graph,
    const PpsResetConfig& config,
    const ClockSourceStatus& clock_status,
    size_t mboard)
{
    PpsAlignmentResult result;
    result.tier = clock_status.current_tier;

    std::cout << "\n[Clock] === PPS-Aligned Time Synchronization ===" << std::endl;
    std::cout << "[Clock] Current tier: "
              << clock_tier_to_string(clock_status.current_tier) << std::endl;

    try {
        auto mb_controller = graph->get_mb_controller(mboard);
        auto timekeeper    = mb_controller->get_timekeeper(0);


        uhd::time_spec_t time_before = timekeeper->get_time_now();
        std::cout << "[Clock] Device time before sync: " << std::fixed
                  << std::setprecision(6) << time_before.get_real_secs() << " seconds"
                  << std::endl;

        // Acquire the reference time based on clock tier
        uhd::time_spec_t reference_time;
        if (clock_status.current_tier == ClockSourceTier::TIER_1_GPSDO) {
            std::cout << "[Clock] Tier 1: Acquiring time from GPSDO..." << std::endl;
            auto gps_result = get_gpsdo_time(graph, mboard);
            if (gps_result.success) {
                reference_time     = gps_result.time;
                result.time_source = NetworkTimeSource::GPS_NETWORK;
            } else {
                throw std::runtime_error(
                    "Failed to get GPSDO time: " + gps_result.message);
            }
        } else {
            std::cout << "[Clock] Tier " << static_cast<int>(clock_status.current_tier)
                      << ": Acquiring time from network/host..." << std::endl;
            auto net_result = acquire_best_network_time(config.clock_config);
            if (net_result.success) {
                reference_time     = net_result.time;
                result.time_source = net_result.source;
            } else {
                throw std::runtime_error(
                    "Failed to acquire reference time: " + net_result.message);
            }
        }

        // Calculate the time to set at next PPS
        int64_t next_second = reference_time.get_full_secs() + 1;

        if (config.use_utc_time) {
            result.aligned_time = uhd::time_spec_t(next_second, 0.0);
            std::cout << "[Clock] Will set device time to " << next_second
                      << " seconds (UTC) at next PPS" << std::endl;
        } else {
            result.aligned_time = uhd::time_spec_t(0.0);
            std::cout << "[Clock] Will reset device time to 0 at next PPS (legacy mode)"
                      << std::endl;
        }

        // CRITICAL: Create the TimeAnchor for TSI timestamp conversion
        // This anchor links the hardware time to real UTC time
        result.time_anchor.unix_time_at_anchor = static_cast<std::time_t>(next_second);
        result.time_anchor.hw_secs_at_anchor   = result.aligned_time.get_full_secs();

        std::cout << "[Clock] TimeAnchor created:" << std::endl;
        std::cout << "[Clock]   unix_time_at_anchor: "
                  << result.time_anchor.unix_time_at_anchor << std::endl;
        std::cout << "[Clock]   hw_secs_at_anchor: "
                  << result.time_anchor.hw_secs_at_anchor << std::endl;

        // Perform the PPS-aligned time set
        std::cout << "[Clock] Calling set_time_next_pps()..." << std::endl;
        timekeeper->set_time_next_pps(result.aligned_time);

        // Wait for PPS to occur
        std::cout << "[Clock] Waiting " << config.wait_time_sec
                  << " seconds for PPS edge..." << std::flush;
        std::this_thread::sleep_for(std::chrono::duration<double>(config.wait_time_sec));
        std::cout << " done" << std::endl;

        // Verify the synchronization
        uhd::time_spec_t time_after = timekeeper->get_time_now();
        std::cout << "[Clock] Device time after sync: " << std::fixed
                  << std::setprecision(6) << time_after.get_real_secs() << " seconds"
                  << std::endl;

        if (config.verify_reset) {
            double expected_time = config.use_utc_time ? static_cast<double>(next_second)
                                                       : 0.0;
            double time_diff     = std::abs(time_after.get_real_secs() - expected_time);
            if (time_diff > config.max_time_after_reset + config.wait_time_sec) {
                std::cerr << "[Clock] WARNING: Time sync may have failed!" << std::endl;
                result.message = "Time sync verification failed - possible PPS issue";
            } else {
                result.success = true;
                result.message = "PPS-aligned sync successful";
                std::cout << "[Clock] Time synchronization verified successfully"
                          << std::endl;
            }
        } else {
            result.success = true;
            result.message = "PPS-aligned sync completed (verification disabled)";
        }

        std::cout << "[Clock] === Synchronization Result ===" << std::endl;
        std::cout << "[Clock] Tier: " << clock_tier_to_string(result.tier) << std::endl;
        std::cout << "[Clock] Time source: "
                  << network_source_to_string(result.time_source) << std::endl;
        std::cout << "[Clock] Aligned time: " << result.aligned_time.get_real_secs()
                  << " seconds" << std::endl;
        std::cout << "[Clock] Status: " << result.message << std::endl;

    } catch (const std::exception& e) {
        result.success = false;
        result.message = std::string("PPS alignment failed: ") + e.what();
        std::cerr << "[Clock] ERROR: " << result.message << std::endl;
    }

    return result;
}

/**
 * @brief Build TSI packet header from PacketBuffer metadata
 *
 * CRITICAL: This function now accepts a TimeAnchor that was created at PPS alignment
 * time. This ensures accurate UTC timestamp conversion for TSI format headers.
 *
 * @param pkt Source PacketBuffer with timestamp and metadata
 * @param tick_rate Device tick rate (used for 5ns conversion)
 * @param stream_id Stream/channel ID (0-7)
 * @param sat_id Satellite ID
 * @param tuning_freq_hz Tuning frequency in Hz
 * @param anchor TimeAnchor created at PPS alignment (MUST be valid for accurate
 * timestamps)
 * @param anchor_valid True if anchor was created from successful PPS alignment
 * @return Populated packetheader structure with accurate UTC timestamps
 */
inline packetheader build_tsi_header_from_packet(const PacketBuffer& pkt,
    double tick_rate,
    size_t stream_id,
    uint16_t sat_id,
    uint32_t tuning_freq_hz,
    const TimeAnchor& anchor,
    bool anchor_valid                    = true,
    SampleProcessingMode processing_mode = SampleProcessingMode::NONE)
{
    packetheader header;

    // Receiver type
    if (processing_mode == SampleProcessingMode::FGB) {
        std::memcpy(header.ReceiverType, TSI_RECEIVER_TYPE_1ST, 4);
    } else {
        std::memcpy(header.ReceiverType, TSI_RECEIVER_TYPE, 4);
    }

    // Packet sequence number
    header.PacketNumber = static_cast<unsigned int>(pkt.packet_number);

    // Satellite ID
    header.SATID = sat_id;

    // Time components - use provided anchor for accurate UTC timestamps
    TsiTimeComponents tc;
    if (pkt.has_timestamp && anchor_valid) {
        // CRITICAL: Use the PPS-aligned TimeAnchor for accurate UTC conversion
        // This anchor was created at PPS alignment time and provides the link
        // between hardware timestamps and real UTC time
        tc = timestamp_to_tsi_time(pkt.timestamp, tick_rate, anchor);
    } else if (pkt.has_timestamp) {
        // Fallback: Create anchor from current time (less accurate)
        // This happens when PPS alignment was not performed or failed
        TimeAnchor fallback_anchor;
        fallback_anchor.unix_time_at_anchor =
            std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        fallback_anchor.hw_secs_at_anchor = pkt.timestamp.get_full_secs();
        tc = timestamp_to_tsi_time(pkt.timestamp, tick_rate, fallback_anchor);
    } else {
        // No timestamp available - use wall clock
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

/**
 * @brief Extract radio block IDs that are actually referenced in the configuration
 * @param graph RFNoC graph
 * @param config Graph configuration
 * @return Set of radio block IDs that are referenced in the config
 */
std::set<std::string> get_configured_radio_blocks(
    uhd::rfnoc::rfnoc_graph::sptr graph, const GraphConfig& config)
{
    std::set<std::string> configured_radios;

    // Helper to check if a block ID is a Radio block
    auto is_radio_block = [](const std::string& block_id) -> bool {
        return block_id.find("Radio") != std::string::npos;
    };

    // 1. Check dynamic_connections
    for (const auto& conn : config.dynamic_connections) {
        if (is_radio_block(conn.src_block)) {
            configured_radios.insert(conn.src_block);
        }
        if (is_radio_block(conn.dst_block)) {
            configured_radios.insert(conn.dst_block);
        }
    }

    // 2. Check signal_paths
    for (const auto& path : config.signal_paths) {
        for (const auto& conn : path.connections) {
            if (is_radio_block(conn.src_block)) {
                configured_radios.insert(conn.src_block);
            }
            if (is_radio_block(conn.dst_block)) {
                configured_radios.insert(conn.dst_block);
            }
        }
    }

    // 3. Check stream_endpoints (radio could be a streaming endpoint)
    for (const auto& sep : config.stream_endpoints) {
        if (is_radio_block(sep.block_id)) {
            configured_radios.insert(sep.block_id);
        }
    }

    // 4. Check block_properties
    for (const auto& [block_id, props] : config.block_properties) {
        if (is_radio_block(block_id)) {
            configured_radios.insert(block_id);
        }
    }

    // 5. Check block_init_order
    for (const auto& block_id : config.block_init_order) {
        if (is_radio_block(block_id)) {
            configured_radios.insert(block_id);
        }
    }

    // 6. Check multi_stream.stream_blocks
    for (const auto& block_id : config.multi_stream.stream_blocks) {
        if (is_radio_block(block_id)) {
            configured_radios.insert(block_id);
        }
    }

    // If no specific radios configured, check DDCs and trace back to their radios
    if (configured_radios.empty()) {
        std::set<std::string> configured_ddcs;

        // Collect all DDCs mentioned in config
        for (const auto& conn : config.dynamic_connections) {
            if (conn.src_block.find("DDC") != std::string::npos) {
                configured_ddcs.insert(conn.src_block);
            }
            if (conn.dst_block.find("DDC") != std::string::npos) {
                configured_ddcs.insert(conn.dst_block);
            }
        }
        for (const auto& path : config.signal_paths) {
            for (const auto& conn : path.connections) {
                if (conn.src_block.find("DDC") != std::string::npos) {
                    configured_ddcs.insert(conn.src_block);
                }
                if (conn.dst_block.find("DDC") != std::string::npos) {
                    configured_ddcs.insert(conn.dst_block);
                }
            }
        }
        for (const auto& sep : config.stream_endpoints) {
            if (sep.block_id.find("DDC") != std::string::npos) {
                configured_ddcs.insert(sep.block_id);
            }
        }
        for (const auto& [block_id, props] : config.block_properties) {
            if (block_id.find("DDC") != std::string::npos) {
                configured_ddcs.insert(block_id);
            }
        }
        for (const auto& block_id : config.multi_stream.stream_blocks) {
            if (block_id.find("DDC") != std::string::npos) {
                configured_ddcs.insert(block_id);
            }
        }

        // For each configured DDC, find the corresponding Radio
        for (const auto& ddc_id_str : configured_ddcs) {
            try {
                uhd::rfnoc::block_id_t ddc_id(ddc_id_str);
                size_t dev   = ddc_id.get_device_no();
                size_t count = ddc_id.get_block_count();

                uhd::rfnoc::block_id_t radio_id(dev, "Radio", count);
                if (graph->has_block(radio_id)) {
                    configured_radios.insert(radio_id.to_string());
                }
            } catch (...) {
            }
        }
    }

    return configured_radios;
}

/**
 * @brief Get radio block IDs as uhd::rfnoc::block_id_t vector
 */
std::vector<uhd::rfnoc::block_id_t> get_configured_radio_block_ids(
    uhd::rfnoc::rfnoc_graph::sptr graph, const GraphConfig& config)
{
    std::vector<uhd::rfnoc::block_id_t> result;

    auto configured = get_configured_radio_blocks(graph, config);
    auto all_radios = graph->find_blocks("Radio");

    for (const auto& radio_id : all_radios) {
        if (configured.find(radio_id.to_string()) != configured.end()) {
            result.push_back(radio_id);
        }
    }

    // Fall back to first radio if no config but radios exist
    if (result.empty() && !all_radios.empty()) {
        bool has_explicit_config =
            !config.dynamic_connections.empty() || !config.signal_paths.empty()
            || !config.stream_endpoints.empty() || !config.block_properties.empty()
            || !config.multi_stream.stream_blocks.empty();

        if (!has_explicit_config) {
            result.push_back(all_radios[0]);
            std::cout << "Note: No explicit Radio configuration found, using only "
                      << all_radios[0].to_string() << std::endl;
        }
    }

    return result;
}

// =============================================================================
// SECTION 5: Modified tsi_file_writer_thread
// =============================================================================

/**
 * @brief TSI format file writer thread
 *
 * @param ctx StreamContext with ring buffer
 * @param stop_writing Atomic flag to signal shutdown
 * @param writer_stats Statistics tracking
 * @param tsi_config TSI-specific configuration
 */
void tsi_file_writer_thread(
    StreamContext& ctx, std::atomic<bool>& stop_writing, FileWriterStats& writer_stats)
{
    // Get TSI config from StreamContext (per-stream configuration)
    const TsiOutputConfig& tsi_config = ctx.tsi_config;

    writer_stats.start_time = std::chrono::steady_clock::now();
    uhd::set_thread_priority_safe(0.5, true);
    // std::string tsi_filename = "stream_" + std::to_string(ctx.stream_id) + ".dat";
    std::string tsi_filename = "stream_" + std::to_string(ctx.stream_id) + ".dat";

    auto cwd             = std::filesystem::current_path();
    const char* env_temp = std::getenv("TEMPSTR_DEFINE");
    std::string temp_str = env_temp ? env_temp : "";
    if (temp_str.empty()) {
        temp_str = TEMPSTR_DEFINE;
    }
    auto fileTime =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

    // Convert to local time
    std::tm local_tm{};
#if defined(_WIN32)
    localtime_s(&local_tm, &fileTime);
#else
    localtime_r(&fileTime, &local_tm);
#endif

    int floored_hr = (local_tm.tm_hour / 4) * 4; // floor to nearest 4 hour block

    local_tm.tm_hour = floored_hr;
    local_tm.tm_min  = 0;
    local_tm.tm_sec  = 0;
    fileTime         = std::mktime(&local_tm);

    tsi_filename = temp_str + "/rawdata_" + std::to_string(ctx.stream_id) + "_"
                   + TimeConverter::TimeTToString("%Y%m%d_%H%M%S", fileTime) + ".bin";


    // Generate TSI output filename
    if (ctx.output_filename.empty()) {
        std::cerr << "[TSI Writer " << ctx.stream_id
                  << "] No output filename specified, Using savedata format."
                  << std::endl;
        auto cwd             = std::filesystem::current_path();
        const char* env_temp = std::getenv("TEMPSTR_DEFINE");
        std::string temp_str = env_temp ? env_temp : "";
        if (temp_str.empty()) {
            temp_str = TEMPSTR_DEFINE;
        }
        auto fileTime =
            std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

        // Convert to local time
        std::tm local_tm{};
#if defined(_WIN32)
        localtime_s(&local_tm, &fileTime);
#else
        localtime_r(&fileTime, &local_tm);
#endif

        int floored_hr = (local_tm.tm_hour / 4) * 4; // floor to nearest 4 hour block

        local_tm.tm_hour = floored_hr;
        local_tm.tm_min  = 0;
        local_tm.tm_sec  = 0;
        fileTime         = std::mktime(&local_tm);
        tsi_filename     = temp_str + "/rawdata_" + std::to_string(ctx.stream_id) + "_"
                       + TimeConverter::TimeTToString("%Y%m%d_%H%M%S", fileTime) + ".bin";

        ctx.output_filename = tsi_filename;
    } else {
        std::cout << "[TSI Writer " << ctx.stream_id
                  << "] Output filename: " << ctx.output_filename << std::endl;
        //    tsi_filename = ctx.output_filename;
        auto cwd             = std::filesystem::current_path();
        const char* env_temp = std::getenv("TEMPSTR_DEFINE");
        std::string temp_str = env_temp ? env_temp : "";
        if (temp_str.empty()) {
            temp_str = TEMPSTR_DEFINE;
        }
        auto fileTime =
            std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

        // Convert to local time
        std::tm local_tm{};
#if defined(_WIN32)
        localtime_s(&local_tm, &fileTime);
#else
        localtime_r(&fileTime, &local_tm);
#endif

        int floored_hr = (local_tm.tm_hour / 4) * 4; // floor to nearest 4 hour block

        local_tm.tm_hour = floored_hr;
        local_tm.tm_min  = 0;
        local_tm.tm_sec  = 0;
        fileTime         = std::mktime(&local_tm);

        tsi_filename = temp_str + "/rawdata_" + std::to_string(ctx.stream_id) + "_"
                       + TimeConverter::TimeTToString("%Y%m%d_%H%M%S", fileTime) + ".bin";
        ctx.output_filename = tsi_filename;
    }


    // Open output file
    std::ofstream output_file(tsi_filename, std::ios::binary);
    if (!output_file.is_open()) {
        std::cerr << "[TSI Writer " << ctx.stream_id
                  << "] Failed to open file: " << tsi_filename << std::endl;
        return;
    }

    // NOTE: No file header/magic number - raw TSI packets only

    // Create CSV writer if configured
    std::unique_ptr<TsiCsvWriter> csv_writer;
    if (tsi_config.csv_max_packets > 0) {
        std::string csv_filename =
            ctx.output_filename.substr(0, ctx.output_filename.rfind('.')) + "_verify.csv";

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

    // Sample processing configuration - now from TsiOutputConfig
    SampleProcessingMode processing_mode = tsi_config.sample_processing_mode;
    size_t decimation_factor             = get_decimation_factor(processing_mode);

    // Log sample processing mode if active
    if (processing_mode != SampleProcessingMode::NONE) {
        std::cout << "[TSI Writer " << ctx.stream_id << "] Sample processing mode: "
                  << sample_processing_mode_to_string(processing_mode)
                  << " (decimation factor: " << decimation_factor << ")" << std::endl;
    }


    // Buffer(s) for processed samples (max size based on typical packet payload)
    // MAX_SAMPLES_PER_PACKET = number of complex samples per packet we expect
    // (conservative)
    constexpr size_t MAX_SAMPLES_PER_PACKET = 8192;
    // processed_buffer_sgb holds complex output samples (I,Q) -> 2 int16_t per complex
    // sample
    std::vector<int16_t> processed_buffer_sgb(MAX_SAMPLES_PER_PACKET * 2);
    std::vector<int16_t> processed_buffer(MAX_SAMPLES_PER_PACKET * 2); // *2 for I/Q pairs

    // processed_buffer_fgb holds real int16_t output samples produced by FGB processing
    std::vector<int16_t> processed_buffer_fgb(MAX_SAMPLES_PER_PACKET * 2);
    // fgb_output_file is opened only if processing_mode == FGB
    std::ofstream fgb_output_file;
    size_t fgb_bytes_written   = 0;
    size_t fgb_packets_written = 0;

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
                    // Build TSI header with PPS-aligned TimeAnchor
                    packetheader header = build_tsi_header_from_packet(pkt,
                        ctx.tick_rate,
                        ctx.stream_id,
                        tsi_config.sat_id,
                        tsi_config.tuning_freq_hz,
                        ctx.time_anchor,
                        ctx.time_anchor_valid,
                        processing_mode == SampleProcessingMode::FGB
                            ? SampleProcessingMode::SGB
                            : processing_mode);

                    // Write TSI header (32 bytes)
                    output_file.write(
                        reinterpret_cast<const char*>(&header), sizeof(packetheader));

                    // Ensure we have opened the FGB output file (one-time)
                    if (processing_mode == SampleProcessingMode::FGB) {
                        packetheader header2 = build_tsi_header_from_packet(pkt,
                            ctx.tick_rate,
                            ctx.stream_id,
                            tsi_config.sat_id,
                            tsi_config.tuning_freq_hz,
                            ctx.time_anchor,
                            ctx.time_anchor_valid,
                            SampleProcessingMode::FGB);
                        if (!fgb_output_file.is_open()) {
                            // create filename by inserting _fgb before extension
                            std::string base = ctx.output_filename;
                            auto pos         = base.find_last_of('.');
                            std::string fgb_name;
                            if (pos == std::string::npos) {
                                fgb_name = base + "_fgb";
                            } else {
                                fgb_name =
                                    base.substr(0, pos) + "_fgb" + base.substr(pos);
                            }
                            fgb_output_file.open(fgb_name, std::ios::binary);
                            if (!fgb_output_file.is_open()) {
                                std::cerr << "[TSI Writer " << ctx.stream_id
                                          << "] Failed to open FGB file: " << fgb_name
                                          << std::endl;
                                // fallback: continue writing only SGB to output_file
                            } else {
                                std::cout << "[TSI Writer " << ctx.stream_id
                                          << "] FGB output file opened: " << fgb_name
                                          << std::endl;
                                fgb_output_file.write(
                                    reinterpret_cast<const char*>(&header2),
                                    sizeof(packetheader));
                            }
                        } else {
                            fgb_output_file.write(reinterpret_cast<const char*>(&header2),
                                sizeof(packetheader));
                        }
                    }

                    // Extract raw payload (strip CHDR header)
                    auto [payload_ptr, payload_size] = extract_payload_from_packet(pkt);
                    if (payload_ptr && payload_size > 0) {
                        const uint8_t* write_ptr = payload_ptr;
                        size_t write_size        = payload_size;

                        // If we are in FGB mode we will produce TWO outputs:
                        //  - SGB-processed output -> write to the existing output_file
                        //  (ctx.output_filename)
                        //  - FGB-processed output -> write to an additional file with
                        //  suffix "_fgb" before extension
                        //
                        // Otherwise (NONE or SGB) behave as before.
                        if (processing_mode == SampleProcessingMode::FGB) {
                            // Input samples count (complex sc16 samples)
                            size_t num_input_samples = payload_size / 4;
                            const int16_t* input_samples =
                                reinterpret_cast<const int16_t*>(payload_ptr);

                            // 1) Produce FGB processed data (real int16_t samples)
                            size_t num_output_samples_fgb =
                                process_samples(SampleProcessingMode::FGB,
                                    input_samples,
                                    num_input_samples,
                                    processed_buffer_fgb.data());

                            const uint8_t* fgb_ptr = reinterpret_cast<const uint8_t*>(
                                processed_buffer_fgb.data());
                            size_t fgb_write_size =
                                num_output_samples_fgb * 2; // each real sample is 2 bytes

                            // 2) Produce SGB processed data (complex int16_t samples:
                            // I,Q)
                            size_t num_output_samples_sgb =
                                process_samples(SampleProcessingMode::SGB,
                                    input_samples,
                                    num_input_samples,
                                    processed_buffer_sgb.data()/*, // Commenting out the inversion requirement
                                    true*/); //inverting the spectrum for SGB processing

                            const uint8_t* sgb_ptr = reinterpret_cast<const uint8_t*>(
                                processed_buffer_sgb.data());
                            size_t sgb_write_size =
                                num_output_samples_sgb * 4; // complex -> 2*2 bytes

                            // Write SGB output to primary output file
                            if (sgb_write_size > 0) {
                                output_file.write(reinterpret_cast<const char*>(sgb_ptr),
                                    sgb_write_size);
                                writer_stats.bytes_written += sgb_write_size;
                            }

                            // Write FGB output to the additional FGB file (if opened)
                            if (fgb_output_file.is_open() && fgb_write_size > 0) {
                                fgb_output_file.write(
                                    reinterpret_cast<const char*>(fgb_ptr),
                                    fgb_write_size);
                                fgb_bytes_written += fgb_write_size;
                                fgb_packets_written++;
                            }

                            // Count the packet as written in the main stats (keeps
                            // compatibility)
                            writer_stats.packets_written++;

                        } else if (processing_mode == SampleProcessingMode::SGB) {
                            // Existing (SGB) behavior - single processed output
                            size_t num_input_samples = payload_size / 4;
                            const int16_t* input_samples =
                                reinterpret_cast<const int16_t*>(payload_ptr);

                            size_t num_output_samples = process_samples(processing_mode,
                                input_samples,
                                num_input_samples,
                                processed_buffer_sgb.data());

                            write_ptr = reinterpret_cast<const uint8_t*>(
                                processed_buffer_sgb.data());
                            write_size = num_output_samples * 4;

                            output_file.write(
                                reinterpret_cast<const char*>(write_ptr), write_size);
                            writer_stats.packets_written++;
                            writer_stats.bytes_written += write_size;

                        } else {
                            // NONE mode - raw payload
                            output_file.write(
                                reinterpret_cast<const char*>(write_ptr), write_size);
                            writer_stats.packets_written++;
                            writer_stats.bytes_written += write_size;
                        }
                        // Write to CSV if enabled
                        if (csv_writer && csv_writer->is_open()) {
                            csv_writer->write_packet(header, write_ptr, write_size);
                        }

                        writer_stats.packets_written++;
                        writer_stats.bytes_written += sizeof(packetheader) + write_size;
                    } else {
                        // Empty payload - just count the header
                        writer_stats.packets_written++;
                        writer_stats.bytes_written += sizeof(packetheader);
                    }
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
                tsi_config.tuning_freq_hz,
                ctx.time_anchor,
                ctx.time_anchor_valid,
                processing_mode);

            output_file.write(
                reinterpret_cast<const char*>(&header), sizeof(packetheader));

            auto [payload_ptr, payload_size] = extract_payload_from_packet(packet);

            if (payload_ptr && payload_size > 0) {
                const uint8_t* write_ptr = payload_ptr;
                size_t write_size        = payload_size;

                if (processing_mode != SampleProcessingMode::NONE) {
                    size_t num_input_samples = payload_size / 4;
                    const int16_t* input_samples =
                        reinterpret_cast<const int16_t*>(payload_ptr);

                    size_t num_output_samples = process_samples(processing_mode,
                        input_samples,
                        num_input_samples,
                        processed_buffer.data());

                    write_ptr = reinterpret_cast<const uint8_t*>(processed_buffer.data());

                    if (processing_mode == SampleProcessingMode::FGB) {
                        write_size = num_output_samples * 2;
                    } else {
                        write_size = num_output_samples * 4;
                    }
                }

                output_file.write(reinterpret_cast<const char*>(write_ptr), write_size);

                if (csv_writer && csv_writer->is_open()) {
                    csv_writer->write_packet(header, write_ptr, write_size);
                }

                writer_stats.packets_written++;
                writer_stats.bytes_written += sizeof(packetheader) + write_size;
            } else {
                writer_stats.packets_written++;
                writer_stats.bytes_written += sizeof(packetheader);
            }
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

/**
 * @brief Network writer thread for parallel socket streaming
 *
 * This function runs in a separate thread and handles network streaming
 * independently from file writing. It reads from net_ring_buffer and
 * sends processed TSI packets over the network socket.
 */
void network_writer_thread(
    StreamContext& ctx, std::atomic<bool>& stop_network, NetworkWriterStats& net_stats)
{
    // Get TSI config from StreamContext (per-stream configuration)
    const TsiOutputConfig& tsi_config = ctx.tsi_config;

    net_stats.start_time = std::chrono::steady_clock::now();
    uhd::set_thread_priority_safe(0.4, true);

    const bool is_server_mode = (ctx.socket_cfg.mode == "server");

    std::cout << "[Net Writer " << ctx.stream_id << "] Started in " << ctx.socket_cfg.mode
              << " mode on port " << ctx.socket_cfg.port << std::endl;

    // Batch buffer for efficient processing
    std::vector<PacketBuffer> write_batch;
    write_batch.reserve(ctx.buffer_config.batch_write_size);

    // Sample processing configuration - now from TsiOutputConfig
    SampleProcessingMode processing_mode = SampleProcessingMode::FGB;

    // Buffer for processed samples
    constexpr size_t MAX_SAMPLES_PER_PACKET = 8192;
    std::vector<int16_t> processed_buffer(MAX_SAMPLES_PER_PACKET * 2);

    // Lambda to send a single packet
    // TODO: Make sure that this section isn't spawned each time in the new tsi_sdr_app
    // Graph and Capture Managers
    auto send_packet = [&](const PacketBuffer& pkt) -> bool {
        if (!ctx.socket_sink || !ctx.socket_sink->is_connected()) {
            return false;
        }

        // Build TSI header
        packetheader header = build_tsi_header_from_packet(pkt,
            ctx.tick_rate,
            ctx.stream_id,
            tsi_config.sat_id,
            tsi_config.tuning_freq_hz,
            ctx.time_anchor,
            ctx.time_anchor_valid,
            processing_mode);

        // Extract raw payload
        auto [payload_ptr, payload_size] = extract_payload_from_packet(pkt);

        if (!payload_ptr || payload_size == 0) {
            return true; // Skip empty packets
        }

        const uint8_t* send_ptr = payload_ptr;
        size_t send_size        = payload_size;

        // Apply sample processing if enabled
        if (processing_mode != SampleProcessingMode::NONE) {
            size_t num_input_samples     = payload_size / 4;
            const int16_t* input_samples = reinterpret_cast<const int16_t*>(payload_ptr);

            size_t num_output_samples = process_samples(processing_mode,
                input_samples,
                num_input_samples,
                processed_buffer.data());

            send_ptr  = reinterpret_cast<const uint8_t*>(processed_buffer.data());
            send_size = (processing_mode == SampleProcessingMode::FGB)
                            ? num_output_samples * 2
                            : num_output_samples * 4;
        }

        // Send: [4-byte length][28-byte TSI header][payload]
        size_t total_size = sizeof(packetheader) + send_size;
        uint32_t netlen   = htonl(static_cast<uint32_t>(total_size));

        boost::system::error_code ec;


        // Create a tx buffer
        std::vector<uint8_t> txbuf;
        txbuf.resize(total_size);

        uint8_t* p = txbuf.data();

        std::memcpy(txbuf.data(), &header, sizeof(packetheader));
        std::memcpy(txbuf.data() + sizeof(packetheader), send_ptr, send_size);


        // memcpy(p, &header, sizeof(header));
        // p += sizeof(header);
        // memcpy(p, send_ptr, send_size);

        size_t total_to_send = txbuf.size();
        // size_t total_sent = 0;

        size_t sent = 0;
        while (sent < total_to_send) {
            if (!ctx.socket_sink || !ctx.socket_sink->is_connected()) {
                return false;
            }

            ec.clear();
            ssize_t n =
                ctx.socket_sink->send(txbuf.data() + sent, total_to_send - sent, ec);

            if (n > 0) {
                sent += static_cast<size_t>(n);
                continue;
            }

            if (n == 0) {
                if (ctx.socket_cfg.drop_on_full) {
                    net_stats.packets_dropped++;
                    break;
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(ctx.socket_cfg.send_retry_delay_ms));
                continue;
            }

            // n < 0: fatal error
            net_stats.send_errors++;
            return false;
        }

        if (sent == total_to_send) {
            net_stats.packets_sent++;
            net_stats.bytes_sent += total_to_send;
        }

        return true;
    };

    // Lambda to drain old packets from buffer (used when new connection established)
    auto drain_stale_packets = [&]() {
        PacketBuffer discard;
        size_t discarded = 0;
        // Keep only the last few packets (or none) to start fresh
        while (ctx.net_ring_buffer->size() > 2) {
            if (ctx.net_ring_buffer->pop(discard)) {
                discarded++;
            }
        }
        if (discarded > 0) {
            std::cout << "[Net Writer " << ctx.stream_id << "] Discarded " << discarded
                      << " stale packets" << std::endl;
        }
    };

    // Main loop - behavior differs for server vs client mode
    while (!stop_network.load()) {
        // === SERVER MODE: Handle connection/reconnection ===
        if (is_server_mode) {
            // Check if we need to (re)establish connection
            if (!ctx.socket_sink || !ctx.socket_sink->is_connected()) {
                // Close any existing socket state
                if (ctx.socket_sink) {
                    ctx.socket_sink->close();
                }

                // Create new socket sink for accepting
                ctx.socket_sink = std::make_shared<BoostTcpSink>();

                std::cout << "[Net Writer " << ctx.stream_id
                          << "] Waiting for client connection on port "
                          << ctx.socket_cfg.port << "..." << std::endl;

                // Try to accept with short timeout so we can check stop_network
                bool connected = ctx.socket_sink->open_server(ctx.socket_cfg.port,
                    1000, // 1 second accept timeout for responsive shutdown
                    ctx.socket_cfg.nonblocking);

                if (!connected) {
                    // No connection yet, loop back and try again (or exit if stopping)
                    continue;
                }

                std::cout << "[Net Writer " << ctx.stream_id
                          << "] Client connected! Starting stream..." << std::endl;

                // Drain stale packets - start streaming from recent data
                drain_stale_packets();
            }
        }
        // === CLIENT MODE: Check connection once ===
        else {
            if (!ctx.socket_sink || !ctx.socket_sink->is_connected()) {
                std::cerr << "[Net Writer " << ctx.stream_id
                          << "] Client socket not connected, exiting" << std::endl;
                break;
            }
        }

        // === STREAMING LOOP (both modes) ===
        while (
            !stop_network.load() && ctx.socket_sink && ctx.socket_sink->is_connected()) {
            PacketBuffer packet;

            // Collect batch of packets
            write_batch.clear();
            while (write_batch.size() < ctx.buffer_config.batch_write_size
                   && ctx.net_ring_buffer->pop(packet)) {
                write_batch.push_back(std::move(packet));
            }

            if (!write_batch.empty()) {
                for (const auto& pkt : write_batch) {
                    if (!send_packet(pkt)) {
                        // Connection lost
                        std::cout << "[Net Writer " << ctx.stream_id
                                  << "] Connection lost during send" << std::endl;
                        if (ctx.socket_sink) {
                            ctx.socket_sink->close();
                        }
                        break;
                    }
                }
            } else {
                // No packets available, brief sleep
                std::this_thread::sleep_for(1ms);
            }
        }

        // If client mode and we exited streaming loop, we're done
        if (!is_server_mode) {
            break;
        }

        // Server mode: connection lost, loop back to accept new connection
        if (!stop_network.load()) {
            std::cout << "[Net Writer " << ctx.stream_id
                      << "] Connection closed, waiting for new client..." << std::endl;
        }
    }

    // Final drain on shutdown (best effort)
    if (ctx.socket_sink && ctx.socket_sink->is_connected()) {
        std::cout << "[Net Writer " << ctx.stream_id << "] Draining remaining packets..."
                  << std::endl;
        PacketBuffer packet;
        while (ctx.net_ring_buffer->pop(packet)) {
            if (!send_packet(packet))
                break;
        }
    }

    // Cleanup
    if (ctx.socket_sink) {
        ctx.socket_sink->close();
    }

    net_stats.end_time = std::chrono::steady_clock::now();
    std::cout << "[Net Writer " << ctx.stream_id << "] Thread exiting. "
              << "Sent: " << net_stats.packets_sent << " packets, "
              << net_stats.bytes_sent << " bytes" << std::endl;
}

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

                std::cout << "  Applying DDC properties for the block ID: "
                          << block_id_str << std::endl;

                for (const auto& [prop, value] : props) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(10)); // Small delay for stability
                    auto [prop_name, chan] = parse_property_with_channel(prop);

                    try {
                        if (prop_name == "freq") {
                            double freq = std::stod(properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan))));
                            ddc->set_freq(freq, chan);
                            std::cout << "  Set freq[" << chan << "] = " << freq << " Hz"
                                      << std::endl;
                        } else if (prop_name == "output_rate") {
                            double rate = std::stod(properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan))));

                            std::cout << "  Setting output rate from key "
                                      << (prop_name + "/" + std::to_string(chan))
                                      << " to: " << rate << std::endl;
                            ddc->set_output_rate(rate, chan);
                            std::cout << "  Set output_rate[" << chan << "] = " << rate
                                      << " sps" << std::endl;
                        } else if (prop_name == "input_rate") {
                            double rate = std::stod(properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan))));
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
            // Radio Block - With Daughterboard Detection and Capability Handling
            // ================================================================
            else if (block_id_str.find("Radio") != std::string::npos) {
                auto radio = graph->get_block<uhd::rfnoc::radio_control>(block_id_str);
                if (!radio) {
                    std::cerr << "  Failed to cast to Radio block control" << std::endl;
                    success = false;
                    continue;
                }

                // =========================================================================
                // DAUGHTERBOARD DETECTION
                // =========================================================================

                struct DaughterboardCapabilities
                {
                    std::string name            = "Unknown";
                    bool has_gain_control       = true;
                    bool has_bandwidth_control  = true;
                    bool has_dc_offset_control  = true;
                    bool has_iq_balance_control = true;
                    bool has_agc                = false;
                    bool has_lo_export          = false;
                    bool has_frequency_tuning = true; // Real LO tuning vs. just metadata
                    double min_gain           = 0.0;
                    double max_gain           = 0.0;
                    double gain_step          = 0.0;
                    std::vector<std::string> available_antennas;
                };

                // Lambda to detect daughterboard capabilities per channel
                auto detect_daughterboard =
                    [&radio](size_t chan) -> DaughterboardCapabilities {
                    DaughterboardCapabilities caps;

                    try {
                        // Get available antennas - this is always available
                        caps.available_antennas = radio->get_rx_antennas(chan);

                        // Get gain range to determine if gain control exists
                        auto gain_range = radio->get_rx_gain_range(chan);


                        caps.min_gain  = gain_range.start();
                        caps.max_gain  = gain_range.stop();
                        caps.gain_step = gain_range.step();


                        // Determine daughterboard type based on characteristics
                        bool has_meaningful_gain = (caps.max_gain - caps.min_gain) > 1.0;

                        // Check antenna names for hints
                        bool has_basicrx_antennas  = false;
                        bool has_twinrx_antennas   = false;
                        bool has_standard_antennas = false;

                        for (const auto& ant : caps.available_antennas) {
                            if (ant == "A" || ant == "B" || ant == "AB" || ant == "BA") {
                                has_basicrx_antennas = true;
                            }
                            if (ant == "RX1" || ant == "RX2") {
                                has_twinrx_antennas = true;
                            }
                            if (ant == "TX/RX" || ant == "RX2") {
                                has_standard_antennas = true;
                            }
                        }

                        auto sensors = radio->get_rx_sensor_names(chan);
                        bool has_lo_locked =
                            std::find(sensors.begin(), sensors.end(), "lo_locked")
                            != sensors.end();

                        // Classify the daughterboard
                        if (has_basicrx_antennas && !has_lo_locked) {
                            caps.name                   = "BasicRX";
                            caps.has_gain_control       = false;
                            caps.has_bandwidth_control  = false;
                            caps.has_dc_offset_control  = false;
                            caps.has_iq_balance_control = false;
                            caps.has_agc                = false;
                            caps.has_frequency_tuning =
                                false; // BasicRX has no LO - frequency is metadata only
                        } else if (has_twinrx_antennas) {
                            caps.name                   = "TwinRX";
                            caps.has_gain_control       = true;
                            caps.has_bandwidth_control  = true;
                            caps.has_dc_offset_control  = true;
                            caps.has_iq_balance_control = true;
                            caps.has_agc              = false; // TwinRX doesn't have AGC
                            caps.has_lo_export        = true;
                            caps.has_frequency_tuning = true;
                        } else {
                            caps.name = "Unknown or not supported";
                            // Assume full capabilities, let errors guide
                        }

                    } catch (const std::exception& e) {
                        std::cerr << "  Warning: Could not fully detect daughterboard "
                                     "capabilities: "
                                  << e.what() << std::endl;
                    }

                    return caps;
                };

                // Detect capabilities for channel 0 (representative of the daughterboard)
                auto db_caps = detect_daughterboard(0);

                std::cout << "\n  === Radio Block " << block_id_str
                          << " ===" << std::endl;
                std::cout << "  Detected Daughterboard: " << db_caps.name << std::endl;
                std::cout << "  Capabilities:" << std::endl;
                std::cout << "    Gain Control:      "
                          << (db_caps.has_gain_control ? "Yes" : "No");
                if (db_caps.has_gain_control) {
                    std::cout << " (" << db_caps.min_gain << " to " << db_caps.max_gain
                              << " dB)";
                }
                std::cout << std::endl;
                std::cout << "    Bandwidth Control: "
                          << (db_caps.has_bandwidth_control ? "Yes" : "No") << std::endl;
                std::cout << "    DC Offset Control: "
                          << (db_caps.has_dc_offset_control ? "Yes" : "No") << std::endl;
                std::cout << "    IQ Balance:        "
                          << (db_caps.has_iq_balance_control ? "Yes" : "No") << std::endl;
                std::cout << "    Frequency Tuning:  "
                          << (db_caps.has_frequency_tuning ? "Yes (has LO)"
                                                           : "No (metadata only)")
                          << std::endl;
                std::cout << "    Available Antennas: ";
                for (const auto& ant : db_caps.available_antennas) {
                    std::cout << "\"" << ant << "\" ";
                }
                std::cout << std::endl;

                std::cout << "\n  Applying properties:" << std::endl;
                std::cout << "  ----------------------------------------" << std::endl;

                for (const auto& [prop, value] : props) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    auto [prop_name, chan] = parse_property_with_channel(prop);

                    // Re-detect for this specific channel if different from channel 0
                    DaughterboardCapabilities chan_caps =
                        (chan == 0) ? db_caps : detect_daughterboard(chan);

                    try {
                        if (prop_name == "antenna") {
                            // Validate antenna name before setting
                            bool antenna_valid = false;
                            for (const auto& valid_ant : chan_caps.available_antennas) {
                                if (valid_ant == value) {
                                    antenna_valid = true;
                                    break;
                                }
                            }

                            if (!antenna_valid) {
                                std::cerr << "  WARNING: Antenna \"" << value
                                          << "\" not in available list for channel "
                                          << chan << std::endl;
                                std::cerr << "           Available: ";
                                for (const auto& ant : chan_caps.available_antennas) {
                                    std::cerr << "\"" << ant << "\" ";
                                }
                                std::cerr << std::endl;
                                std::cerr << "           Attempting to set anyway..."
                                          << std::endl;
                            }
                            auto temp_value =
                                properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan)));
                            radio->set_rx_antenna(temp_value, chan);
                            std::string actual = radio->get_rx_antenna(chan);
                            std::cout << "  Set antenna[" << chan << "] = \"" << value
                                      << "\"";
                            if (actual != value) {
                                std::cout << " (actual: \"" << actual
                                          << "\" - MISMATCH!)";
                                success = false;
                            }
                            std::cout << std::endl;
                        } else if (prop_name == "freq" || prop_name == "frequency") {
                            double freq = std::stod(properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan))));

                            if (!chan_caps.has_frequency_tuning) {
                                std::cout << "  Note: " << chan_caps.name
                                          << " has no LO - frequency " << freq / 1e6
                                          << " MHz is metadata only (no actual tuning)"
                                          << std::endl;
                                continue;
                            }

                            radio->set_rx_frequency(freq, chan);
                            double actual = radio->get_rx_frequency(chan);
                            std::cout << "  Set frequency[" << chan
                                      << "] = " << freq / 1e6 << " MHz";
                            if (chan_caps.has_frequency_tuning) {
                                std::cout << " (actual: " << actual / 1e6 << " MHz)";
                            }
                            std::cout << std::endl;
                        } else if (prop_name == "gain") {
                            if (!chan_caps.has_gain_control) {
                                std::cout
                                    << "  SKIP: gain[" << chan << "] - " << chan_caps.name
                                    << " has no gain control (fixed gain)" << std::endl;
                                continue;
                            }

                            double gain = std::stod(properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan))));

                            // Clamp to valid range
                            if (gain < chan_caps.min_gain) {
                                std::cout << "  Warning: Requested gain " << gain
                                          << " dB below minimum, clamping to "
                                          << chan_caps.min_gain << " dB" << std::endl;
                                gain = chan_caps.min_gain;
                            }
                            if (gain > chan_caps.max_gain) {
                                std::cout << "  Warning: Requested gain " << gain
                                          << " dB above maximum, clamping to "
                                          << chan_caps.max_gain << " dB" << std::endl;
                                gain = chan_caps.max_gain;
                            }

                            radio->set_rx_gain(gain, chan);
                            double actual = radio->get_rx_gain(chan);
                            std::cout << "  Set gain[" << chan << "] = " << gain << " dB"
                                      << " (actual: " << actual << " dB)" << std::endl;
                        } else if (prop_name == "bandwidth" || prop_name == "bw") {
                            if (!chan_caps.has_bandwidth_control) {
                                std::cout << "  SKIP: bandwidth[" << chan << "] - "
                                          << chan_caps.name << " has no bandwidth control"
                                          << std::endl;
                                continue;
                            }

                            double bw = std::stod(properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan))));
                            radio->set_rx_bandwidth(bw, chan);
                            double actual = radio->get_rx_bandwidth(chan);
                            std::cout << "  Set bandwidth[" << chan << "] = " << bw / 1e6
                                      << " MHz"
                                      << " (actual: " << actual / 1e6 << " MHz)"
                                      << std::endl;
                        } else if (prop_name == "rate" || prop_name == "sample_rate") {
                            double rate = std::stod(properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan))));
                            radio->set_rate(rate);
                            double actual = radio->get_rate();
                            std::cout << "  Set sample_rate = " << rate / 1e6 << " Msps"
                                      << " (actual: " << actual / 1e6 << " Msps)"
                                      << std::endl;
                        } else if (prop_name == "dc_offset"
                                   || prop_name == "dc_offset_enabled") {
                            if (!chan_caps.has_dc_offset_control) {
                                std::cout << "  SKIP: dc_offset[" << chan << "] - "
                                          << chan_caps.name << " has no DC offset control"
                                          << std::endl;
                                continue;
                            }

                            auto temp_value =
                                properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan)));
                            bool enable = (temp_value == "true" || temp_value == "1"
                                           || temp_value == "on");
                            radio->set_rx_dc_offset(enable, chan);
                            std::cout << "  Set dc_offset[" << chan
                                      << "] = " << (enable ? "enabled" : "disabled")
                                      << std::endl;
                        } else if (prop_name == "iq_balance"
                                   || prop_name == "iq_balance_enabled") {
                            if (!chan_caps.has_iq_balance_control) {
                                std::cout << "  SKIP: iq_balance[" << chan << "] - "
                                          << chan_caps.name
                                          << " has no IQ balance control" << std::endl;
                                continue;
                            }

                            auto temp_value =
                                properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan)));
                            bool enable = (temp_value == "true" || temp_value == "1"
                                           || temp_value == "on");
                            radio->set_rx_iq_balance(enable, chan);
                            std::cout << "  Set iq_balance[" << chan
                                      << "] = " << (enable ? "enabled" : "disabled")
                                      << std::endl;
                        } else if (prop_name == "agc" || prop_name == "agc_mode") {
                            if (!chan_caps.has_agc) {
                                std::cout << "  SKIP: agc[" << chan << "] - "
                                          << chan_caps.name << " has no AGC support"
                                          << std::endl;
                                continue;
                            }

                            auto temp_value =
                                properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan)));
                            bool enable = (temp_value == "true" || temp_value == "1"
                                           || temp_value == "on");
                            radio->set_rx_agc(enable, chan);
                            std::cout << "  Set agc[" << chan
                                      << "] = " << (enable ? "enabled" : "disabled")
                                      << std::endl;
                        } else if (prop_name == "lo_export"
                                   || prop_name == "lo_export_enabled") {
                            if (!chan_caps.has_lo_export) {
                                std::cout << "  SKIP: lo_export[" << chan << "] - "
                                          << chan_caps.name
                                          << " has no LO export capability" << std::endl;
                                continue;
                            }

                            auto temp_value =
                                properties.at(block_id_str)
                                    .at((prop_name + "/" + std::to_string(chan)));
                            bool enable = (temp_value == "true" || temp_value == "1"
                                           || temp_value == "on");
                            // Note: LO export requires specific UHD API calls
                            // radio->set_rx_lo_export_enabled(enable, "all", chan);
                            std::cout << "  Note: LO export configuration requires "
                                         "additional implementation"
                                      << std::endl;
                        } else {
                            std::cout << "  SKIP: Unknown or unsupported Radio property: "
                                      << prop_name << std::endl;
                        }
                    } catch (const uhd::key_error& e) {
                        std::cerr << "  ERROR: Property '" << prop_name
                                  << "' not supported on " << chan_caps.name << ": "
                                  << e.what() << std::endl;
                    } catch (const uhd::value_error& e) {
                        std::cerr << "  ERROR: Invalid value for " << prop_name << ": "
                                  << e.what() << std::endl;
                        success = false;
                    } catch (const uhd::runtime_error& e) {
                        std::cerr << "  ERROR: Runtime error setting " << prop_name
                                  << ": " << e.what() << std::endl;
                        success = false;
                    } catch (const std::exception& e) {
                        std::cerr << "  ERROR: Failed to set " << prop_name << ": "
                                  << e.what() << std::endl;
                        success = false;
                    }
                }

                // =========================================================================
                // Final Configuration Summary
                // =========================================================================
                std::cout << "\n  === Final Radio Configuration ===" << std::endl;
                size_t num_channels = radio->get_num_output_ports();
                for (size_t ch = 0; ch < num_channels; ++ch) {
                    auto ch_caps = (ch == 0) ? db_caps : detect_daughterboard(ch);

                    std::cout << "  Channel " << ch << " (" << ch_caps.name
                              << "):" << std::endl;
                    try {
                        std::cout << "    Antenna:    \"" << radio->get_rx_antenna(ch)
                                  << "\"" << std::endl;
                        std::cout
                            << "    Frequency:  " << radio->get_rx_frequency(ch) / 1e6
                            << " MHz";
                        if (!ch_caps.has_frequency_tuning) {
                            std::cout << " (metadata only)";
                        }
                        std::cout << std::endl;

                        if (ch_caps.has_gain_control) {
                            std::cout << "    Gain:       " << radio->get_rx_gain(ch)
                                      << " dB" << std::endl;
                        } else {
                            std::cout << "    Gain:       N/A (fixed)" << std::endl;
                        }

                        if (ch_caps.has_bandwidth_control) {
                            std::cout
                                << "    Bandwidth:  " << radio->get_rx_bandwidth(ch) / 1e6
                                << " MHz" << std::endl;
                        } else {
                            std::cout << "    Bandwidth:  N/A (wideband)" << std::endl;
                        }
                    } catch (const std::exception& e) {
                        std::cerr << "    (Error reading channel config: " << e.what()
                                  << ")" << std::endl;
                    }
                }
                std::cout << "  Sample Rate: " << radio->get_rate() / 1e6 << " Msps"
                          << std::endl;
                std::cout << "  ================================================\n"
                          << std::endl;
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
    FileWriterStats& writer_stats)
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

        // Build PacketBuffer
        PacketBuffer packet_buffer;
        packet_buffer.stream_id     = ctx.stream_id;
        packet_buffer.packet_number = ctx.stats.packets_captured;

        size_t payload_bytes    = num_rx_samps * sizeof(samp_type);
        size_t header_bytes     = 8;
        size_t timestamp_bytes  = md.has_time_spec ? 8 : 0;
        size_t total_chdr_bytes = header_bytes + timestamp_bytes + payload_bytes;

        packet_buffer.data.reserve(total_chdr_bytes);

        // Build CHDR header
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

        // Also push to network ring buffer if enabled (copy, not move)
        if (ctx.net_ring_buffer) {
            PacketBuffer net_copy;
            net_copy.stream_id     = ctx.stream_id;
            net_copy.packet_number = ctx.stats.packets_captured;
            net_copy.has_timestamp = md.has_time_spec;
            net_copy.timestamp     = md.time_spec;
            net_copy.data =
                packet_buffer.data; // This is a copy since packet_buffer was moved

            // Re-read from original buffer
            net_copy.data.clear();
            net_copy.data.reserve(total_chdr_bytes);

            // Rebuild header
            for (size_t i = 0; i < sizeof(uint64_t); i++) {
                net_copy.data.push_back((header >> (i * 8)) & 0xFF);
            }
            if (md.has_time_spec) {
                uint64_t timestamp_ticks = md.time_spec.to_ticks(ctx.tick_rate);
                for (size_t i = 0; i < sizeof(uint64_t); i++) {
                    net_copy.data.push_back((timestamp_ticks >> (i * 8)) & 0xFF);
                }
            }
            net_copy.data.insert(
                net_copy.data.end(), sample_bytes, sample_bytes + payload_bytes);

            if (!ctx.net_ring_buffer->push(std::move(net_copy))) {
                // Network buffer overflow - drop packet
            }
        }

        ctx.stats.packets_captured++;
        ctx.stats.total_samples += num_rx_samps;
    }

    // Stop streaming
    uhd::stream_cmd_t stop_cmd(uhd::stream_cmd_t::STREAM_MODE_STOP_CONTINUOUS);
    ctx.rx_streamer->issue_stream_cmd(stop_cmd);

    // Signal writer thread to finish
    stop_writing.store(true);

    ctx.stats.end_time = std::chrono::steady_clock::now();

    double duration =
        std::chrono::duration<double>(ctx.stats.end_time - ctx.stats.start_time).count();

    std::cout << "[Stream " << ctx.stream_id << "] Capture complete."
              << " Packets: " << ctx.stats.packets_captured
              << ", Samples: " << ctx.stats.total_samples << ", Duration: " << std::fixed
              << std::setprecision(2) << duration << "s"
              << ", Overflows: " << ctx.stats.overflow_count
              << ", Buffer overflows: " << ctx.stats.buffer_overflows << std::endl;
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
    const TimeAnchor& time_anchor,
    bool time_anchor_valid)
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

    // Get tick rate and default tuning frequency from radio blocks
    double tick_rate             = DEFAULT_TICKRATE;
    uint32_t default_tuning_freq = 0;

    auto radio_blocks = get_configured_radio_block_ids(graph, config);

    if (!radio_blocks.empty()) {
        std::cout << "\nUsing " << radio_blocks.size()
                  << " configured Radio block(s):" << std::endl;
        for (const auto& radio_id : radio_blocks) {
            std::cout << "  - " << radio_id.to_string() << std::endl;
        }

        auto radio = graph->get_block<uhd::rfnoc::radio_control>(radio_blocks[0]);
        tick_rate  = radio->get_tick_rate();

        try {
            default_tuning_freq = static_cast<uint32_t>(radio->get_rx_frequency(0));
        } catch (...) {
        }
    } else {
        std::cout << "\nNo Radio blocks configured - using default tick rate"
                  << std::endl;
    }

    // Create contexts
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

            // Find matching stream endpoint config and extract per-stream settings
            size_t stream_spp = samps_per_buff;
            TsiOutputConfig stream_tsi_config; // Per-stream TSI config
            SocketConfig socket_cfg_for_stream;

            for (const auto& sep : config.stream_endpoints) {
                if (sep.block_id == block_id && sep.port == port) {
                    // Extract stream args
                    for (const auto& [key, value] : sep.stream_args) {
                        stream_args.args[key] = value;
                        if (key == "spp" || key == "samples_per_packet") {
                            try {
                                stream_spp = std::stoul(value);
                                std::cout << "[TSI Stream " << i
                                          << "] Using per-stream spp=" << stream_spp
                                          << " from config for " << block_id << ":"
                                          << port << std::endl;
                            } catch (...) {
                                std::cerr << "[TSI Stream " << i
                                          << "] Invalid spp value: " << value
                                          << ", using default " << samps_per_buff
                                          << std::endl;
                            }
                        }
                    }

                    // Extract per-stream TSI configuration
                    stream_tsi_config = sep.tsi_config;

                    // Apply default tuning frequency if not specified
                    if (stream_tsi_config.tuning_freq_hz == 0) {
                        stream_tsi_config.tuning_freq_hz = default_tuning_freq;
                    }

                    // Log TSI config for this stream
                    if (stream_tsi_config.enabled
                        || stream_tsi_config.sample_processing_mode
                               != SampleProcessingMode::NONE) {
                        std::cout << "[TSI Stream " << i << "] TSI config: "
                                  << "sat_id=" << stream_tsi_config.sat_id
                                  << ", freq=" << stream_tsi_config.tuning_freq_hz
                                  << ", processing="
                                  << sample_processing_mode_to_string(
                                         stream_tsi_config.sample_processing_mode)
                                  << std::endl;
                    }

                    // Extract socket config
                    socket_cfg_for_stream = sep.socket_cfg;
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

            // Create StreamContext for TSI capture
            StreamContext ctx;
            ctx.stream_id         = i + 1;
            ctx.block_id          = block_id;
            ctx.port              = port;
            ctx.rx_streamer       = rx_streamer;
            ctx.stats.stream_id   = i;
            ctx.stats.block_id    = block_id;
            ctx.stats.port        = port;
            ctx.analysis_packets  = enable_analysis ? &all_analysis_packets : nullptr;
            ctx.analysis_mutex    = &analysis_mutex;
            ctx.tick_rate         = tick_rate;
            ctx.samps_per_buff    = stream_spp;
            ctx.pps_reset_time    = pps_reset_time;
            ctx.pps_reset_used    = pps_reset_used;
            ctx.time_anchor       = time_anchor;
            ctx.time_anchor_valid = time_anchor_valid;
            ctx.buffer_config     = config.multi_stream.buffer_config;

            // Set per-stream TSI configuration (includes sample_processing_mode)
            ctx.tsi_config = stream_tsi_config;

            // Set socket config
            ctx.socket_cfg = socket_cfg_for_stream;

            // Open socket sink if enabled
            if (ctx.socket_cfg.enabled) {
                try {
                    ctx.socket_sink = std::make_shared<BoostTcpSink>();
                    bool opened     = false;

                    if (ctx.socket_cfg.mode == "client") {
                        opened = ctx.socket_sink->open_client(ctx.socket_cfg.host,
                            ctx.socket_cfg.port,
                            ctx.socket_cfg.connect_timeout_ms,
                            ctx.socket_cfg.nonblocking);
                        if (opened) {
                            std::cout << "[stream " << ctx.stream_id
                                      << "] BoostTcpSink connected to "
                                      << ctx.socket_cfg.host << ":" << ctx.socket_cfg.port
                                      << "\n";
                        } else {
                            std::cerr << "[stream " << ctx.stream_id
                                      << "] BoostTcpSink open_client failed to "
                                      << ctx.socket_cfg.host << ":" << ctx.socket_cfg.port
                                      << "\n";
                        }
                    } else if (ctx.socket_cfg.mode == "server") {
                        ctx.socket_sink = std::make_shared<BoostTcpSink>();
                        opened          = true;
                        std::cout
                            << "[stream " << ctx.stream_id
                            << "] Server mode - will accept connections in network thread"
                            << std::endl;
                    } else {
                        std::cerr << "[stream " << ctx.stream_id
                                  << "] Unknown socket mode: " << ctx.socket_cfg.mode
                                  << "\n";
                    }

                    if (!opened) {
                        ctx.socket_sink.reset();
                    }
                } catch (const std::exception& ex) {
                    std::cerr << "[stream " << ctx.stream_id
                              << "] BoostTcpSink exception: " << ex.what() << "\n";
                    ctx.socket_sink.reset();
                }
            }

            // Generate output filename
            auto cwd             = std::filesystem::current_path();
            const char* env_temp = std::getenv("TEMPSTR_DEFINE");
            std::string temp_str = env_temp ? env_temp : "";
            if (temp_str.empty()) {
                temp_str = TEMPSTR_DEFINE;
            }
            auto fileTime =
                std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

            // Convert to local time
            std::tm local_tm{};
#if defined(_WIN32)
            localtime_s(&local_tm, &fileTime);
#else
            localtime_r(&fileTime, &local_tm);
#endif

            int floored_hr = (local_tm.tm_hour / 4) * 4;

            local_tm.tm_hour = floored_hr;
            local_tm.tm_min  = 0;
            local_tm.tm_sec  = 0;
            fileTime         = std::mktime(&local_tm);

            auto tsi_filename =
                temp_str + "/rawdata_" + std::to_string(ctx.stream_id) + "_"
                + TimeConverter::TimeTToString("%Y%m%d_%H%M%S", fileTime) + ".bin";
            ctx.output_filename = tsi_filename;

            // Calculate ring buffer size
            const size_t bytes_per_samp    = sizeof(samp_type);
            const size_t est_payload_bytes = stream_spp * bytes_per_samp;
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

            // Create network ring buffer if socket is enabled
            bool need_net_buffer =
                ctx.socket_cfg.enabled && ctx.socket_sink
                && (ctx.socket_cfg.mode == "server" || ctx.socket_sink->is_connected());

            if (need_net_buffer) {
                ctx.net_ring_buffer =
                    std::make_shared<SPSCRingBuffer<PacketBuffer>>(power_of_2);
                std::cout << "[stream " << ctx.stream_id
                          << "] Network ring buffer created (size: " << power_of_2 << ")"
                          << std::endl;
            }

            contexts.push_back(std::move(ctx));

        } catch (const uhd::rfnoc_error& e) {
            std::cerr << "RFNoC error setting up stream " << i << ": " << e.what()
                      << std::endl;
        } catch (const uhd::exception& e) {
            std::cerr << "UHD Error: " << block_id << ": " << e.what() << std::endl;
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

    // Start TSI writer threads - NO LONGER PASSES GLOBAL TSI CONFIG
    std::vector<std::unique_ptr<std::thread>> writer_threads;
    for (size_t i = 0; i < contexts.size(); ++i) {
        writer_threads.push_back(std::make_unique<std::thread>(tsi_file_writer_thread,
            std::ref(contexts[i]),
            std::ref(stop_writing_flags[i]),
            std::ref(writer_stats[i])));
    }

    // Start network writer threads - NO LONGER PASSES GLOBAL TSI CONFIG
    std::vector<std::atomic<bool>> stop_network_flags(contexts.size());
    std::vector<NetworkWriterStats> net_stats(contexts.size());
    std::vector<std::unique_ptr<std::thread>> network_threads;

    for (size_t i = 0; i < contexts.size(); ++i) {
        if (contexts[i].socket_cfg.enabled && contexts[i].socket_sink
            && contexts[i].net_ring_buffer) {
            stop_network_flags[i].store(false);
            contexts[i].stop_network = &stop_network_flags[i];
            network_threads.push_back(std::make_unique<std::thread>(network_writer_thread,
                std::ref(contexts[i]),
                std::ref(stop_network_flags[i]),
                std::ref(net_stats[i])));
        } else {
            network_threads.push_back(nullptr);
        }
    }

    // Start capture threads - NO LONGER PASSES GLOBAL TSI CONFIG
    std::vector<std::thread> capture_threads;
    std::atomic<bool> start_capture(false);

    for (size_t i = 0; i < contexts.size(); ++i) {
        capture_threads.emplace_back(capture_stream_ringbuffer_tsi<samp_type>,
            std::ref(contexts[i]),
            std::ref(start_capture),
            std::ref(stop_writing_flags[i]),
            num_packets,
            std::ref(writer_stats[i]));
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

    // Signal network threads to stop
    for (size_t i = 0; i < contexts.size(); ++i) {
        stop_network_flags[i].store(true);
    }

    // Wait for writer threads
    std::cout << "\nWaiting for TSI writer threads to finish..." << std::endl;
    for (auto& thread : writer_threads) {
        if (thread && thread->joinable()) {
            thread->join();
        }
    }

    // Wait for network threads
    std::cout << "Waiting for network writer threads to finish..." << std::endl;
    for (auto& thread : network_threads) {
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
        // size_t dot         = tsi_fn.rfind('.');
        // if (dot != std::string::npos)
        //     tsi_fn.insert(dot, "_tsi");
        // else
        //     tsi_fn += "_tsi.dat";

        std::cout << "  Stream " << i << " (" << contexts[i].block_id << ":"
                  << contexts[i].port << "):" << std::endl;
        std::cout << "    File Packets: " << writer_stats[i].packets_written << std::endl;
        std::cout << "    File Bytes: " << writer_stats[i].bytes_written << std::endl;
        std::cout << "    Max buffer: " << contexts[i].stats.max_buffer_usage << " / "
                  << contexts[i].ring_buffer->capacity() << std::endl;
        std::cout << "    Output: " << tsi_fn << std::endl;

        // Print network statistics if socket was enabled
        if (contexts[i].socket_cfg.enabled && contexts[i].net_ring_buffer) {
            std::cout << "    Network Packets: " << net_stats[i].packets_sent
                      << std::endl;
            std::cout << "    Network Bytes: " << net_stats[i].bytes_sent << std::endl;
            if (net_stats[i].packets_dropped > 0) {
                std::cout << "    Network Dropped: " << net_stats[i].packets_dropped
                          << std::endl;
            }
            if (net_stats[i].send_errors > 0) {
                std::cout << "    Network Errors: " << net_stats[i].send_errors
                          << std::endl;
            }
        }
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
    FileWriterStats&/*,
    const TsiOutputConfig&*/);
template void capture_stream_ringbuffer_tsi<std::complex<float>>(StreamContext&,
    std::atomic<bool>&,
    std::atomic<bool>&,
    size_t,
    FileWriterStats&/*,
    const TsiOutputConfig&*/);
template void capture_stream_ringbuffer_tsi<std::complex<double>>(StreamContext&,
    std::atomic<bool>&,
    std::atomic<bool>&,
    size_t,
    FileWriterStats&/*,
    const TsiOutputConfig&*/);

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
    /*const TsiOutputConfig&,*/
    const TimeAnchor&,
    bool);
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
    /*const TsiOutputConfig&,*/
    const TimeAnchor&,
    bool);
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
    /*const TsiOutputConfig&,*/
    const TimeAnchor&,
    bool);

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
                    constexpr size_t BYTES_PER_COMPLEX = 4;

                    if (pkt.data.size() % BYTES_PER_COMPLEX != 0) {
                        std::cerr << "[Writer " << ctx.stream_id
                                  << "] Payload misaligned\n";
                        continue;
                    }

                    const size_t num_complex = pkt.data.size() / BYTES_PER_COMPLEX;
                    const size_t out_complex = num_complex / 2;

                    std::vector<uint8_t> decimated_payload(
                        out_complex * BYTES_PER_COMPLEX);

                    const uint8_t* in = pkt.data.data();
                    uint8_t* out      = decimated_payload.data();

                    for (size_t i = 0; i < out_complex; ++i) {
                        // Copy every 2nd complex sample
                        // Source index = 2*i
                        std::memcpy(out + i * BYTES_PER_COMPLEX,
                            in + (2 * i) * BYTES_PER_COMPLEX,
                            BYTES_PER_COMPLEX);
                    }

                    uint32_t pkt_size = static_cast<uint32_t>(decimated_payload.size());

                    output_file.write(
                        reinterpret_cast<const char*>(&pkt_size), sizeof(pkt_size));

                    output_file.write(
                        reinterpret_cast<const char*>(decimated_payload.data()),
                        decimated_payload.size());

                    writer_stats.packets_written++;
                    writer_stats.bytes_written +=
                        sizeof(pkt_size) + decimated_payload.size();
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

        // Stream endpoints - NOW INCLUDES PER-STREAM TSI CONFIG
        if (root["stream_endpoints"]) {
            for (const auto& sep : root["stream_endpoints"]) {
                StreamEndpointConfig sec;
                sec.block_id    = sep["block_id"].as<std::string>();
                sec.port        = sep["port"].as<size_t>(0);
                sec.direction   = sep["direction"].as<std::string>("rx");
                sec.enabled     = sep["enabled"].as<bool>(true);
                sec.stream_name = sep["name"].as<std::string>("");
                sec.socket_cfg  = parse_socket_config(sep);

                if (sep["stream_args"]) {
                    for (const auto& arg : sep["stream_args"]) {
                        sec.stream_args[arg.first.as<std::string>()] =
                            arg.second.as<std::string>();
                    }
                }

                // Parse per-stream TSI configuration (includes sample_processing_mode)
                sec.tsi_config = parse_tsi_config(sep);

                // Log if TSI config is enabled for this stream
                if (sec.tsi_config.enabled) {
                    std::cout << "  Stream endpoint " << sec.block_id << ":" << sec.port
                              << " TSI config enabled";
                    if (sec.tsi_config.sample_processing_mode
                        != SampleProcessingMode::NONE) {
                        std::cout << ", processing mode: "
                                  << sample_processing_mode_to_string(
                                         sec.tsi_config.sample_processing_mode);
                    }
                    std::cout << std::endl;
                }

                config.stream_endpoints.push_back(sec);
            }
        }

        // Block properties
        if (root["block_properties"]) {
            for (const auto& block : root["block_properties"]) {
                std::string block_id = block.first.as<std::string>();
                for (const auto& prop : block.second) {
                    std::cout << "Setting property " << prop.first.as<std::string>()
                              << " for block " << block_id << " to "
                              << prop.second.as<std::string>() << std::endl;
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

        // Tsi format config
        // if (root["tsi_format"]) {
        //     config.tsi_output.enabled = root["tsi_format"]["enabled"].as<bool>(false);
        //     config.tsi_output.sat_id  = root["tsi_format"]["sat_id"].as<uint16_t>(0);
        //     config.tsi_output.include_file_header =
        //         root["tsi_format"]["include_file_header"].as<bool>(true);
        //     config.tsi_output.tuning_freq_hz =
        //         root["tsi_format"]["tuning_freq_hz"].as<int64_t>(0);
        //     config.tsi_output.csv_max_packets =
        //         root["tsi_format"]["csv_max_packets"].as<size_t>(2000);
        //     config.tsi_output.csv_samples_per_packet =
        //         root["tsi_format"]["csv_samples_per_packet"].as<size_t>(8);
        // }


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

void analyze_and_log_timestamp(const chdr_packet_data& pkt,
    uint64_t first_pkt_offset,
    double tick_rate,
    std::ostream& csv)
{
    if (!pkt.has_timestamp) {
        csv << "N/A,N/A,N/A";
        return;
    }

    // 1. Convert raw ticks → PPS-relative ticks (signed)
    const int64_t pps_relative_ticks =
        static_cast<int64_t>(pkt.timestamp) - static_cast<int64_t>(first_pkt_offset);

    // 2. Convert ticks → UHD time_spec_t
    //    This handles normalization and rollover correctly
    const uhd::time_spec_t ts =
        uhd::time_spec_t::from_ticks(pps_relative_ticks, tick_rate);

    // 3. Extract canonical components
    const int64_t timestamp_sec = ts.get_full_secs();

    const double time_since_pps = ts.get_frac_secs(); // ∈ [0,1)

    // 4. Optional: PPS-relative tick index for debugging / CSV
    const uint64_t temp_timestamp = static_cast<uint64_t>(ts.get_frac_secs() * tick_rate);

    // 5. Log
    csv << temp_timestamp << "," << std::fixed << std::setprecision(12) << timestamp_sec
        << "," << time_since_pps;
}

// Unified Analysis Function
void analyze_packets_unified(const std::vector<chdr_packet_data>& packets,
    const std::string& csv_file,
    double tick_rate,
    const std::vector<StreamStats>& stream_stats,
    uhd::time_spec_t pps_reset_time,
    bool pps_reset_used,
    size_t samps_per_buff,
    double rate)
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
    // CRITICAL: Use actual tick_rate instead of DEFAULT_TICKRATE (200MHz)
    // to correctly handle devices running at different clock rates (e.g., 100MHz)
    uint64_t first_pkt_offset       = 0;
    uint64_t first_pkt_sec_ticks    = 0;
    const uint64_t ticks_per_second = static_cast<uint64_t>(tick_rate);
    std::cout << "Here's the unmodified tick values for packet 0: "
              << packets[0].timestamp << " (tick_rate=" << tick_rate << "Hz)\n\n"
              << std::endl;
    if (pps_reset_used && !packets.empty() && packets[0].has_timestamp
        && packets[0].timestamp > ticks_per_second) {
        first_pkt_offset    = packets[0].timestamp % ticks_per_second;
        first_pkt_sec_ticks = packets[0].timestamp / ticks_per_second;
    }

    double samps_per_sec_num = samps_per_buff * (tick_rate / rate);

    // Analyze each packet
    for (size_t i = 0; i < packets.size(); ++i) {
        const auto& pkt = packets[i];
        // Detect second rollover: fractional ticks wrapped around
        // Use actual ticks_per_second instead of DEFAULT_TICKRATE for correct detection
        const uint64_t curr_frac_ticks = pkt.timestamp % ticks_per_second;
        const uint64_t prev_frac_ticks =
            (packets[i == 0 ? 0 : (i - 1)].timestamp) % ticks_per_second;
        if (curr_frac_ticks < prev_frac_ticks && i > 0) {
            // Second boundary crossed - adjust first_pkt_offset for new second
            std::cout << "Second rollover detected at packet " << i
                      << " (prev_frac=" << prev_frac_ticks
                      << ", curr_frac=" << curr_frac_ticks << ")" << std::endl;
            first_pkt_offset = (pkt.timestamp % ticks_per_second);
        }
        csv << i << "," // Column: packet_num
            << pkt.stream_id << "," // Column: stream_id
            << "\"" << pkt.stream_block << "\"," // Column: stream_block
            << pkt.stream_port << "," // Column: stream_port
            << std::hex << "0x" << std::setw(2) << std::setfill('0') << (int)pkt.vc
            << "," // Column: vc
            << std::dec << (pkt.eob ? "1" : "0") << "," // Column: eob
            << (pkt.eov ? "1" : "0") << "," // Column: eov
            << std::hex << "0x" << (int)pkt.pkt_type << "," // Column: pkt_type
            << pkt.pkt_type_str() << "," // Column: pkt_type_str
            << std::dec << (int)pkt.num_mdata << "," // Column: num_mdata
            << pkt.seq_num << "," // Column: seq_num
            << pkt.length << "," // Column: length
            << std::hex << "0x" << std::setw(4) << std::setfill('0') << pkt.dst_epid
            << "," // Column: dst_epid
            << std::dec << (pkt.has_timestamp ? "1" : "0")
            << ","; // Column: has_timestamp
        int64_t temp_timestamp = 0;
        if (pkt.has_timestamp) {
            analyze_and_log_timestamp(pkt, first_pkt_offset, tick_rate, csv);
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

            // Find matching stream endpoint config and extract per-stream settings
            size_t stream_spp = samps_per_buff; // Default to global samps_per_buff
            SampleProcessingMode stream_processing_mode = SampleProcessingMode::NONE;
            for (const auto& sep : config.stream_endpoints) {
                if (sep.block_id == block_id && sep.port == port) {
                    for (const auto& [key, value] : sep.stream_args) {
                        stream_args.args[key] = value;
                        // Check for per-stream spp configuration
                        if (key == "spp" || key == "samples_per_packet") {
                            try {
                                stream_spp = std::stoul(value);
                                std::cout << "[Stream " << i
                                          << "] Using per-stream spp=" << stream_spp
                                          << " from config for " << block_id << ":"
                                          << port << std::endl;
                            } catch (...) {
                                std::cerr
                                    << "[Stream " << i << "] Invalid spp value: " << value
                                    << ", using default " << samps_per_buff << std::endl;
                            }
                        }
                    }
                    // Extract sample processing mode from stream endpoint config
                    stream_processing_mode = sep.tsi_config.sample_processing_mode;
                    if (stream_processing_mode != SampleProcessingMode::NONE) {
                        std::cout
                            << "[Stream " << i << "] Using sample processing mode: "
                            << sample_processing_mode_to_string(stream_processing_mode)
                            << " for " << block_id << ":" << port << std::endl;
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
            ctx.samps_per_buff   = stream_spp; // Use per-stream spp from config
            ctx.pps_reset_time   = pps_reset_time;
            ctx.pps_reset_used   = pps_reset_used;
            ctx.buffer_config    = config.multi_stream.buffer_config;
            ctx.tsi_config.sample_processing_mode =
                stream_processing_mode; // FGB/SGB sample processing

            if (use_ring_buffer) {
                const size_t bytes_per_samp = sizeof(samp_type);
                const size_t est_payload_bytes =
                    stream_spp * bytes_per_samp; // Use per-stream spp
                const size_t est_pkt_bytes = est_payload_bytes + 16;
                size_t est_pkts            = std::max<size_t>(1,
                    config.multi_stream.buffer_config.ring_buffer_size / est_pkt_bytes);
                size_t power_of_2          = 1;
                while (power_of_2 < est_pkts)
                    power_of_2 <<= 1;
                if (power_of_2 < 2)
                    power_of_2 = 2;
                ctx.ring_buffer =
                    std::make_shared<SPSCRingBuffer<PacketBuffer>>(power_of_2);
                std::cout << "Reached till TEMPSTR check, perhaps this is failing"
                          << std::endl;
                auto cwd             = std::filesystem::current_path();
                const char* env_temp = std::getenv("TEMPSTR_DEFINE");
                std::string temp_str = env_temp ? env_temp : "";
                if (temp_str.empty()) {
                    temp_str = TEMPSTR_DEFINE;
                }
                auto fileTime = std::chrono::system_clock::to_time_t(
                    std::chrono::system_clock::now());

                // Convert to local time
                std::tm local_tm{};
#if defined(_WIN32)
                localtime_s(&local_tm, &fileTime);
#else
                localtime_r(&fileTime, &local_tm);
#endif

                int floored_hr =
                    (local_tm.tm_hour / 4) * 4; // floor to nearest 4 hour block

                local_tm.tm_hour = floored_hr;
                local_tm.tm_min  = 0;
                local_tm.tm_sec  = 0;
                fileTime         = std::mktime(&local_tm);

                auto tsi_filename =
                    temp_str + "/rawdata_" + std::to_string(ctx.stream_id) + "_"
                    + TimeConverter::TimeTToString("%Y%m%d_%H%M%S", fileTime) + ".bin";
                ctx.output_filename = tsi_filename;
                // config.multi_stream.file_prefix + "_" + std::to_string(i) + ".dat";
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
        po::value<size_t>(&num_packets)->default_value(0),
        "packets per stream (0 for continuous)")(
        "rate", po::value<double>(&rate)->default_value(10e6), "sample rate")(
        "freq", po::value<double>(&freq)->default_value(70e6), "center frequency")(
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
        if (!radio_blocks.empty()) {
            // Only configure the first radio when using command-line args without YAML
            std::string id_str                      = radio_blocks[0].to_string();
            config.block_properties[id_str]["freq"] = std::to_string(freq);
            config.block_properties[id_str]["gain"] = std::to_string(gain);
            config.block_properties[id_str]["rate"] = std::to_string(rate);
            if (bw > 0)
                config.block_properties[id_str]["bandwidth"] = std::to_string(bw);

            std::cout << "Note: Using only " << id_str
                      << " (first Radio block) for command-line config" << std::endl;
        }
    }

    if (!config.multi_stream.enable_multi_stream) {
        config.multi_stream.enable_multi_stream = true;
    }

    // ===========================================================================
    // PPS-Aligned Timestamp Synchronization (3-Tier Clock Source Hierarchy)
    // ===========================================================================

    uhd::time_spec_t pps_reset_time(0.0);
    bool pps_reset_used = false;
    TimeAnchor global_time_anchor; // TimeAnchor for TSI timestamp conversion
    bool time_anchor_valid = false;

    if (config.pps_reset.enable_pps_reset) {
        std::cout << "\n=== PPS-Aligned Timestamp Configuration ===" << std::endl;
        std::cout << "Use UTC time: " << (config.pps_reset.use_utc_time ? "Yes" : "No")
                  << std::endl;

        // Step 1: Probe and select the best available clock source
        ClockSourceStatus clock_status =
            probe_and_select_clock_source(graph, config.pps_reset.clock_config);

        // Step 2: Perform PPS-aligned time synchronization
        PpsAlignmentResult alignment_result =
            perform_pps_aligned_sync(graph, config.pps_reset, clock_status);

        if (alignment_result.success) {
            pps_reset_time     = alignment_result.aligned_time;
            pps_reset_used     = true;
            global_time_anchor = alignment_result.time_anchor;
            time_anchor_valid  = true;

            std::cout << "\n=== PPS-Aligned Sync Complete ===" << std::endl;
            std::cout << "Clock tier: " << clock_tier_to_string(alignment_result.tier)
                      << std::endl;
            std::cout << "Time source: "
                      << network_source_to_string(alignment_result.time_source)
                      << std::endl;
            std::cout << "TimeAnchor: unix=" << global_time_anchor.unix_time_at_anchor
                      << ", hw=" << global_time_anchor.hw_secs_at_anchor << std::endl;
            std::cout << "All packet timestamps will be PPS-aligned and reflect "
                      << (config.pps_reset.use_utc_time ? "UTC" : "relative") << " time."
                      << std::endl;
        } else {
            std::cerr << "\n=== PPS-Aligned Sync Failed ===" << std::endl;
            std::cerr << "Reason: " << alignment_result.message << std::endl;
            std::cerr << "Continuing without PPS alignment - timestamps will be relative."
                      << std::endl;
            // Fallback to old perform_pps_reset for backward compatibility
            pps_reset_time = perform_pps_reset(graph, config.pps_reset);
            pps_reset_used = true;
        }
    }

    bool enable_analysis = !csv_file.empty();
    if (enable_analysis && csv_file.empty()) {
        csv_file = file + "_analysis.csv";
    }

    // Determine which capture method to use
    bool use_ring_buffer = config.multi_stream.separate_files && pps_reset_used;

    try {
        if (use_tsi_format) {
            std::cout << "Using TSI proprietary packet format for output." << std::endl;
            std::cout << "Per-stream TSI configuration from YAML stream_endpoints."
                      << std::endl;

            // TSI config is now per-stream, parsed from YAML stream_endpoints
            // No global TsiOutputConfig needed
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
                global_time_anchor,
                time_anchor_valid);
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
    } catch (uhd::rfnoc_error& e) {
        std::cerr << "\nRFNoC Graph Error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "\n=== Capture Completed Successfully ===" << std::endl;
    return EXIT_SUCCESS;
}