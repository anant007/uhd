#include <uhd/exception.hpp>
#include <uhd/rfnoc/ddc_block_control.hpp>
#include <uhd/rfnoc/mb_controller.hpp>
#include <uhd/rfnoc/radio_control.hpp>
#include <uhd/rfnoc_graph.hpp>
#include <uhd/stream.hpp>
#include <uhd/types/tune_request.hpp>
#include <uhd/utils/safe_main.hpp>
#include <boost/program_options.hpp>
#include <yaml-cpp/yaml.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <complex>
#include <csignal>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace po = boost::program_options;

static std::atomic<bool> stop_signal_called{false};

void sig_int_handler(int)
{
    stop_signal_called.store(true);
}

struct RfnocEndpoint
{
    std::string block_id;
    size_t port = 0;
};

struct RfnocConnection
{
    RfnocEndpoint src;
    RfnocEndpoint dst;
};

struct ToolConfig
{
    std::string device_args;
    std::string block_id;
    size_t port = 0;
    std::string radio_block_id;
    size_t radio_channel = 0;
    bool has_frequency   = false;
    double frequency_hz  = 0.0;
    std::string clock_source;
    std::string time_source;
    uhd::device_addr_t stream_args;
    std::vector<RfnocConnection> connections;
    std::vector<std::pair<std::string, uhd::device_addr_t>> block_properties;
};

struct SampleStats
{
    uint64_t samples       = 0;
    uint64_t clipped_i     = 0;
    uint64_t clipped_q     = 0;
    uint64_t near_rail_i   = 0;
    uint64_t near_rail_q   = 0;
    uint64_t overflow_md   = 0;
    uint64_t timeout_md    = 0;
    uint64_t bad_packet_md = 0;
    int32_t min_i          = std::numeric_limits<int16_t>::max();
    int32_t max_i          = std::numeric_limits<int16_t>::min();
    int32_t min_q          = std::numeric_limits<int16_t>::max();
    int32_t max_q          = std::numeric_limits<int16_t>::min();
    int32_t peak_abs_i     = 0;
    int32_t peak_abs_q     = 0;
    long double sum_i      = 0.0;
    long double sum_q      = 0.0;
    long double sum_i2     = 0.0;
    long double sum_q2     = 0.0;
    long double sum_mag2   = 0.0;
};

struct MeasurementState
{
    std::atomic<bool> active{false};
    std::mutex mutex;
    SampleStats stats;
};

struct MeasurementResult
{
    double input_dbm          = 0.0;
    double duration_s         = 0.0;
    bool has_frequency        = false;
    double frequency_hz       = 0.0;
    size_t radio_channel      = 0;
    bool has_zero_dbfs_mark   = false;
    bool is_zero_dbfs_mark    = false;
    double zero_dbfs_power_dbm = 0.0;
    double input_db_relative_to_zero_dbfs = 0.0;
    SampleStats stats;
    double mean_i             = 0.0;
    double mean_q             = 0.0;
    double rms_i              = 0.0;
    double rms_q              = 0.0;
    double rms_mag            = 0.0;
    double rms_component_dbfs = -std::numeric_limits<double>::infinity();
    double peak_dbfs          = -std::numeric_limits<double>::infinity();
    double clipped_percent    = 0.0;
    double near_rail_percent  = 0.0;

    // Hardware ADC fullscale counters from UHD radio_control sensors.
    // The X300 radio block polls REG_RX_DATA at ~1 kHz before any FPGA
    // DSP (DDC/FIR) and counts samples where |I| or |Q| reach the 14-bit
    // ADC saturation level (±0x7FFC in left-aligned 16-bit representation).
    // These are delta counts for the measurement window.
    bool    has_hw_adc_sensors      = false;
    int64_t hw_adc_fullscale_i      = 0;  // I-lane ADC fullscale count
    int64_t hw_adc_fullscale_q      = 0;  // Q-lane ADC fullscale count
    int64_t hw_adc_monitor_count    = 0;  // Total polls during window
    double  hw_adc_fullscale_fraction = 0.0; // (i+q) / (2 * monitor)
};

struct CalibrationContext
{
    std::string radio_block_id;
    size_t radio_channel = 0;
    bool has_frequency   = false;
    double frequency_hz  = 0.0;
};

struct ZeroDbfsMark
{
    double frequency_hz = 0.0;
    size_t radio_channel = 0;
    double power_dbm = 0.0;
    MeasurementResult result;
};

static RfnocEndpoint parse_endpoint(const std::string& text)
{
    const auto separator = text.rfind(':');
    if (separator == std::string::npos || separator == 0 || separator + 1 >= text.size()) {
        throw std::runtime_error("Endpoint must have the form BLOCK_ID:PORT, got: " + text);
    }

    RfnocEndpoint endpoint;
    endpoint.block_id = text.substr(0, separator);
    endpoint.port     = std::stoul(text.substr(separator + 1));
    return endpoint;
}

static RfnocConnection parse_connection(const std::string& text)
{
    const auto separator = text.find('=');
    if (separator == std::string::npos || separator == 0 || separator + 1 >= text.size()) {
        throw std::runtime_error(
            "Connection must have the form SRC_BLOCK:SRC_PORT=DST_BLOCK:DST_PORT, got: "
            + text);
    }

    return {parse_endpoint(text.substr(0, separator)),
        parse_endpoint(text.substr(separator + 1))};
}

static std::pair<std::string, uhd::device_addr_t> parse_block_properties(
    const std::string& text)
{
    const auto separator = text.find(':');
    if (separator == std::string::npos || separator == 0 || separator + 1 >= text.size()) {
        throw std::runtime_error(
            "Block properties must have the form BLOCK_ID:key=value[,key=value], got: "
            + text);
    }

    return {text.substr(0, separator), uhd::device_addr_t(text.substr(separator + 1))};
}

static std::string yaml_map_to_device_addr_string(const YAML::Node& node)
{
    if (!node) {
        return "";
    }
    if (node.IsScalar()) {
        return node.as<std::string>();
    }
    if (!node.IsMap()) {
        throw std::runtime_error("Expected scalar or map for device address properties");
    }

    std::ostringstream oss;
    bool first = true;
    for (const auto& item : node) {
        if (!first) {
            oss << ',';
        }
        first = false;
        oss << item.first.as<std::string>() << '=' << item.second.as<std::string>();
    }
    return oss.str();
}

static void load_yaml_config(const std::string& yaml_file, ToolConfig& config)
{
    const YAML::Node root = YAML::LoadFile(yaml_file);

    auto load_stream_endpoint = [&config](const YAML::Node& endpoint) {
        if (endpoint["block_id"]) {
            config.block_id = endpoint["block_id"].as<std::string>();
        }
        if (endpoint["port"]) {
            config.port = endpoint["port"].as<size_t>();
        }
        if (endpoint["stream_args"]) {
            config.stream_args = uhd::device_addr_t(
                yaml_map_to_device_addr_string(endpoint["stream_args"]));
        }
        if (endpoint["radio_block_id"]) {
            config.radio_block_id = endpoint["radio_block_id"].as<std::string>();
        }
        if (endpoint["radio_channel"]) {
            config.radio_channel = endpoint["radio_channel"].as<size_t>();
        }
        if (endpoint["freq"]) {
            config.frequency_hz = endpoint["freq"].as<double>();
            config.has_frequency = true;
        }
        if (endpoint["frequency_hz"]) {
            config.frequency_hz = endpoint["frequency_hz"].as<double>();
            config.has_frequency = true;
        }
    };

    if (root["device_args"]) {
        config.device_args = root["device_args"].as<std::string>();
    }
    if (root["args"]) {
        config.device_args = root["args"].as<std::string>();
    }
    if (root["clock_source"]) {
        config.clock_source = root["clock_source"].as<std::string>();
    }
    if (root["time_source"]) {
        config.time_source = root["time_source"].as<std::string>();
    }
    if (root["radio_block_id"]) {
        config.radio_block_id = root["radio_block_id"].as<std::string>();
    }
    if (root["radio_channel"]) {
        config.radio_channel = root["radio_channel"].as<size_t>();
    }
    if (root["freq"]) {
        config.frequency_hz = root["freq"].as<double>();
        config.has_frequency = true;
    }
    if (root["frequency_hz"]) {
        config.frequency_hz = root["frequency_hz"].as<double>();
        config.has_frequency = true;
    }
    if (root["calibration"]) {
        const auto calibration = root["calibration"];
        if (calibration["radio_block_id"]) {
            config.radio_block_id = calibration["radio_block_id"].as<std::string>();
        }
        if (calibration["radio_channel"]) {
            config.radio_channel = calibration["radio_channel"].as<size_t>();
        }
        if (calibration["freq"]) {
            config.frequency_hz = calibration["freq"].as<double>();
            config.has_frequency = true;
        }
        if (calibration["frequency_hz"]) {
            config.frequency_hz = calibration["frequency_hz"].as<double>();
            config.has_frequency = true;
        }
    }

    if (root["connections"]) {
        for (const auto& conn : root["connections"]) {
            if (conn.IsScalar()) {
                config.connections.push_back(parse_connection(conn.as<std::string>()));
            } else {
                config.connections.push_back(
                    {parse_endpoint(conn["src"].as<std::string>()),
                        parse_endpoint(conn["dst"].as<std::string>())});
            }
        }
    }

    if (root["stream_endpoint"]) {
        load_stream_endpoint(root["stream_endpoint"]);
    } else if (root["stream_endpoints"]) {
        for (const auto& candidate : root["stream_endpoints"]) {
            if (!candidate["direction"] || candidate["direction"].as<std::string>() == "rx") {
                load_stream_endpoint(candidate);
                break;
            }
        }
    }

    if (root["stream_args"]) {
        config.stream_args = uhd::device_addr_t(yaml_map_to_device_addr_string(root["stream_args"]));
    }

    if (root["block_properties"]) {
        for (const auto& item : root["block_properties"]) {
            config.block_properties.push_back({item.first.as<std::string>(),
                uhd::device_addr_t(yaml_map_to_device_addr_string(item.second))});
        }
    }
}

static void merge_stats(SampleStats& total, const SampleStats& add)
{
    total.samples += add.samples;
    total.clipped_i += add.clipped_i;
    total.clipped_q += add.clipped_q;
    total.near_rail_i += add.near_rail_i;
    total.near_rail_q += add.near_rail_q;
    total.overflow_md += add.overflow_md;
    total.timeout_md += add.timeout_md;
    total.bad_packet_md += add.bad_packet_md;
    total.min_i = std::min(total.min_i, add.min_i);
    total.max_i = std::max(total.max_i, add.max_i);
    total.min_q = std::min(total.min_q, add.min_q);
    total.max_q = std::max(total.max_q, add.max_q);
    total.peak_abs_i = std::max(total.peak_abs_i, add.peak_abs_i);
    total.peak_abs_q = std::max(total.peak_abs_q, add.peak_abs_q);
    total.sum_i += add.sum_i;
    total.sum_q += add.sum_q;
    total.sum_i2 += add.sum_i2;
    total.sum_q2 += add.sum_q2;
    total.sum_mag2 += add.sum_mag2;
}

static SampleStats analyze_samples(const std::complex<int16_t>* samples,
    const size_t num_samples,
    const int32_t clip_threshold,
    const int32_t near_rail_threshold)
{
    SampleStats stats;
    stats.samples = num_samples;

    for (size_t i = 0; i < num_samples; i++) {
        const int32_t i_val = samples[i].real();
        const int32_t q_val = samples[i].imag();
        const int32_t abs_i = std::abs(i_val);
        const int32_t abs_q = std::abs(q_val);

        stats.min_i = std::min(stats.min_i, i_val);
        stats.max_i = std::max(stats.max_i, i_val);
        stats.min_q = std::min(stats.min_q, q_val);
        stats.max_q = std::max(stats.max_q, q_val);
        stats.peak_abs_i = std::max(stats.peak_abs_i, abs_i);
        stats.peak_abs_q = std::max(stats.peak_abs_q, abs_q);

        if (abs_i >= clip_threshold) {
            stats.clipped_i++;
        }
        if (abs_q >= clip_threshold) {
            stats.clipped_q++;
        }
        if (abs_i >= near_rail_threshold) {
            stats.near_rail_i++;
        }
        if (abs_q >= near_rail_threshold) {
            stats.near_rail_q++;
        }

        stats.sum_i += i_val;
        stats.sum_q += q_val;
        stats.sum_i2 += static_cast<long double>(i_val) * i_val;
        stats.sum_q2 += static_cast<long double>(q_val) * q_val;
        stats.sum_mag2 += static_cast<long double>(i_val) * i_val
                          + static_cast<long double>(q_val) * q_val;
    }

    return stats;
}

static double to_dbfs(const double code)
{
    if (code <= 0.0) {
        return -std::numeric_limits<double>::infinity();
    }
    return 20.0 * std::log10(code / 32768.0);
}

static std::string trim_copy(const std::string& text)
{
    const auto first = text.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return "";
    }
    const auto last = text.find_last_not_of(" \t\r\n");
    return text.substr(first, last - first + 1);
}

static bool same_frequency(const double lhs, const double rhs)
{
    const double tolerance = std::max(1.0, std::max(std::abs(lhs), std::abs(rhs))) * 1e-9;
    return std::abs(lhs - rhs) <= tolerance;
}

static const ZeroDbfsMark* find_zero_dbfs_mark(const std::vector<ZeroDbfsMark>& marks,
    const CalibrationContext& calibration)
{
    if (!calibration.has_frequency) {
        return nullptr;
    }

    for (auto it = marks.rbegin(); it != marks.rend(); ++it) {
        if (it->radio_channel == calibration.radio_channel
            && same_frequency(it->frequency_hz, calibration.frequency_hz)) {
            return &(*it);
        }
    }
    return nullptr;
}

static void apply_radio_frequency(
    const uhd::rfnoc::rfnoc_graph::sptr& graph, CalibrationContext& calibration)
{
    if (!calibration.has_frequency || calibration.radio_block_id.empty()) {
        return;
    }

    const auto block_id = uhd::rfnoc::block_id_t(calibration.radio_block_id);
    if (!graph->has_block(block_id)) {
        throw std::runtime_error("No such radio block for calibration context: "
                                 + calibration.radio_block_id);
    }

    auto radio = graph->get_block<uhd::rfnoc::radio_control>(block_id);
    if (!radio) {
        throw std::runtime_error("Calibration block is not a radio block: "
                                 + calibration.radio_block_id);
    }

    const double actual = radio->set_rx_frequency(
        calibration.frequency_hz, calibration.radio_channel);
    calibration.frequency_hz = actual;
    std::cout << "Radio " << calibration.radio_block_id << " channel "
              << calibration.radio_channel << " tuned to " << std::setprecision(12)
              << actual << " Hz" << std::endl;
}

// Snapshot of the hardware ADC fullscale counters exposed via radio_control sensors.
struct AdcSensorSnapshot
{
    bool    valid          = false;
    int64_t fullscale_i    = 0;
    int64_t fullscale_q    = 0;
    int64_t monitor_count  = 0;
};

// Read the ADC fullscale sensor counters from a radio block (channel 0 of the
// radio block is the one monitored by the background polling thread in
// x300_radio_control_impl).  Returns an invalid snapshot if the block or the
// sensors are not available.
static AdcSensorSnapshot read_adc_sensor_snapshot(
    uhd::rfnoc::rfnoc_graph::sptr graph, const std::string& radio_block_id)
{
    AdcSensorSnapshot snap;
    if (radio_block_id.empty()) {
        return snap;
    }
    try {
        const auto block_id = uhd::rfnoc::block_id_t(radio_block_id);
        if (!graph->has_block(block_id)) {
            return snap;
        }
        auto radio = graph->get_block<uhd::rfnoc::radio_control>(block_id);
        if (!radio) {
            return snap;
        }
        // Sensors are only populated for channel 0 (the channel monitored by
        // the x300_radio_control background thread via get_adc_rx_word()).
        const size_t sensor_chan = 0;
        const auto sensor_names = radio->get_rx_sensor_names(sensor_chan);
        const bool has_sensors =
            std::find(sensor_names.begin(), sensor_names.end(), "adc_monitor_count")
            != sensor_names.end();
        if (!has_sensors) {
            return snap;
        }
        snap.fullscale_i   = radio->get_rx_sensor("adc_fullscale_count_i", sensor_chan)
                                 .to_int();
        snap.fullscale_q   = radio->get_rx_sensor("adc_fullscale_count_q", sensor_chan)
                                 .to_int();
        snap.monitor_count = radio->get_rx_sensor("adc_monitor_count", sensor_chan)
                                 .to_int();
        snap.valid = true;
    } catch (const std::exception&) {
        // Silently ignore – sensors are unavailable (non-X300 device, etc.)
    }
    return snap;
}

static MeasurementResult make_result(const double input_dbm,
    const double duration_s,
    const SampleStats& stats,
    const CalibrationContext& calibration,
    const std::vector<ZeroDbfsMark>& zero_dbfs_marks)
{
    MeasurementResult result;
    result.input_dbm  = input_dbm;
    result.duration_s = duration_s;
    result.has_frequency   = calibration.has_frequency;
    result.frequency_hz    = calibration.frequency_hz;
    result.radio_channel   = calibration.radio_channel;
    result.stats      = stats;

    const auto* mark = find_zero_dbfs_mark(zero_dbfs_marks, calibration);
    if (mark) {
        result.has_zero_dbfs_mark = true;
        result.zero_dbfs_power_dbm = mark->power_dbm;
        result.input_db_relative_to_zero_dbfs = input_dbm - mark->power_dbm;
    }

    if (stats.samples == 0) {
        return result;
    }

    const long double sample_count = static_cast<long double>(stats.samples);
    result.mean_i                 = static_cast<double>(stats.sum_i / sample_count);
    result.mean_q                 = static_cast<double>(stats.sum_q / sample_count);
    result.rms_i                  = std::sqrt(static_cast<double>(stats.sum_i2 / sample_count));
    result.rms_q                  = std::sqrt(static_cast<double>(stats.sum_q2 / sample_count));
    result.rms_mag = std::sqrt(static_cast<double>(stats.sum_mag2 / sample_count));

    const double rms_component = std::max(result.rms_i, result.rms_q);
    const double peak_component = static_cast<double>(std::max(stats.peak_abs_i, stats.peak_abs_q));
    result.rms_component_dbfs = to_dbfs(rms_component);
    result.peak_dbfs          = to_dbfs(peak_component);

    const double lane_samples = static_cast<double>(stats.samples) * 2.0;
    result.clipped_percent = 100.0 * static_cast<double>(stats.clipped_i + stats.clipped_q)
                             / lane_samples;
    result.near_rail_percent = 100.0 * static_cast<double>(stats.near_rail_i + stats.near_rail_q)
                               / lane_samples;

    return result;
}

static void write_csv_header(std::ofstream& csv)
{
    csv << "input_dbm,frequency_hz,radio_channel,duration_s,samples,mean_i,mean_q,"
           "rms_i,rms_q,rms_mag,"
           "rms_component_dbfs,peak_i,peak_q,peak_component_dbfs,min_i,max_i,min_q,max_q,"
           "clipped_i,clipped_q,clipped_percent,near_rail_i,near_rail_q,near_rail_percent,"
           "zero_dbfs_power_dbm,input_db_relative_to_zero_dbfs,is_zero_dbfs_mark,"
           "metadata_overflows,metadata_timeouts,bad_packets,"
           "hw_adc_fullscale_i,hw_adc_fullscale_q,hw_adc_monitor_count,"
           "hw_adc_fullscale_fraction\n";
}

static void write_optional_csv_double(
    std::ofstream& csv, const bool has_value, const double value)
{
    if (has_value) {
        csv << value;
    }
}

static void write_csv_row(std::ofstream& csv, const MeasurementResult& result)
{
    const auto& s = result.stats;
    csv << std::setprecision(12) << result.input_dbm << ',';
    write_optional_csv_double(csv, result.has_frequency, result.frequency_hz);
    csv << ',' << result.radio_channel << ',' << result.duration_s << ',' << s.samples << ','
        << result.mean_i << ',' << result.mean_q << ',' << result.rms_i << ','
        << result.rms_q << ',' << result.rms_mag << ','
        << result.rms_component_dbfs << ',' << s.peak_abs_i << ',' << s.peak_abs_q << ','
        << result.peak_dbfs << ',' << s.min_i << ',' << s.max_i << ',' << s.min_q << ','
        << s.max_q << ',' << s.clipped_i << ',' << s.clipped_q << ','
        << result.clipped_percent << ',' << s.near_rail_i << ',' << s.near_rail_q << ','
        << result.near_rail_percent << ',';
    write_optional_csv_double(csv, result.has_zero_dbfs_mark, result.zero_dbfs_power_dbm);
    csv << ',';
    write_optional_csv_double(csv,
        result.has_zero_dbfs_mark,
        result.input_db_relative_to_zero_dbfs);
    csv << ',' << (result.is_zero_dbfs_mark ? 1 : 0) << ',' << s.overflow_md << ','
        << s.timeout_md << ',' << s.bad_packet_md << ',';
    if (result.has_hw_adc_sensors) {
        csv << result.hw_adc_fullscale_i << ',' << result.hw_adc_fullscale_q << ','
            << result.hw_adc_monitor_count << ',' << result.hw_adc_fullscale_fraction;
    } else {
        csv << ",,,";
    }
    csv << '\n';
}

static void print_result(const MeasurementResult& result)
{
    const auto& s = result.stats;
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Input " << result.input_dbm << " dBm";
    if (result.has_frequency) {
        std::cout << " @ " << std::setprecision(6) << result.frequency_hz / 1e6
                  << " MHz, radio_ch=" << result.radio_channel;
    } else {
        std::cout << ", radio_ch=" << result.radio_channel;
    }
    if (result.is_zero_dbfs_mark) {
        std::cout << " [0 dBFS mark]";
    }
    std::cout << ": " << s.samples << " samples, "
              << "rms(I/Q/mag)=" << result.rms_i << '/' << result.rms_q << '/'
              << result.rms_mag << ", peak(I/Q)=" << s.peak_abs_i << '/' << s.peak_abs_q
              << ", rms_component=" << result.rms_component_dbfs << " dBFS, peak="
              << result.peak_dbfs << " dBFS, clipped=" << result.clipped_percent
              << "%, near_rail=" << result.near_rail_percent << "%";
    if (result.has_zero_dbfs_mark) {
        std::cout << ", input_vs_0dBFS=" << result.input_db_relative_to_zero_dbfs
                  << " dB";
    }
    std::cout << std::endl;

    if (s.overflow_md || s.timeout_md || s.bad_packet_md) {
        std::cout << "  metadata: overflow=" << s.overflow_md
                  << ", timeout=" << s.timeout_md << ", bad_packet=" << s.bad_packet_md
                  << std::endl;
    }
    if (result.has_hw_adc_sensors) {
        std::cout << std::fixed << std::setprecision(6);
        std::cout << "  HW ADC fullscale (pre-DSP): I=" << result.hw_adc_fullscale_i
                  << ", Q=" << result.hw_adc_fullscale_q
                  << " / " << result.hw_adc_monitor_count << " polls ("
                  << std::setprecision(4)
                  << result.hw_adc_fullscale_fraction * 100.0 << "% of polls)"
                  << std::endl;
    }
}

static void receiver_loop(uhd::rx_streamer::sptr rx_stream,
    MeasurementState& state,
    const size_t samples_per_buffer,
    const int32_t clip_threshold,
    const int32_t near_rail_threshold)
{
    uhd::rx_metadata_t md;
    std::vector<std::complex<int16_t>> buffer(samples_per_buffer);

    while (!stop_signal_called.load()) {
        const size_t num_rx = rx_stream->recv(buffer.data(), buffer.size(), md, 0.25);

        SampleStats local;
        bool should_merge = state.active.load();

        if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_TIMEOUT) {
            local.timeout_md = 1;
        } else if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
            local.overflow_md = 1;
        } else if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
            local.bad_packet_md = 1;
            std::cerr << "Receiver error: " << md.strerror() << std::endl;
        } else if (num_rx > 0 && should_merge) {
            local = analyze_samples(
                buffer.data(), num_rx, clip_threshold, near_rail_threshold);
        } else {
            should_merge = false;
        }

        if (should_merge) {
            std::lock_guard<std::mutex> lock(state.mutex);
            if (state.active.load()) {
                merge_stats(state.stats, local);
            }
        }
    }
}

static SampleStats measure_window(MeasurementState& state, const double duration_s)
{
    {
        std::lock_guard<std::mutex> lock(state.mutex);
        state.stats = SampleStats{};
        state.active.store(true);
    }

    const auto end_time = std::chrono::steady_clock::now()
                          + std::chrono::milliseconds(
                              static_cast<int64_t>(std::round(duration_s * 1000.0)));
    while (!stop_signal_called.load() && std::chrono::steady_clock::now() < end_time) {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }

    state.active.store(false);
    std::lock_guard<std::mutex> lock(state.mutex);
    return state.stats;
}

static void apply_configured_graph(
    const uhd::rfnoc::rfnoc_graph::sptr& graph, const ToolConfig& config)
{
    if (!config.clock_source.empty()) {
        graph->get_mb_controller(0)->set_clock_source(config.clock_source);
    }
    if (!config.time_source.empty()) {
        graph->get_mb_controller(0)->set_time_source(config.time_source);
    }

    for (const auto& connection : config.connections) {
        std::cout << "Connecting " << connection.src.block_id << ':' << connection.src.port
                  << " -> " << connection.dst.block_id << ':' << connection.dst.port
                  << std::endl;
        graph->connect(connection.src.block_id,
            connection.src.port,
            connection.dst.block_id,
            connection.dst.port);
    }

    for (const auto& block_props : config.block_properties) {
        const auto block_id = uhd::rfnoc::block_id_t(block_props.first);
        if (!graph->has_block(block_id)) {
            throw std::runtime_error("No such block for --prop/block_properties: "
                                     + block_props.first);
        }
        std::cout << "Setting properties on " << block_props.first << ": "
                  << block_props.second.to_string() << std::endl;
        graph->get_block(block_id)->set_properties(block_props.second);
    }
}

int UHD_SAFE_MAIN(int argc, char* argv[])
{
    std::string args;
    std::string yaml_file;
    std::string block_id;
    std::string radio_block_id;
    std::string streamargs;
    std::string csv_file;
    std::vector<std::string> cli_connections;
    std::vector<std::string> cli_block_props;
    size_t port                = 0;
    size_t radio_channel       = 0;
    size_t samples_per_buffer  = 4096;
    size_t samples_per_packet  = 0;
    double frequency_hz        = 0.0;
    double measurement_seconds = 1.0;
    double near_rail_ratio     = 0.98;
    int32_t clip_threshold     = 32760;

    po::options_description desc("Allowed options");
    // clang-format off
    desc.add_options()
        ("help", "help message")
        ("args", po::value<std::string>(&args)->default_value(""), "USRP device address args")
        ("yaml", po::value<std::string>(&yaml_file), "optional YAML graph configuration")
        ("block-id", po::value<std::string>(&block_id), "RFNoC source block ID to stream from, e.g. 0/DDC#0 or 0/Radio#0")
        ("port", po::value<size_t>(&port)->default_value(0), "source output port on --block-id")
        ("radio-block-id", po::value<std::string>(&radio_block_id), "Radio block ID used for frequency/channel calibration context, e.g. 0/Radio#0")
        ("radio-channel", po::value<size_t>(&radio_channel)->default_value(0), "radio block channel for calibration context")
        ("freq", po::value<double>(&frequency_hz), "initial RX frequency in Hz for the calibration context")
        ("connect", po::value<std::vector<std::string>>(&cli_connections)->composing(), "manual graph edge SRC_BLOCK:SRC_PORT=DST_BLOCK:DST_PORT; may be repeated")
        ("prop", po::value<std::vector<std::string>>(&cli_block_props)->composing(), "set block properties BLOCK_ID:key=value[,key=value]; may be repeated")
        ("streamargs", po::value<std::string>(&streamargs)->default_value(""), "stream args passed to create_rx_streamer")
        ("spp", po::value<size_t>(&samples_per_packet), "samples per packet stream arg")
        ("spb", po::value<size_t>(&samples_per_buffer)->default_value(4096), "samples per host receive buffer")
        ("duration", po::value<double>(&measurement_seconds)->default_value(1.0), "seconds to measure after each entered dBm value")
        ("clip-threshold", po::value<int32_t>(&clip_threshold)->default_value(32760), "absolute sc16 lane value counted as clipped")
        ("near-rail", po::value<double>(&near_rail_ratio)->default_value(0.98), "fraction of int16 full scale counted as near rail")
        ("csv", po::value<std::string>(&csv_file), "CSV output file for measurements")
    ;
    // clang-format on

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << "UHD/RFNoC interactive sc16 power calibration " << desc << std::endl;
        std::cout << std::endl
                  << "This tool streams sc16 samples from one RFNoC block/port and, for "
                     "each input power value entered at the prompt, records raw-code "
                     "statistics and clipping indicators.\n"
                  << std::endl;
        return EXIT_SUCCESS;
    }

    ToolConfig config;
    config.device_args = args;
    config.block_id    = block_id;
    config.port        = port;
    config.radio_channel = radio_channel;
    config.stream_args = uhd::device_addr_t(streamargs);

    if (vm.count("yaml")) {
        load_yaml_config(yaml_file, config);
    }
    if (vm.count("args") && !vm["args"].defaulted()) {
        config.device_args = args;
    }
    if (vm.count("block-id")) {
        config.block_id = block_id;
    }
    if (vm.count("port") && !vm["port"].defaulted()) {
        config.port = port;
    }
    if (vm.count("radio-block-id")) {
        config.radio_block_id = radio_block_id;
    }
    if (vm.count("radio-channel") && !vm["radio-channel"].defaulted()) {
        config.radio_channel = radio_channel;
    }
    if (vm.count("freq")) {
        config.frequency_hz = frequency_hz;
        config.has_frequency = true;
    }
    if (vm.count("streamargs") && !vm["streamargs"].defaulted()) {
        config.stream_args = uhd::device_addr_t(streamargs);
    }
    if (vm.count("spp")) {
        config.stream_args["spp"] = std::to_string(samples_per_packet);
    }
    for (const auto& connection : cli_connections) {
        config.connections.push_back(parse_connection(connection));
    }
    for (const auto& block_props : cli_block_props) {
        config.block_properties.push_back(parse_block_properties(block_props));
    }

    if (config.block_id.empty()) {
        std::cerr << "Please specify --block-id or provide stream_endpoint.block_id in YAML"
                  << std::endl;
        return EXIT_FAILURE;
    }
    if (measurement_seconds <= 0.0) {
        std::cerr << "Please specify a positive --duration" << std::endl;
        return EXIT_FAILURE;
    }
    if (near_rail_ratio <= 0.0 || near_rail_ratio > 1.0) {
        std::cerr << "Please specify --near-rail in the range (0, 1]" << std::endl;
        return EXIT_FAILURE;
    }
    if (clip_threshold <= 0 || clip_threshold > 32768) {
        std::cerr << "Please specify --clip-threshold in the range [1, 32768]"
                  << std::endl;
        return EXIT_FAILURE;
    }

    if (config.radio_block_id.empty() && config.block_id.find("Radio") != std::string::npos) {
        config.radio_block_id = config.block_id;
    }

    CalibrationContext calibration;
    calibration.radio_block_id = config.radio_block_id;
    calibration.radio_channel  = config.radio_channel;
    calibration.has_frequency  = config.has_frequency;
    calibration.frequency_hz   = config.frequency_hz;
    std::vector<ZeroDbfsMark> zero_dbfs_marks;

    const int32_t near_rail_threshold = static_cast<int32_t>(
        std::round(32768.0 * near_rail_ratio));

    std::ofstream csv;
    if (vm.count("csv")) {
        csv.open(csv_file.c_str(), std::ofstream::out | std::ofstream::trunc);
        if (!csv.good()) {
            std::cerr << "Could not open CSV file: " << csv_file << std::endl;
            return EXIT_FAILURE;
        }
        write_csv_header(csv);
    }

    std::signal(SIGINT, &sig_int_handler);

    std::cout << "Creating the RFNoC graph with args: " << config.device_args << std::endl;
    auto graph = uhd::rfnoc::rfnoc_graph::make(config.device_args);
    apply_configured_graph(graph, config);
    apply_radio_frequency(graph, calibration);

    const auto source_block_id = uhd::rfnoc::block_id_t(config.block_id);
    if (!graph->has_block(source_block_id)) {
        std::cerr << "No such source block: " << config.block_id << std::endl;
        return EXIT_FAILURE;
    }

    uhd::stream_args_t stream_args("sc16", "sc16");
    stream_args.args = config.stream_args;
    std::cout << "Creating RX streamer with args: " << stream_args.args.to_string()
              << std::endl;
    auto rx_stream = graph->create_rx_streamer(1, stream_args);

    std::cout << "Connecting source " << config.block_id << ':' << config.port
              << " -> HOST:0" << std::endl;
    graph->connect(config.block_id, config.port, rx_stream, 0, true);
    graph->commit();

    std::cout << "Active connections:" << std::endl;
    for (const auto& edge : graph->enumerate_active_connections()) {
        std::cout << "* " << edge.to_string() << std::endl;
    }

    uhd::stream_cmd_t stream_cmd(uhd::stream_cmd_t::STREAM_MODE_START_CONTINUOUS);
    stream_cmd.stream_now = true;
    stream_cmd.time_spec  = uhd::time_spec_t();
    rx_stream->issue_stream_cmd(stream_cmd);

    MeasurementState state;
    std::thread receiver(receiver_loop,
        rx_stream,
        std::ref(state),
        samples_per_buffer,
        clip_threshold,
        near_rail_threshold);

    std::cout << std::endl;
    std::cout << "Streaming from " << config.block_id << ':' << config.port << std::endl;
    std::cout << "Enter calibrated input power in dBm to measure for "
              << measurement_seconds << " s. Type q to quit." << std::endl;
    std::cout << "Commands: freq HZ, chan N, radio BLOCK_ID, context HZ N, "
                 "mark0 DBM [HZ] [CHAN], marks, help"
              << std::endl;
    std::cout << "clip_threshold=" << clip_threshold
              << ", near_rail_threshold=" << near_rail_threshold << std::endl;

    auto record_measurement = [&](const double dbm_in, const bool mark_zero_dbfs) {
        // Snapshot hardware ADC counters before the measurement window so we
        // can report the delta for this window only.
        const auto snap_before =
            read_adc_sensor_snapshot(graph, calibration.radio_block_id);

        const auto stats = measure_window(state, measurement_seconds);

        const auto snap_after =
            read_adc_sensor_snapshot(graph, calibration.radio_block_id);

        auto result = make_result(
            dbm_in, measurement_seconds, stats, calibration, zero_dbfs_marks);

        // Populate hardware ADC sensor delta into the result.
        if (snap_before.valid && snap_after.valid) {
            result.has_hw_adc_sensors   = true;
            result.hw_adc_fullscale_i   = snap_after.fullscale_i   - snap_before.fullscale_i;
            result.hw_adc_fullscale_q   = snap_after.fullscale_q   - snap_before.fullscale_q;
            result.hw_adc_monitor_count = snap_after.monitor_count - snap_before.monitor_count;
            if (result.hw_adc_monitor_count > 0) {
                result.hw_adc_fullscale_fraction =
                    static_cast<double>(result.hw_adc_fullscale_i
                                        + result.hw_adc_fullscale_q)
                    / static_cast<double>(2 * result.hw_adc_monitor_count);
            }
        }

        if (mark_zero_dbfs) {
            if (!calibration.has_frequency) {
                std::cout << "Set a frequency first with 'freq HZ' or use "
                             "'mark0 DBM HZ [CHAN]'."
                          << std::endl;
                return;
            }
            result.has_zero_dbfs_mark = true;
            result.is_zero_dbfs_mark = true;
            result.zero_dbfs_power_dbm = dbm_in;
            result.input_db_relative_to_zero_dbfs = 0.0;
            zero_dbfs_marks.push_back({calibration.frequency_hz,
                calibration.radio_channel,
                dbm_in,
                result});
            std::cout << "Marked 0 dBFS reference: " << dbm_in << " dBm @ "
                      << std::setprecision(12) << calibration.frequency_hz << " Hz, "
                      << "radio_ch=" << calibration.radio_channel << std::endl;
        }

        print_result(result);
        if (csv.is_open()) {
            write_csv_row(csv, result);
            csv.flush();
        }
    };

    std::string line;
    while (!stop_signal_called.load()) {
        std::cout << "dBm> ";
        if (!std::getline(std::cin, line)) {
            break;
        }
        line = trim_copy(line);
        if (line == "q" || line == "quit" || line == "exit") {
            break;
        }
        if (line.empty()) {
            continue;
        }

        try {
            std::istringstream command_stream(line);
            std::string command;
            command_stream >> command;

            if (command == "help") {
                std::cout << "Numeric input measures that dBm value using the current "
                             "frequency/channel context."
                          << std::endl;
                std::cout << "freq HZ: set the calibration frequency and tune "
                             "--radio-block-id when available."
                          << std::endl;
                std::cout << "chan N: set the radio channel used for calibration marks."
                          << std::endl;
                std::cout << "radio BLOCK_ID: set the radio block used by freq tuning."
                          << std::endl;
                std::cout << "context HZ N: set frequency and radio channel together."
                          << std::endl;
                std::cout << "mark0 DBM [HZ] [CHAN]: measure and mark that input power "
                             "as the 0 dBFS reference for the context."
                          << std::endl;
                continue;
            }
            if (command == "freq") {
                double new_frequency = 0.0;
                if (!(command_stream >> new_frequency)) {
                    std::cout << "Usage: freq HZ" << std::endl;
                    continue;
                }
                calibration.frequency_hz = new_frequency;
                calibration.has_frequency = true;
                if (calibration.radio_block_id.empty()) {
                    std::cout << "Frequency context set to " << std::setprecision(12)
                              << calibration.frequency_hz
                              << " Hz. Use 'radio BLOCK_ID' to also tune hardware."
                              << std::endl;
                } else {
                    apply_radio_frequency(graph, calibration);
                }
                continue;
            }
            if (command == "chan") {
                size_t new_channel = 0;
                if (!(command_stream >> new_channel)) {
                    std::cout << "Usage: chan N" << std::endl;
                    continue;
                }
                calibration.radio_channel = new_channel;
                std::cout << "Calibration radio channel set to "
                          << calibration.radio_channel << std::endl;
                if (calibration.has_frequency && !calibration.radio_block_id.empty()) {
                    apply_radio_frequency(graph, calibration);
                }
                continue;
            }
            if (command == "radio") {
                std::string new_radio_block_id;
                if (!(command_stream >> new_radio_block_id)) {
                    std::cout << "Usage: radio BLOCK_ID" << std::endl;
                    continue;
                }
                calibration.radio_block_id = new_radio_block_id;
                std::cout << "Calibration radio block set to "
                          << calibration.radio_block_id << std::endl;
                if (calibration.has_frequency) {
                    apply_radio_frequency(graph, calibration);
                }
                continue;
            }
            if (command == "context") {
                double new_frequency = 0.0;
                size_t new_channel = 0;
                if (!(command_stream >> new_frequency >> new_channel)) {
                    std::cout << "Usage: context HZ CHANNEL" << std::endl;
                    continue;
                }
                calibration.frequency_hz = new_frequency;
                calibration.has_frequency = true;
                calibration.radio_channel = new_channel;
                if (calibration.radio_block_id.empty()) {
                    std::cout << "Calibration context set to " << std::setprecision(12)
                              << calibration.frequency_hz << " Hz, radio_ch="
                              << calibration.radio_channel
                              << ". Use 'radio BLOCK_ID' to also tune hardware."
                              << std::endl;
                } else {
                    apply_radio_frequency(graph, calibration);
                }
                continue;
            }
            if (command == "marks") {
                if (zero_dbfs_marks.empty()) {
                    std::cout << "No 0 dBFS marks recorded." << std::endl;
                    continue;
                }
                for (size_t i = 0; i < zero_dbfs_marks.size(); i++) {
                    const auto& mark = zero_dbfs_marks[i];
                    std::cout << i << ": " << std::setprecision(12)
                              << mark.frequency_hz << " Hz, radio_ch="
                              << mark.radio_channel << ", 0dBFS="
                              << mark.power_dbm << " dBm" << std::endl;
                }
                continue;
            }
            if (command == "mark0") {
                double dbm_in = 0.0;
                if (!(command_stream >> dbm_in)) {
                    std::cout << "Usage: mark0 DBM [HZ] [CHANNEL]" << std::endl;
                    continue;
                }
                double new_frequency = 0.0;
                if (command_stream >> new_frequency) {
                    calibration.frequency_hz = new_frequency;
                    calibration.has_frequency = true;
                    size_t new_channel = 0;
                    if (command_stream >> new_channel) {
                        calibration.radio_channel = new_channel;
                    }
                    if (!calibration.radio_block_id.empty()) {
                        apply_radio_frequency(graph, calibration);
                    }
                }
                record_measurement(dbm_in, true);
                continue;
            }

            size_t consumed     = 0;
            const double dbm_in = std::stod(line, &consumed);
            if (consumed != line.find_last_not_of(" \t\r\n") + 1) {
                throw std::invalid_argument("trailing characters");
            }

            record_measurement(dbm_in, false);
        } catch (const std::exception&) {
            std::cout << "Enter a numeric dBm value, or q to quit." << std::endl;
        }
    }

    stop_signal_called.store(true);
    uhd::stream_cmd_t stop_cmd(uhd::stream_cmd_t::STREAM_MODE_STOP_CONTINUOUS);
    rx_stream->issue_stream_cmd(stop_cmd);
    if (receiver.joinable()) {
        receiver.join();
    }

    std::cout << std::endl << "Done!" << std::endl;
    return EXIT_SUCCESS;
}