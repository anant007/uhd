// =============================================================================
// daughterboard_caps.h - Daughterboard Capability Detection for UHD/RFNoC
// =============================================================================
//
// This header provides structures and utilities for detecting and handling
// different USRP daughterboard types and their varying capabilities.
//
// Supported Daughterboards:
// -------------------------
// | Daughterboard | Gain | BW   | DC/IQ | Freq Tuning | Antenna Names       |
// |---------------|------|------|-------|-------------|---------------------|
// | BasicRX       | No   | No   | No    | No (meta)   | A, B, AB, BA        |
// | BasicTX       | No   | No   | No    | No (meta)   | A, B, AB, BA        |
// | TwinRX        | Yes  | Yes  | Yes   | Yes         | RX1, RX2            |
// | SBX           | Yes  | Yes  | Yes   | Yes         | TX/RX, RX2          |
// | UBX           | Yes  | Yes  | Yes   | Yes         | TX/RX, RX2          |
// | WBX           | Yes  | Yes  | Yes   | Yes         | TX/RX, RX2          |
// | CBX           | Yes  | Yes  | Yes   | Yes         | TX/RX, RX2          |
// | LFRX          | No   | No   | No    | No (meta)   | A, B, AB, BA        |
// | LFTX          | No   | No   | No    | No (meta)   | A, B, AB, BA        |
// =============================================================================

#ifndef DAUGHTERBOARD_CAPS_H
#define DAUGHTERBOARD_CAPS_H

#include <uhd/rfnoc/radio_control.hpp>
#include <string>
#include <vector>
#include <algorithm>

namespace sdr_framework {

// =============================================================================
// Daughterboard Type Enumeration
// =============================================================================
enum class DaughterboardType {
    UNKNOWN,
    BASIC_RX,       // BasicRX - No gain, no LO, wideband passive
    BASIC_TX,       // BasicTX - No gain, no LO
    TWIN_RX,        // TwinRX - Dual channel, full featured
    SBX,            // SBX - 400-4400 MHz
    UBX,            // UBX - 10-6000 MHz
    WBX,            // WBX - 50-2200 MHz
    CBX,            // CBX - 1.2-6 GHz
    LFRX,           // LFRX - DC-30 MHz, no gain
    LFTX,           // LFTX - DC-30 MHz, no gain
    CUSTOM          // Custom/Unknown with full capabilities assumed
};

// =============================================================================
// Daughterboard Capabilities Structure
// =============================================================================
struct DaughterboardCapabilities {
    DaughterboardType type = DaughterboardType::UNKNOWN;
    std::string name = "Unknown";
    
    // Feature flags
    bool has_gain_control = true;
    bool has_bandwidth_control = true;
    bool has_dc_offset_control = true;
    bool has_iq_balance_control = true;
    bool has_agc = false;
    bool has_lo_export = false;
    bool has_lo_source_select = false;
    bool has_frequency_tuning = true;  // True LO tuning vs metadata-only
    
    // Gain range
    double min_gain = 0.0;
    double max_gain = 0.0;
    double gain_step = 1.0;
    
    // Frequency range
    double min_freq = 0.0;
    double max_freq = 0.0;
    
    // Available antennas
    std::vector<std::string> available_antennas;
    
    // Human-readable description
    std::string description() const {
        std::string desc = name + ": ";
        if (has_frequency_tuning) {
            desc += "Freq tuning, ";
        } else {
            desc += "No LO (metadata only), ";
        }
        if (has_gain_control) {
            desc += "Gain " + std::to_string((int)min_gain) + "-" + 
                    std::to_string((int)max_gain) + "dB, ";
        } else {
            desc += "Fixed gain, ";
        }
        desc += "Antennas: ";
        for (size_t i = 0; i < available_antennas.size(); ++i) {
            desc += available_antennas[i];
            if (i < available_antennas.size() - 1) desc += "/";
        }
        return desc;
    }
};

// =============================================================================
// Daughterboard Detection Function
// =============================================================================
/**
 * @brief Detect daughterboard type and capabilities from a radio_control
 * 
 * This function queries the radio block to determine the daughterboard type
 * and its available features. It uses a combination of:
 * - Available antenna names (most reliable indicator)
 * - Gain range (BasicRX/LFRX have no gain)
 * - Frequency range patterns
 * 
 * @param radio Pointer to radio_control block
 * @param channel Channel index to query
 * @return DaughterboardCapabilities with detected features
 */
inline DaughterboardCapabilities detect_daughterboard_capabilities(
    uhd::rfnoc::radio_control::sptr radio,
    size_t channel = 0)
{
    DaughterboardCapabilities caps;
    
    if (!radio) {
        caps.name = "Error: Null radio pointer";
        return caps;
    }
    
    try {
        // =====================================================================
        // Step 1: Get available antennas - always works
        // =====================================================================
        caps.available_antennas = radio->get_rx_antennas(channel);
        
        // =====================================================================
        // Step 2: Get gain range
        // =====================================================================
        try {
            auto gain_range = radio->get_rx_gain_range(channel);
            caps.min_gain = gain_range.start();
            caps.max_gain = gain_range.stop();
            caps.gain_step = gain_range.step();
        } catch (...) {
            caps.has_gain_control = false;
        }
        
        // =====================================================================
        // Step 3: Get frequency range
        // =====================================================================
        try {
            auto freq_range = radio->get_rx_frequency_range(channel);
            caps.min_freq = freq_range.start();
            caps.max_freq = freq_range.stop();
        } catch (...) {
            // Default wide range
            caps.min_freq = 0;
            caps.max_freq = 6e9;
        }
        
        // =====================================================================
        // Step 4: Classify based on antenna names and gain characteristics
        // =====================================================================
        bool has_meaningful_gain = (caps.max_gain - caps.min_gain) > 1.0;
        
        // Helper lambda to check if any antenna matches
        auto has_antenna = [&caps](const std::vector<std::string>& patterns) -> bool {
            for (const auto& ant : caps.available_antennas) {
                for (const auto& pattern : patterns) {
                    if (ant == pattern) return true;
                }
            }
            return false;
        };
        
        // BasicRX/LFRX pattern: A, B, AB, BA antennas, no meaningful gain
        if (has_antenna({"A", "B", "AB", "BA"}) && !has_meaningful_gain) {
            if (caps.max_freq < 50e6) {
                caps.type = DaughterboardType::LFRX;
                caps.name = "LFRX";
            } else {
                caps.type = DaughterboardType::BASIC_RX;
                caps.name = "BasicRX";
            }
            caps.has_gain_control = false;
            caps.has_bandwidth_control = false;
            caps.has_dc_offset_control = false;
            caps.has_iq_balance_control = false;
            caps.has_agc = false;
            caps.has_frequency_tuning = false;  // No LO - frequency is metadata only
        }
        // TwinRX pattern: RX1, RX2 antennas
        else if (has_antenna({"RX1", "RX2"})) {
            caps.type = DaughterboardType::TWIN_RX;
            caps.name = "TwinRX";
            caps.has_gain_control = true;
            caps.has_bandwidth_control = true;
            caps.has_dc_offset_control = true;
            caps.has_iq_balance_control = true;
            caps.has_agc = false;
            caps.has_lo_export = true;
            caps.has_lo_source_select = true;
            caps.has_frequency_tuning = true;
        }
        // Standard transceiver pattern: TX/RX, RX2 antennas
        else if (has_antenna({"TX/RX", "RX2"})) {
            // Determine specific type by frequency range
            if (caps.min_freq >= 10e6 && caps.max_freq >= 6e9) {
                caps.type = DaughterboardType::UBX;
                caps.name = "UBX";
            } else if (caps.min_freq >= 400e6 && caps.max_freq <= 4.4e9) {
                caps.type = DaughterboardType::SBX;
                caps.name = "SBX";
            } else if (caps.min_freq >= 50e6 && caps.max_freq <= 2.2e9) {
                caps.type = DaughterboardType::WBX;
                caps.name = "WBX";
            } else if (caps.min_freq >= 1.2e9 && caps.max_freq <= 6e9) {
                caps.type = DaughterboardType::CBX;
                caps.name = "CBX";
            } else {
                caps.type = DaughterboardType::CUSTOM;
                caps.name = "Standard Transceiver";
            }
            caps.has_gain_control = true;
            caps.has_bandwidth_control = true;
            caps.has_dc_offset_control = true;
            caps.has_iq_balance_control = true;
            caps.has_agc = false;
            caps.has_frequency_tuning = true;
        }
        // Unknown - assume full capabilities
        else {
            caps.type = DaughterboardType::UNKNOWN;
            caps.name = "Unknown (assuming full capabilities)";
            caps.has_gain_control = has_meaningful_gain;
            caps.has_bandwidth_control = true;
            caps.has_dc_offset_control = true;
            caps.has_iq_balance_control = true;
            caps.has_frequency_tuning = true;
        }
        
    } catch (const std::exception& e) {
        caps.name = std::string("Detection error: ") + e.what();
    }
    
    return caps;
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * @brief Check if a daughterboard supports a specific feature
 */
inline bool supports_feature(const DaughterboardCapabilities& caps, 
                            const std::string& feature) {
    if (feature == "gain") return caps.has_gain_control;
    if (feature == "bandwidth") return caps.has_bandwidth_control;
    if (feature == "dc_offset") return caps.has_dc_offset_control;
    if (feature == "iq_balance") return caps.has_iq_balance_control;
    if (feature == "agc") return caps.has_agc;
    if (feature == "lo_export") return caps.has_lo_export;
    if (feature == "frequency_tuning") return caps.has_frequency_tuning;
    return false;
}

/**
 * @brief Get daughterboard type as string
 */
inline std::string daughterboard_type_to_string(DaughterboardType type) {
    switch (type) {
        case DaughterboardType::BASIC_RX: return "BasicRX";
        case DaughterboardType::BASIC_TX: return "BasicTX";
        case DaughterboardType::TWIN_RX: return "TwinRX";
        case DaughterboardType::SBX: return "SBX";
        case DaughterboardType::UBX: return "UBX";
        case DaughterboardType::WBX: return "WBX";
        case DaughterboardType::CBX: return "CBX";
        case DaughterboardType::LFRX: return "LFRX";
        case DaughterboardType::LFTX: return "LFTX";
        case DaughterboardType::CUSTOM: return "Custom";
        default: return "Unknown";
    }
}

/**
 * @brief Print capabilities summary to stream
 */
inline void print_capabilities(std::ostream& os, 
                              const DaughterboardCapabilities& caps,
                              const std::string& prefix = "  ") {
    os << prefix << "Daughterboard: " << caps.name << "\n";
    os << prefix << "Type: " << daughterboard_type_to_string(caps.type) << "\n";
    os << prefix << "Features:\n";
    os << prefix << "  Gain Control:      " << (caps.has_gain_control ? "Yes" : "No");
    if (caps.has_gain_control) {
        os << " (" << caps.min_gain << " to " << caps.max_gain << " dB)";
    }
    os << "\n";
    os << prefix << "  Bandwidth Control: " << (caps.has_bandwidth_control ? "Yes" : "No") << "\n";
    os << prefix << "  DC Offset:         " << (caps.has_dc_offset_control ? "Yes" : "No") << "\n";
    os << prefix << "  IQ Balance:        " << (caps.has_iq_balance_control ? "Yes" : "No") << "\n";
    os << prefix << "  Frequency Tuning:  " << (caps.has_frequency_tuning ? "Yes" : "No (metadata only)") << "\n";
    os << prefix << "  LO Export:         " << (caps.has_lo_export ? "Yes" : "No") << "\n";
    os << prefix << "Frequency Range: " << caps.min_freq/1e6 << " - " << caps.max_freq/1e6 << " MHz\n";
    os << prefix << "Available Antennas: ";
    for (size_t i = 0; i < caps.available_antennas.size(); ++i) {
        os << "\"" << caps.available_antennas[i] << "\"";
        if (i < caps.available_antennas.size() - 1) os << ", ";
    }
    os << "\n";
}

} // namespace sdr_framework

#endif // DAUGHTERBOARD_CAPS_H