#!/usr/bin/env python3
"""
Spectral Analysis Utility supporting CHDR and TSI-style packet dumps.

FIXED VERSION - Properly handles TSI format from rfnoc_stream_tool with SGB processing.

Key fixes:
 - Correct TSI header struct (32 bytes, matching Packet_header.h)
 - TSI header has NO payload_len field - must use --spp parameter
 - SGB decimation support via --sgb flag (halves samples per packet)
 - Proper raw-tsi parsing without relying on non-existent fields
"""
from pathlib import Path
from typing import Optional, Tuple, List
import argparse
import numpy as np
import matplotlib.pyplot as plt
from scipy import signal
import struct
import sys
import os
import math
import traceback


class SpectrumAnalyzer:
    """Analyze and plot spectrum from captured SDR samples in CHDR or TSI packet formats"""

    # Common UHD/USRP data formats
    FORMATS = {
        'sc16': (np.int16, 2),      # 16-bit signed complex (I,Q pairs)
        'sc8': (np.int8, 2),        # 8-bit signed complex
        'fc32': (np.float32, 2),    # 32-bit float complex
        'fc64': (np.float64, 2),    # 64-bit float complex
    }

    # CHDR packet types
    PKT_TYPE_DATA_NO_TS = 0x6
    PKT_TYPE_DATA_WITH_TS = 0x7

    # =========================================================================
    # TSI Header Definition (from Packet_header.h)
    # =========================================================================
    # struct PACKET_HEADER {
    #     unsigned char   ReceiverType[4];    // 4 bytes  - "meo2" or "meo "
    #     unsigned int    PacketNumber;       // 4 bytes  (offset 4)
    #     unsigned short  SATID;              // 2 bytes  (offset 8)
    #     unsigned short  YearMonth;          // 2 bytes  (offset 10) - packed: year(12bits) | month(4bits)
    #     unsigned char   Hour;               // 1 byte   (offset 12)
    #     unsigned char   Day;                // 1 byte   (offset 13)
    #     unsigned char   Sec;                // 1 byte   (offset 14)
    #     unsigned char   Minute;             // 1 byte   (offset 15)
    #     unsigned int    FiveNanoSecCount;   // 4 bytes  (offset 16)
    #     unsigned int    TuningFreq;         // 4 bytes  (offset 20)
    #     unsigned short  ReceiverFlag;       // 2 bytes  (offset 24) - channel in bits 0-2
    #     unsigned short  Reserved;           // 2 bytes  (offset 26)
    # };
    # Total: 28 bytes (no padding needed - naturally aligned)
    #
    # CRITICAL: There is NO payload_len field! Payload size must be known externally.
    # =========================================================================

    TSI_HEADER_LENGTH = 28  # 4+4+2+2+1+1+1+1+4+4+2+2 = 28 bytes
    TSI_STRUCT_FMT = '<4sIHHBBBBIIHH'  # Little-endian, struct.calcsize() = 28
    TSI_VALID_RECEIVER_TYPES = [b'meo2', b'meo ']

    def __init__(self, filepath: str, format_type: str = 'sc16',
                 sample_rate: Optional[float] = None, center_freq: float = 0.0,
                 force_tsi: bool = False, force_chdr: bool = False, raw_tsi: bool = False,
                 spp: int = 1024, sgb_enabled: bool = False):
        self.filepath = Path(filepath)
        self.format_type = format_type
        self._user_sample_rate = sample_rate
        self.sample_rate = sample_rate if sample_rate else 200e6
        self.center_freq = center_freq
        self.file_header = None
        self.packet_info = []
        self.samples = None
        self.force_tsi = force_tsi
        self.force_chdr = force_chdr
        self.raw_tsi = raw_tsi

        # TSI-specific parameters
        self.spp = spp  # Samples per packet (BEFORE any decimation)
        self.sgb_enabled = sgb_enabled  # SGB decimation by 2

        if format_type not in self.FORMATS:
            raise ValueError(f"Unknown format: {format_type}. Supported: {list(self.FORMATS.keys())}")
        self.dtype, self.components = self.FORMATS[format_type]

    def get_tsi_payload_size(self) -> int:
        """
        Calculate TSI payload size in bytes.
        
        CRITICAL: TSI header has NO payload_len field!
        Size must be calculated from spp and sgb_enabled.
        """
        if self.sgb_enabled:
            # SGB decimates by 2: output has half the samples
            actual_samples = self.spp // 2
        else:
            actual_samples = self.spp

        # sc16 format: 4 bytes per complex sample (I16 + Q16)
        bytes_per_sample = 4
        return actual_samples * bytes_per_sample

    def parse_tsi_header(self, header_bytes: bytes) -> dict:
        """
        Parse TSI packet header (32 bytes) according to Packet_header.h
        
        Returns dict with all header fields, or None if invalid.
        """
        if len(header_bytes) < self.TSI_HEADER_LENGTH:
            return None

        try:
            # Unpack according to struct definition
            (receiver_type, packet_number, sat_id, year_month,
             hour, day, sec, minute, five_ns_count, tuning_freq,
             receiver_flag, reserved) = struct.unpack(
                self.TSI_STRUCT_FMT, header_bytes[:self.TSI_HEADER_LENGTH])

            # Validate receiver type
            is_valid = receiver_type in self.TSI_VALID_RECEIVER_TYPES

            # Extract packed year/month
            year = (year_month >> 4) & 0xFFF   # bits 4-15
            month = year_month & 0xF           # bits 0-3

            # Extract channel from receiver flag (bits 0-2)
            channel = receiver_flag & 0x7

            return {
                'receiver_type': receiver_type,
                'receiver_type_str': receiver_type.decode('ascii', errors='replace'),
                'is_valid_header': is_valid,
                'packet_number': packet_number,
                'sat_id': sat_id,
                'year': year,
                'month': month,
                'day': day,
                'hour': hour,
                'minute': minute,
                'second': sec,
                'five_ns_count': five_ns_count,
                'tuning_freq_hz': tuning_freq,
                'receiver_flag': receiver_flag,
                'channel': channel,
                'reserved': reserved,
                # For compatibility with existing code
                'seq_num': packet_number,
                'pkt_type': None,
                'timestamp': five_ns_count,  # Use 5ns count as timestamp proxy
            }
        except struct.error as e:
            print(f"Warning: Failed to unpack TSI header: {e}")
            return None

    def validate_tsi_file_structure(self) -> bool:
        """
        Validate that file structure matches expected TSI format.
        Returns True if valid, prints diagnostics and returns False otherwise.
        """
        payload_size = self.get_tsi_payload_size()
        packet_size = self.TSI_HEADER_LENGTH + payload_size

        with open(self.filepath, 'rb') as f:
            file_size = f.seek(0, 2)
            f.seek(0)

            expected_packets = file_size / packet_size
            
            print(f"\n=== TSI File Structure Validation ===")
            print(f"  File size: {file_size:,} bytes")
            print(f"  SPP (config): {self.spp}")
            print(f"  SGB enabled: {self.sgb_enabled}")
            print(f"  Actual samples/packet: {self.spp // 2 if self.sgb_enabled else self.spp}")
            print(f"  Payload size: {payload_size} bytes")
            print(f"  Packet size (header+payload): {packet_size} bytes")
            print(f"  Expected packets: {expected_packets:.2f}")

            if expected_packets != int(expected_packets):
                print(f"\n  ⚠️  WARNING: File size not evenly divisible by packet size!")
                print(f"      This suggests wrong --spp or --sgb setting!")
                
                # Try to find correct configuration
                print(f"\n      Trying to detect correct settings...")
                for test_spp in [self.spp, self.spp * 2, self.spp // 2]:
                    for test_sgb in [False, True]:
                        test_samples = test_spp // 2 if test_sgb else test_spp
                        test_payload = test_samples * 4
                        test_packet = self.TSI_HEADER_LENGTH + test_payload
                        if file_size % test_packet == 0:
                            num_pkts = file_size // test_packet
                            print(f"      ✓ spp={test_spp}, sgb={test_sgb} → {num_pkts} packets")
                return False

            # Validate first few packet headers
            print(f"\n  Validating first 3 packet headers...")
            for i in range(min(3, int(expected_packets))):
                f.seek(i * packet_size)
                header_bytes = f.read(self.TSI_HEADER_LENGTH)
                header = self.parse_tsi_header(header_bytes)
                
                if header is None or not header['is_valid_header']:
                    print(f"    Packet {i}: ❌ INVALID (receiver_type={header_bytes[:4] if header_bytes else 'N/A'})")
                    return False
                else:
                    print(f"    Packet {i}: ✓ {header['receiver_type_str']} pkt#{header['packet_number']}")

            print(f"  ✓ File structure validated\n")
            return True

    # -------------------------
    # CHDR helpers (unchanged)
    # -------------------------
    def parse_chdr_header(self, header: int) -> dict:
        """Parse CHDR header fields from 64-bit value (little-endian)"""
        return {
            'dst_epid': header & 0xFFFF,
            'length': (header >> 16) & 0xFFFF,
            'seq_num': (header >> 32) & 0xFFFF,
            'num_mdata': (header >> 48) & 0x1F,
            'pkt_type': (header >> 53) & 0x7,
            'eov': (header >> 56) & 0x1,
            'eob': (header >> 57) & 0x1,
            'vc': (header >> 58) & 0x3F
        }

    def parse_file_header(self, f) -> Optional[dict]:
        """Parse ChdrFileHeader at beginning of file (72 bytes total)."""
        pos = f.tell()
        try:
            magic_bytes = f.read(4)
            if len(magic_bytes) < 4:
                f.seek(pos)
                return None
            magic = int.from_bytes(magic_bytes, byteorder='little', signed=False)
            if magic != 0x43484452:  # 'CHDR'
                f.seek(pos)
                return None

            version = int.from_bytes(f.read(4), byteorder='little', signed=False)
            chdr_width = int.from_bytes(f.read(4), byteorder='little', signed=False)
            f.read(4)  # padding
            tick_rate = np.frombuffer(f.read(8), dtype=np.float64)[0]
            pps_reset_used = int.from_bytes(f.read(4), byteorder='little', signed=False)
            f.read(4)  # padding
            pps_reset_time_sec = np.frombuffer(f.read(8), dtype=np.float64)[0]
            num_streams = int.from_bytes(f.read(4), byteorder='little', signed=False)
            ring_buffer_used = int.from_bytes(f.read(4), byteorder='little', signed=False)
            f.read(20)  # reserved
            f.read(4)   # tail padding

            return {
                'magic': magic,
                'version': version,
                'chdr_width': chdr_width,
                'tick_rate': tick_rate,
                'pps_reset_used': pps_reset_used,
                'pps_reset_time_sec': pps_reset_time_sec,
                'num_streams': num_streams,
                'ring_buffer_used': ring_buffer_used
            }
        finally:
            pass

    def parse_stream_header(self, f) -> dict:
        """Parse StreamHeader (96 bytes)"""
        stream_id = int.from_bytes(f.read(4), byteorder='little', signed=False)
        block_id = f.read(64).decode('utf-8', errors='ignore').rstrip('\x00')
        port = int.from_bytes(f.read(4), byteorder='little', signed=False)
        ring_buffer_size = int.from_bytes(f.read(4), byteorder='little', signed=False)
        f.read(20)  # reserved
        return {
            'stream_id': stream_id,
            'block_id': block_id,
            'port': port,
            'ring_buffer_size': ring_buffer_size
        }

    def _parse_payload_samples(self, payload: bytes) -> Optional[np.ndarray]:
        """Parse payload bytes into complex samples"""
        if len(payload) == 0:
            return None

        try:
            data = np.frombuffer(payload, dtype=self.dtype)
        except Exception as e:
            print(f"Warning: Failed to parse payload into dtype {self.dtype}: {e}")
            return None

        if len(data) % self.components != 0:
            trim = len(data) % self.components
            data = data[:-trim]
            if data.size == 0:
                return None

        if data.size == 0:
            return None

        try:
            iq_pairs = data.reshape(-1, self.components)
        except Exception as e:
            print(f"Warning: couldn't reshape payload to IQ pairs: {e}")
            return None

        if self.format_type.startswith('sc'):
            max_val = np.iinfo(self.dtype).max
            samples = (iq_pairs[:, 0].astype(np.float64) + 1j * iq_pairs[:, 1].astype(np.float64)) / max_val
        else:
            samples = iq_pairs[:, 0] + 1j * iq_pairs[:, 1]

        return samples

    def load_samples(self, max_samples: Optional[int] = None) -> np.ndarray:
        """Load samples from CHDR packet file or TSI-style file"""
        all_samples: List[np.ndarray] = []
        total_samples = 0
        packets_parsed = 0

        with open(self.filepath, 'rb') as f:
            # Try parse CHDR file header first
            self.file_header = self.parse_file_header(f)

            if self.file_header:
                print(f"CHDR file header found:")
                print(f"  Version: {self.file_header['version']}")
                print(f"  Tick rate: {self.file_header['tick_rate']/1e6:.6f} MHz")
                print(f"  Number of streams: {self.file_header['num_streams']}")
                if self._user_sample_rate is None:
                    self.sample_rate = self.file_header['tick_rate']
                    print(f"  -> Using tick_rate as sample rate: {self.sample_rate/1e6:.6f} MHz")

                for stream_idx in range(max(1, self.file_header['num_streams'])):
                    try:
                        stream_header = self.parse_stream_header(f)
                        print(f"Stream {stream_idx}: {stream_header['block_id']} (port {stream_header['port']})")
                    except Exception:
                        break
            else:
                print("No CHDR file header detected.")

            # Determine parsing mode
            parse_mode = None
            if self.force_chdr and self.force_tsi:
                print("Warning: both --force-chdr and --force-tsi set; using --force-chdr")
                self.force_tsi = False

            if self.force_chdr:
                parse_mode = 'chdr'
            elif self.force_tsi or self.raw_tsi:
                parse_mode = 'raw-tsi'
            else:
                # Autodetect
                pos = f.tell()
                peek = f.read(8)
                f.seek(pos)

                if len(peek) < 4:
                    parse_mode = 'raw-tsi'
                else:
                    # Check if first 4 bytes look like TSI receiver type
                    if peek[:4] in self.TSI_VALID_RECEIVER_TYPES:
                        parse_mode = 'raw-tsi'
                        print(f"  Detected TSI header (receiver_type: {peek[:4]})")
                    else:
                        # Check for size-prefixed packets
                        possible_size = int.from_bytes(peek[0:4], byteorder='little', signed=False)
                        file_size = os.fstat(f.fileno()).st_size
                        if 8 <= possible_size <= max(1024, file_size // 2):
                            parse_mode = 'chdr'
                        else:
                            parse_mode = 'raw-tsi'

            print(f"Selected parsing mode: {parse_mode}")

            # =====================================================================
            # CHDR parsing (size-prefixed packets)
            # =====================================================================
            if parse_mode == 'chdr':
                while True:
                    size_bytes = f.read(4)
                    if len(size_bytes) < 4:
                        break

                    packet_size = int.from_bytes(size_bytes, byteorder='little', signed=False)
                    if packet_size == 0:
                        continue
                    if packet_size > 50_000_000:
                        print(f"Warning: packet size {packet_size} too large; aborting")
                        break

                    packet_data = f.read(packet_size)
                    if len(packet_data) < packet_size:
                        break

                    header_info = None
                    timestamp = None
                    payload_offset = 8

                    if len(packet_data) >= 8:
                        header = int.from_bytes(packet_data[0:8], byteorder='little', signed=False)
                        header_info = self.parse_chdr_header(header)
                        has_ts = (header_info['pkt_type'] == self.PKT_TYPE_DATA_WITH_TS)
                        payload_offset = 8 + (8 if has_ts else 0)
                        if has_ts and len(packet_data) >= 16:
                            timestamp = int.from_bytes(packet_data[8:16], byteorder='little', signed=False)

                    payload = packet_data[payload_offset:]

                    self.packet_info.append({
                        'packet_num': packets_parsed,
                        'packet_size': packet_size,
                        'header': header_info,
                        'timestamp': timestamp,
                        'payload_size': len(payload)
                    })

                    if len(payload) > 0:
                        packet_samples = self._parse_payload_samples(payload)
                        if packet_samples is not None and packet_samples.size > 0:
                            all_samples.append(packet_samples)
                            total_samples += packet_samples.size

                    packets_parsed += 1
                    if max_samples is not None and total_samples >= max_samples:
                        break

            # =====================================================================
            # TSI parsing (raw packets, NO size prefix, fixed packet size)
            # =====================================================================
            elif parse_mode == 'raw-tsi':
                # Calculate payload size from spp and sgb settings
                payload_size = self.get_tsi_payload_size()
                packet_size = self.TSI_HEADER_LENGTH + payload_size

                print(f"\nTSI parsing configuration:")
                print(f"  SPP (samples per packet before decimation): {self.spp}")
                print(f"  SGB enabled (decimation by 2): {self.sgb_enabled}")
                print(f"  Actual samples per packet: {self.spp // 2 if self.sgb_enabled else self.spp}")
                print(f"  Payload size: {payload_size} bytes")
                print(f"  Total packet size: {packet_size} bytes")

                # Validate file structure first
                file_size = os.fstat(f.fileno()).st_size
                remaining = file_size - f.tell()
                expected_packets = remaining / packet_size

                if expected_packets != int(expected_packets):
                    print(f"\n⚠️  WARNING: File size ({remaining} bytes) not evenly divisible by packet size ({packet_size})!")
                    print(f"    Expected packets: {expected_packets:.2f}")
                    print(f"    Check your --spp and --sgb settings!")
                    print(f"\n    Hint: If SGB is enabled, actual payload = (spp/2) * 4 bytes")
                    print(f"          If SGB is disabled, actual payload = spp * 4 bytes")
                    
                    # Try to suggest correct settings
                    for test_spp in [self.spp, self.spp * 2, self.spp // 2, 512, 1024, 2048, 4096]:
                        for test_sgb in [False, True]:
                            test_samples = test_spp // 2 if test_sgb else test_spp
                            test_payload = test_samples * 4
                            test_packet = self.TSI_HEADER_LENGTH + test_payload
                            if remaining % test_packet == 0 and test_packet > self.TSI_HEADER_LENGTH:
                                num = remaining // test_packet
                                print(f"    → Try: --spp {test_spp} {'--sgb' if test_sgb else ''} ({num} packets)")

                while True:
                    # Read TSI header
                    header_bytes = f.read(self.TSI_HEADER_LENGTH)
                    if len(header_bytes) < self.TSI_HEADER_LENGTH:
                        break

                    header = self.parse_tsi_header(header_bytes)

                    # Validate header
                    if header is None or not header.get('is_valid_header', False):
                        if packets_parsed == 0:
                            print(f"ERROR: First packet has invalid TSI header!")
                            print(f"  Got: {header_bytes[:4] if header_bytes else 'N/A'}")
                            print(f"  Expected: b'meo2' or b'meo '")
                            break
                        else:
                            # Mid-file corruption or wrong packet size
                            print(f"Warning: Invalid header at packet {packets_parsed}, stopping")
                            print(f"  This usually means wrong --spp or --sgb setting!")
                            break

                    # Read payload (fixed size based on spp and sgb)
                    payload = f.read(payload_size)
                    if len(payload) < payload_size:
                        print(f"Warning: Truncated payload at packet {packets_parsed}")
                        break

                    # Store packet info
                    self.packet_info.append({
                        'packet_num': packets_parsed,
                        'packet_size': self.TSI_HEADER_LENGTH + len(payload),
                        'header': header,
                        'timestamp': header.get('five_ns_count'),
                        'payload_size': len(payload),
                        'tsi_packet_number': header.get('packet_number'),
                        'tsi_channel': header.get('channel'),
                    })

                    # Parse samples
                    if len(payload) > 0:
                        packet_samples = self._parse_payload_samples(payload)
                        if packet_samples is not None and packet_samples.size > 0:
                            all_samples.append(packet_samples)
                            total_samples += packet_samples.size

                    packets_parsed += 1

                    # Progress indicator
                    if packets_parsed % 1000 == 0:
                        print(f"  Parsed {packets_parsed} packets, {total_samples:,} samples...")

                    if max_samples is not None and total_samples >= max_samples:
                        break

        if not all_samples:
            raise ValueError("No valid samples found in file. Check --spp and --sgb settings!")

        self.samples = np.concatenate(all_samples)
        if max_samples is not None and self.samples.size > max_samples:
            self.samples = self.samples[:max_samples]

        print(f"\nLoaded {self.samples.size:,} complex samples from {packets_parsed} packets")
        print(f"  Sample rate: {self.sample_rate/1e6:.6f} MHz")
        print(f"  Duration: {self.samples.size / self.sample_rate * 1e3:.3f} ms")
        print(f"  Full bandwidth: {self.sample_rate/1e6:.6f} MHz")

        return self.samples

    # -------------------------
    # Spectrum computation and plotting
    # -------------------------
    def compute_spectrum(self, nfft: int = 8192, overlap: float = 0.5,
                        span: Optional[float] = None,
                        span_center: Optional[float] = None) -> Tuple[np.ndarray, np.ndarray]:
        if self.samples is None:
            raise RuntimeError("Must call load_samples() first")

        samples = self.samples
        effective_rate = self.sample_rate
        freq_offset = 0.0

        if span is not None and span < self.sample_rate * 0.95:
            samples, effective_rate, freq_offset = self._zoom_fft_preprocess(
                samples, span, span_center or 0.0
            )
            print(f"  Zoom FFT: span={span/1e6:.6f} MHz, effective_rate={effective_rate/1e6:.6f} MHz")

        if len(samples) < nfft:
            print(f"Warning: Only {len(samples)} samples available, reducing FFT size")
            nfft = len(samples)

        window = np.hanning(nfft)
        step = max(1, int(nfft * (1 - overlap)))
        num_segments = max(1, (len(samples) - nfft) // step + 1)

        psd = np.zeros(nfft, dtype=np.float64)
        for i in range(num_segments):
            start = i * step
            end = start + nfft
            if end > len(samples):
                break
            segment = samples[start:end] * window
            fft_result = np.fft.fft(segment, n=nfft)
            psd += np.abs(fft_result) ** 2

        psd /= num_segments
        psd_db = 10 * np.log10(psd + 1e-12)
        psd_db = np.fft.fftshift(psd_db)
        freqs = np.fft.fftshift(np.fft.fftfreq(nfft, 1/effective_rate))
        freqs = freqs + freq_offset + self.center_freq

        return freqs, psd_db

    def _zoom_fft_preprocess(self, samples: np.ndarray, span: float,
                             span_center: float) -> Tuple[np.ndarray, float, float]:
        target_rate = span * 1.25
        decim_factor = int(np.floor(self.sample_rate / target_rate))
        decim_factor = max(1, decim_factor)
        max_decim = max(1, len(samples) // 1000)
        decim_factor = min(decim_factor, max_decim)
        new_sample_rate = self.sample_rate / decim_factor

        if span_center != 0:
            t = np.arange(len(samples)) / self.sample_rate
            shift = np.exp(-2j * np.pi * span_center * t)
            samples = samples * shift

        if decim_factor > 1:
            cutoff = 0.8 / decim_factor
            numtaps = min(255, len(samples) // 10)
            numtaps = max(15, numtaps)
            if numtaps % 2 == 0:
                numtaps += 1
            fir_coeffs = signal.firwin(numtaps, cutoff, window='hamming')
            samples = signal.lfilter(fir_coeffs, 1.0, samples)
            samples = samples[::decim_factor]

        return samples, new_sample_rate, span_center

    def compute_spectrogram(self, nfft: int = 1024,
                            overlap: float = 0.75) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        if self.samples is None:
            raise RuntimeError("Must call load_samples() first")

        step = int(nfft * (1 - overlap))
        num_segments = (len(self.samples) - nfft) // step + 1

        if num_segments < 1:
            raise ValueError(f"Not enough samples for spectrogram with FFT size {nfft}")

        window = np.hanning(nfft)
        spectrogram = np.zeros((num_segments, nfft))

        for i in range(num_segments):
            start = i * step
            end = start + nfft
            segment = self.samples[start:end] * window
            fft_result = np.fft.fftshift(np.fft.fft(segment))
            spectrogram[i, :] = np.abs(fft_result) ** 2

        spectrogram_db = 10 * np.log10(spectrogram + 1e-12)
        times = np.arange(num_segments) * step / self.sample_rate
        freqs = np.fft.fftshift(np.fft.fftfreq(nfft, 1/self.sample_rate)) + self.center_freq

        return times, freqs, spectrogram_db

    def plot_spectrum(self, nfft: int = 8192, max_samples: Optional[int] = None,
                      overlap: float = 0.5, show_time: bool = True,
                      save_path: Optional[str] = None,
                      span: Optional[float] = None,
                      span_center: Optional[float] = None,
                      show_spectrogram: bool = False):
        if self.samples is None:
            self.load_samples(max_samples)

        if show_spectrogram:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        elif show_time:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
        else:
            fig, ax2 = plt.subplots(1, 1, figsize=(12, 5))
            ax1 = None

        if show_spectrogram:
            times, freqs, spec_db = self.compute_spectrogram(nfft=1024, overlap=0.75)
            im = ax1.pcolormesh(times * 1e3, freqs / 1e6, spec_db.T, shading='auto', cmap='viridis')
            ax1.set_xlabel('Time (ms)')
            ax1.set_ylabel('Frequency (MHz)')
            ax1.set_title('Spectrogram')
            plt.colorbar(im, ax=ax1, label='Power (dB)')
        elif show_time:
            num_display = min(10000, self.samples.size)
            time_axis = np.arange(num_display) / self.sample_rate * 1e6
            ax1.plot(time_axis, np.real(self.samples[:num_display]), label='I (Real)', alpha=0.7, linewidth=0.5)
            ax1.plot(time_axis, np.imag(self.samples[:num_display]), label='Q (Imag)', alpha=0.7, linewidth=0.5)
            ax1.set_xlabel('Time (μs)')
            ax1.set_ylabel('Amplitude')
            ax1.set_title(f'Time Domain (first {num_display} samples)')
            ax1.legend()
            ax1.grid(True, alpha=0.3)

        freqs, psd_db = self.compute_spectrum(nfft, overlap, span=span, span_center=span_center)
        ax2.plot(freqs / 1e6, psd_db, linewidth=0.8)
        ax2.set_xlabel('Frequency (MHz)')
        ax2.set_ylabel('Power (dB)')

        actual_span = freqs[-1] - freqs[0]
        title = f'Power Spectral Density (FFT: {nfft}'
        if span is not None:
            title += f', Zoom Span: {span/1e6:.3f} MHz'
        title += ')'
        ax2.set_title(title)
        ax2.grid(True, alpha=0.3)

        center_freq_str = f"{self.center_freq/1e6:.6f} MHz" if self.center_freq != 0 else "Baseband"
        sgb_str = "Yes (÷2)" if self.sgb_enabled else "No"
        stats_text = (f"File: {self.filepath.name}\n"
                      f"Format: {self.format_type}\n"
                      f"Samples: {self.samples.size:,}\n"
                      f"Sample Rate: {self.sample_rate/1e6:.6f} MHz\n"
                      f"Center Freq: {center_freq_str}\n"
                      f"SPP: {self.spp}, SGB: {sgb_str}\n"
                      f"Duration: {self.samples.size/self.sample_rate*1e3:.2f} ms")
        ax2.text(0.02, 0.98, stats_text, transform=ax2.transAxes,
                 verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5),
                 fontsize=9, family='monospace')

        plt.tight_layout()

        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"Saved plot to {save_path}")
        else:
            plt.show()


def parse_freq(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    value = value.strip()
    multipliers = {'k': 1e3, 'K': 1e3, 'm': 1e6, 'M': 1e6, 'g': 1e9, 'G': 1e9}
    for suffix, mult in multipliers.items():
        if value.endswith(suffix):
            return float(value[:-1]) * mult
    return float(value)


def main():
    parser = argparse.ArgumentParser(
        description='Analyze and plot spectrum from captured SDR samples (CHDR or TSI packet formats).',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
TSI Format Notes:
  The TSI header (from rfnoc_stream_tool) has NO payload_len field!
  You MUST specify --spp (samples per packet) and --sgb if SGB processing was used.

  Examples:
    # TSI file with SGB enabled, 1024 samples per packet (before decimation)
    %(prog)s capture.bin --raw-tsi --spp 1024 --sgb -r 10M

    # TSI file without SGB
    %(prog)s capture.bin --raw-tsi --spp 1024 -r 20M

    # CHDR format (auto-detected, no special flags needed)
    %(prog)s capture.dat -r 20M
        """
    )

    parser.add_argument('file', help='Path to .dat/.bin file containing packets')
    parser.add_argument('-f', '--format', default='sc16', choices=['sc16', 'sc8', 'fc32', 'fc64'],
                        help='Sample format (default: sc16)')
    parser.add_argument('-r', '--sample-rate', type=str, default=None,
                        help='Sample rate (e.g., 10M, 200M). For SGB: use rate AFTER decimation.')
    parser.add_argument('-c', '--center-freq', type=str, default='0',
                        help='Center frequency (e.g., 406M)')

    # TSI-specific arguments
    parser.add_argument('--spp', type=int, default=1024,
                        help='Samples per packet BEFORE any decimation (default: 1024)')
    parser.add_argument('--sgb', action='store_true',
                        help='Enable SGB mode (samples per packet halved due to decimation)')

    parser.add_argument('--span', type=str, default=None, help='Zoom span (e.g., 1M)')
    parser.add_argument('--span-center', type=str, default=None, help='Zoom center offset')
    parser.add_argument('--fft-size', type=int, default=8192, help='FFT size (default: 8192)')
    parser.add_argument('--overlap', type=float, default=0.5, help='FFT overlap (default: 0.5)')
    parser.add_argument('--max-samples', type=int, default=None, help='Max samples to load')
    parser.add_argument('--no-time', action='store_true', help='Skip time domain plot')
    parser.add_argument('--spectrogram', action='store_true', help='Show spectrogram instead of time')
    parser.add_argument('-o', '--output', default=None, help='Save plot to file')
    parser.add_argument('--show-packets', action='store_true', help='Show packet info')
    parser.add_argument('--validate', action='store_true', help='Validate TSI file structure and exit')

    parser.add_argument('--force-tsi', action='store_true', help='Force TSI parsing')
    parser.add_argument('--force-chdr', action='store_true', help='Force CHDR parsing')
    parser.add_argument('--raw-tsi', action='store_true',
                        help='Treat file as raw TSI (no 4-byte size prefix, fixed packet size)')

    args = parser.parse_args()

    sample_rate = parse_freq(args.sample_rate) if args.sample_rate else None
    center_freq = parse_freq(args.center_freq)
    span = parse_freq(args.span) if args.span else None
    span_center = parse_freq(args.span_center) if args.span_center else None

    analyzer = SpectrumAnalyzer(
        filepath=args.file,
        format_type=args.format,
        sample_rate=sample_rate,
        center_freq=center_freq,
        force_tsi=args.force_tsi,
        force_chdr=args.force_chdr,
        raw_tsi=args.raw_tsi,
        spp=args.spp,
        sgb_enabled=args.sgb,
    )

    # Validation mode
    if args.validate:
        valid = analyzer.validate_tsi_file_structure()
        return 0 if valid else 1

    try:
        analyzer.load_samples(max_samples=args.max_samples)

        if args.show_packets:
            print("\nPacket Information (first 20):")
            print("=" * 100)
            for i, pkt in enumerate(analyzer.packet_info[:20]):
                hdr = pkt.get('header', {})
                if hdr and 'receiver_type_str' in hdr:
                    # TSI packet
                    print(f"Pkt {i:4d}: TSI#{hdr.get('packet_number', 'N/A'):6d} "
                          f"Ch={hdr.get('channel', 'N/A')} "
                          f"5ns={hdr.get('five_ns_count', 'N/A'):12d} "
                          f"Payload={pkt.get('payload_size', 0)} bytes")
                else:
                    # CHDR packet
                    print(f"Pkt {i:4d}: Size={pkt['packet_size']} "
                          f"Payload={pkt['payload_size']} "
                          f"Seq={hdr.get('seq_num') if hdr else 'N/A'} "
                          f"TS={pkt.get('timestamp')}")
            if len(analyzer.packet_info) > 20:
                print(f"... {len(analyzer.packet_info)-20} more packets")
            print("=" * 100)

        analyzer.plot_spectrum(
            nfft=args.fft_size,
            max_samples=args.max_samples,
            overlap=args.overlap,
            show_time=not args.no_time and not args.spectrogram,
            save_path=args.output,
            span=span,
            span_center=span_center,
            show_spectrogram=args.spectrogram
        )

    except FileNotFoundError:
        print(f"Error: File not found: {args.file}", file=sys.stderr)
        return 1
    except Exception as e:
        print("Error while processing file:", file=sys.stderr)
        traceback.print_exc()
        return 1

    return 0


if __name__ == '__main__':
    sys.exit(main())