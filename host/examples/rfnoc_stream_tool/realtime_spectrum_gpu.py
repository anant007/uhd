#!/usr/bin/env python3
"""
TSI SDR Real-Time Spectrum Analyzer (Enhanced + Fixed)

High-performance spectrum visualization for TSI SDR applications.
Designed to integrate with rfnoc_stream_tool for live streaming visualization.

FIXES IN THIS VERSION:
 - Time-based lag tolerance (default 1.0 second acceptable lag)
 - GPU memory leak prevention (explicit memory pool clearing)
 - Aggressive frame dropping to maintain realtime
 - Smaller queue to prevent backup

FEATURES:
 - Live follow mode (--follow): Watch file being written in real-time
 - Real-time sync mode (--realtime-sync): Maintain buffer behind write position
 - Real-time playback mode: Play back recorded files
 - Static analysis mode: Load and analyze complete files
 - GPU acceleration: CuPy (NVIDIA) or dpnp (Intel)
 - OpenGL rendering via PyQtGraph
 - Peak tracking with Doppler rate calculation
 - Waterfall display
 - Context menus for runtime control of averaging, filtering, display

REALTIME SYNC MODE:
 When streaming to a file, the writer writes packets in batches (e.g., 200 pkts).
 The reader maintains a configurable buffer of packets behind EOF to ensure
 stable reading without conflicts with the writer.
 
 Buffer parameters:
   --max-lag 1.0           Maximum acceptable lag in seconds (default: 1.0)
   --drop-threshold 0.5    Start dropping when lag exceeds this (default: 0.5)
   --realtime-buffer 250   Target buffer (default: 250 packets)

INTEGRATION WITH rfnoc_stream_tool:
 - Use --follow to watch output file as it's being written
 - Use --realtime-sync to maintain proper buffer behind write position
 - Auto-waits for file to have data before starting
 - Graceful handling when file stops growing

BUILDING STANDALONE EXECUTABLE:
    pip install pyinstaller
    pyinstaller --onefile --name tsi_spectrum_analyzer \\
        --hidden-import pyqtgraph.graphicsItems.ViewBox.axisCtrlTemplate_pyqt6 \\
        --hidden-import pyqtgraph.graphicsItems.PlotItem.plotConfigTemplate_pyqt6 \\
        --hidden-import pyqtgraph.imageview.ImageViewTemplate_pyqt6 \\
        tsi_spectrum_analyzer_enhanced_fixed.py

Requirements:
    pip install pyqtgraph PyQt6 numpy scipy
    pip install cupy-cuda12x   # NVIDIA GPU (optional)
    pip install dpnp dpctl     # Intel GPU (optional)
"""

import sys
import os
import time
import struct
import threading
import queue
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Any, Dict, Callable
from collections import deque
import argparse
import traceback
import datetime

import numpy as np
from scipy import signal

# PyQtGraph and Qt imports
try:
    import pyqtgraph as pg
    from pyqtgraph.Qt import QtCore, QtWidgets, QtGui
    PYQTGRAPH_AVAILABLE = True
    
    # Qt compatibility helpers for different Qt versions (PyQt5, PyQt6, PySide2, PySide6)
    # PyQt6/PySide6 use nested enums, PyQt5/PySide2 use flat enums
    def _get_qt_enum(parent, *names):
        """Get Qt enum value, handling different Qt binding styles"""
        obj = parent
        for name in names:
            if hasattr(obj, name):
                obj = getattr(obj, name)
            else:
                return None
        return obj
    
    # Key constants
    Qt_Key_Space = _get_qt_enum(QtCore.Qt, 'Key', 'Key_Space') or _get_qt_enum(QtCore.Qt, 'Key_Space')
    Qt_Key_R = _get_qt_enum(QtCore.Qt, 'Key', 'Key_R') or _get_qt_enum(QtCore.Qt, 'Key_R')
    Qt_Key_A = _get_qt_enum(QtCore.Qt, 'Key', 'Key_A') or _get_qt_enum(QtCore.Qt, 'Key_A')
    Qt_Key_T = _get_qt_enum(QtCore.Qt, 'Key', 'Key_T') or _get_qt_enum(QtCore.Qt, 'Key_T')
    Qt_Key_V = _get_qt_enum(QtCore.Qt, 'Key', 'Key_V') or _get_qt_enum(QtCore.Qt, 'Key_V')
    Qt_Key_M = _get_qt_enum(QtCore.Qt, 'Key', 'Key_M') or _get_qt_enum(QtCore.Qt, 'Key_M')
    Qt_Key_N = _get_qt_enum(QtCore.Qt, 'Key', 'Key_N') or _get_qt_enum(QtCore.Qt, 'Key_N')
    Qt_Key_S = _get_qt_enum(QtCore.Qt, 'Key', 'Key_S') or _get_qt_enum(QtCore.Qt, 'Key_S')
    Qt_Key_Q = _get_qt_enum(QtCore.Qt, 'Key', 'Key_Q') or _get_qt_enum(QtCore.Qt, 'Key_Q')
    
    # Mouse button constants
    Qt_RightButton = _get_qt_enum(QtCore.Qt, 'MouseButton', 'RightButton') or _get_qt_enum(QtCore.Qt, 'RightButton')
    
    # Pen style constants
    Qt_DashLine = _get_qt_enum(QtCore.Qt, 'PenStyle', 'DashLine') or _get_qt_enum(QtCore.Qt, 'DashLine')
    Qt_DotLine = _get_qt_enum(QtCore.Qt, 'PenStyle', 'DotLine') or _get_qt_enum(QtCore.Qt, 'DotLine')
    
    # Color role constants (for palette)
    Qt_ColorRole_Window = _get_qt_enum(QtGui.QPalette, 'ColorRole', 'Window') or _get_qt_enum(QtGui.QPalette, 'Window')
    Qt_ColorRole_WindowText = _get_qt_enum(QtGui.QPalette, 'ColorRole', 'WindowText') or _get_qt_enum(QtGui.QPalette, 'WindowText')
    
except ImportError:
    PYQTGRAPH_AVAILABLE = False
    print("ERROR: PyQtGraph not installed. Install with: pip install pyqtgraph PyQt6")

# GPU acceleration
try:
    import cupy as cp
    CUPY_AVAILABLE = True
except ImportError:
    CUPY_AVAILABLE = False
    cp = None

try:
    import dpnp
    import dpctl
    DPNP_AVAILABLE = True
    try:
        devices = dpctl.get_devices(device_type="gpu")
        INTEL_GPU_AVAILABLE = len(devices) > 0
    except Exception:
        INTEL_GPU_AVAILABLE = False
except ImportError:
    DPNP_AVAILABLE = False
    INTEL_GPU_AVAILABLE = False
    dpnp = None
    dpctl = None


# =============================================================================
# Version Info (for PyInstaller)
# =============================================================================
__version__ = "1.2.0"  # Added time-based realtime sync fixes
__author__ = "TSI Engineering"


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class PlaybackConfig:
    """Configuration for playback/visualization"""
    update_rate_hz: float = 60.0
    use_gpu: bool = False
    prefer_intel: bool = False
    waterfall_history: int = 200
    show_waterfall: bool = True
    show_time_domain: bool = True
    loop_playback: bool = False
    playback_speed: float = 1.0
    nfft: int = 4096
    overlap: float = 0.5
    # Window function
    window_type: str = 'hann'  # hann, hamming, blackman, kaiser, flattop
    kaiser_beta: float = 14.0
    # Follow mode (live streaming)
    follow_mode: bool = False
    follow_poll_interval: float = 0.1  # seconds
    follow_timeout: float = 5.0  # seconds without new data = EOF
    # Realtime sync mode - maintains buffer behind file write position
    realtime_sync: bool = False
    realtime_buffer_packets: int = 250    # Target packets behind EOF (> write batch size)
    realtime_min_buffer: int = 100        # Min buffer before slowing down  
    realtime_max_buffer: int = 500        # Max buffer before skipping to catch up
    # NEW: Time-based lag tolerance (LAXER defaults)
    max_lag_seconds: float = 1.0          # Maximum acceptable lag in seconds
    drop_threshold_seconds: float = 0.5   # Start dropping when lag exceeds this
    # Peak tracking
    enable_peak_tracking: bool = False
    peak_threshold_db: float = -60.0
    peak_history_seconds: float = 60.0
    peak_smoothing: int = 3
    doppler_window: float = 5.0
    # Averaging
    enable_averaging: bool = False
    avg_count: int = 10
    avg_mode: str = 'linear'  # linear, exponential
    exp_alpha: float = 0.1  # For exponential averaging
    # Max hold
    enable_max_hold: bool = False
    max_hold_decay: float = 0.0  # 0 = no decay, >0 = decay rate per frame
    # Min hold
    enable_min_hold: bool = False
    # Zoom
    span: Optional[float] = None
    span_center: Optional[float] = None


@dataclass
class PeakTrackingState:
    """State for peak tracking"""
    peak_freq_history: deque = field(default_factory=lambda: deque(maxlen=10000))
    peak_power_history: deque = field(default_factory=lambda: deque(maxlen=10000))
    smoothing_buffer: deque = field(default_factory=lambda: deque(maxlen=5))
    last_peak_freq: float = 0.0
    last_peak_power: float = -100.0
    doppler_rate: float = 0.0
    track_locked: bool = False
    lock_freq: float = 0.0
    lock_bandwidth: float = 1000.0


@dataclass
class RealtimeSyncState:
    """State for realtime synchronization - maintains buffer behind write position"""
    sync_enabled: bool = False
    # Buffer management - stay this many packets behind end of file
    buffer_packets: int = 250        # Target buffer (should be > write batch size)
    min_buffer_packets: int = 100    # Minimum safe buffer before we slow down
    max_buffer_packets: int = 500    # Maximum buffer before we speed up/skip
    # Tracking
    packets_skipped: int = 0
    current_buffer_packets: int = 0  # How many packets behind EOF we currently are
    sync_established: bool = False
    last_packet_timestamp: float = 0.0
    # Status
    status: str = "INIT"  # INIT, BUFFERING, LIVE, CATCHING_UP, DROPPING
    
    # NEW: Time-based lag tracking (MORE IMPORTANT than packet-based)
    current_lag_seconds: float = 0.0
    max_lag_seconds: float = 1.0         # LAXER default - 1 second OK
    drop_threshold_seconds: float = 0.5  # Start dropping above this
    frames_dropped: int = 0
    stream_start_wallclock: float = 0.0  # When we started reading
    first_packet_timestamp: float = 0.0  # Timestamp from first packet


# =============================================================================
# GPU Accelerator with Memory Management
# =============================================================================

class GPUAccelerator:
    """GPU acceleration wrapper with explicit memory management"""
    
    def __init__(self, use_gpu: bool = True, prefer_intel: bool = False):
        self.backend = 'numpy'
        self.xp = np
        self.device_name = "CPU"
        self.use_float32 = False
        self._frame_count = 0
        self._memory_clear_interval = 100  # Clear memory pool every N frames
        
        if not use_gpu:
            return
        
        backends = ['intel', 'nvidia'] if prefer_intel else ['nvidia', 'intel']
        
        for backend in backends:
            if backend == 'nvidia' and CUPY_AVAILABLE:
                try:
                    self.xp = cp
                    self.backend = 'cupy'
                    try:
                        device_id = cp.cuda.Device().id
                        props = cp.cuda.runtime.getDeviceProperties(device_id)
                        self.device_name = props['name'].decode() if isinstance(props['name'], bytes) else props['name']
                    except:
                        self.device_name = "NVIDIA GPU"
                    return
                except:
                    continue
                    
            elif backend == 'intel' and DPNP_AVAILABLE and INTEL_GPU_AVAILABLE:
                try:
                    self.xp = dpnp
                    self.backend = 'dpnp'
                    self.use_float32 = True
                    try:
                        devices = dpctl.get_devices(device_type="gpu")
                        if devices:
                            self.device_name = devices[0].name
                            dpctl.set_default_device(devices[0])
                    except:
                        self.device_name = "Intel GPU"
                    return
                except:
                    continue
    
    def get_complex_dtype(self):
        return np.complex64 if self.use_float32 else np.complex128
    
    def get_float_dtype(self):
        return np.float32 if self.use_float32 else np.float64
    
    def clear_gpu_memory(self, force: bool = False):
        """Clear GPU memory pool to prevent leaks"""
        self._frame_count += 1
        
        # Only clear periodically unless forced
        if not force and self._frame_count % self._memory_clear_interval != 0:
            return
        
        if self.backend == 'cupy' and cp is not None:
            try:
                mempool = cp.get_default_memory_pool()
                pinned_mempool = cp.get_default_pinned_memory_pool()
                mempool.free_all_blocks()
                pinned_mempool.free_all_blocks()
            except Exception:
                pass
        # dpnp doesn't have explicit memory pool management
    
    def compute_spectrum_batched(self, samples: np.ndarray, nfft: int, 
                                  window: np.ndarray, overlap: float = 0.5) -> np.ndarray:
        """Compute power spectrum using batched FFT with memory management"""
        n_samples = len(samples)
        
        if n_samples < nfft:
            padded = np.zeros(nfft, dtype=self.get_complex_dtype())
            padded[:n_samples] = samples.astype(self.get_complex_dtype())
            samples = padded
            n_samples = nfft
        
        step = max(1, int(nfft * (1 - overlap)))
        num_segments = max(1, (n_samples - nfft) // step + 1)
        
        from numpy.lib.stride_tricks import as_strided
        samples_typed = samples.astype(self.get_complex_dtype())
        itemsize = samples_typed.strides[0]
        
        segments = as_strided(
            samples_typed,
            shape=(num_segments, nfft),
            strides=(step * itemsize, itemsize)
        ).copy()
        
        if self.backend == 'cupy' and cp is not None:
            try:
                segments_gpu = cp.asarray(segments)
                window_gpu = cp.asarray(window.astype(self.get_float_dtype()))
                windowed = segments_gpu * window_gpu
                fft_result = cp.fft.fft(windowed, axis=1)
                power = cp.abs(fft_result) ** 2
                psd = cp.mean(power, axis=0)
                psd = cp.fft.fftshift(psd)
                psd_cpu = cp.asnumpy(psd)
                
                # CRITICAL: Delete GPU arrays to free memory immediately
                del segments_gpu, window_gpu, windowed, fft_result, power, psd
                
                # Periodically clear memory pool
                self.clear_gpu_memory()
                
            except Exception as e:
                # Fallback to CPU on error (including OOM)
                self.clear_gpu_memory(force=True)
                windowed = segments * window
                fft_result = np.fft.fft(windowed, axis=1)
                power = np.abs(fft_result) ** 2
                psd = np.mean(power, axis=0)
                psd_cpu = np.fft.fftshift(psd)
                
        elif self.backend == 'dpnp' and dpnp is not None:
            try:
                segments_gpu = dpnp.asarray(segments)
                window_gpu = dpnp.asarray(window.astype(self.get_float_dtype()))
                windowed = segments_gpu * window_gpu
                fft_result = dpnp.fft.fft(windowed, axis=1)
                power = dpnp.abs(fft_result) ** 2
                psd = dpnp.mean(power, axis=0)
                psd = dpnp.fft.fftshift(psd)
                psd_cpu = dpnp.asnumpy(psd)
                
                # Delete GPU arrays
                del segments_gpu, window_gpu, windowed, fft_result, power, psd
                
            except Exception:
                windowed = segments * window
                fft_result = np.fft.fft(windowed, axis=1)
                power = np.abs(fft_result) ** 2
                psd = np.mean(power, axis=0)
                psd_cpu = np.fft.fftshift(psd)
        else:
            windowed = segments * window
            fft_result = np.fft.fft(windowed, axis=1)
            power = np.abs(fft_result) ** 2
            psd = np.mean(power, axis=0)
            psd_cpu = np.fft.fftshift(psd)
        
        return 10 * np.log10(psd_cpu.astype(np.float64) + 1e-12)


# =============================================================================
# Window Functions
# =============================================================================

def create_window(window_type: str, size: int, kaiser_beta: float = 14.0) -> np.ndarray:
    """Create window function of specified type"""
    window_type = window_type.lower()
    if window_type == 'hann' or window_type == 'hanning':
        return np.hanning(size)
    elif window_type == 'hamming':
        return np.hamming(size)
    elif window_type == 'blackman':
        return np.blackman(size)
    elif window_type == 'kaiser':
        return np.kaiser(size, kaiser_beta)
    elif window_type == 'flattop':
        return signal.windows.flattop(size)
    elif window_type == 'rectangular' or window_type == 'rect':
        return np.ones(size)
    elif window_type == 'bartlett':
        return np.bartlett(size)
    else:
        print(f"Unknown window type '{window_type}', using Hann")
        return np.hanning(size)


# =============================================================================
# File Reader with Follow Mode and Realtime Sync
# =============================================================================

class StreamingPacketReader:
    """
    Packet reader with live follow mode and realtime sync support.
    Can watch a file being written and read new data as it arrives.
    Supports syncing to packet timestamps for realtime display.
    """
    
    FORMATS = {
        'sc16': (np.int16, 2),
        'sc8': (np.int8, 2),
        'fc32': (np.float32, 2),
        'fc64': (np.float64, 2),
    }
    
    PKT_TYPE_DATA_NO_TS = 0x6
    PKT_TYPE_DATA_WITH_TS = 0x7
    
    TSI_HEADER_LENGTH = 28
    TSI_STRUCT_FMT = '<4sIHHBBBBIIHH'
    TSI_VALID_RECEIVER_TYPES = [b'meo2', b'meo ']

    def __init__(self, filepath: str, format_type: str = 'sc16',
                 sample_rate: Optional[float] = None, center_freq: float = 0.0,
                 force_tsi: bool = False, force_chdr: bool = False,
                 raw_tsi: bool = False, spp: int = 1024, sgb_enabled: bool = False,
                 follow_mode: bool = False, realtime_sync: bool = False):
        self.filepath = Path(filepath)
        self.format_type = format_type
        self._user_sample_rate = sample_rate
        self.sample_rate = sample_rate if sample_rate else 200e6
        self.center_freq = center_freq
        self.force_tsi = force_tsi
        self.force_chdr = force_chdr
        self.raw_tsi = raw_tsi
        self.spp = spp
        self.sgb_enabled = sgb_enabled
        self.follow_mode = follow_mode
        self.realtime_sync = realtime_sync
        
        if format_type not in self.FORMATS:
            raise ValueError(f"Unknown format: {format_type}")
        self.dtype, self.components = self.FORMATS[format_type]
        
        self._file = None
        self._parse_mode = None
        self._packet_size = None
        self._payload_size = None
        self._file_size = 0
        self._last_file_size = 0
        self._data_start_pos = 0
        self._total_samples_read = 0
        self._packets_parsed = 0
        self._last_read_time = 0
        self._eof_reached = False
        
        # Realtime sync state
        self.rt_sync = RealtimeSyncState(sync_enabled=realtime_sync)
        self._last_packet_header = None
        
        # NEW: Time-based lag tracking
        self._stream_start_wallclock = None
        self._first_packet_timestamp = None
        
        self.file_header = None
        self.stream_headers = []
        
    def get_tsi_payload_size(self) -> int:
        actual_samples = self.spp // 2 if self.sgb_enabled else self.spp
        return actual_samples * 4

    def parse_tsi_header(self, header_bytes: bytes) -> Optional[Dict]:
        if len(header_bytes) < self.TSI_HEADER_LENGTH:
            return None
        try:
            (receiver_type, packet_number, sat_id, year_month,
             hour, day, sec, minute, five_ns_count, tuning_freq,
             receiver_flag, reserved) = struct.unpack(
                self.TSI_STRUCT_FMT, header_bytes[:self.TSI_HEADER_LENGTH])

            # Extract timestamp components
            year = (year_month >> 4) + 2000
            month = year_month & 0x0F
            
            return {
                'receiver_type': receiver_type,
                'is_valid_header': receiver_type in self.TSI_VALID_RECEIVER_TYPES,
                'packet_number': packet_number,
                'five_ns_count': five_ns_count,
                'channel': receiver_flag & 0x7,
                # Timestamp fields
                'year': year,
                'month': month,
                'day': day,
                'hour': hour,
                'minute': minute,
                'second': sec,
                'nanoseconds': five_ns_count * 5,
            }
        except:
            return None
    
    def tsi_header_to_timestamp(self, header: Dict) -> float:
        """Convert TSI header timestamp to Unix timestamp (seconds since epoch)"""
        try:
            dt = datetime.datetime(
                year=header['year'],
                month=max(1, min(12, header['month'])),
                day=max(1, min(31, header['day'])),
                hour=header['hour'],
                minute=header['minute'],
                second=header['second'],
                microsecond=header['nanoseconds'] // 1000
            )
            return dt.timestamp() + (header['nanoseconds'] % 1000) / 1e9
        except Exception as e:
            return 0.0

    def parse_chdr_header(self, header: int) -> Dict:
        return {
            'pkt_type': (header >> 53) & 0x7,
            'length': (header >> 16) & 0xFFFF,
            'seq_num': (header >> 32) & 0xFFFF,
        }

    def parse_file_header(self, f) -> Optional[Dict]:
        pos = f.tell()
        try:
            magic_bytes = f.read(4)
            if len(magic_bytes) < 4 or int.from_bytes(magic_bytes, 'little') != 0x43484452:
                f.seek(pos)
                return None
            
            version = int.from_bytes(f.read(4), 'little')
            chdr_width = int.from_bytes(f.read(4), 'little')
            f.read(4)
            tick_rate = np.frombuffer(f.read(8), dtype=np.float64)[0]
            f.read(24)
            num_streams = int.from_bytes(f.read(4), 'little')
            f.read(24)
            
            return {'tick_rate': tick_rate, 'num_streams': num_streams, 'version': version}
        except:
            f.seek(pos)
            return None

    def parse_stream_header(self, f) -> Dict:
        stream_id = int.from_bytes(f.read(4), 'little')
        block_id = f.read(64).decode('utf-8', errors='ignore').rstrip('\x00')
        port = int.from_bytes(f.read(4), 'little')
        f.read(24)
        return {'stream_id': stream_id, 'block_id': block_id, 'port': port}

    def _parse_payload_samples(self, payload: bytes) -> Optional[np.ndarray]:
        if len(payload) == 0:
            return None
        try:
            data = np.frombuffer(payload, dtype=self.dtype)
        except:
            return None
        
        if len(data) % self.components != 0:
            data = data[:-(len(data) % self.components)]
        if data.size == 0:
            return None
        
        iq_pairs = data.reshape(-1, self.components)
        
        if self.format_type.startswith('sc'):
            max_val = np.iinfo(self.dtype).max
            samples = (iq_pairs[:, 0].astype(np.float64) + 1j * iq_pairs[:, 1].astype(np.float64)) / max_val
        else:
            samples = iq_pairs[:, 0] + 1j * iq_pairs[:, 1]
        return samples

    @staticmethod
    def wait_for_file(filepath: Path, min_size: int = 1024, 
                      timeout: float = 30.0, poll_interval: float = 0.5) -> bool:
        """
        Wait for file to exist and have minimum size.
        Returns True if file is ready, False on timeout.
        """
        start_time = time.time()
        print(f"Waiting for file: {filepath}")
        
        while time.time() - start_time < timeout:
            if filepath.exists():
                size = filepath.stat().st_size
                if size >= min_size:
                    print(f"  File ready: {size:,} bytes")
                    return True
                else:
                    print(f"  File size: {size} bytes (waiting for {min_size})...", end='\r')
            else:
                print(f"  File not found, waiting...", end='\r')
            time.sleep(poll_interval)
        
        print(f"\nTimeout waiting for file!")
        return False

    def open(self):
        """Open file and detect format"""
        # In follow mode, wait for file to have data
        if self.follow_mode:
            min_size = self.TSI_HEADER_LENGTH + self.get_tsi_payload_size() if self.raw_tsi else 100
            if not self.wait_for_file(self.filepath, min_size=min_size * 5):
                raise FileNotFoundError(f"File not ready: {self.filepath}")
        
        self._file = open(self.filepath, 'rb')
        self._file_size = os.fstat(self._file.fileno()).st_size
        self._last_file_size = self._file_size
        
        # Parse CHDR header if present
        self.file_header = self.parse_file_header(self._file)
        
        if self.file_header:
            print(f"CHDR header: tick_rate={self.file_header['tick_rate']/1e6:.3f} MHz")
            if self._user_sample_rate is None:
                self.sample_rate = self.file_header['tick_rate']
            for _ in range(max(1, self.file_header.get('num_streams', 0))):
                try:
                    self.stream_headers.append(self.parse_stream_header(self._file))
                except:
                    break
        
        # Determine parse mode
        if self.force_chdr:
            self._parse_mode = 'chdr'
        elif self.force_tsi or self.raw_tsi:
            self._parse_mode = 'raw-tsi'
        else:
            pos = self._file.tell()
            peek = self._file.read(8)
            self._file.seek(pos)
            
            if peek[:4] in self.TSI_VALID_RECEIVER_TYPES:
                self._parse_mode = 'raw-tsi'
            else:
                self._parse_mode = 'chdr'
        
        if self._parse_mode == 'raw-tsi':
            self._payload_size = self.get_tsi_payload_size()
            self._packet_size = self.TSI_HEADER_LENGTH + self._payload_size
        
        self._data_start_pos = self._file.tell()
        self._last_read_time = time.time()
        return self

    def close(self):
        if self._file:
            self._file.close()
            self._file = None

    def get_total_samples(self) -> int:
        if self._parse_mode == 'raw-tsi':
            remaining = self._file_size - self._data_start_pos
            num_packets = remaining // self._packet_size
            samples_per_packet = self.spp // 2 if self.sgb_enabled else self.spp
            return num_packets * samples_per_packet
        return 0

    def reset(self):
        if self._file:
            self._file.seek(self._data_start_pos)
            self._total_samples_read = 0
            self._packets_parsed = 0
            self._eof_reached = False
            self.rt_sync.sync_established = False
            self.rt_sync.packets_skipped = 0
            self.rt_sync.frames_dropped = 0
            self.rt_sync.current_lag_seconds = 0.0
            self._stream_start_wallclock = None
            self._first_packet_timestamp = None

    def seek_to_end(self):
        """Seek to target buffer position behind end of file for realtime sync"""
        if self._file and self._parse_mode == 'raw-tsi' and self._packet_size:
            # Refresh file size
            try:
                self._file_size = self.filepath.stat().st_size
            except:
                pass
            
            # Seek to buffer_packets behind EOF
            target_packets_behind = self.rt_sync.buffer_packets
            target_pos = self._file_size - (target_packets_behind * self._packet_size)
            
            if target_pos > self._data_start_pos:
                # Align to packet boundary
                offset_from_start = target_pos - self._data_start_pos
                aligned_offset = (offset_from_start // self._packet_size) * self._packet_size
                self._file.seek(self._data_start_pos + aligned_offset)
                actual_behind = int((self._file_size - self._file.tell()) / self._packet_size)
                print(f"  Seeked to {actual_behind} packets behind EOF")
            else:
                self._file.seek(self._data_start_pos)
                print(f"  File too small, starting from beginning")

    def _check_for_new_data(self) -> bool:
        """Check if file has grown (for follow mode)"""
        if not self.follow_mode:
            return False
        
        try:
            current_size = self.filepath.stat().st_size
            if current_size > self._file_size:
                self._file_size = current_size
                self._last_read_time = time.time()
                return True
        except:
            pass
        return False
    
    def _calculate_time_lag(self, packet_timestamp: float) -> float:
        """
        Calculate current lag in seconds between packet time and wall clock.
        Returns positive value if we're behind (packet is old).
        """
        if packet_timestamp <= 0:
            return 0.0
        
        now = time.time()
        
        # Initialize on first packet with valid timestamp
        if self._stream_start_wallclock is None:
            self._stream_start_wallclock = now
            self._first_packet_timestamp = packet_timestamp
            return 0.0
        
        # Calculate expected vs actual elapsed time
        packet_elapsed = packet_timestamp - self._first_packet_timestamp
        wall_elapsed = now - self._stream_start_wallclock
        
        # Lag = how far behind realtime we are
        # Positive = we're behind (displaying old data)
        lag = wall_elapsed - packet_elapsed
        
        return max(0.0, lag)
    
    def _should_skip_for_realtime(self, samples_in_packet: int) -> bool:
        """
        Determine if packet should be skipped for realtime sync.
        
        Uses TIME-BASED lag as primary metric (more intuitive than packets).
        Falls back to packet-based if timestamps unavailable.
        """
        if not self.rt_sync.sync_enabled:
            return False
        
        if self._file is None or self._packet_size is None or self._packet_size <= 0:
            return False
        
        # Check TIME-BASED lag first (if we have timestamp data)
        time_lag = self.rt_sync.current_lag_seconds
        if time_lag > 0:
            if time_lag > self.rt_sync.max_lag_seconds:
                self.rt_sync.status = "CATCHING_UP"
                self.rt_sync.packets_skipped += 1
                return True  # DROP this packet
            elif time_lag > self.rt_sync.drop_threshold_seconds:
                self.rt_sync.status = "DROPPING"
                # Don't skip yet, but we're getting behind
            else:
                self.rt_sync.status = "LIVE"
            return False
        
        # Fall back to packet-based logic
        try:
            current_pos = self._file.tell()
            file_size = self._file_size
            
            # Refresh file size in follow mode
            if self.follow_mode:
                try:
                    file_size = self.filepath.stat().st_size
                    self._file_size = file_size
                except:
                    pass
            
            # Calculate packets behind EOF
            bytes_behind = file_size - current_pos
            packets_behind = int(bytes_behind / self._packet_size)
            
            self.rt_sync.current_buffer_packets = packets_behind
            
            # First time setup - seek to target buffer position
            if not self.rt_sync.sync_established:
                target_pos = file_size - (self.rt_sync.buffer_packets * self._packet_size)
                if target_pos > self._data_start_pos:
                    # Align to packet boundary
                    offset_from_start = target_pos - self._data_start_pos
                    aligned_offset = (offset_from_start // self._packet_size) * self._packet_size
                    seek_pos = self._data_start_pos + aligned_offset
                    self._file.seek(seek_pos)
                    packets_behind = int((file_size - seek_pos) / self._packet_size)
                    self.rt_sync.current_buffer_packets = packets_behind
                    print(f"  RT Sync: Positioned {packets_behind} packets behind EOF (target: {self.rt_sync.buffer_packets})")
                else:
                    print(f"  RT Sync: File too small, starting from beginning ({packets_behind} packets available)")
                
                self.rt_sync.sync_established = True
                self.rt_sync.status = "LIVE"
                return False
            
            # Determine status and whether to skip
            if packets_behind < self.rt_sync.min_buffer_packets:
                # Too close to EOF - we're reading faster than writing
                self.rt_sync.status = "BUFFERING"
                return False
                
            elif packets_behind > self.rt_sync.max_buffer_packets:
                # Too far behind - skip packets to catch up
                self.rt_sync.status = "CATCHING_UP"
                self.rt_sync.packets_skipped += 1
                return True
                
            else:
                # In the sweet spot
                self.rt_sync.status = "LIVE"
                return False
                
        except Exception as e:
            return False

    def read_samples(self, num_samples: int) -> Tuple[Optional[np.ndarray], Optional[Dict]]:
        """
        Read samples, with follow mode and realtime sync support.
        Returns (samples, last_header) tuple.
        """
        if self._file is None:
            return None, None
        
        all_samples = []
        samples_collected = 0
        last_header = None
        
        while samples_collected < num_samples:
            # Check current file position vs file size
            current_pos = self._file.tell()
            
            # In follow mode, check for new data if we're at EOF
            if self.follow_mode and current_pos >= self._file_size:
                if self._check_for_new_data():
                    continue  # New data available, try reading
                else:
                    # No new data yet, return what we have
                    break
            
            if self._parse_mode == 'chdr':
                size_bytes = self._file.read(4)
                if len(size_bytes) < 4:
                    if self.follow_mode:
                        self._file.seek(current_pos)  # Go back
                    break
                
                packet_size = int.from_bytes(size_bytes, 'little')
                if packet_size == 0 or packet_size > 50_000_000:
                    break
                
                packet_data = self._file.read(packet_size)
                if len(packet_data) < packet_size:
                    if self.follow_mode:
                        self._file.seek(current_pos)
                    break
                
                payload_offset = 8
                if len(packet_data) >= 8:
                    header = int.from_bytes(packet_data[0:8], 'little')
                    header_info = self.parse_chdr_header(header)
                    if header_info['pkt_type'] == self.PKT_TYPE_DATA_WITH_TS:
                        payload_offset = 16
                
                payload = packet_data[payload_offset:]
                if payload:
                    packet_samples = self._parse_payload_samples(payload)
                    if packet_samples is not None:
                        all_samples.append(packet_samples)
                        samples_collected += len(packet_samples)
                
                self._packets_parsed += 1
            
            elif self._parse_mode == 'raw-tsi':
                header_bytes = self._file.read(self.TSI_HEADER_LENGTH)
                if len(header_bytes) < self.TSI_HEADER_LENGTH:
                    if self.follow_mode:
                        self._file.seek(current_pos)
                    break
                
                header = self.parse_tsi_header(header_bytes)
                if header is None or not header.get('is_valid_header', False):
                    if self.follow_mode:
                        self._file.seek(current_pos)
                    break
                
                # Calculate time-based lag for realtime sync
                if self.rt_sync.sync_enabled:
                    packet_ts = self.tsi_header_to_timestamp(header)
                    if packet_ts > 0:
                        self.rt_sync.current_lag_seconds = self._calculate_time_lag(packet_ts)
                        self.rt_sync.last_packet_timestamp = packet_ts
                
                # Check realtime sync - skip if we're too far behind
                samples_per_packet = self.spp // 2 if self.sgb_enabled else self.spp
                if self._should_skip_for_realtime(samples_per_packet):
                    # Skip this packet's payload
                    self._file.seek(self._payload_size, os.SEEK_CUR)
                    self._packets_parsed += 1
                    continue
                
                payload = self._file.read(self._payload_size)
                if len(payload) < self._payload_size:
                    if self.follow_mode:
                        self._file.seek(current_pos)
                    break
                
                packet_samples = self._parse_payload_samples(payload)
                if packet_samples is not None:
                    all_samples.append(packet_samples)
                    samples_collected += len(packet_samples)
                    last_header = header
                
                self._packets_parsed += 1
        
        if not all_samples:
            return None, last_header
        
        result = np.concatenate(all_samples)
        self._total_samples_read += len(result)
        self._last_read_time = time.time()
        self._last_packet_header = last_header
        return result, last_header

    def is_stale(self, timeout: float = 5.0) -> bool:
        """Check if we haven't received new data for timeout seconds"""
        return self.follow_mode and (time.time() - self._last_read_time) > timeout

    def __enter__(self):
        return self.open()
    
    def __exit__(self, *args):
        self.close()


# =============================================================================
# Threaded Data Reader with Frame Dropping
# =============================================================================

class ThreadedDataReader(threading.Thread):
    """Threaded data reader with follow mode, realtime sync, and frame dropping support"""
    
    def __init__(self, reader: StreamingPacketReader, samples_per_chunk: int,
                 playback_speed: float = 1.0, loop: bool = False,
                 follow_timeout: float = 5.0, realtime_sync: bool = False):
        super().__init__(daemon=True)
        self.reader = reader
        self.samples_per_chunk = samples_per_chunk
        self.playback_speed = playback_speed
        self.loop = loop
        self.follow_timeout = follow_timeout
        self.realtime_sync = realtime_sync
        
        # SMALLER queue to prevent backup - only keep latest frames
        self.data_queue = queue.Queue(maxsize=3)
        self.running = False
        self.paused = False
        self._pause_event = threading.Event()
        self._pause_event.set()
        
        # Runtime control
        self._realtime_sync_enabled = realtime_sync
        
        # Stats
        self.frames_produced = 0
        self.frames_dropped_queue_full = 0
        
    def set_realtime_sync(self, enabled: bool):
        """Enable/disable realtime sync at runtime"""
        self._realtime_sync_enabled = enabled
        self.reader.rt_sync.sync_enabled = enabled
        if enabled and not self.reader.rt_sync.sync_established:
            # Seek to end of file to start fresh
            self.reader.seek_to_end()
            self.reader.rt_sync.sync_established = False
        
    def run(self):
        self.running = True
        chunk_duration = self.samples_per_chunk / self.reader.sample_rate
        
        # If realtime sync enabled, seek to near end of file
        if self._realtime_sync_enabled:
            self.reader.seek_to_end()
        
        while self.running:
            self._pause_event.wait()
            if not self.running:
                break
            
            start_time = time.perf_counter()
            samples, header = self.reader.read_samples(self.samples_per_chunk)
            
            if samples is None or len(samples) == 0:
                if self.reader.follow_mode:
                    # In follow mode, check if stream is stale
                    if self.reader.is_stale(self.follow_timeout):
                        print("\nStream appears to have ended (no new data)")
                        try:
                            self.data_queue.put((None, None), timeout=0.1)
                        except:
                            pass
                        break
                    # Otherwise wait a bit and try again
                    time.sleep(0.05)
                    continue
                elif self.loop:
                    self.reader.reset()
                    continue
                else:
                    try:
                        self.data_queue.put((None, None), timeout=0.1)
                    except:
                        pass
                    break
            
            self.frames_produced += 1
            
            # Try to put in queue - DROP if full (don't block!)
            try:
                self.data_queue.put_nowait((samples, header))
            except queue.Full:
                # Queue is full - drop old frames to stay realtime
                self.frames_dropped_queue_full += 1
                # Clear the queue to get fresher data
                try:
                    while True:
                        self.data_queue.get_nowait()
                except queue.Empty:
                    pass
                # Now put the current (newest) frame
                try:
                    self.data_queue.put_nowait((samples, header))
                except:
                    pass
            
            # Pacing (not in follow mode or realtime sync - we want real-time there)
            if not self.reader.follow_mode and not self._realtime_sync_enabled:
                elapsed = time.perf_counter() - start_time
                sleep_time = (chunk_duration / self.playback_speed) - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)
    
    def pause(self):
        self.paused = True
        self._pause_event.clear()
    
    def resume(self):
        self.paused = False
        self._pause_event.set()
    
    def stop(self):
        self.running = False
        self._pause_event.set()


# =============================================================================
# Peak Tracker
# =============================================================================

class PeakTracker:
    """Peak detection and Doppler tracking"""
    
    def __init__(self, config: PlaybackConfig, freqs: np.ndarray):
        self.config = config
        self.freqs = freqs
        self.state = PeakTrackingState(
            peak_freq_history=deque(maxlen=int(config.peak_history_seconds * config.update_rate_hz)),
            peak_power_history=deque(maxlen=int(config.peak_history_seconds * config.update_rate_hz)),
            smoothing_buffer=deque(maxlen=config.peak_smoothing)
        )
    
    def update(self, spectrum: np.ndarray, playback_time: float) -> Tuple[float, float, float]:
        threshold_mask = spectrum >= self.config.peak_threshold_db
        
        if self.state.track_locked:
            freq_mask = np.abs(self.freqs - self.state.lock_freq) <= self.state.lock_bandwidth
            search_mask = threshold_mask & freq_mask
        else:
            search_mask = threshold_mask
        
        if not np.any(search_mask):
            return self.state.last_peak_freq, self.state.last_peak_power, self.state.doppler_rate
        
        masked_spectrum = np.where(search_mask, spectrum, -np.inf)
        peak_idx = np.argmax(masked_spectrum)
        peak_freq = self.freqs[peak_idx]
        peak_power = spectrum[peak_idx]
        
        self.state.smoothing_buffer.append(peak_freq)
        smoothed_freq = np.median(list(self.state.smoothing_buffer))
        
        if not self.state.track_locked and peak_power >= self.config.peak_threshold_db:
            self.state.track_locked = True
            self.state.lock_freq = smoothed_freq
        
        if self.state.track_locked:
            self.state.lock_freq = 0.95 * self.state.lock_freq + 0.05 * smoothed_freq
        
        self.state.peak_freq_history.append((playback_time, smoothed_freq))
        self.state.peak_power_history.append((playback_time, peak_power))
        
        self._calculate_doppler_rate(playback_time)
        
        self.state.last_peak_freq = smoothed_freq
        self.state.last_peak_power = peak_power
        
        return smoothed_freq, peak_power, self.state.doppler_rate
    
    def _calculate_doppler_rate(self, playback_time: float):
        if len(self.state.peak_freq_history) < 10:
            return
        
        history = list(self.state.peak_freq_history)
        times = np.array([t for t, f in history])
        freqs = np.array([f for t, f in history])
        
        recent_mask = times >= (playback_time - self.config.doppler_window)
        if np.sum(recent_mask) < 5:
            return
        
        recent_times = times[recent_mask]
        recent_freqs = freqs[recent_mask]
        
        if len(recent_times) >= 5:
            try:
                coeffs = np.polyfit(recent_times, recent_freqs, 1)
                self.state.doppler_rate = coeffs[0]
            except:
                pass
    
    def toggle_lock(self):
        self.state.track_locked = not self.state.track_locked
        if not self.state.track_locked:
            self.state.lock_freq = 0
    
    def reset(self):
        self.state.peak_freq_history.clear()
        self.state.peak_power_history.clear()
        self.state.smoothing_buffer.clear()
        self.state.track_locked = False
        self.state.lock_freq = 0
    
    def get_history_arrays(self) -> Tuple[np.ndarray, np.ndarray]:
        if not self.state.peak_freq_history:
            return np.array([]), np.array([])
        history = list(self.state.peak_freq_history)
        return np.array([t for t, f in history]), np.array([f for t, f in history])


# =============================================================================
# Spectrum Analyzer Widget with Context Menus
# =============================================================================

class SpectrumAnalyzerWidget(QtWidgets.QWidget):
    """High-performance spectrum analyzer widget with context menus"""
    
    def __init__(self, reader: StreamingPacketReader, gpu: GPUAccelerator,
                 config: PlaybackConfig):
        super().__init__()
        
        self.reader = reader
        self.gpu = gpu
        self.config = config
        
        # Initialize RT sync parameters from config
        self.reader.rt_sync.buffer_packets = config.realtime_buffer_packets
        self.reader.rt_sync.min_buffer_packets = config.realtime_min_buffer
        self.reader.rt_sync.max_buffer_packets = config.realtime_max_buffer
        self.reader.rt_sync.max_lag_seconds = config.max_lag_seconds
        self.reader.rt_sync.drop_threshold_seconds = config.drop_threshold_seconds
        
        self.samples_per_update = int(reader.sample_rate / config.update_rate_hz)
        self._update_window()
        
        # Frequency arrays
        self.freqs = np.fft.fftshift(np.fft.fftfreq(config.nfft, 1/reader.sample_rate))
        self.freqs += reader.center_freq
        self.freqs_mhz = self.freqs / 1e6
        
        self.waterfall_data = np.full((config.waterfall_history, config.nfft), -100.0, dtype=np.float32)
        
        self.peak_tracker = PeakTracker(config, self.freqs) if config.enable_peak_tracking else None
        
        # Averaging buffer
        self.avg_buffer = deque(maxlen=config.avg_count) if config.enable_averaging else None
        self.exp_avg_spectrum = None  # For exponential averaging
        
        # Max/Min hold
        self.max_hold_spectrum = None
        self.min_hold_spectrum = None
        
        self.current_samples = None
        self.current_spectrum = None
        self.playback_time = 0.0
        
        self.frame_count = 0
        self.fps_timer = time.perf_counter()
        self.current_fps = 0.0
        self.compute_times = deque(maxlen=60)
        
        # Auto-scale control
        self.auto_scale_enabled = True
        self.last_y_min = -100
        self.last_y_max = 0
        self.auto_scale_update_counter = 0
        self.auto_scale_update_interval = 10
        
        self._setup_ui()
        self._setup_context_menus()
        
        self.data_reader = ThreadedDataReader(
            reader, self.samples_per_update, 
            config.playback_speed, config.loop_playback,
            config.follow_timeout, config.realtime_sync
        )
        
        self.update_timer = QtCore.QTimer()
        self.update_timer.timeout.connect(self._update_display)
    
    def _update_window(self):
        """Update window function based on config"""
        self.window = create_window(
            self.config.window_type, 
            self.config.nfft, 
            self.config.kaiser_beta
        ).astype(self.gpu.get_float_dtype())
        
    def _setup_ui(self):
        pg.setConfigOptions(antialias=True, useOpenGL=True)
        
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(3)
        
        # Status bar
        self.status_label = QtWidgets.QLabel()
        self.status_label.setStyleSheet(
            "QLabel { background-color: #1e1e1e; color: #00ff00; "
            "font-family: monospace; font-size: 10px; padding: 3px; }"
        )
        layout.addWidget(self.status_label)
        
        # Mode indicator
        mode_parts = []
        if self.config.follow_mode:
            mode_parts.append("LIVE")
        if self.config.realtime_sync:
            mode_parts.append("RT-SYNC")
        if not mode_parts:
            mode_parts.append("PLAYBACK")
        mode_text = " | ".join(mode_parts)
        
        is_realtime = self.config.follow_mode or self.config.realtime_sync
        self.mode_label = QtWidgets.QLabel(f"Mode: {mode_text}")
        self.mode_label.setStyleSheet(
            f"QLabel {{ background-color: {'#1a3a1a' if is_realtime else '#1a1a3a'}; "
            f"color: {'#00ff00' if is_realtime else '#6666ff'}; "
            "font-family: monospace; font-size: 10px; padding: 3px; }"
        )
        layout.addWidget(self.mode_label)
        
        # Realtime sync status
        if self.config.realtime_sync:
            self.rt_sync_label = QtWidgets.QLabel("RT Sync: Initializing...")
            self.rt_sync_label.setStyleSheet(
                "QLabel { background-color: #2a2a1a; color: #ffff66; "
                "font-family: monospace; font-size: 10px; padding: 3px; }"
            )
            layout.addWidget(self.rt_sync_label)
        
        if self.config.enable_peak_tracking:
            self.peak_label = QtWidgets.QLabel()
            self.peak_label.setStyleSheet(
                "QLabel { background-color: #2a1a1a; color: #ff6666; "
                "font-family: monospace; font-size: 10px; padding: 3px; }"
            )
            layout.addWidget(self.peak_label)
        
        # Filter status label
        self.filter_label = QtWidgets.QLabel()
        self.filter_label.setStyleSheet(
            "QLabel { background-color: #1a1a2a; color: #aaaaff; "
            "font-family: monospace; font-size: 10px; padding: 3px; }"
        )
        self._update_filter_label()
        layout.addWidget(self.filter_label)
        
        # Graphics
        self.graphics_layout = pg.GraphicsLayoutWidget()
        self.graphics_layout.setBackground('#1e1e1e')
        layout.addWidget(self.graphics_layout)
        
        row = 0
        
        # Time domain
        if self.config.show_time_domain:
            self.time_plot = self.graphics_layout.addPlot(row=row, col=0, title="Time Domain (I/Q)")
            self.time_plot.setLabel('bottom', 'Time', 'μs')
            self.time_plot.setLabel('left', 'Amplitude')
            self.time_plot.showGrid(x=True, y=True, alpha=0.3)
            self.time_plot.setYRange(-1.1, 1.1)
            self.time_plot.setMenuEnabled(False)
            
            self.curve_i = self.time_plot.plot(pen=pg.mkPen('y', width=1))
            self.curve_q = self.time_plot.plot(pen=pg.mkPen('m', width=1))
            
            row += 1
            self.graphics_layout.nextRow()
        
        # Waterfall
        if self.config.show_waterfall:
            self.waterfall_plot = self.graphics_layout.addPlot(row=row, col=0, title="Waterfall")
            self.waterfall_plot.setLabel('bottom', 'Frequency', 'MHz')
            self.waterfall_plot.setLabel('left', 'Time', 'frames')
            self.waterfall_plot.setMenuEnabled(False)
            
            self.waterfall_img = pg.ImageItem()
            self.waterfall_plot.addItem(self.waterfall_img)
            
            colormap = pg.colormap.get('viridis')
            self.waterfall_img.setLookupTable(colormap.getLookupTable())
            self.waterfall_img.setImage(self.waterfall_data.T)
            
            freq_min, freq_max = self.freqs_mhz[0], self.freqs_mhz[-1]
            self.waterfall_img.setRect(freq_min, 0, freq_max - freq_min, self.config.waterfall_history)
            
            row += 1
            self.graphics_layout.nextRow()
        
        # Peak history
        if self.config.enable_peak_tracking:
            self.peak_history_plot = self.graphics_layout.addPlot(row=row, col=0, title="Peak Frequency History")
            self.peak_history_plot.setLabel('bottom', 'Time', 's')
            self.peak_history_plot.setLabel('left', 'Frequency', 'MHz')
            self.peak_history_plot.showGrid(x=True, y=True, alpha=0.3)
            self.peak_history_plot.setMenuEnabled(False)
            
            self.peak_history_curve = self.peak_history_plot.plot(pen=pg.mkPen('r', width=2))
            self.doppler_fit_curve = self.peak_history_plot.plot(
                pen=pg.mkPen('g', width=1, style=Qt_DashLine)
            )
            
            row += 1
            self.graphics_layout.nextRow()
        
        # Spectrum plot
        self.spectrum_plot = self.graphics_layout.addPlot(row=row, col=0, title="Power Spectral Density")
        self.spectrum_plot.setLabel('bottom', 'Frequency', 'MHz')
        self.spectrum_plot.setLabel('left', 'Power', 'dB')
        self.spectrum_plot.showGrid(x=True, y=True, alpha=0.3)
        self.spectrum_plot.setYRange(-100, 0)
        self.spectrum_plot.setXRange(self.freqs_mhz[0], self.freqs_mhz[-1])
        self.spectrum_plot.setMenuEnabled(False)
        
        # Main spectrum curve
        self.curve_spectrum = pg.PlotDataItem(
            pen=pg.mkPen(color=(0, 255, 255), width=1),
            stepMode=False
        )
        self.spectrum_plot.addItem(self.curve_spectrum)
        
        # Average curve
        self.curve_avg = pg.PlotDataItem(
            pen=pg.mkPen(color=(255, 255, 0), width=2),
            stepMode=False
        )
        self.spectrum_plot.addItem(self.curve_avg)
        self.curve_avg.setVisible(self.config.enable_averaging)
        
        # Max hold curve
        self.curve_max_hold = pg.PlotDataItem(
            pen=pg.mkPen(color=(255, 100, 100), width=1, style=Qt_DotLine),
            stepMode=False
        )
        self.spectrum_plot.addItem(self.curve_max_hold)
        self.curve_max_hold.setVisible(self.config.enable_max_hold)
        
        # Min hold curve
        self.curve_min_hold = pg.PlotDataItem(
            pen=pg.mkPen(color=(100, 100, 255), width=1, style=Qt_DotLine),
            stepMode=False
        )
        self.spectrum_plot.addItem(self.curve_min_hold)
        self.curve_min_hold.setVisible(self.config.enable_min_hold)
        
        # Peak marker
        self.peak_marker = pg.ScatterPlotItem(
            size=10, pen=pg.mkPen('r', width=2), brush=pg.mkBrush(255, 0, 0, 100)
        )
        self.spectrum_plot.addItem(self.peak_marker)
        
        self.peak_text = pg.TextItem(anchor=(0, 1), color='r')
        self.spectrum_plot.addItem(self.peak_text)
        
        if self.config.enable_peak_tracking:
            self.threshold_line = pg.InfiniteLine(
                pos=self.config.peak_threshold_db, angle=0,
                pen=pg.mkPen('y', width=1, style=Qt_DotLine)
            )
            self.spectrum_plot.addItem(self.threshold_line)
        
        self.setMinimumSize(1000, 700)
    
    def _setup_context_menus(self):
        """Setup context menus for all plots"""
        # Spectrum plot context menu
        self.spectrum_plot.scene().sigMouseClicked.connect(self._on_spectrum_click)
        
        # Create the context menu
        self.context_menu = QtWidgets.QMenu(self)
        self._build_context_menu()
    
    def _build_context_menu(self):
        """Build the context menu with current settings"""
        self.context_menu.clear()
        
        # === Averaging submenu ===
        avg_menu = self.context_menu.addMenu("Averaging")
        
        avg_enable_action = avg_menu.addAction("Enable Averaging")
        avg_enable_action.setCheckable(True)
        avg_enable_action.setChecked(self.config.enable_averaging)
        avg_enable_action.triggered.connect(self._toggle_averaging)
        
        avg_menu.addSeparator()
        
        # Averaging mode
        avg_mode_menu = avg_menu.addMenu("Mode")
        for mode in ['linear', 'exponential']:
            action = avg_mode_menu.addAction(mode.capitalize())
            action.setCheckable(True)
            action.setChecked(self.config.avg_mode == mode)
            action.triggered.connect(lambda checked, m=mode: self._set_avg_mode(m))
        
        # Averaging count
        avg_count_menu = avg_menu.addMenu("Count")
        for count in [5, 10, 20, 50, 100]:
            action = avg_count_menu.addAction(str(count))
            action.setCheckable(True)
            action.setChecked(self.config.avg_count == count)
            action.triggered.connect(lambda checked, c=count: self._set_avg_count(c))
        
        # Exponential alpha
        if self.config.avg_mode == 'exponential':
            alpha_menu = avg_menu.addMenu("Exp Alpha")
            for alpha in [0.05, 0.1, 0.2, 0.3, 0.5]:
                action = alpha_menu.addAction(f"{alpha:.2f}")
                action.setCheckable(True)
                action.setChecked(abs(self.config.exp_alpha - alpha) < 0.01)
                action.triggered.connect(lambda checked, a=alpha: self._set_exp_alpha(a))
        
        # === Hold functions ===
        self.context_menu.addSeparator()
        hold_menu = self.context_menu.addMenu("Hold Functions")
        
        max_hold_action = hold_menu.addAction("Max Hold")
        max_hold_action.setCheckable(True)
        max_hold_action.setChecked(self.config.enable_max_hold)
        max_hold_action.triggered.connect(self._toggle_max_hold)
        
        min_hold_action = hold_menu.addAction("Min Hold")
        min_hold_action.setCheckable(True)
        min_hold_action.setChecked(self.config.enable_min_hold)
        min_hold_action.triggered.connect(self._toggle_min_hold)
        
        hold_menu.addSeparator()
        clear_hold_action = hold_menu.addAction("Clear Hold Data")
        clear_hold_action.triggered.connect(self._clear_hold_data)
        
        # === Window function ===
        self.context_menu.addSeparator()
        window_menu = self.context_menu.addMenu("Window Function")
        for win_type in ['hann', 'hamming', 'blackman', 'kaiser', 'flattop', 'rectangular']:
            action = window_menu.addAction(win_type.capitalize())
            action.setCheckable(True)
            action.setChecked(self.config.window_type == win_type)
            action.triggered.connect(lambda checked, w=win_type: self._set_window_type(w))
        
        # Kaiser beta (only if kaiser selected)
        if self.config.window_type == 'kaiser':
            beta_menu = window_menu.addMenu("Kaiser Beta")
            for beta in [4.0, 8.0, 14.0, 20.0]:
                action = beta_menu.addAction(f"{beta:.1f}")
                action.setCheckable(True)
                action.setChecked(abs(self.config.kaiser_beta - beta) < 0.1)
                action.triggered.connect(lambda checked, b=beta: self._set_kaiser_beta(b))
        
        # === FFT settings ===
        self.context_menu.addSeparator()
        fft_menu = self.context_menu.addMenu("FFT Settings")
        
        # FFT size
        fft_size_menu = fft_menu.addMenu("FFT Size")
        for size in [1024, 2048, 4096, 8192, 16384]:
            action = fft_size_menu.addAction(str(size))
            action.setCheckable(True)
            action.setChecked(self.config.nfft == size)
            action.triggered.connect(lambda checked, s=size: self._set_fft_size(s))
        
        # Overlap
        overlap_menu = fft_menu.addMenu("Overlap")
        for overlap in [0.0, 0.25, 0.5, 0.75]:
            action = overlap_menu.addAction(f"{int(overlap*100)}%")
            action.setCheckable(True)
            action.setChecked(abs(self.config.overlap - overlap) < 0.01)
            action.triggered.connect(lambda checked, o=overlap: self._set_overlap(o))
        
        # === Realtime sync ===
        self.context_menu.addSeparator()
        rt_menu = self.context_menu.addMenu("Realtime Sync")
        
        rt_enable_action = rt_menu.addAction("Enable Realtime Sync")
        rt_enable_action.setCheckable(True)
        rt_enable_action.setChecked(self.config.realtime_sync)
        rt_enable_action.triggered.connect(self._toggle_realtime_sync)
        
        rt_menu.addSeparator()
        
        # Max lag tolerance
        lag_menu = rt_menu.addMenu("Max Lag (seconds)")
        for lag in [0.5, 1.0, 2.0, 5.0, 10.0]:
            action = lag_menu.addAction(f"{lag:.1f}s")
            action.setCheckable(True)
            action.setChecked(abs(self.config.max_lag_seconds - lag) < 0.1)
            action.triggered.connect(lambda checked, l=lag: self._set_max_lag(l))
        
        # Drop threshold
        drop_menu = rt_menu.addMenu("Drop Threshold (seconds)")
        for thresh in [0.25, 0.5, 1.0, 2.0]:
            action = drop_menu.addAction(f"{thresh:.2f}s")
            action.setCheckable(True)
            action.setChecked(abs(self.config.drop_threshold_seconds - thresh) < 0.05)
            action.triggered.connect(lambda checked, t=thresh: self._set_drop_threshold(t))
        
        # Target buffer (packets behind EOF)
        buffer_menu = rt_menu.addMenu("Target Buffer (packets)")
        for pkts in [100, 200, 250, 500, 1000]:
            action = buffer_menu.addAction(str(pkts))
            action.setCheckable(True)
            action.setChecked(self.config.realtime_buffer_packets == pkts)
            action.triggered.connect(lambda checked, p=pkts: self._set_realtime_buffer(p))
        
        # Max buffer before catching up
        max_buffer_menu = rt_menu.addMenu("Max Buffer Before Skip (packets)")
        for pkts in [300, 500, 1000, 2000, 5000]:
            action = max_buffer_menu.addAction(str(pkts))
            action.setCheckable(True)
            action.setChecked(self.config.realtime_max_buffer == pkts)
            action.triggered.connect(lambda checked, p=pkts: self._set_realtime_max_buffer(p))
        
        rt_menu.addSeparator()
        rt_resync_action = rt_menu.addAction("Re-sync Now (Seek to Buffer Position)")
        rt_resync_action.triggered.connect(self._resync_realtime)
        
        # === Display options ===
        self.context_menu.addSeparator()
        display_menu = self.context_menu.addMenu("Display")
        
        autoscale_action = display_menu.addAction("Auto Scale Y-Axis")
        autoscale_action.setCheckable(True)
        autoscale_action.setChecked(self.auto_scale_enabled)
        autoscale_action.triggered.connect(self._toggle_autoscale)
        
        display_menu.addSeparator()
        
        show_waterfall_action = display_menu.addAction("Show Waterfall")
        show_waterfall_action.setCheckable(True)
        show_waterfall_action.setChecked(self.config.show_waterfall)
        show_waterfall_action.triggered.connect(self._toggle_waterfall)
        
        show_time_action = display_menu.addAction("Show Time Domain")
        show_time_action.setCheckable(True)
        show_time_action.setChecked(self.config.show_time_domain)
        show_time_action.triggered.connect(self._toggle_time_domain)
        
        # === Peak tracking ===
        if self.config.enable_peak_tracking or self.peak_tracker:
            self.context_menu.addSeparator()
            peak_menu = self.context_menu.addMenu("Peak Tracking")
            
            peak_enable_action = peak_menu.addAction("Enable Peak Tracking")
            peak_enable_action.setCheckable(True)
            peak_enable_action.setChecked(self.config.enable_peak_tracking)
            peak_enable_action.triggered.connect(self._toggle_peak_tracking)
            
            if self.peak_tracker:
                peak_lock_action = peak_menu.addAction("Toggle Lock")
                peak_lock_action.triggered.connect(lambda: self.peak_tracker.toggle_lock())
                
                peak_reset_action = peak_menu.addAction("Reset Tracking")
                peak_reset_action.triggered.connect(lambda: self.peak_tracker.reset())
        
        # === Reset/Clear ===
        self.context_menu.addSeparator()
        reset_action = self.context_menu.addAction("Reset All")
        reset_action.triggered.connect(self._reset_all)
    
    def _on_spectrum_click(self, event):
        """Handle mouse click on spectrum plot"""
        if event.button() == Qt_RightButton:
            # Rebuild menu to reflect current state
            self._build_context_menu()
            self.context_menu.exec(QtGui.QCursor.pos())
    
    def _update_filter_label(self):
        """Update the filter status label"""
        parts = []
        parts.append(f"Window: {self.config.window_type}")
        parts.append(f"FFT: {self.config.nfft}")
        parts.append(f"Overlap: {int(self.config.overlap*100)}%")
        
        if self.config.enable_averaging:
            if self.config.avg_mode == 'exponential':
                parts.append(f"Avg: Exp(α={self.config.exp_alpha})")
            else:
                parts.append(f"Avg: {self.config.avg_count}")
        
        if self.config.enable_max_hold:
            parts.append("MaxHold")
        if self.config.enable_min_hold:
            parts.append("MinHold")
        
        self.filter_label.setText(" | ".join(parts))
    
    # === Context menu action handlers ===
    
    def _toggle_averaging(self, enabled: bool):
        self.config.enable_averaging = enabled
        if enabled:
            self.avg_buffer = deque(maxlen=self.config.avg_count)
            self.exp_avg_spectrum = None
        else:
            self.avg_buffer = None
            self.exp_avg_spectrum = None
        self.curve_avg.setVisible(enabled)
        self._update_filter_label()
    
    def _set_avg_mode(self, mode: str):
        self.config.avg_mode = mode
        self.avg_buffer = deque(maxlen=self.config.avg_count)
        self.exp_avg_spectrum = None
        self._update_filter_label()
    
    def _set_avg_count(self, count: int):
        self.config.avg_count = count
        if self.avg_buffer is not None:
            self.avg_buffer = deque(maxlen=count)
        self._update_filter_label()
    
    def _set_exp_alpha(self, alpha: float):
        self.config.exp_alpha = alpha
        self._update_filter_label()
    
    def _toggle_max_hold(self, enabled: bool):
        self.config.enable_max_hold = enabled
        if not enabled:
            self.max_hold_spectrum = None
        self.curve_max_hold.setVisible(enabled)
        self._update_filter_label()
    
    def _toggle_min_hold(self, enabled: bool):
        self.config.enable_min_hold = enabled
        if not enabled:
            self.min_hold_spectrum = None
        self.curve_min_hold.setVisible(enabled)
        self._update_filter_label()
    
    def _clear_hold_data(self):
        self.max_hold_spectrum = None
        self.min_hold_spectrum = None
    
    def _set_window_type(self, window_type: str):
        self.config.window_type = window_type
        self._update_window()
        self._update_filter_label()
    
    def _set_kaiser_beta(self, beta: float):
        self.config.kaiser_beta = beta
        self._update_window()
        self._update_filter_label()
    
    def _set_fft_size(self, size: int):
        self.config.nfft = size
        self._update_window()
        # Update frequency arrays
        self.freqs = np.fft.fftshift(np.fft.fftfreq(size, 1/self.reader.sample_rate))
        self.freqs += self.reader.center_freq
        self.freqs_mhz = self.freqs / 1e6
        # Reset waterfall
        self.waterfall_data = np.full((self.config.waterfall_history, size), -100.0, dtype=np.float32)
        # Reset holds
        self.max_hold_spectrum = None
        self.min_hold_spectrum = None
        self.exp_avg_spectrum = None
        if self.avg_buffer:
            self.avg_buffer.clear()
        self._update_filter_label()
    
    def _set_overlap(self, overlap: float):
        self.config.overlap = overlap
        self._update_filter_label()
    
    def _toggle_realtime_sync(self, enabled: bool):
        self.config.realtime_sync = enabled
        self.reader.rt_sync.sync_enabled = enabled
        self.data_reader.set_realtime_sync(enabled)
        
        # Update mode label
        mode_parts = []
        if self.config.follow_mode:
            mode_parts.append("LIVE")
        if enabled:
            mode_parts.append("RT-SYNC")
        if not mode_parts:
            mode_parts.append("PLAYBACK")
        self.mode_label.setText(f"Mode: {' | '.join(mode_parts)}")
        
        # Show/hide RT sync label
        if hasattr(self, 'rt_sync_label'):
            self.rt_sync_label.setVisible(enabled)
    
    def _set_max_lag(self, lag: float):
        """Set maximum acceptable lag in seconds"""
        self.config.max_lag_seconds = lag
        self.reader.rt_sync.max_lag_seconds = lag
    
    def _set_drop_threshold(self, threshold: float):
        """Set drop threshold in seconds"""
        self.config.drop_threshold_seconds = threshold
        self.reader.rt_sync.drop_threshold_seconds = threshold
    
    def _set_realtime_buffer(self, packets: int):
        """Set target buffer packets behind EOF"""
        self.config.realtime_buffer_packets = packets
        self.reader.rt_sync.buffer_packets = packets
        self._update_filter_label()
    
    def _set_realtime_max_buffer(self, packets: int):
        """Set max buffer before skipping to catch up"""
        self.config.realtime_max_buffer = packets
        self.reader.rt_sync.max_buffer_packets = packets
        self._update_filter_label()
    
    def _resync_realtime(self):
        """Force re-synchronization"""
        self.reader.rt_sync.sync_established = False
        self.reader.rt_sync.packets_skipped = 0
        self.reader.rt_sync.frames_dropped = 0
        self.reader.rt_sync.current_lag_seconds = 0.0
        self.reader._stream_start_wallclock = None
        self.reader._first_packet_timestamp = None
        self.reader.seek_to_end()
    
    def _toggle_autoscale(self, enabled: bool):
        self.auto_scale_enabled = enabled
        if not enabled:
            self.spectrum_plot.setYRange(-120, 0)
    
    def _toggle_waterfall(self, enabled: bool):
        self.config.show_waterfall = enabled
        if hasattr(self, 'waterfall_plot'):
            self.waterfall_plot.setVisible(enabled)
    
    def _toggle_time_domain(self, enabled: bool):
        self.config.show_time_domain = enabled
        if hasattr(self, 'time_plot'):
            self.time_plot.setVisible(enabled)
    
    def _toggle_peak_tracking(self, enabled: bool):
        self.config.enable_peak_tracking = enabled
        if enabled and self.peak_tracker is None:
            self.peak_tracker = PeakTracker(self.config, self.freqs)
        elif not enabled:
            self.peak_tracker = None
    
    def _reset_all(self):
        """Reset all state"""
        self.reader.reset()
        self.waterfall_data.fill(-100)
        self.max_hold_spectrum = None
        self.min_hold_spectrum = None
        self.exp_avg_spectrum = None
        if self.avg_buffer:
            self.avg_buffer.clear()
        if self.peak_tracker:
            self.peak_tracker.reset()
        self.reader.rt_sync.sync_established = False
        self.reader.rt_sync.packets_skipped = 0
        self.reader.rt_sync.frames_dropped = 0
        self.reader.rt_sync.current_lag_seconds = 0.0
        
    def start(self):
        self.data_reader.start()
        interval_ms = int(1000 / self.config.update_rate_hz)
        self.update_timer.start(interval_ms)
        
    def stop(self):
        self.update_timer.stop()
        self.data_reader.stop()
        self.data_reader.join(timeout=1.0)
        # Clear GPU memory on stop
        self.gpu.clear_gpu_memory(force=True)
        
    def pause(self):
        self.data_reader.pause()
        
    def resume(self):
        self.data_reader.resume()
        
    def _update_display(self):
        compute_start = time.perf_counter()
        
        # Drain queue to get the LATEST frame (drop old ones for realtime)
        samples = None
        header = None
        frames_drained = 0
        
        while True:
            try:
                new_samples, new_header = self.data_reader.data_queue.get_nowait()
                if new_samples is not None:
                    samples = new_samples
                    header = new_header
                    frames_drained += 1
                else:
                    # End-of-stream signal
                    samples = None
                    header = None
                    break
            except queue.Empty:
                break
        
        # Track dropped frames (frames we drained but didn't display)
        if frames_drained > 1:
            self.reader.rt_sync.frames_dropped += frames_drained - 1
        
        if samples is None:
            if header is None and frames_drained > 0:
                # End of stream signal received
                self.update_timer.stop()
                self.status_label.setText("Stream ended")
                self.mode_label.setText("Mode: ENDED")
                self.mode_label.setStyleSheet(
                    "QLabel { background-color: #3a1a1a; color: #ff6666; "
                    "font-family: monospace; font-size: 10px; padding: 3px; }"
                )
            return
        
        self.current_samples = samples
        self.playback_time = self.reader._total_samples_read / self.reader.sample_rate
        
        # Compute spectrum
        spectrum = self.gpu.compute_spectrum_batched(
            samples, self.config.nfft, self.window, self.config.overlap
        )
        self.current_spectrum = spectrum
        
        # Averaging
        if self.config.enable_averaging:
            if self.config.avg_mode == 'exponential':
                if self.exp_avg_spectrum is None:
                    self.exp_avg_spectrum = spectrum.copy()
                else:
                    self.exp_avg_spectrum = (self.config.exp_alpha * spectrum + 
                                              (1 - self.config.exp_alpha) * self.exp_avg_spectrum)
                avg_spectrum = self.exp_avg_spectrum
            else:
                self.avg_buffer.append(spectrum)
                avg_spectrum = np.mean(list(self.avg_buffer), axis=0)
            self.curve_avg.setData(self.freqs_mhz, avg_spectrum)
        
        # Max hold
        if self.config.enable_max_hold:
            if self.max_hold_spectrum is None:
                self.max_hold_spectrum = spectrum.copy()
            else:
                self.max_hold_spectrum = np.maximum(self.max_hold_spectrum, spectrum)
                if self.config.max_hold_decay > 0:
                    self.max_hold_spectrum -= self.config.max_hold_decay
            self.curve_max_hold.setData(self.freqs_mhz, self.max_hold_spectrum)
        
        # Min hold
        if self.config.enable_min_hold:
            if self.min_hold_spectrum is None:
                self.min_hold_spectrum = spectrum.copy()
            else:
                self.min_hold_spectrum = np.minimum(self.min_hold_spectrum, spectrum)
            self.curve_min_hold.setData(self.freqs_mhz, self.min_hold_spectrum)
        
        # Update spectrum
        self.curve_spectrum.setData(self.freqs_mhz, spectrum)
        
        # Auto-scale
        if self.auto_scale_enabled:
            self.auto_scale_update_counter += 1
            if self.auto_scale_update_counter >= self.auto_scale_update_interval:
                self.auto_scale_update_counter = 0
                
                valid = spectrum[np.isfinite(spectrum)]
                if len(valid) > 0:
                    new_y_min = max(-120, np.percentile(valid, 1) - 5)
                    new_y_max = min(20, np.percentile(valid, 99) + 10)
                    
                    if (abs(new_y_min - self.last_y_min) > 5 or 
                        abs(new_y_max - self.last_y_max) > 5):
                        self.spectrum_plot.setYRange(new_y_min, new_y_max)
                        self.last_y_min = new_y_min
                        self.last_y_max = new_y_max
        
        # Peak tracking
        if self.peak_tracker:
            peak_freq, peak_power, doppler_rate = self.peak_tracker.update(spectrum, self.playback_time)
            peak_freq_mhz = peak_freq / 1e6
            
            self.peak_marker.setData([peak_freq_mhz], [peak_power])
            
            lock_status = "LOCKED" if self.peak_tracker.state.track_locked else "SEARCHING"
            self.peak_text.setText(f'{peak_freq_mhz:.6f} MHz\n{peak_power:.1f} dB')
            self.peak_text.setPos(peak_freq_mhz, peak_power)
            
            self.peak_label.setText(
                f"Peak: {peak_freq_mhz:.6f} MHz @ {peak_power:.1f} dB | "
                f"Doppler: {doppler_rate:.2f} Hz/s | {lock_status}"
            )
            
            times, freqs = self.peak_tracker.get_history_arrays()
            if len(times) > 0:
                self.peak_history_curve.setData(times, freqs / 1e6)
                self.peak_history_plot.setXRange(
                    max(0, self.playback_time - self.config.peak_history_seconds),
                    self.playback_time
                )
        else:
            peak_idx = np.argmax(spectrum)
            peak_freq_mhz = self.freqs_mhz[peak_idx]
            peak_power = spectrum[peak_idx]
            self.peak_marker.setData([peak_freq_mhz], [peak_power])
            self.peak_text.setText(f'{peak_freq_mhz:.4f} MHz\n{peak_power:.1f} dB')
            self.peak_text.setPos(peak_freq_mhz, peak_power)
        
        # Time domain
        if self.config.show_time_domain and hasattr(self, 'time_plot'):
            n_display = min(2000, len(samples))
            time_axis = np.arange(n_display) / self.reader.sample_rate * 1e6
            self.curve_i.setData(time_axis, np.real(samples[:n_display]))
            self.curve_q.setData(time_axis, np.imag(samples[:n_display]))
            self.time_plot.setXRange(0, time_axis[-1])
        
        # Waterfall
        if self.config.show_waterfall and hasattr(self, 'waterfall_plot'):
            self.waterfall_data = np.roll(self.waterfall_data, 1, axis=0)
            self.waterfall_data[0, :] = spectrum.astype(np.float32)
            self.waterfall_img.setImage(self.waterfall_data.T, autoLevels=False)
            
            valid_wf = self.waterfall_data[self.waterfall_data > -100]
            if len(valid_wf) > 0:
                vmin = max(-100, np.percentile(valid_wf, 5))
                vmax = min(20, np.percentile(valid_wf, 95))
                self.waterfall_img.setLevels([vmin, vmax])
        
        # Realtime sync status - now with TIME-BASED lag display
        if self.config.realtime_sync and hasattr(self, 'rt_sync_label'):
            rt_state = self.reader.rt_sync
            if rt_state.sync_established or rt_state.current_lag_seconds > 0:
                buffer_pkts = rt_state.current_buffer_packets
                time_lag = rt_state.current_lag_seconds
                
                # Color based on status
                status = rt_state.status
                if status == "LIVE":
                    lag_color = '#00ff00'  # Green
                elif status == "BUFFERING":
                    lag_color = '#66ffff'  # Cyan
                elif status == "DROPPING":
                    lag_color = '#ffff00'  # Yellow
                elif status == "CATCHING_UP":
                    lag_color = '#ff6666'  # Red
                else:
                    lag_color = '#aaaaaa'
                
                self.rt_sync_label.setText(
                    f"RT: {status} | Lag: {time_lag:.2f}s | "
                    f"Buffer: {buffer_pkts} pkts | "
                    f"Dropped: {rt_state.packets_skipped} pkts, {rt_state.frames_dropped} frames"
                )
                self.rt_sync_label.setStyleSheet(
                    f"QLabel {{ background-color: #2a2a1a; color: {lag_color}; "
                    "font-family: monospace; font-size: 10px; padding: 3px; }"
                )
            else:
                self.rt_sync_label.setText("RT Sync: Initializing...")
        
        # Performance stats
        compute_time = time.perf_counter() - compute_start
        self.compute_times.append(compute_time)
        self.frame_count += 1
        
        now = time.perf_counter()
        if now - self.fps_timer >= 1.0:
            self.current_fps = self.frame_count / (now - self.fps_timer)
            self.frame_count = 0
            self.fps_timer = now
        
        avg_compute = np.mean(list(self.compute_times)) * 1000 if self.compute_times else 0
        total_samples = self.reader.get_total_samples()
        progress = (self.reader._total_samples_read / total_samples * 100) if total_samples > 0 else 0
        
        status = (
            f"{self.reader.filepath.name} | "
            f"Rate: {self.reader.sample_rate/1e6:.3f}MHz | "
            f"Time: {self.playback_time:.1f}s ({progress:.1f}%) | "
            f"FPS: {self.current_fps:.1f} | "
            f"Compute: {avg_compute:.1f}ms | "
            f"GPU: {self.gpu.backend.upper()}"
        )
        self.status_label.setText(status)
    
    def keyPressEvent(self, event):
        if event.key() == Qt_Key_Space:
            if self.data_reader.paused:
                self.resume()
            else:
                self.pause()
        elif event.key() == Qt_Key_R:
            self._reset_all()
        elif event.key() == Qt_Key_A:
            self.auto_scale_enabled = not self.auto_scale_enabled
            status = "ENABLED" if self.auto_scale_enabled else "DISABLED"
            print(f"Auto-scale: {status}")
            if not self.auto_scale_enabled:
                self.spectrum_plot.setYRange(-120, 0)
        elif event.key() == Qt_Key_T and self.peak_tracker:
            self.peak_tracker.toggle_lock()
        elif event.key() == Qt_Key_V:
            # Toggle averaging
            self._toggle_averaging(not self.config.enable_averaging)
        elif event.key() == Qt_Key_M:
            # Toggle max hold
            self._toggle_max_hold(not self.config.enable_max_hold)
        elif event.key() == Qt_Key_N:
            # Toggle min hold
            self._toggle_min_hold(not self.config.enable_min_hold)
        elif event.key() == Qt_Key_S:
            # Toggle realtime sync
            self._toggle_realtime_sync(not self.config.realtime_sync)
        elif event.key() == Qt_Key_Q:
            self.stop()
            QtWidgets.QApplication.quit()
        else:
            super().keyPressEvent(event)


# =============================================================================
# Main Window
# =============================================================================

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, reader: StreamingPacketReader, gpu: GPUAccelerator,
                 config: PlaybackConfig):
        super().__init__()
        
        title = "TSI SDR Spectrum Analyzer"
        if config.follow_mode:
            title += " [LIVE]"
        if config.realtime_sync:
            title += " [RT-SYNC]"
        self.setWindowTitle(title)
        self.setStyleSheet("background-color: #1e1e1e;")
        
        self.analyzer = SpectrumAnalyzerWidget(reader, gpu, config)
        self.setCentralWidget(self.analyzer)
        
        menubar = self.menuBar()
        menubar.setStyleSheet("background-color: #2d2d2d; color: white;")
        
        file_menu = menubar.addMenu("File")
        file_menu.addAction("Quit", self.close, "Ctrl+Q")
        
        control_menu = menubar.addMenu("Control")
        control_menu.addAction("Pause/Resume", self._toggle_pause, "Space")
        control_menu.addAction("Restart", self._restart, "R")
        control_menu.addSeparator()
        control_menu.addAction("Toggle Auto-Scale", self._toggle_autoscale, "A")
        control_menu.addAction("Toggle Averaging", self._toggle_averaging, "V")
        control_menu.addAction("Toggle Max Hold", self._toggle_max_hold, "M")
        control_menu.addAction("Toggle Min Hold", self._toggle_min_hold, "N")
        control_menu.addSeparator()
        control_menu.addAction("Toggle Realtime Sync", self._toggle_realtime_sync, "S")
        if config.enable_peak_tracking:
            control_menu.addAction("Toggle Lock", self._toggle_lock, "T")
        
        self.statusBar().showMessage(
            "SPACE=pause R=restart A=autoscale V=avg M=max N=min S=rtsync T=lock Q=quit | Right-click for menu"
        )
        self.statusBar().setStyleSheet("color: #888;")
        
    def _toggle_pause(self):
        if self.analyzer.data_reader.paused:
            self.analyzer.resume()
        else:
            self.analyzer.pause()
    
    def _restart(self):
        self.analyzer._reset_all()
    
    def _toggle_autoscale(self):
        self.analyzer.auto_scale_enabled = not self.analyzer.auto_scale_enabled
        status = "enabled" if self.analyzer.auto_scale_enabled else "disabled"
        self.statusBar().showMessage(f"Auto-scale {status}", 2000)
        if not self.analyzer.auto_scale_enabled:
            self.analyzer.spectrum_plot.setYRange(-120, 0)
    
    def _toggle_averaging(self):
        self.analyzer._toggle_averaging(not self.analyzer.config.enable_averaging)
        status = "enabled" if self.analyzer.config.enable_averaging else "disabled"
        self.statusBar().showMessage(f"Averaging {status}", 2000)
    
    def _toggle_max_hold(self):
        self.analyzer._toggle_max_hold(not self.analyzer.config.enable_max_hold)
        status = "enabled" if self.analyzer.config.enable_max_hold else "disabled"
        self.statusBar().showMessage(f"Max hold {status}", 2000)
    
    def _toggle_min_hold(self):
        self.analyzer._toggle_min_hold(not self.analyzer.config.enable_min_hold)
        status = "enabled" if self.analyzer.config.enable_min_hold else "disabled"
        self.statusBar().showMessage(f"Min hold {status}", 2000)
    
    def _toggle_realtime_sync(self):
        self.analyzer._toggle_realtime_sync(not self.analyzer.config.realtime_sync)
        status = "enabled" if self.analyzer.config.realtime_sync else "disabled"
        self.statusBar().showMessage(f"Realtime sync {status}", 2000)
    
    def _toggle_lock(self):
        if self.analyzer.peak_tracker:
            self.analyzer.peak_tracker.toggle_lock()
    
    def showEvent(self, event):
        super().showEvent(event)
        self.analyzer.start()
    
    def closeEvent(self, event):
        self.analyzer.stop()
        super().closeEvent(event)


# =============================================================================
# Argument Parser
# =============================================================================

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
        description='TSI SDR Real-Time Spectrum Analyzer (Enhanced + Fixed)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
MODES:
  --follow         Live mode: Watch file being written (for rfnoc_stream_tool)
  --realtime-sync  Realtime sync: Maintain buffer behind write position
  (default)        Playback mode: Play back recorded file
  --static         Static mode: Load all, analyze, display/save

REALTIME SYNC (LAXER DEFAULTS):
  The tool now uses TIME-BASED lag measurement instead of packet counts.
  Default tolerance is 1.0 second of lag before dropping packets.
  
  --max-lag 1.0           Maximum acceptable lag in seconds (default: 1.0)
  --drop-threshold 0.5    Start "DROPPING" status when lag exceeds this
  --realtime-buffer 250   Target packets behind EOF (fallback)

  Status indicators:
    LIVE       - Lag within acceptable range
    DROPPING   - Lag exceeds drop threshold (warning)
    CATCHING_UP - Lag exceeds max, actively dropping packets
    BUFFERING  - Waiting for more data

INTEGRATION WITH rfnoc_stream_tool:
  Start rfnoc_stream_tool first, then run:
    %(prog)s output.bin --raw-tsi --spp 512 -r 100k --follow --realtime-sync

BUILDING STANDALONE EXECUTABLE:
  pip install pyinstaller
  pyinstaller --onefile --name tsi_spectrum_analyzer \\
      --hidden-import pyqtgraph.graphicsItems.ViewBox.axisCtrlTemplate_pyqt6 \\
      tsi_spectrum_analyzer_enhanced_fixed.py

EXAMPLES:
  # Live streaming with realtime sync (1 second lag tolerance - default)
  %(prog)s capture.bin --raw-tsi --spp 512 -r 100k --follow --realtime-sync

  # Even more lax realtime (2 second lag OK)
  %(prog)s capture.bin --raw-tsi --spp 512 -r 100k --follow --realtime-sync --max-lag 2.0

  # Playback with GPU
  %(prog)s capture.bin --raw-tsi --spp 512 -r 100k --gpu

  # Peak tracking with averaging
  %(prog)s capture.bin --raw-tsi --spp 512 -r 100k --peak-track --average 10

CONTROLS:
  SPACE  Pause/Resume
  R      Restart
  A      Toggle auto-scale
  V      Toggle averaging
  M      Toggle max hold
  N      Toggle min hold
  S      Toggle realtime sync
  T      Toggle peak lock
  Q      Quit

Right-click on spectrum for context menu with all options.
        """
    )

    parser.add_argument('file', help='Input file')
    parser.add_argument('-f', '--format', default='sc16', choices=['sc16', 'sc8', 'fc32', 'fc64'])
    parser.add_argument('-r', '--sample-rate', type=str, default=None)
    parser.add_argument('-c', '--center-freq', type=str, default='0')
    parser.add_argument('--spp', type=int, default=1024)
    parser.add_argument('--sgb', action='store_true')
    parser.add_argument('--raw-tsi', action='store_true')
    parser.add_argument('--force-tsi', action='store_true')
    parser.add_argument('--force-chdr', action='store_true')
    
    # Mode
    parser.add_argument('--follow', '-F', action='store_true',
                       help='Live mode: watch file being written')
    parser.add_argument('--follow-timeout', type=float, default=5.0,
                       help='Seconds without new data before considering stream ended')
    parser.add_argument('--realtime-sync', '-S', action='store_true',
                       help='Realtime sync: maintain buffer behind write position')
    # NEW: Time-based lag parameters
    parser.add_argument('--max-lag', type=float, default=1.0,
                       help='Maximum acceptable lag in seconds before dropping (default: 1.0)')
    parser.add_argument('--drop-threshold', type=float, default=0.5,
                       help='Start DROPPING status when lag exceeds this (default: 0.5)')
    # Packet-based fallback parameters
    parser.add_argument('--realtime-buffer', type=int, default=250,
                       help='Target packets behind EOF (should be > write batch size, default: 250)')
    parser.add_argument('--realtime-min-buffer', type=int, default=100,
                       help='Minimum buffer packets before slowing down (default: 100)')
    parser.add_argument('--realtime-max-buffer', type=int, default=500,
                       help='Maximum buffer packets before skipping to catch up (default: 500)')
    parser.add_argument('--static', action='store_true', help='Static analysis mode')
    
    # GPU
    parser.add_argument('--gpu', action='store_true')
    parser.add_argument('--intel-gpu', action='store_true')
    
    # Display
    parser.add_argument('--update-rate', type=float, default=60.0)
    parser.add_argument('--fft-size', type=int, default=4096)
    parser.add_argument('--overlap', type=float, default=0.5)
    parser.add_argument('--window', default='hann', 
                       choices=['hann', 'hamming', 'blackman', 'kaiser', 'flattop', 'rectangular'])
    parser.add_argument('--kaiser-beta', type=float, default=14.0)
    parser.add_argument('--waterfall-history', type=int, default=200)
    parser.add_argument('--speed', type=float, default=1.0)
    parser.add_argument('--loop', action='store_true')
    parser.add_argument('--no-waterfall', action='store_true')
    parser.add_argument('--no-time-domain', action='store_true')
    
    # Peak tracking
    parser.add_argument('--peak-track', action='store_true')
    parser.add_argument('--peak-threshold', type=float, default=-60.0)
    parser.add_argument('--peak-history', type=float, default=60.0)
    
    # Averaging and hold
    parser.add_argument('--average', type=int, default=0, metavar='N',
                       help='Enable N-frame linear averaging')
    parser.add_argument('--exp-average', type=float, default=0, metavar='ALPHA',
                       help='Enable exponential averaging with alpha')
    parser.add_argument('--max-hold', action='store_true', help='Enable max hold')
    parser.add_argument('--min-hold', action='store_true', help='Enable min hold')
    
    # Static mode
    parser.add_argument('--max-samples', type=int, default=None)
    parser.add_argument('-o', '--output', default=None)
    
    # Version
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')

    args = parser.parse_args()

    sample_rate = parse_freq(args.sample_rate)
    center_freq = parse_freq(args.center_freq)

    print("=" * 60)
    print(f"TSI SDR SPECTRUM ANALYZER v{__version__} (Enhanced + Fixed)")
    print("=" * 60)

    # Create reader
    reader = StreamingPacketReader(
        filepath=args.file,
        format_type=args.format,
        sample_rate=sample_rate,
        center_freq=center_freq,
        force_tsi=args.force_tsi,
        force_chdr=args.force_chdr,
        raw_tsi=args.raw_tsi,
        spp=args.spp,
        sgb_enabled=args.sgb,
        follow_mode=args.follow,
        realtime_sync=args.realtime_sync,
    )

    # GPU
    use_gpu = args.gpu or args.intel_gpu
    gpu = GPUAccelerator(use_gpu=use_gpu, prefer_intel=args.intel_gpu)
    print(f"GPU: {gpu.backend.upper()} ({gpu.device_name})")

    # Open file
    try:
        reader.open()
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        return 1

    print(f"File: {reader.filepath.name}")
    mode_str = []
    if args.follow:
        mode_str.append("LIVE (follow)")
    if args.realtime_sync:
        mode_str.append("REALTIME-SYNC")
    if not mode_str:
        mode_str.append("PLAYBACK")
    print(f"Mode: {' | '.join(mode_str)}")
    print(f"Sample rate: {reader.sample_rate/1e6:.6f} MHz")
    
    if args.realtime_sync:
        print(f"Realtime sync: max_lag={args.max_lag}s, drop_threshold={args.drop_threshold}s")

    # Static mode
    if args.static:
        print("\n--- STATIC ANALYSIS ---")
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print("ERROR: matplotlib required for static mode")
            return 1
        
        samples = reader.load_all_samples(args.max_samples)
        window = create_window(args.window, args.fft_size, args.kaiser_beta).astype(gpu.get_float_dtype())
        spectrum = gpu.compute_spectrum_batched(samples, args.fft_size, window)
        freqs = np.fft.fftshift(np.fft.fftfreq(args.fft_size, 1/reader.sample_rate))
        freqs += reader.center_freq
        
        plt.figure(figsize=(12, 6))
        plt.plot(freqs/1e6, spectrum, 'c', lw=0.8)
        plt.xlabel('Frequency (MHz)')
        plt.ylabel('Power (dB)')
        plt.title(f'Spectrum: {reader.filepath.name}')
        plt.grid(True, alpha=0.3)
        
        if args.output:
            plt.savefig(args.output, dpi=150)
            print(f"Saved: {args.output}")
        else:
            plt.show()
        
        reader.close()
        return 0

    # Real-time mode
    if not PYQTGRAPH_AVAILABLE:
        print("ERROR: PyQtGraph required")
        return 1

    # Determine averaging settings
    enable_averaging = args.average > 0 or args.exp_average > 0
    avg_mode = 'exponential' if args.exp_average > 0 else 'linear'
    avg_count = args.average if args.average > 0 else 10
    exp_alpha = args.exp_average if args.exp_average > 0 else 0.1

    config = PlaybackConfig(
        update_rate_hz=args.update_rate,
        use_gpu=use_gpu,
        prefer_intel=args.intel_gpu,
        waterfall_history=args.waterfall_history,
        show_waterfall=not args.no_waterfall,
        show_time_domain=not args.no_time_domain,
        loop_playback=args.loop,
        playback_speed=args.speed,
        nfft=args.fft_size,
        overlap=args.overlap,
        window_type=args.window,
        kaiser_beta=args.kaiser_beta,
        follow_mode=args.follow,
        follow_timeout=args.follow_timeout,
        realtime_sync=args.realtime_sync,
        realtime_buffer_packets=args.realtime_buffer,
        realtime_min_buffer=args.realtime_min_buffer,
        realtime_max_buffer=args.realtime_max_buffer,
        max_lag_seconds=args.max_lag,
        drop_threshold_seconds=args.drop_threshold,
        enable_peak_tracking=args.peak_track,
        peak_threshold_db=args.peak_threshold,
        peak_history_seconds=args.peak_history,
        enable_averaging=enable_averaging,
        avg_count=avg_count,
        avg_mode=avg_mode,
        exp_alpha=exp_alpha,
        enable_max_hold=args.max_hold,
        enable_min_hold=args.min_hold,
    )

    print(f"Update rate: {config.update_rate_hz:.0f} Hz")
    print(f"Samples/frame: {int(reader.sample_rate / config.update_rate_hz):,}")
    print(f"Window: {config.window_type}, FFT: {config.nfft}, Overlap: {int(config.overlap*100)}%")
    if enable_averaging:
        print(f"Averaging: {avg_mode} (count={avg_count}, alpha={exp_alpha})")

    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('Fusion')
    
    palette = QtGui.QPalette()
    palette.setColor(Qt_ColorRole_Window, QtGui.QColor(30, 30, 30))
    palette.setColor(Qt_ColorRole_WindowText, QtGui.QColor(255, 255, 255))
    app.setPalette(palette)

    window = MainWindow(reader, gpu, config)
    window.resize(1200, 900)
    window.show()

    print("\nStarting... Right-click for context menu")
    print("Keys: SPACE=pause R=restart A=autoscale V=avg M=max N=min S=rtsync T=lock Q=quit")

    try:
        ret = app.exec()
    finally:
        reader.close()
        # Final GPU memory cleanup
        gpu.clear_gpu_memory(force=True)
    
    return ret


if __name__ == '__main__':
    sys.exit(main())