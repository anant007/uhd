#!/usr/bin/env python3
"""
TCP Client for receiving TSI packets from rfnoc_stream_tool
Parses packets, checks for drops via sequence numbers, writes to file.
"""

import socket
import struct
import time
import argparse
from collections import defaultdict
from dataclasses import dataclass
from typing import BinaryIO

# TSI packet header structure (28 bytes)
# Based on packetheader struct:
#   uint16_t sat_id
#   uint16_t channel_id  
#   uint32_t seq_num
#   uint32_t timestamp_sec
#   uint32_t timestamp_usec
#   uint32_t num_samples
#   uint32_t tuning_freq_khz
#   uint16_t sample_rate_khz
#   uint16_t flags
TSI_HEADER_FORMAT = '<HHIIIIIHH'  # Little-endian
TSI_HEADER_SIZE = 28

@dataclass
class TsiHeader:
    sat_id: int
    channel_id: int
    seq_num: int
    timestamp_sec: int
    timestamp_usec: int
    num_samples: int
    tuning_freq_khz: int
    sample_rate_khz: int
    flags: int

    @classmethod
    def from_bytes(cls, data: bytes) -> 'TsiHeader':
        fields = struct.unpack(TSI_HEADER_FORMAT, data)
        return cls(*fields)


class PacketStats:
    def __init__(self):
        self.packets_received = 0
        self.bytes_received = 0
        self.packets_dropped = 0
        self.out_of_order = 0
        self.parse_errors = 0
        self.start_time = time.perf_counter()
        
        # Track sequence numbers per channel
        self.expected_seq: dict[int, int] = defaultdict(lambda: -1)
        self.first_seq: dict[int, int] = {}
        self.last_seq: dict[int, int] = {}
        
    def check_sequence(self, channel_id: int, seq_num: int) -> int:
        """Check sequence number, return number of dropped packets (0 if ok)"""
        expected = self.expected_seq[channel_id]
        
        if expected == -1:
            # First packet for this channel
            self.first_seq[channel_id] = seq_num
            self.expected_seq[channel_id] = (seq_num + 1) & 0xFFFFFFFF
            return 0
        
        if seq_num == expected:
            # Perfect, as expected
            self.expected_seq[channel_id] = (seq_num + 1) & 0xFFFFFFFF
            self.last_seq[channel_id] = seq_num
            return 0
        
        # Calculate gap (handle wraparound)
        if seq_num > expected:
            gap = seq_num - expected
        else:
            # Wraparound case
            gap = (0xFFFFFFFF - expected + seq_num + 1)
        
        if gap > 0x7FFFFFFF:
            # Likely out of order (negative gap)
            self.out_of_order += 1
            return 0
        
        # Dropped packets
        self.expected_seq[channel_id] = (seq_num + 1) & 0xFFFFFFFF
        self.last_seq[channel_id] = seq_num
        return gap
    
    def report(self):
        elapsed = time.perf_counter() - self.start_time
        rate_mbps = (self.bytes_received * 8 / 1e6) / elapsed if elapsed > 0 else 0
        pps = self.packets_received / elapsed if elapsed > 0 else 0
        
        print(f"\n{'='*60}")
        print(f"RECEPTION STATISTICS")
        print(f"{'='*60}")
        print(f"Duration:          {elapsed:.2f} seconds")
        print(f"Packets received:  {self.packets_received:,}")
        print(f"Bytes received:    {self.bytes_received:,} ({self.bytes_received/1e6:.2f} MB)")
        print(f"Throughput:        {rate_mbps:.2f} Mbps ({pps:.0f} packets/sec)")
        print(f"Packets dropped:   {self.packets_dropped:,}")
        print(f"Out of order:      {self.out_of_order:,}")
        print(f"Parse errors:      {self.parse_errors:,}")
        
        if self.packets_received > 0:
            drop_rate = (self.packets_dropped / (self.packets_received + self.packets_dropped)) * 100
            print(f"Drop rate:         {drop_rate:.4f}%")
        
        print(f"\nPer-channel statistics:")
        for ch in sorted(self.first_seq.keys()):
            first = self.first_seq.get(ch, 0)
            last = self.last_seq.get(ch, 0)
            expected_count = last - first + 1 if last >= first else 0
            print(f"  Channel {ch}: seq {first} -> {last} (expected {expected_count} packets)")


def recv_exact(sock: socket.socket, n: int, buffer: bytearray) -> bool:
    """Receive exactly n bytes into buffer. Returns False on connection close."""
    view = memoryview(buffer)[:n]
    received = 0
    while received < n:
        try:
            chunk = sock.recv_into(view[received:], n - received)
            if chunk == 0:
                return False
            received += chunk
        except BlockingIOError:
            continue
    return True


def run_client(host: str, port: int, output_file: str, verbose: bool = False):
    """Connect to server and receive packets."""
    
    stats = PacketStats()
    
    # Pre-allocate buffers for speed
    len_buffer = bytearray(4)
    header_buffer = bytearray(TSI_HEADER_SIZE)
    payload_buffer = bytearray(64 * 1024)  # 64KB max payload
    
    print(f"Connecting to {host}:{port}...")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)  # 4MB recv buffer
        sock.connect((host, port))
        print(f"Connected!")
        
        # Open output file
        outfile: BinaryIO | None = None
        if output_file:
            outfile = open(output_file, 'wb', buffering=1024*1024)  # 1MB write buffer
            print(f"Writing to: {output_file}")
        
        last_report = time.perf_counter()
        report_interval = 2.0  # seconds
        
        try:
            while True:
                # Read 4-byte length prefix (network byte order)
                if not recv_exact(sock, 4, len_buffer):
                    print("\nConnection closed by server")
                    break
                
                msg_len = struct.unpack('!I', len_buffer)[0]
                
                if msg_len < TSI_HEADER_SIZE:
                    stats.parse_errors += 1
                    if verbose:
                        print(f"Invalid message length: {msg_len}")
                    continue
                
                payload_len = msg_len - TSI_HEADER_SIZE
                
                # Read TSI header
                if not recv_exact(sock, TSI_HEADER_SIZE, header_buffer):
                    print("\nConnection closed during header read")
                    break
                
                # Parse header
                try:
                    header = TsiHeader.from_bytes(bytes(header_buffer))
                except struct.error as e:
                    stats.parse_errors += 1
                    if verbose:
                        print(f"Header parse error: {e}")
                    # Drain payload
                    if payload_len > 0:
                        recv_exact(sock, payload_len, payload_buffer)
                    continue
                
                # Check sequence number
                dropped = stats.check_sequence(header.channel_id, header.seq_num)
                if dropped > 0:
                    stats.packets_dropped += dropped
                    if verbose:
                        print(f"DROPPED {dropped} packets! ch={header.channel_id} seq={header.seq_num}")
                
                # Read payload
                if payload_len > 0:
                    if payload_len > len(payload_buffer):
                        payload_buffer = bytearray(payload_len)
                    
                    if not recv_exact(sock, payload_len, payload_buffer):
                        print("\nConnection closed during payload read")
                        break
                
                # Write to file (length prefix + header + payload)
                if outfile:
                    outfile.write(len_buffer)
                    outfile.write(header_buffer)
                    if payload_len > 0:
                        outfile.write(memoryview(payload_buffer)[:payload_len])
                
                # Update stats
                stats.packets_received += 1
                stats.bytes_received += 4 + msg_len
                
                # Periodic progress report
                now = time.perf_counter()
                if now - last_report >= report_interval:
                    elapsed = now - stats.start_time
                    rate = stats.bytes_received / elapsed / 1e6
                    print(f"\r[{elapsed:.1f}s] Packets: {stats.packets_received:,} | "
                          f"Dropped: {stats.packets_dropped:,} | "
                          f"Rate: {rate:.2f} MB/s", end='', flush=True)
                    last_report = now
                    
        except KeyboardInterrupt:
            print("\n\nInterrupted by user")
        finally:
            if outfile:
                outfile.close()
    
    stats.report()
    return stats


def main():
    parser = argparse.ArgumentParser(description='TCP client for TSI packet reception')
    parser.add_argument('--host', '-H', default='127.0.0.1', help='Server host (default: 127.0.0.1)')
    parser.add_argument('--port', '-p', type=int, default=6000, help='Server port (default: 6000)')
    parser.add_argument('--output', '-o', default='received_packets.bin', help='Output file (default: received_packets.bin)')
    parser.add_argument('--no-file', action='store_true', help='Do not write to file (stats only)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output (show drops)')
    
    args = parser.parse_args()
    
    output = None if args.no_file else args.output
    run_client(args.host, args.port, output, args.verbose)


if __name__ == '__main__':
    main()