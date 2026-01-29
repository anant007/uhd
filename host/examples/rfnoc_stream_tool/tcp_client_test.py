#!/usr/bin/env python3
"""
TCP Client for testing rfnoc_stream_tool in SERVER mode.
Connects to the tool, receives TSI packets, validates sequence numbers.

Usage:
  1. Start rfnoc_stream_tool with server mode config
  2. Run this script to connect and receive data
"""
import socket
import struct
import time
import argparse
import sys
from collections import defaultdict

# TSI Header: 28 bytes (little-endian)
# uint16_t sat_id
# uint16_t channel_id
# uint32_t seq_num
# uint32_t timestamp_sec
# uint32_t timestamp_usec
# uint32_t num_samples
# uint32_t tuning_freq_khz
# uint16_t sample_rate_khz
# uint16_t flags
TSI_HEADER_SIZE = 28
TSI_HEADER_FMT = '<HHIIIIIHH'


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes or raise exception."""
    chunks = []
    received = 0
    while received < n:
        chunk = sock.recv(n - received)
        if not chunk:
            raise ConnectionError(f"Connection closed, got {received}/{n} bytes")
        chunks.append(chunk)
        received += len(chunk)
    return b''.join(chunks)


def parse_tsi_header(data: bytes) -> dict:
    """Parse 28-byte TSI header."""
    fields = struct.unpack(TSI_HEADER_FMT, data)
    return {
        'sat_id': fields[0],
        'channel_id': fields[1],
        'seq_num': fields[2],
        'timestamp_sec': fields[3],
        'timestamp_usec': fields[4],
        'num_samples': fields[5],
        'tuning_freq_khz': fields[6],
        'sample_rate_khz': fields[7],
        'flags': fields[8],
    }


def main():
    parser = argparse.ArgumentParser(
        description='TCP Client for rfnoc_stream_tool server mode testing')
    parser.add_argument('--host', '-H', default='127.0.0.1',
                        help='Server host (default: 127.0.0.1)')
    parser.add_argument('--port', '-p', type=int, default=9009,
                        help='Server port (default: 9009)')
    parser.add_argument('--output', '-o', default=None,
                        help='Output file to save received data (optional)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Print each packet info')
    parser.add_argument('--timeout', '-t', type=float, default=30.0,
                        help='Connection timeout in seconds (default: 30)')
    parser.add_argument('--retry', '-r', action='store_true',
                        help='Retry connection until successful')
    args = parser.parse_args()

    print("=" * 70)
    print("TSI Packet TCP Client - For testing rfnoc_stream_tool SERVER mode")
    print("=" * 70)
    print(f"Target: {args.host}:{args.port}")
    print()

    # Connection with retry logic
    sock = None
    while sock is None:
        try:
            print(f"Connecting to {args.host}:{args.port}...")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(args.timeout)
            sock.connect((args.host, args.port))
            print(f">>> CONNECTED!")
        except (socket.timeout, ConnectionRefusedError, OSError) as e:
            print(f"Connection failed: {e}")
            if sock:
                sock.close()
                sock = None
            if args.retry:
                print("Retrying in 2 seconds... (Ctrl+C to abort)")
                try:
                    time.sleep(2)
                except KeyboardInterrupt:
                    print("\nAborted.")
                    sys.exit(1)
            else:
                print("Use --retry to keep trying, or start rfnoc_stream_tool first.")
                sys.exit(1)

    # Configure socket for performance
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)  # 8MB
    sock.settimeout(10.0)  # Read timeout

    # Open output file if specified
    outfile = None
    if args.output:
        outfile = open(args.output, 'wb', buffering=1024 * 1024)
        print(f"Writing to: {args.output}")

    # Statistics
    packet_count = 0
    total_bytes = 0
    parse_errors = 0
    dropped_packets = 0
    
    # Sequence tracking per channel
    expected_seq = defaultdict(lambda: None)
    first_seq = {}
    last_seq = {}
    
    start_time = time.perf_counter()
    last_report = start_time
    first_packet_printed = False

    print()
    print("Receiving packets... (Ctrl+C to stop)")
    print("-" * 70)

    try:
        while True:
            try:
                # Read 4-byte length prefix (network byte order = big endian)
                len_data = recv_exact(sock, 4)
                msg_len = struct.unpack('!I', len_data)[0]

                # Sanity check
                if msg_len < TSI_HEADER_SIZE:
                    print(f"\nWARNING: Invalid msg_len={msg_len}, skipping")
                    parse_errors += 1
                    continue

                if msg_len > 10 * 1024 * 1024:  # >10MB is suspicious
                    print(f"\nWARNING: Huge msg_len={msg_len}, likely corrupt")
                    parse_errors += 1
                    break

                # Read full message (header + payload)
                msg_data = recv_exact(sock, msg_len)

                # Parse TSI header
                header = parse_tsi_header(msg_data[:TSI_HEADER_SIZE])
                payload = msg_data[TSI_HEADER_SIZE:]
                payload_len = len(payload)

                # Print first packet details
                if not first_packet_printed:
                    print(f"First packet received:")
                    print(f"  sat_id:      {header['sat_id']}")
                    print(f"  channel_id:  {header['channel_id']}")
                    print(f"  seq_num:     {header['seq_num']}")
                    print(f"  timestamp:   {header['timestamp_sec']}.{header['timestamp_usec']:06d}")
                    print(f"  num_samples: {header['num_samples']}")
                    print(f"  tuning_freq: {header['tuning_freq_khz']} kHz")
                    print(f"  sample_rate: {header['sample_rate_khz']} kHz")
                    print(f"  payload:     {payload_len} bytes")
                    print("-" * 70)
                    first_packet_printed = True

                # Check sequence number
                ch = header['channel_id']
                seq = header['seq_num']
                
                if ch not in first_seq:
                    first_seq[ch] = seq
                    expected_seq[ch] = seq
                
                if expected_seq[ch] is not None and seq != expected_seq[ch]:
                    # Calculate gap (handle 32-bit wraparound)
                    if seq > expected_seq[ch]:
                        gap = seq - expected_seq[ch]
                    else:
                        gap = (0xFFFFFFFF - expected_seq[ch]) + seq + 1
                    
                    if gap < 0x7FFFFFFF:  # Forward gap = dropped packets
                        dropped_packets += gap
                        if args.verbose:
                            print(f"\n!!! DROPPED {gap} packets on ch={ch}, "
                                  f"expected seq={expected_seq[ch]}, got seq={seq}")
                
                expected_seq[ch] = (seq + 1) & 0xFFFFFFFF
                last_seq[ch] = seq

                # Write to file
                if outfile:
                    # outfile.write(len_data)
                    outfile.write(msg_data)

                # Update stats
                packet_count += 1
                total_bytes += 4 + msg_len

                # Verbose per-packet output
                if args.verbose and packet_count <= 10:
                    print(f"  Pkt #{packet_count}: ch={ch} seq={seq} "
                          f"samples={header['num_samples']} payload={payload_len}B")

                # Periodic progress report
                now = time.perf_counter()
                if now - last_report >= 1.0:
                    elapsed = now - start_time
                    rate_mbps = (total_bytes * 8 / 1e6) / elapsed if elapsed > 0 else 0
                    pps = packet_count / elapsed if elapsed > 0 else 0
                    print(f"\r[{elapsed:6.1f}s] Pkts: {packet_count:>10,} | "
                          f"Dropped: {dropped_packets:>6,} | "
                          f"Rate: {rate_mbps:>7.2f} Mbps | "
                          f"{pps:>8.0f} pkt/s", end='', flush=True)
                    last_report = now

            except socket.timeout:
                print("\n>>> Socket timeout - no data received")
                continue

    except ConnectionError as e:
        print(f"\n>>> {e}")
    except KeyboardInterrupt:
        print("\n\n>>> Interrupted by user")
    except Exception as e:
        print(f"\n>>> Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        sock.close()
        if outfile:
            outfile.close()

    # Final report
    elapsed = time.perf_counter() - start_time
    
    print()
    print("=" * 70)
    print("FINAL STATISTICS")
    print("=" * 70)
    print(f"Duration:           {elapsed:.2f} seconds")
    print(f"Packets received:   {packet_count:,}")
    print(f"Total bytes:        {total_bytes:,} ({total_bytes/1e6:.2f} MB)")
    print(f"Packets dropped:    {dropped_packets:,}")
    print(f"Parse errors:       {parse_errors:,}")
    
    if elapsed > 0 and packet_count > 0:
        print(f"Average rate:       {total_bytes/elapsed/1e6:.2f} MB/s")
        print(f"Average pkt rate:   {packet_count/elapsed:.0f} packets/sec")
    
    if dropped_packets > 0 and packet_count > 0:
        total_expected = packet_count + dropped_packets
        drop_pct = (dropped_packets / total_expected) * 100
        print(f"Drop rate:          {drop_pct:.4f}%")
    
    if first_seq:
        print()
        print("Per-channel summary:")
        for ch in sorted(first_seq.keys()):
            f = first_seq[ch]
            l = last_seq.get(ch, f)
            expected = l - f + 1 if l >= f else 0
            print(f"  Channel {ch}: seq {f} -> {l} ({expected:,} expected)")
    
    print("=" * 70)
    
    # Exit code: 0 if no drops, 1 if drops detected
    sys.exit(0 if dropped_packets == 0 else 1)


if __name__ == '__main__':
    main()