"""
Parsing Collected NTP Logs

Oisín Mc Laughlin - 22441106
"""
from collections import defaultdict
from datetime import datetime
import re
import os
import csv

INPUT_FILE = "ntp-logs/ntplog.txt"
OUTPUT_DIR = "parsed-ntp-logs"

SERVER_NAMES = {
    '140.203.204.77': 'Ireland',
    'ntp0.cam.ac.uk': 'UK',
    'ptbtime1.ptb.de': 'Germany',
    'time-a-g.nist.g': 'US',
    'ntp1.tuxfamily.org': 'France',
    'ns1.anu.edu.au': 'Australia',
    'ntp-b3.nict.go.': 'Japan'
}


def parse_ntp_log(input_file):
    """
    Parses the NTP log file and extracts relevant data points.
    :param input_file: Path to NTP log file
    :return: List of dictionaries containing parsed data points
    """
    data_rows = []
    current_timestamp = None

    with open(input_file, 'r') as f:
        for line in f:
            line = line.strip()

            # Check for timestamp line
            timestamp_match = re.match(r'=== (.+?) ===', line) # Match lines like "=== 2024-06-01 12:00:00 UTC ==="
            if timestamp_match:
                timestamp_str = timestamp_match.group(1) # Match the timestamp part
                try:
                    current_timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S %Z') # Try parsing with timezone
                except ValueError:
                    print("Error failed to parse timestamp")
                    break

            # Skip header lines and empty lines
            if not line or line.startswith('remote') or line.startswith('===') or line.startswith('='):
                continue

            # Split the line into parts based on whitespace
            parts = line.split()

            # Ensure we have enough parts to parse (at least 9 fields)
            if len(parts) < 9:
                continue

            # Extract the server status symbol if present
            remote = parts[0]
            status = ''
            if remote and remote[0] in ['*', '+', '-', ' ', 'x', 'o', '#']:
                status = remote[0]
                remote = remote[1:]

            # Map the remote server to a known name if possible
            refid = parts[1]

            # Skip lines with .STEP., .INIT., or other error indicators
            if refid in ['.STEP.', '.INIT.', '.RATE.']:
                continue

            try:
                st = int(parts[2])
                t = parts[3]
                when = parts[4]
                poll = int(parts[5])
                reach = int(parts[6])
                delay = float(parts[7])
                offset = float(parts[8])
                jitter = float(parts[9])

                # Convert 'when' field
                when_seconds = 0
                if when != '-':
                    if 'm' in when:
                        when_seconds = int(when.replace('m', '')) * 60
                    elif 'h' in when:
                        when_seconds = int(when.replace('h', '')) * 3600
                    else:
                        try:
                            when_seconds = int(when)
                        except ValueError:
                            when_seconds = 0

                data_rows.append({
                    'timestamp': current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'timestamp_obj': current_timestamp,
                    'status': status,
                    'remote': remote,
                    'refid': refid,
                    'stratum': st,
                    'type': t,
                    'when_seconds': when_seconds,
                    'poll': poll,
                    'reach': reach,
                    'delay_ms': delay,
                    'offset_ms': offset,
                    'jitter_ms': jitter
                })
            except (ValueError, IndexError) as e:
                print(f"Warning: Skipping malformed line: {line}")
                continue

    print(f"Successfully parsed {len(data_rows)} data points")
    return data_rows


def write_main_csv(data_rows, output_dir):
    """
    Writes the parsed NTP data to a main CSV file.
    :param data_rows: List of dictionaries containing parsed data points
    :param output_dir: Directory to save the output CSV file
    :return: Path to the created CSV file
    """

    output_file = os.path.join(output_dir, "ntp_data_all.csv")

    fieldnames = ['timestamp', 'status', 'remote', 'refid', 'stratum', 'type',
                  'when_seconds', 'poll', 'reach', 'delay_ms', 'offset_ms', 'jitter_ms']

    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data_rows:
            row_copy = row.copy()
            row_copy.pop('timestamp_obj', None)
            writer.writerows([row_copy])

    print(f"Created: {output_file}")
    return output_file


def write_combined_csv(data_rows, output_dir):
    """
    Writes a combined CSV file that includes the server location based on the remote server IP or hostname.
    :param data_rows: List of dictionaries containing parsed data points
    :param output_dir: Directory to save the output CSV file
    :return: Path to the created CSV file
    """

    output_file = os.path.join(output_dir, "ntp_data_with_locations.csv")

    with open(output_file, 'w', newline='') as f:
        fieldnames = ['timestamp', 'location', 'server', 'delay_ms', 'offset_ms', 'jitter_ms']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in data_rows:
            server = row['remote']
            location = SERVER_NAMES.get(server, server)
            writer.writerow({
                'timestamp': row['timestamp'],
                'location': location,
                'server': server,
                'delay_ms': row['delay_ms'],
                'offset_ms': row['offset_ms'],
                'jitter_ms': row['jitter_ms']
            })

    print(f"Created: {output_file}")
    return output_file


def write_per_server_files(data_rows, output_dir):
    """
        Writes separate CSV files for each server, containing only the relevant data points for that server.
    :param data_rows: List of dictionaries containing parsed data points
    :param output_dir: Directory to save the output CSV files
    :return: List of paths to the created CSV files
    """

    servers_data = defaultdict(list) # Group data by server

    for row in data_rows:
        server = row['remote']
        servers_data[server].append(row)

    files_created = []

    for server, data in servers_data.items():
        location_name = SERVER_NAMES.get(server, server.replace('.', '_'))
        filename = os.path.join(output_dir, f"{location_name}.csv")

        with open(filename, 'w', newline='') as f:
            fieldnames = ['timestamp', 'delay_ms', 'offset_ms', 'jitter_ms']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for row in data:
                writer.writerow({
                    'timestamp': row['timestamp'],
                    'delay_ms': row['delay_ms'],
                    'offset_ms': row['offset_ms'],
                    'jitter_ms': row['jitter_ms']
                })

        files_created.append(filename)
        print(f"Created: {filename}")

    return files_created


def main():
    data_rows = parse_ntp_log(INPUT_FILE)

    write_main_csv(data_rows, OUTPUT_DIR)

    write_combined_csv(data_rows, OUTPUT_DIR)

    write_per_server_files(data_rows, OUTPUT_DIR)


if __name__ == "__main__":
    main()
