import pandas as pd
from datetime import datetime
import os
from trace_id import extract_unique_trace_ids


def calculate_end_time_unix_nano(csv_file, outputfile):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # List to store EndTimeUnixNano values
    end_time_nano_list = []

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        # Split the timestamp to separate nanoseconds
        timestamp_str = row['Timestamp']
        dt_part, nano_part = timestamp_str.split('.')

        # Parse the datetime part
        timestamp_dt = datetime.strptime(dt_part, '%Y-%m-%d %H:%M:%S')

        # Convert timestamp to Unix time in seconds
        unix_time_seconds = int(timestamp_dt.timestamp())

        # Convert Unix time to nanoseconds and add nanoseconds part
        start_time_nano = unix_time_seconds * 10 ** 9 + int(nano_part)

        # Get the duration in microseconds and convert it to nanoseconds
        duration_microseconds = row['Duration']
        duration_nano = duration_microseconds * 1000

        # Calculate the EndTimeUnixNano
        end_time_nano = start_time_nano + duration_nano

        # Append the result to the list
        end_time_nano_list.append(end_time_nano)

    # Add the EndTimeUnixNano as a new column in the original DataFrame
    df['EndTimeUnixNano'] = end_time_nano_list


    # Save the updated DataFrame to a new CSV file
    df.to_csv(outputfile, index=False)


def process_trace_data(output_file, output_dir, traceid_output_dir):
    # Read the CSV file
    df = pd.read_csv(output_file)

    # Define a list to store results
    results = []

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        # Split the timestamp to separate nanoseconds
        timestamp_str = row['Timestamp']
        dt_part, nano_part = timestamp_str.split('.')

        # Parse the datetime part
        timestamp_dt = datetime.strptime(dt_part, '%Y-%m-%d %H:%M:%S')

        # Convert timestamp to Unix time in seconds
        unix_time_seconds = int(timestamp_dt.timestamp())

        # Convert Unix time to nanoseconds and add nanoseconds part
        start_time_nano = unix_time_seconds * 10 ** 9 + int(nano_part)

        # Get EndTimeUnixNano from the row
        end_time_nano = row['EndTimeUnixNano']

        # Append the reformatted data to results
        results.append({
            'TraceID': row['TraceId'],
            'SpanID': row['SpanId'],
            'ParentID': row['ParentSpanId'],
            'PodName': row['ServiceName'],
            'OperationName': row['SpanName'],
            'Timestamp': row['Timestamp'],
            'StartTimeUnixNano': start_time_nano,
            'EndTimeUnixNano': end_time_nano
        })

    results_df = pd.DataFrame(results)

    results_df['Timestamp'] = pd.to_datetime(results_df['Timestamp'])
    start_time = results_df['Timestamp'].min()
    # hour_time = start_time.hour

    # Split into 10 one-minute chunks
    for i in range(10):
        chunk_start = start_time + pd.Timedelta(minutes=i)
        chunk_end = chunk_start + pd.Timedelta(minutes=1)

        hour_time = chunk_start.hour

        # Filter rows for the current chunk
        chunk_df = results_df[(results_df['Timestamp'] >= chunk_start) & (results_df['Timestamp'] < chunk_end)]

        # Skip if the chunk is empty
        if chunk_df.empty:
            continue

        # Generate the file name
        filename = f"{hour_time}_{chunk_start:%M}_trace.csv"
        chunk_output_path = output_dir / filename

        # Save the chunk to a CSV file
        chunk_df.to_csv(chunk_output_path, index=False)
        print(f"Saved chunk to {chunk_output_path}")

        trace_id_filename = filename = f"{hour_time}_{chunk_start:%M}_traceid.csv"
        traceid_chunk_file = traceid_output_dir / trace_id_filename

        # 输入trace file，输出traceid file
        extract_unique_trace_ids(chunk_output_path, traceid_chunk_file)



