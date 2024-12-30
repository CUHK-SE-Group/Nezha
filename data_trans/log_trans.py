import pandas as pd
import os
import datetime

# Function to calculate TimeUnixNano from Timestamp
def calculate_time_unix_nano(timestamp):
    # Remove everything after the first 9 digits of the fractional seconds
    timestamp = timestamp.split('.')[0] + '.' + timestamp.split('.')[1][:6] if '.' in timestamp else timestamp
    dt = datetime.datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
    return int(dt.timestamp() * 1e9)

def modify_log_data(csv_file, output_dir):
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Remove the SeverityText and SeverityNumber columns
    df.drop(columns=['SeverityText', 'SeverityNumber'], inplace=True)


    df['Node'] = '33.33.33.80'  # All values set to empty strings

    # Add the Container column with the value 'server'
    df['Container'] = 'server'  # All values set to 'server'

    # Rename columns
    df.rename(columns={
        'ServiceName': 'PodName',
        'Body': 'Log',
        'SpanId': 'SpanID',
        'TraceId': 'TraceID'
    }, inplace=True)

    # Add the TimeUnixNano column
    df['TimeUnixNano'] = df['Timestamp'].apply(calculate_time_unix_nano)

    # Specify the new column order
    new_order = ['Timestamp', 'TimeUnixNano', 'Node', 'PodName', 'Container', 'TraceID', 'SpanID', 'Log']

    # Reorder the DataFrame columns
    df = df[new_order]

    # Convert the Timestamp column to datetime for splitting
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    # Determine the start time of the log file
    start_time = df['Timestamp'].min()

    # Split into 10 one-minute chunks
    for i in range(10):
        chunk_start = start_time + pd.Timedelta(minutes=i)
        chunk_end = chunk_start + pd.Timedelta(minutes=1)

        hour_time = chunk_start.hour

        # Filter rows for the current chunk
        chunk_df = df[(df['Timestamp'] >= chunk_start) & (df['Timestamp'] < chunk_end)]

        # Skip if the chunk is empty
        if chunk_df.empty:
            continue

        # Generate the file name
        filename = f"{hour_time}_{chunk_start:%M}_log.csv"
        chunk_output_path = output_dir / filename

        # Save the chunk to a CSV file
        chunk_df.to_csv(chunk_output_path, index=False)
        print(f"Saved chunk to {chunk_output_path}")

