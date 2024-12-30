import os
import glob
from pathlib import Path
import pandas as pd
import time
from log_trans import modify_log_data
from trace_trans import calculate_end_time_unix_nano, process_trace_data
from metric import (
    split_and_save_by_service,
    calculate_weighted_sum,
    add_latency_columns,
    process_folder,
    process_files_in_folder,
)
from merge import (
    process_data_merge,
    merge_files,
    process_csv_file_merge,
    trans_nezha,
)

def log_runner(input_dir, output_dir):
    # print(f"Processing log data from {input_dir} to {output_dir}...")
    modify_log_data(input_dir, output_dir)


def trace_runner(input_dir, output_dir, traceid_output_dir):
    output_file = output_dir / "trace.csv"
    calculate_end_time_unix_nano(input_dir, output_file)
    process_trace_data(output_file, output_dir, traceid_output_dir)


def metric_runner(input_dir, output_dir):

    df =pd.read_csv(input_dir)
    os.makedirs(output_dir, exist_ok=True)
    split_and_save_by_service(df, output_dir)
    process_files_in_folder(output_dir)

    
    # Define weights for latency calculations
    weights = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 10]

    for filename in os.listdir(output_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(output_dir, filename)
            df = pd.read_csv(file_path)

            df['P90'] = df['Latency_P90'].apply(lambda x: calculate_weighted_sum(x, weights)) / 10
            df['P95'] = df['Latency_P95'].apply(lambda x: calculate_weighted_sum(x, weights)) / 5
            df['P99'] = df['Latency_P99'].apply(lambda x: calculate_weighted_sum(x, weights))

            df = add_latency_columns(df)

            # 只保留需要的列
            df = df[['TimeUnix', 'client_P90', 'client_P95', 'client_P99', 'server_P90', 'server_P95', 'server_P99']]

            # 保存修改后的DataFrame，覆盖原文件
            df.to_csv(file_path, index=False)
            print(f"Updated and saved {file_path}")
    process_folder(output_dir, output_dir)


def merge_runner(input_dir, output_dir, merged_output_dir, output_folder, metric_output_path):
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(output_folder, f'{filename}')
            try:
                df = pd.read_csv(input_file)
                result = process_data_merge(df)
                result.to_csv(output_file, index=False)

                print(f"结果已保存到 {output_file}")
            except KeyError as e:
                print(f"跳过文件 {filename}，错误：{e}")
            except Exception as e:
                print(f"处理文件 {filename} 时发生错误：{e}")
    merge_files(output_dir, output_folder, merged_output_dir)
    trans_nezha(merged_output_dir, metric_output_path)


def process_metric(metric_path, metric_output_path):
    df = pd.read_csv(metric_path)
    df.rename(columns={'TimeUnix': 'Time'}, inplace=True)
    df['Time'] = pd.to_datetime(df['Time'], errors='coerce')
    if df['Time'].isna().any():
        df.dropna(subset=['Time'], inplace=True)
    df['TimeStamp'] = df['Time'].apply(lambda x: int(time.mktime(x.timetuple())))
    pod_name = os.path.splitext(os.path.basename(metric_path))[0]
    df['PodName'] = pod_name
    first_columns = ['Time', 'TimeStamp', 'PodName']
    other_columns = [col for col in df.columns if col not in first_columns]
    df = df[first_columns + other_columns]
    output_file_path = os.path.join(metric_output_path, os.path.basename(metric_path))
    df.to_csv(output_file_path, index=False)


def main():
    root_base_dir = Path(r"E:\Project\Git\git\testset\ts-1024")
    output_base_dir = Path(r"E:\Project\Git\git\testset\nezha-ts-normal")
    # output_base_dir = Path(r"E:\Project\Git\git\testset\nezha-ts-1024")

    # Ensure output directories exist
    output_base_dir.mkdir(parents=True, exist_ok=True)
    metric_output_path = output_base_dir / 'metric'
    metric_output_path.mkdir(parents=True, exist_ok=True)

    log_output_path = output_base_dir / "log"
    log_output_path.mkdir(parents=True, exist_ok=True)

    trace_output_path = output_base_dir / "trace"
    trace_output_path.mkdir(parents=True, exist_ok=True)

    traceid_output_path = output_base_dir / "traceid"
    traceid_output_path.mkdir(parents=True, exist_ok=True)

    for prefix_dir in root_base_dir.iterdir():
        if prefix_dir.is_dir():
            # Process only abnormal data
            # abnormal_dir = prefix_dir / "abnormal"
            abnormal_dir = prefix_dir / "normal"

            subdir_name = prefix_dir.name  # 故障名
            sub_dir = output_base_dir / subdir_name
            sub_dir.mkdir(parents=True, exist_ok=True)
            sub_request_dir = sub_dir / "request"
            sub_request_dir.mkdir(parents=True, exist_ok=True)
            sub_metric_dir = sub_dir / "metric"
            sub_metric_dir.mkdir(parents=True, exist_ok=True)
            input_processed_metrics_dir = abnormal_dir / 'processed_metrics'
            output_processed_metrics_dir = abnormal_dir / 'nezha_processed_metrics'
            output_processed_metrics_dir.mkdir(parents=True, exist_ok=True)
            
            log_runner(abnormal_dir / "logs.csv", log_output_path)
            # trace_runner(abnormal_dir / "traces.csv", trace_output_path, traceid_output_path)

            # metric_runner(abnormal_dir / "request_metrics.csv", sub_request_dir)
            # merge_runner(input_processed_metrics_dir, sub_request_dir, sub_metric_dir, output_processed_metrics_dir, metric_output_path)
            
            # for metric_path in glob.glob(os.path.join(metric_output_path, '*.csv')):
            #     process_metric(metric_path, metric_output_path)

    print("All tasks completed successfully!")


if __name__ == "__main__":
    main()
