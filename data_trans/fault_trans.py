from pathlib import Path
import toml
import json
from collections import defaultdict
from datetime import datetime

# 读取 TOML 文件
def read_toml_to_dict(toml_file_path):
    with open(toml_file_path, 'r', encoding='utf-8') as file:
        return toml.load(file)
    

def convert_toml_to_json(toml_data):
    fault_dict = defaultdict(list)
    for chaos in toml_data.get("chaos_injection", []):
        timestamp = chaos["timestamp"]
        if isinstance(timestamp, datetime):  # 确保是 datetime 对象
            timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        hour = timestamp.split()[1].split(":")[0]  # 提取小时部分
        fault_entry = {
            "inject_time": timestamp,
            "inject_pod": chaos["service"],
            "inject_type": chaos["chaos_type"]
        }
        fault_dict[hour].append(fault_entry)
    return fault_dict



# 写入 JSON 文件
def write_json(data, json_file_path):
    with open(json_file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    root_base_dir = Path(r"E:\Project\Git\git\testset\ts-1024")
    output_base_dir = Path(r"E:\Project\Git\git\testset\nezha-ts-1024")

    toml_file_path = root_base_dir / "fault_injection.toml"
    json_file_path = output_base_dir / "2024-10-24-fault_list.json"

    # 读取 TOML 文件
    toml_data = read_toml_to_dict(toml_file_path)

    # 转换为 JSON 格式
    json_data = convert_toml_to_json(toml_data)

    # 写入 JSON 文件
    write_json(json_data, json_file_path)

    print(f"JSON 数据已成功写入到 {json_file_path}")
