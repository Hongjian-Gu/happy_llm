# encoding=utf-8
import os
import json
import yaml
import logging
import jsonlines
from tqdm import tqdm
from typing import Union


def load_json_file(
        input_file: Union[str, list],
        force_jsonl: bool = False,
        encoding: str = 'utf-8',
        skip_error: bool = False,
        include_keys: list[str] = None) -> list[dict]:
    """
    load json files
    :param input_file: input json file, can be string (one file path) or list (list of one file paths)
    :param force_jsonl: True means all files will be loaded as .jsonl files,
                        False means .json/.jsonl will be loaded differently
    :param encoding: default utf-8
    :param skip_error: False means an error will be raised when parse json error, True means skip invalid json
    :return: list of json dict
    """
    if isinstance(input_file, str):
        input_file_list = [input_file]
    elif isinstance(input_file, list):
        input_file_list = input_file
    else:
        raise ValueError("input_file must be str or list")

    json_datas = []
    for input_file in input_file_list:
        if input_file.endswith(".json") and not force_jsonl:
            with open(input_file, "r", encoding=encoding) as f:
                content = f.read()
                try:
                    content = json.loads(content)
                    if isinstance(content, list):
                        if include_keys:
                            content = [{k: v for k, v in ele.items() if k in include_keys} for ele in content]
                        json_datas.extend(content)
                    else:
                        if include_keys:
                            content = {k: v for k, v in content.items() if k in include_keys}
                        json_datas.append(content)
                except Exception as e:
                    if skip_error:
                        logging.warning(f"error when parsing {input_file}, not a valid .json file. error msg: {e}")
                    else:
                        raise Exception(f"error when parsing {input_file}, not a valid .json file. error msg: {e}")
        elif input_file.endswith(".jsonl") or force_jsonl:
            with open(input_file, "r", encoding=encoding, errors="ignore") as f:
                for line_num, line in enumerate(f, start=1):
                    try:
                        line_json = json.loads(line)
                        if include_keys:
                            line_json = {k: v for k, v in line_json.items() if k in include_keys}
                        json_datas.append(line_json)
                    except Exception as e:
                        if skip_error:
                            logging.warning(f"error when parsing JSON at line {line_num} from {input_file}: {e}")
                            continue
                        else:
                            raise Exception(f"Error parsing JSON at line {line_num}: {e}, file: {input_file}")
        else:
            logging.warning(f"skip file with invalid tail: {input_file}")
    return json_datas


def dump_json_file(
        data_list: Union[list[dict], dict], file_path: str, indent=4,
        encoding: str = 'utf-8', ensure_ascii: bool = False,
        force_jsonl: bool = False, mode: str = "w", verbose: bool = False):
    if file_path.endswith(".jsonl") or force_jsonl:
        with jsonlines.open(file_path, mode) as writer:
            for item in tqdm(data_list, desc="dump json file", disable=not verbose):
                writer.write(item)
    elif file_path.endswith(".json"):
        with open(file_path, mode, encoding=encoding) as writer:
            writer.write(json.dumps(data_list, ensure_ascii=ensure_ascii, indent=indent))


def load_list(file_path, encoding="utf-8", filter_prefix="#"):
    results = []
    with open(file_path, "r", encoding=encoding) as f:
        for line in f.readlines():
            results.append(line.strip())
    results = [x for x in results if x and not x.startswith(filter_prefix)]
    return results


def dump_list(data_list, file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        for data in data_list:
            f.write(str(data) + "\n")


def count_lines_in_directory(directory, recursive=False, verbose=True):
    total_lines = 0

    if not recursive:
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):  # 只处理文件
                lines_in_file = count_lines_in_file(file_path)
                print(f"{filename}: {lines_in_file} lines")
                total_lines += lines_in_file
    else:
        for root, _, files in os.walk(directory):
            for filename in files:
                file_path = os.path.join(root, filename)
                lines_in_file = count_lines_in_file(file_path)
                print(f"{file_path}: {lines_in_file} lines")
                total_lines += lines_in_file
    if verbose:
        print(f"\nTotal lines in all files: {total_lines}")
    return total_lines


def count_lines_in_file(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        return sum(1 for _ in f)


def rename_jsonl_to_json(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)

        if os.path.isfile(file_path) and filename.endswith('.jsonl'):
            new_filename = filename[:-6] + '.json'
            new_file_path = os.path.join(directory, new_filename)

            os.rename(file_path, new_file_path)
            print(f"Renamed: {file_path} -> {new_file_path}")


def write_list_to_file(file_path, data_list):
    with open(file_path, "w", encoding="utf-8") as file:
        for item in data_list:
            file.write(f"{item}\n")


def load_yaml(file_path: str) -> dict:
    with open(file_path, mode='r', encoding='utf-8') as f:
        return yaml.safe_load(f)
