import argparse
import os
import json
import pickle


def parse_arguments():
    parser = argparse.ArgumentParser(description="Convert between JSON and PKL files.")
    parser.add_argument(
        "-i", "--input_file", required=True, help="Input file (.json or .pkl)"
    )
    args = parser.parse_args()
    return args


def json_to_pkl(json_path, pkl_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    with open(pkl_path, "wb") as f:
        pickle.dump(data, f)
    print(f"Converted {json_path} to {pkl_path}")


def pkl_to_json(pkl_path, json_path):
    with open(pkl_path, "rb") as f:
        data = pickle.load(f)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Converted {pkl_path} to {json_path}")


def main():
    args = parse_arguments()
    input_file = args.input_file
    base, ext = os.path.splitext(input_file)
    if ext.lower() == ".json":
        out_file = base + ".pkl"
        json_to_pkl(input_file, out_file)
    elif ext.lower() == ".pkl":
        out_file = base + ".json"
        pkl_to_json(input_file, out_file)
    else:
        print("Unsupported file extension. Please provide a .json or .pkl file.")


if __name__ == "__main__":
    main()
