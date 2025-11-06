import os
import argparse
import json


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_file", help="USDM OpenAPI JSON file")
    args = parser.parse_args()
    return args


args = parse_arguments()

filename = os.path.split(args.input_file)[-1]

outfname = "".join(filename.split(".")[0]) + "_schemas"

with open(args.input_file) as f:
    openapi = json.load(f)

jschema = {"$defs": {}}


def replace_deep(data, a, b):
    if isinstance(data, str):
        return data.replace(a, b)
    elif isinstance(data, dict):
        return {k: replace_deep(v, a, b) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_deep(v, a, b) for v in data]
    else:
        # nothing to do?
        return data


for sn, sd in openapi["components"]["schemas"].items():
    if sn == "Wrapper-Input":
        for k, v in sd.items():
            jschema[k] = replace_deep(
                replace_deep(v, "components/schemas", "$defs"), "-Input", ""
            )
    elif not sn.endswith("-Output"):
        jschema["$defs"][sn.replace("-Input", "")] = replace_deep(
            replace_deep(sd, "components/schemas", "$defs"), "-Input", ""
        )

for v in jschema["$defs"].values():
    v.update({"additionalProperties": False})
    for pn, pd in v.get("properties", {}).items():
        if pn in v.get("required", []) and pd.get("type", "") == "array":
            pd.update({"minItems": 1})

with open(
    os.path.join("".join(os.path.split(args.input_file)[0:-1]), outfname + ".json"),
    "w",
    encoding="utf-8",
) as f:
    json.dump(jschema, f, ensure_ascii=False, indent=4)
