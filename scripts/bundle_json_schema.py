import json
from pathlib import Path
from typing import Dict, Any
import re

SCHEMA_DIR = Path(__file__).parent.parent / "resources" / "schema" / "rule-merged"
OUTPUT_PATH = SCHEMA_DIR / "CORE-bundled.json"
BASE_SCHEMA_NAME = "CORE-base.json"


def load_schema(path: Path) -> Dict[str, Any]:
    """Load a JSON schema from a file path."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def find_refs(obj: Any) -> set:
    """Recursively find all $ref values in a schema object."""
    refs = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "$ref" and isinstance(v, str):
                refs.add(v)
            else:
                refs.update(find_refs(v))
    elif isinstance(obj, list):
        for item in obj:
            refs.update(find_refs(item))
    return refs


def rewrite_refs(obj: Any, ref_map: Dict[str, str]):
    """Recursively update $ref values using ref_map and fix base schema $defs refs."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "$ref" and isinstance(v, str):
                # Always rewrite refs like '#/$defs/CORE-base.json/$defs/VariableName' to '#/$defs/VariableName'
                pattern = rf"#\/\$defs\/{re.escape(BASE_SCHEMA_NAME)}\/\$defs\/([A-Za-z0-9_.-]+)"
                fixed = re.sub(pattern, r"#/$defs/\1", v)
                m = re.match(r"^(.*\.json)(#.*)?$", fixed)
                if m:
                    ref_file = m.group(1)
                    ref_fragment = m.group(2) or ""
                    if fixed in ref_map:
                        obj[k] = ref_map[fixed]
                    elif ref_file in ref_map:
                        obj[k] = (
                            f"#/$defs/{ref_file}{ref_fragment[1:] if ref_fragment.startswith('#/') else ''}"
                        )
                    else:
                        obj[k] = fixed
                else:
                    obj[k] = fixed
            else:
                rewrite_refs(v, ref_map)
    elif isinstance(obj, list):
        for item in obj:
            rewrite_refs(item, ref_map)


def bundle_schemas():
    """Bundle all referenced schemas into a single schema file."""
    schema_files = {f.name: f for f in SCHEMA_DIR.glob("*.json")}
    base_schema_path = schema_files[BASE_SCHEMA_NAME]
    base_schema = load_schema(base_schema_path)
    bundled = base_schema.copy()
    bundled["$defs"] = bundled.get("$defs", {}).copy()

    ref_map = {}
    unresolved = set(find_refs(bundled))
    while unresolved:
        ref = unresolved.pop()
        m = re.match(r"^([A-Za-z0-9_.-]+\.json)(#.*)?$", ref)
        if not m:
            continue
        ref_file = m.group(1)
        ref_fragment = m.group(2) or ""
        if ref_file not in schema_files:
            continue
        schema = load_schema(schema_files[ref_file])
        schema.pop("$id", None)
        schema.pop("$schema", None)
        defs = schema.pop("$defs", None)
        new_refs = find_refs(schema)
        if defs:
            for def_key, def_val in defs.items():
                rewrite_refs(def_val, ref_map)
                bundled["$defs"].setdefault(def_key, def_val)
            for def_val in defs.values():
                new_refs.update(find_refs(def_val))
        rewrite_refs(schema, ref_map)
        if ref_file != BASE_SCHEMA_NAME:
            bundled["$defs"][ref_file] = schema
        ref_map[ref] = (
            f"#/$defs/{ref_file}{ref_fragment[1:] if ref_fragment.startswith('#/') else ''}"
        )
        if ref_file not in ref_map:
            ref_map[ref_file] = f"#/$defs/{ref_file}"
        unresolved.update(r for r in new_refs if r not in ref_map)

    # Update all $ref values and fix base refs in one pass
    rewrite_refs(bundled, ref_map)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(bundled, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"Bundled schema written to: {OUTPUT_PATH}")


if __name__ == "__main__":
    bundle_schemas()
