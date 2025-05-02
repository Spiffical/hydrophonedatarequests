#!/usr/bin/env python
"""
expand_onc_openapi.py
─────────────────────
Fetch the Oceans 3.0 root spec, manually inline every external $ref, and
write one monolithic YAML / JSON.  Also supports splitting into 14 k-token
chunks for ChatGPT / Claude.

Pure-Python; only needs:  requests, pyyaml, tiktoken
    pip install requests pyyaml tiktoken
"""

from __future__ import annotations
import argparse, json, re, sys, yaml, requests, tiktoken
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs

LANDING  = "https://data.oceannetworks.ca/OpenAPI"
FALLBACK = "https://data.oceannetworks.ca/api/definition"

OUT_YAML = Path("onc_openapi_expanded.yaml")
OUT_JSON = Path("onc_openapi_expanded.json")
CHUNK_DIR = Path("onc_openapi_chunks")
MAX_TOK   = 14_000                        # safe for GPT-4-32k


# ─────────────────────────────────────────────────────────────────────────────
def root_spec_url() -> str:
    try:
        text = requests.get(LANDING, timeout=20).text
    except Exception as e:
        print(f"⚠ landing page unavailable ({e}); falling back.")
        return FALLBACK
    m = re.search(r'spec[-_]?url\s*=\s*["\']([^"\']+)', text, re.I)
    if m:
        return urljoin(LANDING, m.group(1))
    return FALLBACK


def fetch(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    if "yaml" in r.headers.get("content-type", "") or url.lower().endswith(".yaml"):
        return yaml.safe_load(r.text)
    return r.json()


# ─────────────────────────────────────────────────────────────────────────────
def bundle(root: dict, base_url: str) -> dict:
    """
    Replace every external $ref (only ever used by ONC under /paths and
    /components/schemas) with the real object.  Works around the malformed
    fragment (`#/paths/deviceCategories` instead of `#/paths/~1deviceCategories`)
    by simply copying *all* paths / schemas from the service-level file.
    """
    cache: dict[str, dict] = {}

    # helper ---------------------------------------------------------------
    def service_spec(ref_url: str) -> dict:
        srv_url = ref_url.split("#")[0]
        if srv_url not in cache:
            cache[srv_url] = fetch(srv_url)
        return cache[srv_url]

    # expand paths ---------------------------------------------------------
    new_paths = {}
    for placeholder, ref_obj in root["paths"].items():
        if "$ref" not in ref_obj:
            new_paths[placeholder] = ref_obj
            continue
        spec = service_spec(ref_obj["$ref"])
        new_paths.update(spec.get("paths", {}))   # copy *all* real paths
    root["paths"] = new_paths

    # expand component schemas (same bug pattern) --------------------------
    comps = root.setdefault("components", {}).setdefault("schemas", {})
    for key, ref in list(comps.items()):
        if isinstance(ref, dict) and "$ref" in ref:
            spec = service_spec(ref["$ref"])
            comps.update(spec.get("components", {}).get("schemas", {}))
            comps.pop(key, None)   # remove placeholder

    return root


# ─────────────────────────────────────────────────────────────────────────────
def write_chunks(text: str):
    enc = tiktoken.get_encoding("cl100k_base")
    CHUNK_DIR.mkdir(exist_ok=True)
    buf, idx = [], 0
    for line in text.splitlines(keepends=True):
        buf.append(line)
        if len(enc.encode("".join(buf))) > MAX_TOK:
            idx += 1
            (CHUNK_DIR / f"onc_openapi_{idx:02d}.txt").write_text("".join(buf))
            buf.clear()
    if buf:
        idx += 1
        (CHUNK_DIR / f"onc_openapi_{idx:02d}.txt").write_text("".join(buf))
    print(f"✓ split into {idx} chunk(s) in {CHUNK_DIR}/")


# ─────────────────────────────────────────────────────────────────────────────
def main(write_json: bool, split: bool):
    root_url = root_spec_url()
    print("root spec →", root_url)
    root = fetch(root_url)
    full = bundle(root, root_url)

    OUT_YAML.write_text(yaml.safe_dump(full, sort_keys=False))
    print(f"✓ saved {OUT_YAML}  ({OUT_YAML.stat().st_size/1024:.1f} kB)")

    if write_json:
        OUT_JSON.write_text(json.dumps(full, indent=2))
        print(f"✓ saved {OUT_JSON}")

    if split:
        write_chunks(OUT_YAML.read_text())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--write_json",  action="store_true",
                    help="also write onc_openapi_expanded.json")
    ap.add_argument("--split", action="store_true",
                    help="split YAML into ≈14 k-token text files")
    main(**vars(ap.parse_args()))
