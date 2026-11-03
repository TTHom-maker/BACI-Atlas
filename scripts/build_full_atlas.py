#!/usr/bin/env python3
"""Build a complete, exact-row BACI HS22 (2022-2024) static atlas.
CEPII BACI V202601. Every original observation is preserved, including missing q.
Each flow is indexed once by exporter and once by importer for fast mobile queries.
"""
from __future__ import annotations
import csv
from contextlib import ExitStack
import gzip
import io
import json
import os
from pathlib import Path
import re
import shutil
import tempfile
import time
import urllib.request
import zipfile

VERSION = "202601"
SOURCE_URL = "https://www.cepii.fr/DATA_DOWNLOAD/baci/data/BACI_HS22_V202601.zip"
YEARS = (2022, 2023, 2024)
EXPECTED = {2022: 11007862, 2023: 11641922, 2024: 11250411}
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "_site"
DATA = OUT / "data"


def download(destination: Path) -> Path:
    existing = os.environ.get("BACI_SOURCE_ZIP", "").strip()
    if existing:
        p = Path(existing).resolve()
        if not zipfile.is_zipfile(p):
            raise ValueError(f"Invalid BACI_SOURCE_ZIP: {p}")
        print(f"Using local source {p}", flush=True)
        return p
    if destination.exists() and zipfile.is_zipfile(destination):
        return destination
    print(f"Downloading CEPII BACI HS22 {VERSION} from {SOURCE_URL}", flush=True)
    request = urllib.request.Request(SOURCE_URL, headers={"User-Agent": "BACI-Atlas/1.0 research"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=180) as response, destination.open("wb") as f:
                shutil.copyfileobj(response, f, length=1024*1024)
            if not zipfile.is_zipfile(destination):
                raise ValueError("Downloaded file is not a valid ZIP archive")
            print(f"Downloaded {destination.stat().st_size:,} bytes", flush=True)
            return destination
        except Exception as exc:
            destination.unlink(missing_ok=True)
            if attempt == 2:
                raise
            print(f"Download attempt {attempt + 1} failed: {exc}", flush=True)
            time.sleep(10 * (attempt + 1))
    raise RuntimeError("Download failed")


def source_member(z: zipfile.ZipFile, basename: str) -> str:
    names = [n for n in z.namelist() if Path(n).name == basename]
    if len(names) != 1:
        raise ValueError(f"Expected exactly one {basename}, found {len(names)}")
    return names[0]


def read_csv(z: zipfile.ZipFile, basename: str):
    with z.open(source_member(z, basename)) as raw:
        yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))


def write_catalog(z: zipfile.ZipFile) -> tuple[set[str], set[str]]:
    parts = []
    for i in range(1, 4):
        src = ROOT / "payload" / f"catalog-{i:02d}.js"
        txt = src.read_text(encoding="utf-8")
        m = re.search(r"\.push\((.*)\)\s*;?\s*$", txt, re.S)
        if not m:
            raise ValueError(f"Cannot decode {src}")
        parts.append(json.loads(m.group(1)))
    catalog = json.loads("".join(parts))
    products = {str(x["hs6"]).zfill(6) for x in catalog["products"]}
    countries = {str(int(x["id"])) for x in catalog["countries"]}
    assert len(products) == 5609, f"Expected 5609 products, found {len(products)}"
    assert len(countries) == 238, f"Expected 238 countries, found {len(countries)}"
    csv_products = {r["code"].zfill(6) for r in read_csv(z, f"product_codes_HS22_V{VERSION}.csv")}
    csv_countries = {str(int(r["country_code"])) for r in read_csv(z, f"country_codes_V{VERSION}.csv")}
    if products != csv_products or countries != csv_countries:
        raise ValueError("Catalog codes differ from CEPII source metadata")
    (OUT / "catalog.json").write_text(json.dumps(catalog, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return countries, products


def pack_year(z: zipfile.ZipFile, year: int, known_countries: set[str], known_products: set[str], temp: Path):
    out_year = DATA / str(year)
    temp_year = temp / str(year)
    # Append-only temporary CSVs preserve each observation. 2 orientations allow
    # querying a single country with one download rather than a world-sized scan.
    writers: dict[tuple[str, str], object] = {}
    counter = 0
    missing_q = 0
    checked_codes: set[str] = set()
    with ExitStack() as stack:
        member = source_member(z, f"BACI_HS22_Y{year}_V{VERSION}.csv")
        with z.open(member) as raw:
            reader = csv.reader(io.TextIOWrapper(raw, encoding="utf-8-sig", newline=""))
            header = next(reader)
            indices = {name.strip(): idx for idx, name in enumerate(header)}
            if set(indices) != {"t", "i", "j", "k", "v", "q"}:
                raise ValueError(f"Unexpected columns for {year}: {header}")
            for values in reader:
                if not values:
                    continue
                t = values[indices["t"]]
                exporter = str(int(values[indices["i"]]))
                importer = str(int(values[indices["j"]]))
                product = values[indices["k"]].zfill(6)
                v = values[indices["v"]]
                q = values[indices["q"]]
                if t != str(year) or exporter not in known_countries or importer not in known_countries:
                    raise ValueError(f"Invalid year/country code at row {counter+1} ({year})")
                if product not in known_products:
                    raise ValueError(f"Unknown product {product} in {year}")
                checked_codes.add(product)
                if not v:
                    raise ValueError(f"Missing trade value in {year}, row {counter+1}")
                if not q or q.upper() == "NA":
                    missing_q += 1
                # Country code and year are encoded losslessly by the shard path.
                # Preserve the original value and quantity fields as CSV text.
                for direction, c, other in (("export", exporter, importer), ("import", importer, exporter)):
                    key = (direction, c)
                    w = writers.get(key)
                    if w is None:
                        d = temp_year / direction
                        d.mkdir(parents=True, exist_ok=True)
                        w = stack.enter_context((d / f"{c}.csv").open("w", encoding="ascii", buffering=65536))
                        writers[key] = w
                    w.write(f"{other},{product},{v},{q}\n")
                counter += 1
                if counter % 2_000_000 == 0:
                    print(f"{year}: {counter:,} rows processed", flush=True)
    if not os.environ.get("BACI_ALLOW_TEST_ROWS"):
        if counter != EXPECTED[year]:
            raise ValueError(f"{year}: expected {EXPECTED[year]:,} records, found {counter:,}")
    total_compressed = 0
    shard_count = 0
    maximum_shard = 0
    for direction in ("export", "import"):
        d = out_year / direction
        d.mkdir(parents=True, exist_ok=True)
        for src in sorted((temp_year / direction).glob("*.csv")):
            target = d / f"{src.stem}.csv.gz"
            with src.open("rb") as inf, gzip.open(target, "wb", compresslevel=6) as outf:
                shutil.copyfileobj(inf, outf, length=1024 * 1024)
            length = target.stat().st_size
            total_compressed += length
            maximum_shard = max(maximum_shard, length)
            shard_count += 1
            src.unlink()
        print(f"{year} {direction}: {shard_count} cumulative shards; {total_compressed/1e6:.1f} MB", flush=True)
    shutil.rmtree(temp_year, ignore_errors=True)
    return {"year": year, "rows": counter, "missing_quantities": missing_q,
            "shards": shard_count, "compressed_bytes": total_compressed,
            "largest_shard_bytes": maximum_shard, "products_observed": len(checked_codes)}


def main():
    OUT.mkdir(exist_ok=True)
    DATA.mkdir(exist_ok=True)
    shutil.copyfile(ROOT / "full.html", OUT / "index.html")
    (OUT / "payload").mkdir(exist_ok=True)
    shutil.copyfile(ROOT / "payload" / "geo-01.js", OUT / "payload" / "geo-01.js")
    (OUT / ".nojekyll").touch()
    (OUT / "README.txt").write_text(
        "BACI Atlas HS22 2022-2024, release V202601. Source: CEPII BACI.\n"
        "Values in thousands of USD, quantities in metric tonnes. Missing q is not zero.\n"
        "Original observations preserved by (year, exporter, importer, product, value, quantity).\n"
        "An exporter and importer shard index duplicate each observation for fast searching.\n"
        "CEPII BACI licence: Etalab Open Licence 2.0; source: https://www.cepii.fr/\n",
        encoding="utf-8",
    )
    with tempfile.TemporaryDirectory(prefix="baci_") as td:
        p = download(Path(td) / "source.zip")
        with zipfile.ZipFile(p) as z:
            countries, products = write_catalog(z)
            print(f"Verified metadata: {len(countries)} countries, {len(products)} products", flush=True)
            manifest = {"source": "CEPII BACI HS22 V202601", "years": [], "countries": len(countries), "products": len(products), "version": VERSION}
            for year in YEARS:
                manifest["years"].append(pack_year(z, year, countries, products, Path(td) / "spool"))
    manifest["rows"] = sum(x["rows"] for x in manifest["years"])
    manifest["shards"] = sum(x["shards"] for x in manifest["years"])
    manifest["missing_quantities"] = sum(x["missing_quantities"] for x in manifest["years"])
    manifest["site_bytes"] = sum(p.stat().st_size for p in OUT.rglob("*") if p.is_file())
    if not os.environ.get("BACI_ALLOW_TEST_ROWS") and manifest["rows"] != 33900195:
        raise ValueError("Total BACI row count mismatch")
    if manifest["site_bytes"] > 950_000_000:
        raise ValueError("Site exceeds safe GitHub Pages deployment size")
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print("COMPLETE MANIFEST:", json.dumps(manifest), flush=True)


if __name__ == "__main__":
    main()
