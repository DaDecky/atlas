#!/usr/bin/env python3
"""Reproduce the development reference from a hash-pinned geoBoundaries file.

The 2017 geography has 34 provinces. It is a documented fallback until the
assistants supply their authoritative reference table, not a current boundary map.
"""
import hashlib
import argparse
import json
from pathlib import Path
import urllib.request

URL = "https://github.com/wmgeolab/geoBoundaries/raw/9469f09/releaseData/gbOpen/IDN/ADM1/geoBoundaries-IDN-ADM1_simplified.geojson"
SHA256 = "5a6be3d1484166132751fe535dd164e1491e657f2d38dced177f067a7bc00d8f"


def ring_centroid(ring):
    twice_area = x_sum = y_sum = 0.0
    for a, b in zip(ring, ring[1:]):
        cross = a[0] * b[1] - b[0] * a[1]
        twice_area += cross
        x_sum += (a[0] + b[0]) * cross
        y_sum += (a[1] + b[1]) * cross
    if abs(twice_area) < 1e-15:
        return 0.0, 0.0, 0.0
    return abs(twice_area / 2), x_sum / (3 * twice_area), y_sum / (3 * twice_area)


def centroid(geometry):
    polygons = [geometry["coordinates"]] if geometry["type"] == "Polygon" else geometry["coordinates"]
    area = lon_sum = lat_sum = 0.0
    for polygon in polygons:
        for index, ring in enumerate(polygon):
            weight, lon, lat = ring_centroid(ring)
            weight *= 1 if index == 0 else -1
            area += weight
            lon_sum += lon * weight
            lat_sum += lat * weight
    if area <= 0:
        raise ValueError("invalid province polygon")
    return round(lat_sum / area, 8), round(lon_sum / area, 8)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, help="optional previously downloaded source; checksum still verified")
    args = parser.parse_args()
    if args.input:
        raw = args.input.read_bytes()
    else:
        with urllib.request.urlopen(URL, timeout=60) as response:
            raw = response.read()
    if hashlib.sha256(raw).hexdigest() != SHA256:
        raise ValueError("reference source checksum changed")
    rows = []
    for feature in json.loads(raw)["features"]:
        lat, lon = centroid(feature["geometry"])
        rows.append({"name": feature["properties"]["shapeName"], "latitude": lat, "longitude": lon})
    rows.sort(key=lambda row: row["name"])
    data = {"version": "geoboundaries-IDN-ADM1-65028918-2017-planar-centroids", "source": URL, "source_sha256": SHA256, "license": "ODbL-1.0", "attribution": "OpenStreetMap contributors, Wambacher; distributed by geoBoundaries", "provinces": rows}
    output = Path(__file__).resolve().parents[1] / "services/bnpb/aggregator/internal/satellite/reference/provinces.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("Wrote", len(rows), "versioned development reference centroids")


if __name__ == "__main__":
    main()
