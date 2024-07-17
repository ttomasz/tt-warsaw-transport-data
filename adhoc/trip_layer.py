import json
# from collections import defaultdict
from pathlib import Path
import os


this_file_path = Path(__file__)
data_file_path = this_file_path.parent / "data.json"

if not data_file_path.is_file():
    raise FileNotFoundError(f"File: {data_file_path} not found.")


def get_speed_category(x: float) -> int:
    if x < 5.0:
        return 0
    if 5.0 <= x < 15.0:
        return 1
    if 15.0 <= x < 30.0:
        return 2
    if 30.0 <= x < 50.0:
        return 3
    if x >= 50.0:
        return 4
    else:
        return -1


def get_data():
    last_line = ""
    last_time_bucket = ""
    last_brigade = ""
    last_vehicle_number = ""
    last_speed_cat = -2
    data = {
        "type": "Feature",
        "properties": {},
        "geometry": {
            "type": "LineString",
            "coordinates": [],
        },
    }
    for idx, row in enumerate(data_file_path.open("r", encoding="utf-8")):
        doc = json.loads(row)
        line = doc["line"]
        time_bucket = doc["time_bucket_15min"]
        brigade = doc["brigade"]
        vehicle_number = doc["vehicle_number"]
        coord = doc["coord"]
        speed_kmh = doc["speed_kmh"]
        speed_cat = get_speed_category(speed_kmh)
        if idx == 0:
            data = {
                "type": "Feature",
                "properties": {
                    "line": line,
                    "speed_cat": speed_cat,
                    "time_bucket": time_bucket,
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [coord],
                },
            }
        elif any([
            line != last_line,
            time_bucket != last_time_bucket,
            brigade != last_brigade,
            vehicle_number != last_vehicle_number,
            speed_cat != last_speed_cat,
        ]):
            yield json.dumps(data)
            data = {
                "type": "Feature",
                "properties": {
                    "line": line,
                    "speed_cat": speed_cat,
                    "time_bucket": time_bucket,
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [coord],
                },
            }
        else:
            data["geometry"]["coordinates"].append(coord)
        last_line = line
        last_time_bucket = time_bucket
        last_brigade = brigade
        last_vehicle_number = vehicle_number
        last_speed_cat = speed_cat
    if data:
        return json.dumps(data)

# for idex, d in enumerate(get_data()):
#     print(d[:50])
#     # d = json.loads(d)
#     # print(idex, d["properties"]["line"], len(d["geometry"]["coordinates"]))
#     if idex >= 0:
#         break

header = [
    "{\n",
    '"type": "FeatureCollection",\n',
    '"features": [\n',
]

NUMBER_OF_FILES = 18

output_file_paths = {
    i: this_file_path.parent / f"trips{i}.geojson"
    for i in range(NUMBER_OF_FILES)
}
output_file_handles = {
    k: v.open("w", encoding="utf-8")
    for k, v in output_file_paths.items()
}
try:
    for fp in output_file_handles.values():
        fp.writelines(header)
    for idx, row in enumerate(get_data()):
        output_file_handles[idx % NUMBER_OF_FILES].write(row + ",\n")
    for fp in output_file_handles.values():
        fp.flush()
        fp.seek(fp.tell()-2)
    for fp in output_file_handles.values():
        fp.write("\n]\n}\n")
finally:
    for fp in output_file_handles.values():
        fp.close()
