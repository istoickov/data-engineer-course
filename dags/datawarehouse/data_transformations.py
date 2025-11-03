from datetime import timedelta, datetime


def parse_duration(duration_str):
    duration_str = duration_str.replace("P", "").replace("T", " ")

    components = ["D", "H", "M", "S"]
    values = {
        "D": 0,
        "H": 0,
        "M": 0,
        "S": 0,
    }

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component, 1)
            values[component] = int(value.strip()) if value.strip() else 0

    total_duration = timedelta(
        days=values["D"],
        hours=values["H"],
        minutes=values["M"],
        seconds=values["S"],
    )

    return total_duration


def transform_video_data(row):
    duration_td = parse_duration(row.content_details.duration)

    row["durations"] = (datetime.min + duration_td).time()
    row["video_type"] = "Shorts" if duration_td < timedelta(minutes=1) else "Normal"

    return row
