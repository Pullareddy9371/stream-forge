from datetime import datetime


REQUIRED_FIELDS = {
    "truck_id",
    "timestamp",
    "latitude",
    "longitude",
    "speed_kmph",
    "temperature",
    "fuel_level",
}


def validate_telemetry(data):
    """Validate one truck telemetry event."""

    if not isinstance(data, dict):
        return False, "Telemetry must be a JSON object"

    missing_fields = REQUIRED_FIELDS - data.keys()

    if missing_fields:
        return False, f"Missing fields: {sorted(missing_fields)}"

    if not isinstance(data["truck_id"], str):
        return False, "truck_id must be a string"

    if not data["truck_id"].strip():
        return False, "truck_id cannot be empty"

    try:
        datetime.fromisoformat(
            data["timestamp"].replace("Z", "+00:00")
        )
    except (ValueError, AttributeError):
        return False, "Invalid timestamp"

    if not isinstance(data["latitude"], (int, float)):
        return False, "latitude must be numeric"

    if not -90 <= data["latitude"] <= 90:
        return False, "latitude must be between -90 and 90"

    if not isinstance(data["longitude"], (int, float)):
        return False, "longitude must be numeric"

    if not -180 <= data["longitude"] <= 180:
        return False, "longitude must be between -180 and 180"

    if not isinstance(data["speed_kmph"], (int, float)):
        return False, "speed_kmph must be numeric"

    if data["speed_kmph"] < 0:
        return False, "speed_kmph cannot be negative"

    if not isinstance(data["temperature"], (int, float)):
        return False, "temperature must be numeric"

    if not isinstance(data["fuel_level"], (int, float)):
        return False, "fuel_level must be numeric"

    if not 0 <= data["fuel_level"] <= 100:
        return False, "fuel_level must be between 0 and 100"

    return True, None