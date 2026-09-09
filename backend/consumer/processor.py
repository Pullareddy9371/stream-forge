def get_speed_status(speed_kmph):
    """Classify truck speed."""

    if speed_kmph < 0:
        return "INVALID"

    if speed_kmph == 0:
        return "STOPPED"

    if speed_kmph <= 60:
        return "NORMAL"

    if speed_kmph <= 80:
        return "FAST"

    return "OVERSPEED"


def get_temperature_status(temperature):
    """Classify truck temperature."""

    if temperature < -20 or temperature > 60:
        return "CRITICAL"

    if temperature < 5:
        return "LOW"

    if temperature <= 40:
        return "NORMAL"

    if temperature <= 50:
        return "HIGH"

    return "CRITICAL"


def get_fuel_status(fuel_level):
    """Classify truck fuel level."""

    if fuel_level < 0 or fuel_level > 100:
        return "INVALID"

    if fuel_level <= 10:
        return "CRITICAL"

    if fuel_level <= 25:
        return "LOW"

    return "NORMAL"


def process_telemetry(data):
    """
    Process validated truck telemetry.

    Returns a dictionary containing
    the calculated vehicle statuses.
    """

    speed_status = get_speed_status(
        data["speed_kmph"]
    )

    temperature_status = get_temperature_status(
        data["temperature"]
    )

    fuel_status = get_fuel_status(
        data["fuel_level"]
    )

    # Determine overall vehicle status
    if (
        speed_status == "INVALID"
        or temperature_status == "CRITICAL"
        or fuel_status == "INVALID"
    ):
        overall_status = "CRITICAL"

    elif (
        speed_status == "OVERSPEED"
        or temperature_status == "HIGH"
        or fuel_status == "LOW"
    ):
        overall_status = "WARNING"

    else:
        overall_status = "NORMAL"

    return {
        "truck_id": data["truck_id"],
        "speed_status": speed_status,
        "temperature_status": temperature_status,
        "fuel_status": fuel_status,
        "overall_status": overall_status,
    }