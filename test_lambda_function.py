from datetime import datetime, date
import zoneinfo

import lambda_function as lf

UK = zoneinfo.ZoneInfo('Europe/London')


# --- create_time_from_hour_minute ---

def test_create_time_from_hour_minute():
    d = date(2025, 6, 15)
    result = lf.create_time_from_hour_minute(14, 30, d)
    assert result == datetime(2025, 6, 15, 14, 30, tzinfo=UK)


def test_create_time_from_hour_minute_midnight():
    d = date(2025, 1, 1)
    result = lf.create_time_from_hour_minute(0, 0, d)
    assert result == datetime(2025, 1, 1, 0, 0, tzinfo=UK)


# --- is_in_off_peak / is_in_peak ---

def test_is_in_off_peak_before_peak_start():
    t = datetime(2025, 6, 15, 3, 0, tzinfo=UK)
    assert lf.is_in_off_peak(t) is True
    assert lf.is_in_peak(t) is False


def test_is_in_off_peak_at_peak_boundary():
    """05:30 is the start of peak — should be peak."""
    t = datetime(2025, 6, 15, 5, 30, tzinfo=UK)
    assert lf.is_in_off_peak(t) is False
    assert lf.is_in_peak(t) is True


def test_is_in_off_peak_just_before_peak():
    t = datetime(2025, 6, 15, 5, 29, tzinfo=UK)
    assert lf.is_in_off_peak(t) is True


def test_is_in_off_peak_at_off_peak_boundary():
    """23:30 is the start of off-peak — should be off-peak."""
    t = datetime(2025, 6, 15, 23, 30, tzinfo=UK)
    assert lf.is_in_off_peak(t) is True
    assert lf.is_in_peak(t) is False


def test_is_in_off_peak_just_before_off_peak():
    t = datetime(2025, 6, 15, 23, 29, tzinfo=UK)
    assert lf.is_in_off_peak(t) is False


def test_is_in_peak_midday():
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    assert lf.is_in_peak(t) is True


# --- ev_is_plugged_in ---

def test_ev_is_plugged_in_with_schedule():
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    schedule = [{'start': '2025-06-15T23:30:00+01:00', 'end': '2025-06-16T05:30:00+01:00'}]
    assert lf.ev_is_plugged_in(t, schedule) is True


def test_ev_is_not_plugged_in_empty_schedule():
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    assert lf.ev_is_plugged_in(t, []) is False


def test_ev_is_not_plugged_in_none_schedule():
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    assert lf.ev_is_plugged_in(t, None) is False


# --- get_battery_percentage_for_consumption ---

def test_get_battery_percentage_for_consumption():
    # 4.176 kWh usable battery (5.22 * 0.80)
    kwh = lf.GIVENERGY_USABLE_BATTERY_SIZE_KWH
    assert lf.get_battery_percentage_for_consumption(kwh) == 100.0


def test_get_battery_percentage_for_consumption_half():
    kwh = lf.GIVENERGY_USABLE_BATTERY_SIZE_KWH / 2
    assert abs(lf.get_battery_percentage_for_consumption(kwh) - 50.0) < 0.01


def test_get_battery_percentage_for_consumption_zero():
    assert lf.get_battery_percentage_for_consumption(0) == 0.0


# --- get_minutes_needed_to_export_battery_at_full_power ---

def test_get_minutes_needed_to_export_full_battery():
    max_export = lf.get_max_amount_to_export_from_battery()  # 96%
    minutes = lf.get_minutes_needed_to_export_battery_at_full_power(max_export)
    # 96 * (5.22 * 0.80 / 2.6 / 100) * 60 ≈ 92.4 minutes
    expected = max_export * lf.GIVENERGY_BATTERY_DISCHARGE_MINUTES_PER_PERCENT
    assert abs(minutes - expected) < 0.01


def test_get_minutes_needed_to_export_zero():
    assert lf.get_minutes_needed_to_export_battery_at_full_power(0) == 0
