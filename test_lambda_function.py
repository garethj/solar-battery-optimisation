from datetime import datetime, date, timedelta, timezone
from unittest.mock import patch, MagicMock
import zoneinfo

import pytest
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


# --- get_battery_percent_needed_for_consumption (variance fix) ---

def test_consumption_variance_applies_10_percent():
    """The variance should multiply consumption by 1.1x, not 2.0x."""
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    off_peak = datetime(2025, 6, 15, 23, 30, tzinfo=UK)
    solar_forecast = []  # no solar

    with patch.object(lf, 'predict_consumption', return_value=1.0), \
         patch.object(lf, 'get_remaining_solar_generation_for_today', return_value=0.0):
        result = lf.get_battery_percent_needed_for_consumption(t, solar_forecast, off_peak)

    expected_kwh = 1.0 * 1.1  # 10% variance
    expected_percent = (expected_kwh / lf.GIVENERGY_USABLE_BATTERY_SIZE_KWH) * 100
    assert abs(result - expected_percent) < 0.01


# --- get_battery_soc (unbound variable fix) ---

def _mock_response(status_code, json_data=None, text='error'):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    if json_data is not None:
        resp.json.return_value = json_data
    return resp


def test_get_battery_soc_success():
    resp = _mock_response(200, {'data': {'battery': {'percent': 75}}})
    with patch.object(lf.requests, 'get', return_value=resp):
        assert lf.get_battery_soc() == 75


def test_get_battery_soc_failure_raises():
    resp = _mock_response(500)
    with patch.object(lf.requests, 'get', return_value=resp):
        with pytest.raises(RuntimeError, match='Failed to get battery status'):
            lf.get_battery_soc()


# --- get_battery_settings (unbound variable fix) ---

def test_get_battery_settings_success():
    resp = _mock_response(200, {'data': {'some': 'settings'}})
    with patch.object(lf.requests, 'get', return_value=resp):
        assert lf.get_battery_settings() == {'some': 'settings'}


def test_get_battery_settings_failure_raises():
    resp = _mock_response(500)
    with patch.object(lf.requests, 'get', return_value=resp):
        with pytest.raises(RuntimeError, match='Failed to get current battery presets'):
            lf.get_battery_settings()


# --- get_battery_export_settings (unbound variable fix) ---

def test_get_battery_export_settings_success():
    resp = _mock_response(200, {'data': {'enabled': True, 'slots': []}})
    with patch.object(lf.requests, 'get', return_value=resp):
        assert lf.get_battery_export_settings() == {'enabled': True, 'slots': []}


def test_get_battery_export_settings_failure_raises():
    resp = _mock_response(500)
    with patch.object(lf.requests, 'get', return_value=resp):
        with pytest.raises(RuntimeError, match='Failed to get current battery export settings'):
            lf.get_battery_export_settings()


# --- get_current_ev_charging_slot_end_time (unbound variable fix) ---

def test_get_current_ev_charging_slot_end_time_no_current_slot():
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    schedule = [{'start': '2025-06-15T23:30:00+01:00', 'end': '2025-06-16T05:30:00+01:00'}]
    assert lf.get_current_ev_charging_slot_end_time(t, schedule) is None


def test_get_current_ev_charging_slot_end_time_in_slot():
    t = datetime(2025, 6, 16, 1, 0, tzinfo=UK)
    schedule = [{'start': '2025-06-15T23:30:00+01:00', 'end': '2025-06-16T05:30:00+01:00'}]
    result = lf.get_current_ev_charging_slot_end_time(t, schedule)
    assert result == datetime(2025, 6, 16, 5, 30, tzinfo=UK)


# --- export_settings_need_updating (renamed from battery_export_is_already_set_correctly) ---

def test_export_settings_need_updating_when_disabled():
    settings = {'enabled': False, 'slots': [{'end_time': '23:30'}]}
    desired_end = datetime(2025, 6, 15, 23, 30, tzinfo=UK)
    with patch.object(lf, 'get_battery_export_settings', return_value=settings):
        assert lf.export_settings_need_updating(desired_end) is True


def test_export_settings_need_updating_when_end_time_differs():
    settings = {'enabled': True, 'slots': [{'end_time': '22:00'}]}
    desired_end = datetime(2025, 6, 15, 23, 30, tzinfo=UK)
    with patch.object(lf, 'get_battery_export_settings', return_value=settings):
        assert lf.export_settings_need_updating(desired_end) is True


def test_export_settings_no_update_needed():
    settings = {'enabled': True, 'slots': [{'end_time': '23:30'}]}
    desired_end = datetime(2025, 6, 15, 23, 30, tzinfo=UK)
    with patch.object(lf, 'get_battery_export_settings', return_value=settings):
        assert lf.export_settings_need_updating(desired_end) is False


# --- should_update_solar_forecast (S3 error handling) ---

def test_should_update_solar_forecast_s3_error_returns_true():
    """If the S3 file doesn't exist, should return True to trigger a fresh fetch."""
    t = datetime(2025, 6, 15, 10, 0, tzinfo=UK)  # peak, before 13:00
    lf.s3_client.head_object.side_effect = Exception('NoSuchKey')
    assert lf.should_update_solar_forecast(t) is True
    lf.s3_client.head_object.side_effect = None


def test_should_update_solar_forecast_recent_file_returns_false():
    """If the file was modified recently, no update needed."""
    t = datetime(2025, 6, 15, 10, 0, tzinfo=UK)
    lf.s3_client.head_object.return_value = {
        'LastModified': (t - timedelta(minutes=10)).astimezone(timezone.utc)
    }
    assert lf.should_update_solar_forecast(t) is False


def test_should_update_solar_forecast_old_file_returns_true():
    """If the file was modified long ago, should update."""
    t = datetime(2025, 6, 15, 10, 0, tzinfo=UK)
    lf.s3_client.head_object.return_value = {
        'LastModified': (t - timedelta(minutes=60)).astimezone(timezone.utc)
    }
    assert lf.should_update_solar_forecast(t) is True


def test_should_update_solar_forecast_off_peak_returns_false():
    """During off-peak, never update."""
    t = datetime(2025, 6, 15, 3, 0, tzinfo=UK)
    assert lf.should_update_solar_forecast(t) is False


# --- predict_consumption (7-day averaging) ---

def _make_consumption_data(base_date, hour, minute, consumption_wh, days_ago):
    """Create a consumption data point for a given number of days ago."""
    dt = datetime(base_date.year, base_date.month, base_date.day, hour, minute, tzinfo=UK) - timedelta(days=days_ago)
    return {
        'time': dt.isoformat(),
        'today': {'consumption': consumption_wh}
    }


def test_predict_consumption_averages_multiple_days():
    start = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 23, 30, tzinfo=UK)
    # Create data for 3 days: 1000Wh, 2000Wh, 3000Wh at 15:00
    data = [
        _make_consumption_data(date(2025, 6, 15), 15, 0, 1000, days_ago=1),
        _make_consumption_data(date(2025, 6, 15), 15, 0, 2000, days_ago=2),
        _make_consumption_data(date(2025, 6, 15), 15, 0, 3000, days_ago=3),
    ]
    with patch.object(lf, 'get_recent_consumption', return_value=data):
        result = lf.predict_consumption(start, end)
    # Average: (1.0 + 2.0 + 3.0) / 3 = 2.0 kWh
    assert abs(result - 2.0) < 0.01


def test_predict_consumption_excludes_zero_data_days():
    start = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 23, 30, tzinfo=UK)
    # Only 1 day has data in the time window
    data = [
        _make_consumption_data(date(2025, 6, 15), 15, 0, 1500, days_ago=1),
    ]
    with patch.object(lf, 'get_recent_consumption', return_value=data):
        result = lf.predict_consumption(start, end)
    # Only 1 day with data: 1.5 kWh
    assert abs(result - 1.5) < 0.01


def test_predict_consumption_no_data_returns_zero():
    start = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 23, 30, tzinfo=UK)
    with patch.object(lf, 'get_recent_consumption', return_value=[]):
        result = lf.predict_consumption(start, end)
    assert result == 0
