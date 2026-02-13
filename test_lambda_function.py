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


# --- ensure_time_is_not_now ---

def test_ensure_time_is_not_now_far_from_now():
    """Time far from now should be returned unchanged."""
    far_time = datetime(2099, 1, 1, 12, 0, tzinfo=UK)
    result = lf.ensure_time_is_not_now(far_time)
    assert result == far_time


# --- get_remaining_solar_generation_for_today ---

def test_get_remaining_solar_generation_sums_today_only():
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    forecast = [
        {'period_end': '2025-06-15T13:00:00+01:00', 'pv_estimate': 1.5},
        {'period_end': '2025-06-15T13:30:00+01:00', 'pv_estimate': 2.0},
        {'period_end': '2025-06-16T13:00:00+01:00', 'pv_estimate': 3.0},  # tomorrow
        {'period_end': '2025-06-15T11:00:00+01:00', 'pv_estimate': 1.0},  # before start_time
    ]
    result = lf.get_remaining_solar_generation_for_today(t, forecast)
    assert abs(result - 3.5) < 0.01  # 1.5 + 2.0


def test_get_remaining_solar_generation_empty_forecast():
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    assert lf.get_remaining_solar_generation_for_today(t, []) == 0


def test_get_remaining_solar_generation_pessimistic():
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    forecast = [
        {'period_end': '2025-06-15T13:00:00+01:00', 'pv_estimate': 2.0, 'pv_estimate10': 0.5},
    ]
    result = lf.get_remaining_solar_generation_for_today(t, forecast, lf.SOLCAST_OPTIMISM_PESSIMISTIC)
    assert abs(result - 0.5) < 0.01


# --- get_solar_generation_kw_time ---

def _make_forecast(hour, minute, kw, day=15):
    """Helper: create a forecast period ending at the given hour:minute on June day."""
    return {
        'period_end': f'2025-06-{day:02d}T{hour:02d}:{minute:02d}:00+01:00',
        'pv_estimate': kw,
    }


def test_get_solar_generation_kw_time_finds_earliest_start():
    t = datetime(2025, 6, 15, 6, 0, tzinfo=UK)
    forecast = [
        _make_forecast(8, 0, 0.5),
        _make_forecast(9, 0, 2.0),
        _make_forecast(10, 0, 3.5),
        _make_forecast(11, 0, 3.0),
    ]
    result = lf.get_solar_generation_kw_time(t, forecast, 2.0, want_generation_end_time=False)
    # Earliest period where generation >= 2.0 kW, using start time (period_end - 30min)
    assert result == datetime(2025, 6, 15, 8, 30, tzinfo=UK)


def test_get_solar_generation_kw_time_finds_latest_end():
    t = datetime(2025, 6, 15, 6, 0, tzinfo=UK)
    forecast = [
        _make_forecast(9, 0, 2.0),
        _make_forecast(10, 0, 3.5),
        _make_forecast(11, 0, 2.5),
        _make_forecast(12, 0, 1.0),
    ]
    result = lf.get_solar_generation_kw_time(t, forecast, 2.0, want_generation_end_time=True)
    # Latest period where generation >= 2.0 kW
    assert result == datetime(2025, 6, 15, 11, 0, tzinfo=UK)


def test_get_solar_generation_kw_time_no_match_returns_none():
    t = datetime(2025, 6, 15, 6, 0, tzinfo=UK)
    forecast = [
        _make_forecast(9, 0, 0.5),
        _make_forecast(10, 0, 0.8),
    ]
    result = lf.get_solar_generation_kw_time(t, forecast, 5.0, want_generation_end_time=False)
    assert result is None


def test_get_solar_generation_kw_time_peak_fallback():
    """When generation never reaches target, fall back to max if want_peak_generation."""
    t = datetime(2025, 6, 15, 8, 0, tzinfo=UK)
    forecast = [
        _make_forecast(9, 0, 0.5),
        _make_forecast(10, 0, 2.0),
        _make_forecast(11, 0, 1.0),
    ]
    result = lf.get_solar_generation_kw_time(
        t, forecast, 5.0, want_generation_end_time=False, want_peak_generation=True
    )
    # Max generation is 2.0 at 09:30 (start of period ending 10:00)
    assert result == datetime(2025, 6, 15, 9, 30, tzinfo=UK)


# --- get_solar_generation_peak_start ---

def test_get_solar_generation_peak_start_sunny_day():
    t = datetime(2025, 6, 15, 6, 0, tzinfo=UK)
    forecast = [
        _make_forecast(8, 0, 0.5),
        _make_forecast(9, 0, 1.5),
        _make_forecast(10, 0, 3.5),  # >= SOLAR_GENERATION_PEAK_FORECAST_KW (~3.496)
        _make_forecast(11, 0, 3.6),
    ]
    result = lf.get_solar_generation_peak_start(t, forecast)
    assert result == datetime(2025, 6, 15, 9, 30, tzinfo=UK)


# --- get_minutes_needed_to_export_battery (with solar forecast) ---

def test_get_minutes_needed_to_export_with_solar_reduces_rate():
    t = datetime(2025, 6, 15, 8, 0, tzinfo=UK)
    export_end = datetime(2025, 6, 15, 11, 0, tzinfo=UK)
    # Solar above SOLAR_GENERATION_EXPORT_PEAK_KW (1.08) reduces discharge
    forecast = [
        _make_forecast(9, 0, 0.5),    # no reduction
        _make_forecast(9, 30, 1.5),   # exceeds 1.08, reduces discharge
        _make_forecast(10, 0, 2.0),   # exceeds 1.08, reduces more
        _make_forecast(10, 30, 0.8),  # no reduction
        _make_forecast(11, 0, 0.3),   # no reduction
    ]
    # Small amount to export, so solar impact is visible
    result = lf.get_minutes_needed_to_export_battery(t, 10, export_end_time=export_end, solar_forecast=forecast)
    full_power_mins = lf.get_minutes_needed_to_export_battery_at_full_power(10)
    # With solar slowing discharge, total time should be >= full power time
    assert result >= full_power_mins


def test_get_minutes_needed_to_export_without_solar():
    t = datetime(2025, 6, 15, 8, 0, tzinfo=UK)
    result = lf.get_minutes_needed_to_export_battery(t, 50)
    expected = lf.get_minutes_needed_to_export_battery_at_full_power(50)
    assert result == expected


# --- get_minutes_left_to_export_battery ---

def test_get_minutes_left_to_export_battery():
    start = datetime(2025, 6, 15, 10, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 11, 30, tzinfo=UK)
    assert lf.get_minutes_left_to_export_battery(start, end) == 90


# --- get_ev_charging_schedule ---

def test_get_ev_charging_schedule_success():
    auth_resp = _mock_response(200, {'data': {'obtainKrakenToken': {'token': 'tok123'}}})
    schedule_resp = _mock_response(200, {'data': {'plannedDispatches': [{'start': 'a', 'end': 'b'}]}})
    with patch.object(lf.requests, 'post', side_effect=[auth_resp, schedule_resp]):
        result = lf.get_ev_charging_schedule(datetime(2025, 6, 15, 12, 0, tzinfo=UK))
    assert result == [{'start': 'a', 'end': 'b'}]


def test_get_ev_charging_schedule_auth_failure():
    auth_resp = _mock_response(401)
    with patch.object(lf.requests, 'post', return_value=auth_resp):
        result = lf.get_ev_charging_schedule(datetime(2025, 6, 15, 12, 0, tzinfo=UK))
    assert result is None


# --- get_solar_forecast ---

def test_get_solar_forecast_fetches_fresh_when_needed():
    t = datetime(2025, 6, 15, 10, 0, tzinfo=UK)
    forecast_data = [{'period_end': '2025-06-15T12:00:00+01:00', 'pv_estimate': 2.0}]
    resp = _mock_response(200, {'forecasts': forecast_data})
    with patch.object(lf, 'should_update_solar_forecast', return_value=True), \
         patch.object(lf.requests, 'get', return_value=resp):
        result = lf.get_solar_forecast(t)
    assert result == forecast_data


def test_get_solar_forecast_uses_file_when_no_update():
    t = datetime(2025, 6, 15, 10, 0, tzinfo=UK)
    file_data = [{'period_end': '2025-06-15T12:00:00+01:00', 'pv_estimate': 1.0}]
    with patch.object(lf, 'should_update_solar_forecast', return_value=False), \
         patch.object(lf, 'get_solar_forecast_from_file', return_value=file_data):
        result = lf.get_solar_forecast(t)
    assert result == file_data


# --- start_battery_export ---

def test_start_battery_export_sends_request_when_needed():
    t = datetime(2025, 6, 15, 10, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    post_resp = _mock_response(201)
    with patch.object(lf, 'export_settings_need_updating', return_value=True), \
         patch.object(lf.requests, 'post', return_value=post_resp) as mock_post:
        lf.start_battery_export(t, end)
    mock_post.assert_called_once()
    call_json = mock_post.call_args.kwargs['json']
    assert call_json['enabled'] is True
    assert call_json['slots'][0]['end_time'] == '12:00'


def test_start_battery_export_skips_when_already_correct():
    t = datetime(2025, 6, 15, 10, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    with patch.object(lf, 'export_settings_need_updating', return_value=False), \
         patch.object(lf.requests, 'post') as mock_post:
        lf.start_battery_export(t, end)
    mock_post.assert_not_called()


# --- disable_battery_export ---

def test_disable_battery_export_when_enabled():
    post_resp = _mock_response(201)
    with patch.object(lf, 'get_battery_export_status', return_value=True), \
         patch.object(lf.requests, 'post', return_value=post_resp) as mock_post:
        lf.disable_battery_export()
    mock_post.assert_called_once()
    assert mock_post.call_args.kwargs['json'] == {'enabled': False}


def test_disable_battery_export_already_off():
    with patch.object(lf, 'get_battery_export_status', return_value=False), \
         patch.object(lf.requests, 'post') as mock_post:
        lf.disable_battery_export()
    mock_post.assert_not_called()


# --- change_battery_eco_mode ---

def test_change_battery_eco_mode_turns_on():
    get_resp = _mock_response(200, {'data': {'enabled': False}})
    post_resp = _mock_response(201)
    with patch.object(lf.requests, 'get', return_value=get_resp), \
         patch.object(lf.requests, 'post', return_value=post_resp) as mock_post:
        lf.change_battery_eco_mode(True)
    mock_post.assert_called_once()
    assert mock_post.call_args.kwargs['json'] == {'enabled': True}


def test_change_battery_eco_mode_already_correct():
    get_resp = _mock_response(200, {'data': {'enabled': True}})
    with patch.object(lf.requests, 'get', return_value=get_resp), \
         patch.object(lf.requests, 'post') as mock_post:
        lf.change_battery_eco_mode(True)
    mock_post.assert_not_called()


# --- handle_battery_export ---

def test_handle_battery_export_starts_when_time_to_export():
    t = datetime(2025, 6, 15, 10, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 11, 30, tzinfo=UK)  # 90 mins
    with patch.object(lf, 'get_current_amount_to_export_from_battery', return_value=50), \
         patch.object(lf, 'get_minutes_needed_to_export_battery', return_value=100), \
         patch.object(lf, 'get_minutes_left_to_export_battery', return_value=90), \
         patch.object(lf, 'start_battery_export') as mock_start:
        lf.handle_battery_export(t, end)
    mock_start.assert_called_once_with(t, end)


def test_handle_battery_export_waits_when_too_early():
    t = datetime(2025, 6, 15, 8, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 12, 0, tzinfo=UK)  # 240 mins
    with patch.object(lf, 'get_current_amount_to_export_from_battery', return_value=50), \
         patch.object(lf, 'get_minutes_needed_to_export_battery', return_value=60), \
         patch.object(lf, 'get_minutes_left_to_export_battery', return_value=240), \
         patch.object(lf, 'turn_on_battery_eco_mode') as mock_eco:
        lf.handle_battery_export(t, end)
    mock_eco.assert_called_once()


def test_handle_battery_export_battery_too_low():
    t = datetime(2025, 6, 15, 10, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    # Battery at 4% (min), so amount_to_export = 0
    with patch.object(lf, 'get_current_amount_to_export_from_battery', return_value=0), \
         patch.object(lf, 'turn_on_battery_eco_mode') as mock_eco:
        lf.handle_battery_export(t, end)
    mock_eco.assert_called_once()


def test_handle_battery_export_past_end_time():
    t = datetime(2025, 6, 15, 13, 0, tzinfo=UK)
    end = datetime(2025, 6, 15, 12, 0, tzinfo=UK)  # already past
    with patch.object(lf, 'turn_on_battery_eco_mode') as mock_eco:
        lf.handle_battery_export(t, end)
    mock_eco.assert_called_once()


# --- run_action_based_on_current_time ---

def test_run_action_off_peak():
    t = datetime(2025, 6, 15, 3, 0, tzinfo=UK)
    with patch.object(lf, 'turn_on_battery_eco_mode') as mock_eco:
        lf.run_action_based_on_current_time(t)
    mock_eco.assert_called_once()


def test_run_action_peak_ev_plugged_in():
    t = datetime(2025, 6, 15, 12, 0, tzinfo=UK)
    ev_schedule = [{'start': '2025-06-15T23:30:00+01:00', 'end': '2025-06-16T05:30:00+01:00'}]
    with patch.object(lf, 'get_ev_charging_schedule', return_value=ev_schedule), \
         patch.object(lf, 'get_solar_forecast', return_value=[]), \
         patch.object(lf, 'run_action_for_ev_plugged_in') as mock_ev:
        lf.run_action_based_on_current_time(t)
    mock_ev.assert_called_once_with(t, ev_schedule)


def test_run_action_peak_before_solar_peak():
    t = datetime(2025, 6, 15, 8, 0, tzinfo=UK)
    solar_peak = datetime(2025, 6, 15, 11, 0, tzinfo=UK)
    with patch.object(lf, 'get_ev_charging_schedule', return_value=[]), \
         patch.object(lf, 'get_solar_forecast', return_value=[]), \
         patch.object(lf, 'get_solar_generation_peak_start', return_value=solar_peak), \
         patch.object(lf, 'get_battery_percent_needed_for_consumption', return_value=20), \
         patch.object(lf, 'handle_battery_export') as mock_export:
        lf.run_action_based_on_current_time(t)
    mock_export.assert_called_once()
    call_kwargs = mock_export.call_args
    assert call_kwargs.kwargs['export_end_time'] == solar_peak
    assert call_kwargs.kwargs['battery_reserve'] == 20


def test_run_action_peak_after_solar_peak():
    t = datetime(2025, 6, 15, 14, 0, tzinfo=UK)
    off_peak = datetime(2025, 6, 15, 23, 30, tzinfo=UK)
    with patch.object(lf, 'get_ev_charging_schedule', return_value=[]), \
         patch.object(lf, 'get_solar_forecast', return_value=[]), \
         patch.object(lf, 'get_solar_generation_peak_start', return_value=None), \
         patch.object(lf, 'handle_battery_export') as mock_export:
        lf.run_action_based_on_current_time(t)
    mock_export.assert_called_once()
    assert mock_export.call_args.kwargs['export_end_time'] == off_peak


# --- lambda_handler ---

def test_lambda_handler_success():
    with patch.object(lf, 'run_action_based_on_current_time'):
        result = lf.lambda_handler(None, None)
    assert result['statusCode'] == 200


def test_lambda_handler_exception():
    with patch.object(lf, 'run_action_based_on_current_time', side_effect=RuntimeError('test error')), \
         patch.object(lf, 'send_notification_to_user'):
        result = lf.lambda_handler(None, None)
    assert result['statusCode'] == 500
