# TODO
# - Throw exceptions on failures instead of sending notification, so script exits nicely rather than breaking
# - Put code on GitHub to (a) improve my external reputation, and (b) learn how to develop Llambda with version control (and another IDE?)
# - Remove any unused variables or functions
# - Create alerts for when I near AWS Lambda free tier limits
import os
from datetime import datetime, timedelta, time
import zoneinfo
import json
import requests
import boto3

# Load environment variables (from Lambda configuration)
OCTOPUS_ENERGY_ACCOUNT_NUMBER = os.getenv('OCTOPUS_ENERGY_ACCOUNT_NUMBER')
OCTOPUS_ENERGY_API_KEY = os.getenv('OCTOPUS_ENERGY_API_KEY')
GIVENERGY_INVERTER_ID = os.getenv('GIVENERGY_INVERTER_ID')
GIVENERGY_API_TOKEN = os.getenv('GIVENERGY_API_TOKEN')
SOLCAST_PROPERTY_ID = os.getenv('SOLCAST_PROPERTY_ID')
SOLCAST_API_KEY = os.getenv('SOLCAST_API_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# PREFERENCES
END_EVENING_EXPORT_HOUR = 23 # End export by 21:30 to avoid noisy iBoost+ near bedtime
END_EVENING_EXPORT_MINUTE = 30
CONSUMPTION_PREDICTION_VARIANCE_PERCENT = 10 # % to increase consumption prediction by, in case we use more today, to ensure we don't discharge too much
CONSUMPTION_AVERAGE_DAYS = 7 # Number of days to average consumption over (weekday + weekend coverage)
PEAK_GENERATION_FORECAST_VARIANCE_PERCENT = 5 # % generation forecast needs to be below inverter maximum to discharge before peak
SOLAR_GENERATION_EXPORT_END_KW = 0.5 # Aim to finish morning export when solar generation reaches this kW (if generation doesn't reach inverter max today)
MINS_TO_ALLOW_FOR_SOLAR_EXPORT_CHANGES = 30 # Minutes to add to export time to account for solar forecast changes (e.g. peak forecast changes from 10:30 to 11:00 during export)

# CONSTANTS
UK_TIMEZONE = zoneinfo.ZoneInfo('Europe/London')
TARIFF_PEAK_START_HOUR = 5
TARIFF_PEAK_START_MINUTE = 30
TARIFF_OFF_PEAK_START_HOUR = 23
TARIFF_OFF_PEAK_START_MINUTE = 30
OCTOPUS_ENERGY_API_URL = 'https://api.octopus.energy/v1/graphql/'
GIVENERGY_HEADERS = {
    'Authorization': f'Bearer {GIVENERGY_API_TOKEN}',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}
GIVENERGY_URL_PREFIX = f'https://api.givenergy.cloud/v1'
GIVENERGY_INVERTER_URL = f'{GIVENERGY_URL_PREFIX}/inverter/{GIVENERGY_INVERTER_ID}'
GIVENERGY_STATUS_URL = f'{GIVENERGY_INVERTER_URL}/system-data/latest'
GIVENERGY_SETTINGS_PRESETS_URL = f'{GIVENERGY_INVERTER_URL}/presets'
GIVENERGY_EXPORT_URL = f'{GIVENERGY_INVERTER_URL}/presets/timed-export'
GIVENERGY_ECO_URL = f'{GIVENERGY_INVERTER_URL}/presets/eco-mode'
GIVENERGY_DATA_POINTS_URL = f'{GIVENERGY_INVERTER_URL}/data-points/'
GIVENERGY_NOTIFICATION_URL = f'{GIVENERGY_URL_PREFIX}/notification/send'
GIVENERGY_BATTERY_SIZE_KWH = 5.22
GIVENERGY_USABLE_BATTERY_DEPTH_OF_DISCHARGE_PERCENT = 80
GIVENERGY_USABLE_BATTERY_SIZE_KWH = GIVENERGY_BATTERY_SIZE_KWH * (GIVENERGY_USABLE_BATTERY_DEPTH_OF_DISCHARGE_PERCENT / 100)
GIVENERGY_DISCHARGE_POWER_KW = 2.6
GIVENERGY_INVERTER_MAX_KW = 3.68
GIVENERGY_BATTERY_DISCHARGE_MINUTES_PER_PERCENT = (GIVENERGY_USABLE_BATTERY_SIZE_KWH / GIVENERGY_DISCHARGE_POWER_KW / 100) * 60
GIVENERGY_MIN_BATTERY_PERCENT = 4
SOLCAST_URL = 'https://api.solcast.com.au/rooftop_sites/' + SOLCAST_PROPERTY_ID + '/forecasts?format=json&api_key=' + SOLCAST_API_KEY
SOLCAST_OPTIMISM_OPTIMISTIC = 'pv_estimate90'
SOLCAST_OPTIMISM_NORMAL = 'pv_estimate'
SOLCAST_OPTIMISM_PESSIMISTIC = 'pv_estimate10'
SOLAR_GENERATION_EXPORT_PEAK_KW = GIVENERGY_INVERTER_MAX_KW - GIVENERGY_DISCHARGE_POWER_KW # Max solar generation before we can't discharge battery at full power
SOLAR_GENERATION_PEAK_FORECAST_KW = GIVENERGY_INVERTER_MAX_KW * (1 - (PEAK_GENERATION_FORECAST_VARIANCE_PERCENT / 100))
SOLAR_FORECAST_FILE = 'solar_forecast.json'
SOLAR_LATEST_PEAK_HOUR = 13 # Solar is highly likely to be past its peak at 1pm (13:00)
SOLAR_FORECAST_MINS_BETWEEN_UPDATES = 50 # Only refresh the solar forecast every 30 mins (and only in peak time before SOLAR_LATEST_PEAK_HOUR), because we can only request 9 the forecast times per day in the free tier

s3_client = boto3.client('s3') # Initialize Boto3 S3 client

def log(message):
    print(message)

def send_notification_to_user(notification_text):
    log(f'Sending notification to user: {notification_text}')
    requests.post(url=GIVENERGY_NOTIFICATION_URL, headers=GIVENERGY_HEADERS, json={
        'platforms': ['push'],
        'title': 'Battery export',
        'body': notification_text,
        'icon': 'mdi-account-outline'
    })

def create_time_from_hour_minute(hour, minute, date):
    return datetime.combine(date, time(hour=hour, minute=minute), tzinfo=UK_TIMEZONE)

def get_time_in_server_timezone(time_in_other_timezone):
     # Script doesn't necessarily run in UK time (likely UTC)
    server_time = time_in_other_timezone.astimezone(datetime.now().astimezone().tzinfo)
    return server_time

def ensure_time_is_not_now(time):
    now = datetime.now(UK_TIMEZONE)
    time_difference = abs(time - now)
    time_delta = timedelta(minutes=2)
    if time_difference <= time_delta:
        new_time = time + time_delta
        log(f'{time:%H:%M} is within a couple of minutes from now, so adding a couple of minutes to it (new time: {new_time:%H:%M})')
    else:
        new_time = time
    return new_time

def get_ev_charging_api_authorisation():
    log('Getting Octopus Energy API authorisation token')
    query = """
        mutation krakenTokenAuthentication($api: String!) {
            obtainKrakenToken(input: {APIKey: $api}) {
                token
            }
        }
    """
    variables = {
        'api': OCTOPUS_ENERGY_API_KEY
    }
    response = requests.post(OCTOPUS_ENERGY_API_URL, json={
        'query': query,
        'variables': variables
    })
    if response.status_code == 200:
        log('Successfully retrieved Octopus Energy API authorisation token')
        api_token = response.json()['data']['obtainKrakenToken']['token']
    else:
        send_notification_to_user(f'Failed to retrieve Octopus Energy API authorisation token: {response.text}')
        api_token = None
    return api_token

def get_ev_charging_schedule(script_start_time):
    log('Checking EV charging schedule')
    api_token = get_ev_charging_api_authorisation()
    if api_token == None:
        return None
    query = """
        query getData($input: String!) {
            plannedDispatches(accountNumber: $input) {
                start
                end
            }
        }
    """
    variables = {
        'input': OCTOPUS_ENERGY_ACCOUNT_NUMBER
    }
    headers = {
        'Authorization': api_token
    }
    response = requests.post(OCTOPUS_ENERGY_API_URL, json={
        'query': query,
        'variables': variables,
        'operationName': 'getData'
    }, headers=headers)
    if response.status_code == 200:
        plannedDispatches = response.json()['data']['plannedDispatches']
        log(f'Successfully retrieved Octopus Energy EV charging schedule: {plannedDispatches}')
    else:
        send_notification_to_user(f'Failed to retrieve Octopus Energy EV charging schedule: {response.text}')
        plannedDispatches = None
    return plannedDispatches

def ev_is_plugged_in(script_start_time, ev_schedule):
    plugged_in = False
    if ev_schedule != None and len(ev_schedule) > 0: # Assume EV is plugged in if a charging schedule exists
        plugged_in = True
        log('EV is plugged in')
    else:
        log('EV is not plugged in')
    return plugged_in

def get_current_ev_charging_slot(script_start_time, ev_schedule):
    current_charging_slot = None
    for charging_slot in ev_schedule:
        start_time = get_start_time_for_ev_charging_slot(charging_slot)
        end_time = get_end_time_for_ev_charging_slot(charging_slot)
        if start_time <= script_start_time <= end_time:
            log(f'Current EV charging slot started at {start_time:%H:%M} and ends at {end_time:%H:%M}')
            current_charging_slot = charging_slot
    return current_charging_slot

def get_next_ev_charging_slot(script_start_time, ev_schedule):
    next_charging_slot = None
    earliest_end_time = None
    for charging_slot in ev_schedule:
        start_time = get_start_time_for_ev_charging_slot(charging_slot)
        end_time = get_end_time_for_ev_charging_slot(charging_slot)
        if end_time > script_start_time and (earliest_end_time == None or end_time < earliest_end_time):
            log(f'Next EV charging slot starts at {start_time:%H:%M} and ends at {end_time:%H:%M}')
            next_charging_slot = charging_slot
            earliest_end_time = end_time
    return next_charging_slot

def ev_is_charging(script_start_time, ev_schedule):
    is_charging = False
    current_charging_slot = get_current_ev_charging_slot(script_start_time, ev_schedule)
    if current_charging_slot:
        is_charging = True
        log('EV is charging right now')
    else:
        log('EV is not currently charging')
    return is_charging

def get_end_time_for_ev_charging_slot(charging_slot):
    return datetime.fromisoformat(charging_slot['end']).astimezone(UK_TIMEZONE)

def get_start_time_for_ev_charging_slot(charging_slot):
    return datetime.fromisoformat(charging_slot['start']).astimezone(UK_TIMEZONE)

def get_current_ev_charging_slot_end_time(script_start_time, ev_schedule):
    time = None
    charging_slot = get_current_ev_charging_slot(script_start_time, ev_schedule)
    if charging_slot:
        time = get_end_time_for_ev_charging_slot(charging_slot)
    return time

def get_next_ev_charging_slot_start_time(script_start_time, ev_schedule):
    time = None
    charging_slot = get_next_ev_charging_slot(script_start_time, ev_schedule)
    if charging_slot:
        time = get_start_time_for_ev_charging_slot(charging_slot)
    return time

def handle_ev_charging(script_start_time, ev_schedule):
    end_time = get_current_ev_charging_slot_end_time(script_start_time, ev_schedule)
    stop_discharging_battery()

def get_remaining_solar_generation_for_today(script_start_time, solar_forecast, forecast_optimism=SOLCAST_OPTIMISM_NORMAL):
    log(f'Getting remaining solar generation for today using {forecast_optimism}')
    total_generation = 0
    for next_forecast in solar_forecast:
        forecast_time = datetime.fromisoformat(next_forecast['period_end']).astimezone(UK_TIMEZONE)
        if forecast_time > script_start_time and forecast_time.date() == script_start_time.date():
            total_generation += next_forecast[forecast_optimism]
    total_generation_as_battery_percentage = (total_generation / GIVENERGY_USABLE_BATTERY_SIZE_KWH) * 100
    log(f'{total_generation:.3f} kWh more to be generated today ({total_generation_as_battery_percentage:.0f}% of total battery capacity) using {forecast_optimism}')
    return total_generation

def get_solar_generation_kw_time(script_start_time, solar_forecast, generation_kw, want_generation_end_time, want_peak_generation=False, forecast_optimism=SOLCAST_OPTIMISM_NORMAL):
    requested_generation_time = None
    earliest_requested_generation_time = None
    latest_requested_generation_time = None
    max_generation_today_time = None
    max_generation_today_kw = 0
    earliest_any_generation_time = None
    earliest_any_generation_kw = 0
    for forecast in solar_forecast:
        forecast_end_time = datetime.fromisoformat(forecast['period_end']).astimezone(UK_TIMEZONE)
        if want_generation_end_time:
            forecast_time = forecast_end_time
        else:
            forecast_time = forecast_end_time - timedelta(minutes=30)
        forecast_generation = forecast[forecast_optimism]
        if forecast_time.date() == script_start_time.date():
            if forecast_generation > max_generation_today_kw:
                max_generation_today_kw = forecast_generation
                max_generation_today_time = forecast_time
            if forecast_time > script_start_time:
                if forecast_generation > 0 and (earliest_any_generation_time == None or earliest_any_generation_time > forecast_time):
                    earliest_any_generation_time = forecast_time
                    earliest_any_generation_kw = forecast_generation
                if forecast_generation >= generation_kw:
                    if earliest_requested_generation_time == None or earliest_requested_generation_time > forecast_time:
                        earliest_requested_generation_time = forecast_time
                    if latest_requested_generation_time == None or latest_requested_generation_time < forecast_time:
                        latest_requested_generation_time = forecast_time
    if want_generation_end_time:
        requested_generation_time = latest_requested_generation_time
    else:
        requested_generation_time = earliest_requested_generation_time
    if earliest_any_generation_time != None and earliest_any_generation_time == requested_generation_time:
        log(f'We are already generating {earliest_any_generation_kw:.3f} kW using {forecast_optimism} (at {earliest_any_generation_time:%H:%M})')
        requested_generation_time = None
    elif requested_generation_time == None:
        log(f'Generation will not reach {generation_kw:.3f} kW for the rest of today using {forecast_optimism} (earliest generation is {earliest_any_generation_kw:.3f} kW, max generation is {max_generation_today_kw:.3f} kW)')
        latest_peak_solar = create_time_from_hour_minute(SOLAR_LATEST_PEAK_HOUR, 0, date=script_start_time.date()) # Make sure we don't detect peak solar generation late in the day
        if want_peak_generation and max_generation_today_time != None and max_generation_today_time > script_start_time and script_start_time < latest_peak_solar:
            requested_generation_time = max_generation_today_time
            log(f'Peak generation today is {max_generation_today_kw:.3f} kW at {requested_generation_time:%H:%M}')
    elif want_generation_end_time:
        log(f'Generation should drop below {generation_kw:.3f} kW at {requested_generation_time:%H:%M} using {forecast_optimism} (earliest generation is {earliest_any_generation_kw:.3f} kW at {earliest_any_generation_time:%H:%M}, max generation is {max_generation_today_kw:.3f} kW at {max_generation_today_time:%H:%M})')
    else:
        log(f'Generation should get to {generation_kw:.3f} kW at {requested_generation_time:%H:%M} using {forecast_optimism} (earliest generation is {earliest_any_generation_kw:.3f} kW at {earliest_any_generation_time:%H:%M}, max generation is {max_generation_today_kw:.3f} kW at {max_generation_today_time:%H:%M})')
    return requested_generation_time

def get_solar_generation_peak_start(script_start_time, solar_forecast):
    return get_solar_generation_kw_time(script_start_time, solar_forecast, SOLAR_GENERATION_PEAK_FORECAST_KW, want_generation_end_time=False, want_peak_generation=True, forecast_optimism=SOLCAST_OPTIMISM_NORMAL)

def get_off_peak_start(script_start_time):
    return create_time_from_hour_minute(TARIFF_OFF_PEAK_START_HOUR, TARIFF_OFF_PEAK_START_MINUTE, date=script_start_time.date())

def is_in_off_peak(script_start_time):
    in_off_peak = False
    tariff_off_peak_start = get_off_peak_start(script_start_time)
    tariff_peak_start = create_time_from_hour_minute(TARIFF_PEAK_START_HOUR, TARIFF_PEAK_START_MINUTE, date=script_start_time.date())
    if script_start_time >= tariff_off_peak_start or script_start_time < tariff_peak_start:
        in_off_peak = True
        log(f'In off peak hours')
    else:
        log(f'In peak hours')
    return in_off_peak

def is_in_peak(script_start_time):
    return not is_in_off_peak(script_start_time)

def get_solar_forecast_from_file(script_start_time):
    forecasts = None
    try:
        log('Loading most recent solar forecast from file')
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=SOLAR_FORECAST_FILE)
        forecasts = json.loads(response['Body'].read().decode('utf-8'))
    except Exception as s3_e:
        log(f'Failed to load forecast from S3: {s3_e}. No forecast data available.')
        forecasts = []
    return forecasts

def should_update_solar_forecast(script_start_time):
    update = False
    if is_in_peak(script_start_time):
        latest_peak_solar = create_time_from_hour_minute(SOLAR_LATEST_PEAK_HOUR, 0, date=script_start_time.date())
        if script_start_time < latest_peak_solar:
            try:
                response = s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=SOLAR_FORECAST_FILE)
                last_modified = response['LastModified'].astimezone(UK_TIMEZONE)
                time_difference = script_start_time - last_modified
                if time_difference > timedelta(minutes=SOLAR_FORECAST_MINS_BETWEEN_UPDATES):
                    log(f'Solar forecast updated over {SOLAR_FORECAST_MINS_BETWEEN_UPDATES} mins ago (last updated: {last_modified:%H:%M})')
                    update = True
            except Exception as e:
                log(f'Could not check solar forecast file age: {e}. Will fetch a fresh forecast.')
                update = True
    return update

def get_solar_forecast(script_start_time):
    log('Getting solar forecast')
    if should_update_solar_forecast(script_start_time):
        response = requests.get(url=SOLCAST_URL)
        if response.status_code == 200:
            forecasts = response.json()['forecasts']
            log(f'Successfully retrieved solar forecast')
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=SOLAR_FORECAST_FILE,
                Body=json.dumps(forecasts, indent=2)
            )
            log(f'Successfully saved solar forecast to: {SOLAR_FORECAST_FILE}')
        else:
            log(f'Failed to get solar forecast: {response} - {response.text}')
            forecasts = get_solar_forecast_from_file(script_start_time)
    else:
        log(f'Getting solar forecast from file to minimise Solcast API calls')
        forecasts = get_solar_forecast_from_file(script_start_time)
    return forecasts

def get_battery_settings(): # Only used for debugging
    log('Getting a list of all battery presets')
    response = requests.get(url=GIVENERGY_SETTINGS_PRESETS_URL, headers=GIVENERGY_HEADERS)
    if response.status_code == 200:
        response_data = response.json()
        settings = response_data['data']
        log(f'Current battery presets: {settings}')
        return settings
    else:
        raise RuntimeError(f'Failed to get current battery presets: {response.text}')

def get_battery_soc():
    response = requests.get(url=GIVENERGY_STATUS_URL, headers=GIVENERGY_HEADERS)
    if response.status_code == 200:
        battery_soc = response.json()['data']['battery']['percent']
        log(f'Battery percentage is {battery_soc}%')
        return battery_soc
    else:
        raise RuntimeError(f'Failed to get battery status: {response.text}')

def get_battery_percentage_for_consumption(kWh):
    battery_percent = (kWh / GIVENERGY_USABLE_BATTERY_SIZE_KWH) * 100
    return battery_percent

def get_recent_consumption(start_time):
    earliest_date = start_time - timedelta(days=8)
    latest_date = start_time - timedelta(days=1)
    formatted_earliest_date = earliest_date.strftime('%Y-%m-%d')
    formatted_latest_date = latest_date.strftime('%Y-%m-%d')
    log(f'Getting consumption data from {formatted_earliest_date} to {formatted_latest_date}')
    url = GIVENERGY_DATA_POINTS_URL + formatted_latest_date
    params = {
        'page': 1,
        'pageSize': 500
    }
    response = requests.get(url=url, headers=GIVENERGY_HEADERS, params=params)
    if response.status_code == 200:
        consumption = response.json()['data']
        log('Successfully retrieved recent consumption data')
    else:
        send_notification_to_user(f'Failed to get recent consumption data: {response.text}')
        consumption = []
    return consumption

def predict_consumption(start_time, end_time):
    recent_consumption = get_recent_consumption(start_time)
    daily_totals = []
    for days_ago in range(1, CONSUMPTION_AVERAGE_DAYS + 1):
        day_start = start_time - timedelta(days=days_ago, minutes=5)
        day_end = end_time - timedelta(days=days_ago, minutes=-5)
        day_total = 0
        for consumption_period in recent_consumption:
            consumption_period_time = datetime.fromisoformat(consumption_period['time']).astimezone(UK_TIMEZONE)
            if consumption_period_time >= day_start and consumption_period_time <= day_end:
                day_total += (consumption_period['today']['consumption'] / 1000)
        if day_total > 0:
            daily_totals.append(day_total)
    if daily_totals:
        average_consumption = sum(daily_totals) / len(daily_totals)
        log(f'Average consumption between {start_time:%H:%M} and {end_time:%H:%M} over {len(daily_totals)} days: {average_consumption:.3f} kWh')
    else:
        average_consumption = 0
        log(f'No consumption data found for the last {CONSUMPTION_AVERAGE_DAYS} days')
    return average_consumption

def get_max_amount_to_export_from_battery():
    max_export = max(100 - GIVENERGY_MIN_BATTERY_PERCENT, 0)
    return max_export

def get_current_amount_to_export_from_battery():
    battery_soc = get_battery_soc()
    amount_to_export = max(battery_soc - GIVENERGY_MIN_BATTERY_PERCENT, 0)
    return amount_to_export

def get_minutes_left_to_export_battery(start_time, end_time):
    time_left_to_export = end_time - start_time
    seconds_left_to_export = time_left_to_export.total_seconds()
    total_minutes_left_to_export = seconds_left_to_export // 60
    hours_left_to_export = round(seconds_left_to_export // 3600)
    remainder_minutes_left_to_export = round((seconds_left_to_export % 3600) // 60)
    log(f'There are {hours_left_to_export} hours and {remainder_minutes_left_to_export} minutes left to export')
    return total_minutes_left_to_export

def get_minutes_needed_to_export_battery_at_full_power(amount_to_export):
    total_minutes_to_export = amount_to_export * GIVENERGY_BATTERY_DISCHARGE_MINUTES_PER_PERCENT
    hours_to_export = round(total_minutes_to_export // 60)
    remainder_minutes_to_export = round(total_minutes_to_export % 60)
    log(f'Need {hours_to_export} hours and {remainder_minutes_to_export} minutes to export {amount_to_export:.0f}% at full power')
    return total_minutes_to_export

def get_minutes_needed_to_export_battery(script_start_time, amount_to_export, export_end_time=None, solar_forecast=None):
    minimum_minutes_to_export = get_minutes_needed_to_export_battery_at_full_power(amount_to_export)
    if export_end_time != None and solar_forecast != None:
        log(f'Calculating how long it will take to export {amount_to_export:.0f}% by {export_end_time:%H:%M} considering generation slows export')
        # Filter solar forecasts to only include relevant future periods
        relevant_forecasts = [
            f for f in solar_forecast
            if script_start_time < datetime.fromisoformat(f['period_end']) <= export_end_time
        ]
        # Iterate backwards in 30-minute intervals from the export_end_time
        minutes_exported = 0
        total_minutes_to_export = 0
        for forecast in reversed(relevant_forecasts):
            if minutes_exported < minimum_minutes_to_export:
                period_end = datetime.fromisoformat(forecast['period_end']).astimezone(UK_TIMEZONE)
                period_start = period_end - timedelta(minutes=30)
                solar_generation_kw = forecast['pv_estimate']
                if solar_generation_kw > SOLAR_GENERATION_EXPORT_PEAK_KW:
                    excess_solar_kw = solar_generation_kw - SOLAR_GENERATION_EXPORT_PEAK_KW
                    dischargable_battery_kw = GIVENERGY_DISCHARGE_POWER_KW - excess_solar_kw
                    if dischargable_battery_kw < 0:
                        dischargable_battery_kw = 0
                    discharge_rate_ratio = dischargable_battery_kw / GIVENERGY_DISCHARGE_POWER_KW
                else:
                    discharge_rate_ratio = 1.0
                effective_minutes_in_period = 30 * discharge_rate_ratio
                minutes_exported += effective_minutes_in_period
                if minutes_exported >= minimum_minutes_to_export:
                    overshoot_export_minutes = minutes_exported - minimum_minutes_to_export
                    needed_real_minutes = overshoot_export_minutes / discharge_rate_ratio
                    total_minutes_to_export += needed_real_minutes
                else:
                    total_minutes_to_export += 30
        total_minutes_to_export += MINS_TO_ALLOW_FOR_SOLAR_EXPORT_CHANGES # Add time to allow for solar forecast changes
        log(f'Can export the equivalent of {minutes_exported:.0f} mins over {total_minutes_to_export:.0f} mins due to solar generation. We need {minimum_minutes_to_export:.0f} mins.')
        total_minutes_to_export = max(total_minutes_to_export, minimum_minutes_to_export)
    else:
        total_minutes_to_export = minimum_minutes_to_export
    hours_to_export = round(total_minutes_to_export // 60)
    remainder_minutes_to_export = round(total_minutes_to_export % 60)
    log(f'Need {hours_to_export} hours and {remainder_minutes_to_export} minutes to export {amount_to_export:.0f}%')
    return total_minutes_to_export

def get_battery_percent_needed_for_consumption(script_start_time, solar_forecast, end_time):
    total_generation = get_remaining_solar_generation_for_today(script_start_time, solar_forecast, SOLCAST_OPTIMISM_PESSIMISTIC)
    total_consumption = predict_consumption(script_start_time, end_time)
    total_consumption *= (1 + (CONSUMPTION_PREDICTION_VARIANCE_PERCENT / 100))
    battery_kwh_needed = max(total_consumption - total_generation, 0)
    battery_percent_needed = get_battery_percentage_for_consumption(battery_kwh_needed)
    log(f'Need {battery_percent_needed:.0f}% battery ({battery_kwh_needed:.1f} kWh) for forecast {total_consumption:.3f} kWh consumption and {total_generation:.3f} kWh generation')
    return battery_percent_needed

def get_battery_export_settings():
    log('Checking current battery export settings')
    response = requests.get(url=GIVENERGY_EXPORT_URL, headers=GIVENERGY_HEADERS)
    if response.status_code == 200:
        response_data = response.json()
        settings = response_data['data']
        log(f'Current battery export settings: {settings}')
        return settings
    else:
        raise RuntimeError(f'Failed to get current battery export settings: {response.text}')

def export_settings_need_updating(desired_end_time):
    settings = get_battery_export_settings()
    current_export_status = settings['enabled']
    current_export_end = settings['slots'][0]['end_time']
    desired_export_end = desired_end_time.strftime('%H:%M')
    need_updating = current_export_status != True or current_export_end != desired_export_end
    return need_updating

def start_battery_export(script_start_time, desired_end_time):
    need_to_start_export = export_settings_need_updating(desired_end_time)
    if need_to_start_export:
        log('Current export settings do not match desired settings')
        desired_export_start = script_start_time.strftime('%H:%M')
        desired_export_end = desired_end_time.strftime('%H:%M')
        response = requests.post(url=GIVENERGY_EXPORT_URL, headers=GIVENERGY_HEADERS, json={
            'enabled': True,
            'slots': [{
                'start_time': desired_export_start,
                'end_time': desired_export_end
            }]
        })
        if response.status_code == 201:
            log(f'Starting battery export to discharge by {desired_export_end}')
        else:
            send_notification_to_user(f'Failed to set battery export timing: {response.text}')
    else:
        log('Current export settings match desired ones, so doing nothing')

def get_battery_export_status():
    settings = get_battery_export_settings()
    current_export_status = settings['enabled']
    return current_export_status

def disable_battery_export():
    current_export_status = get_battery_export_status()
    if current_export_status:
        log('Export is turned on, so need to disable it')
        response = requests.post(url=GIVENERGY_EXPORT_URL, headers=GIVENERGY_HEADERS, json={
            'enabled': False,
        })
        if response.status_code == 201:
            log('Export successfully disabled')
        else:
            send_notification_to_user(f'Failed to turn off battery export: {response.text}')
    else:
        log('Export is already turned off')

def change_battery_eco_mode(enabled):
    if enabled:
        log('Trying to turn on Eco mode')
    else:
        log('Trying to turn off Eco mode')
    response = requests.get(url=GIVENERGY_ECO_URL, headers=GIVENERGY_HEADERS)
    if response.status_code == 200:
        current_eco_mode = response.json()['data']['enabled']
        if current_eco_mode != enabled:
            log(f'Eco mode is {current_eco_mode}, so will change it')
            response = requests.post(url=GIVENERGY_ECO_URL, headers=GIVENERGY_HEADERS, json={
                'enabled': enabled
            })
            if response.status_code == 201:
                if enabled:
                    log(f'Successfully turned on Eco mode')
                else:
                    log(f'Successfully turned off Eco mode')
            else:
                send_notification_to_user(f'Failed to change Eco mode: {response.text}')
        else:
            log(f'Eco mode is already {enabled}, so doing nothing')
    else:
        send_notification_to_user(f'Failed to get Eco mode: {response.text}')

def turn_on_battery_eco_mode():
    change_battery_eco_mode(True)

def stop_discharging_battery():
    disable_battery_export()
    change_battery_eco_mode(False)

def stop_charging_battery(script_start_time, desired_end_time):
    change_battery_eco_mode(False)

def get_time_to_check_on_export(script_start_time, export_end_time):
    check_time = None
    time_difference = export_end_time - script_start_time
    if time_difference <= timedelta(minutes=20):
        check_time = export_end_time
        log(f'Export end time {export_end_time:%H:%M} is less than 20 minutes from now, so will check at end of export')
    else:
        check_time = script_start_time + (time_difference / 2)
        log(f'Export end time {export_end_time:%H:%M} is greater than 20 minutes from now, so will check at {check_time:%H:%M}')
    return check_time

def handle_battery_export(script_start_time, export_end_time, battery_reserve=0, export_now=False, solar_forecast=None):
    log(f'Aiming to end export by {export_end_time:%H:%M} (leaving {battery_reserve:.0f}% usable battery for consumption)')
    if script_start_time < export_end_time:
        amount_to_export = get_current_amount_to_export_from_battery()
        amount_to_export = max(amount_to_export - battery_reserve, 0)
        log(f'We want to export {amount_to_export:.0f}% (leaving {battery_reserve:.0f}% usable battery for consumption)')
        if amount_to_export > 0:
            minutes_needed_to_export = get_minutes_needed_to_export_battery(script_start_time, amount_to_export, export_end_time=export_end_time, solar_forecast=solar_forecast)
            minutes_until_export_end = get_minutes_left_to_export_battery(script_start_time, export_end_time)
            if export_now or minutes_needed_to_export >= minutes_until_export_end:
                log(f'We need to start exporting now to export {amount_to_export:.0f}% by {export_end_time:%H:%M}')
                start_battery_export(script_start_time, export_end_time)
            else:
                log('There is too much time left to start exporting now')
                turn_on_battery_eco_mode()
        else:
            log('Battery is too low to export')
            if export_now:
                stop_charging_battery(script_start_time, export_end_time)
            else:
                turn_on_battery_eco_mode()
    else:
        log('It is too late to export')
        turn_on_battery_eco_mode()

def run_action_for_ev_plugged_in(script_start_time, ev_schedule):
    if ev_is_charging(script_start_time, ev_schedule):
        log('EV is plugged in and charging (stop discharging battery because it will just charge the car)')
        handle_ev_charging(script_start_time, ev_schedule)
    else:
        log('EV is plugged in and not charging (no battery export because it will just charge the car)')
        turn_on_battery_eco_mode()
        next_run_time = get_next_ev_charging_slot_start_time(script_start_time, ev_schedule)
        if next_run_time == None:
            log('EV charging has finished')

def run_action_based_on_current_time(script_start_time):
    tariff_off_peak_start = get_off_peak_start(script_start_time)
    in_off_peak = is_in_off_peak(script_start_time)
    if in_off_peak:
        log('In tariff off-peak period')
        turn_on_battery_eco_mode()
    else:
        log('In tariff peak period')
        ev_schedule = get_ev_charging_schedule(script_start_time)
        solar_forecast = get_solar_forecast(script_start_time)
        solar_generation_peak_start = get_solar_generation_peak_start(script_start_time, solar_forecast)
        if ev_is_plugged_in(script_start_time, ev_schedule):
            run_action_for_ev_plugged_in(script_start_time, ev_schedule)
        elif solar_generation_peak_start != None and script_start_time < solar_generation_peak_start:
            log('In tariff peak period, before solar generation is at its peak')
            battery_needed = get_battery_percent_needed_for_consumption(script_start_time, solar_forecast, tariff_off_peak_start)
            handle_battery_export(script_start_time, export_end_time=solar_generation_peak_start, solar_forecast=solar_forecast, battery_reserve=battery_needed)
        else:
            log('In tariff peak period, after solar generation peak')
            handle_battery_export(script_start_time, export_end_time=tariff_off_peak_start)

def lambda_handler(event, context):
    try:
        script_start_time = datetime.now(UK_TIMEZONE)
        log(f'Starting script at {script_start_time:%H:%M} (server time: {get_time_in_server_timezone(script_start_time):%H:%M})')
        run_action_based_on_current_time(script_start_time)
        return {'statusCode': 200, 'body': json.dumps('Script executed successfully!')}
    except Exception as exception:
        if hasattr(exception, 'text'):
            error_message = f'An error occurred: {exception.text}'
        else:
            error_message = f'An error occurred: {exception}'
        send_notification_to_user(error_message)
        return {'statusCode': 500, 'body': json.dumps(error_message)}

def main():
    lambda_handler(None, None)

if __name__ == '__main__':
    main()