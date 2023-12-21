import json
import random
import requests
import time
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime, timedelta

# Function to read configuration file
def read_config(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def get_group_id(group_name, api_port):
    url = f"http://local.adspower.net:{api_port}/api/v1/group/list"
    params = {"group_name": group_name}
    try:
        time.sleep(1)
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data.get("code") == 0:
                return next((group["group_id"] for group in data["data"]["list"] if group["group_name"].lower() == group_name.lower()), None)
            else:
                error_message = data.get('msg', 'Unknown error')
                print(f"Error retrieving group ID: {error_message}")
        else:
            print(f"HTTP Error {response.status_code} while retrieving group ID")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

    return None  # Return None if the group ID is not found or an error occurs

def get_user_ids_by_group(group_id, api_port):
    url = f"http://local.adspower.net:{api_port}/api/v1/user/list"
    total_user_count = 0
    page = 1
    page_size = 100

    while True:
        time.sleep(1)
        params = {"group_id": group_id, "page": page, "page_size": page_size}
        time.sleep(1)
        response = requests.get(url, params=params)
        time.sleep(1)
        if response.status_code != 200:
            print(f"Failed to fetch data for page {page}: Status code {response.status_code}")
            break

        response_data = response.json()
        if 'code' not in response_data or response_data['code'] != 0:
            print(f"API error for page {page}: {response_data}")
            break

        data = response_data.get('data', {}).get('list', [])
        total_user_count += len(data)

        if len(data) < page_size:
            break  # Remove the print statement here

        page += 1

    return total_user_count

# Fetch Group Profile Count
def get_current_browser_counts_per_group(api_port, groups_config):
    group_counts = {}
    for group_name in groups_config:
        group_id = get_group_id(group_name, api_port)
        if group_id:
            user_count = get_user_ids_by_group(group_id, api_port)  # get_user_ids_by_group now returns an int
            group_counts[group_name] = user_count
        else:
            group_counts[group_name] = 0
        print(f"Group: {group_name}, Count: {group_counts[group_name]}")
    return group_counts
    
# Calculate Profiles to Create Per Group
def calculate_profiles_to_create(group_counts, max_limit_per_group, groups_config):
    profiles_to_create = {}
    for group_name in groups_config.keys():
        current_count = group_counts.get(group_name, 0)
        profiles_to_create[group_name] = max(0, max_limit_per_group - current_count)
    return profiles_to_create

# Setup Google Sheets API client
def setup_google_sheets_api(service_account_info):
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    service = build('sheets', 'v4', credentials=credentials)
    return service

# Fetch data from Google Sheets
def fetch_data_from_google_sheets(sheet_service, spreadsheet_id, range_name):
    print(f"Fetching data from spreadsheet: {spreadsheet_id}, range: {range_name}")
    sheet = sheet_service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])
    formatted_values = [item for sublist in values for item in sublist]  # Flatten the list
    print(f"Fetched {len(formatted_values)} items")
    time.sleep(1)  # Add a 1-second delay
    return formatted_values

# Generate unique profile names
def generate_profile_names(male_names, female_names, last_names, count):
    profiles = []
    for _ in range(count):
        first_name = random.choice(male_names + female_names)
        last_name = random.choice(last_names)
        while first_name == last_name:
            last_name = random.choice(last_names)
        profiles.append(f"{first_name} {last_name}")
    return profiles

# Create AdsPower profile
def create_ads_power_profile(profile_name, group_name, api_port, config):
    print(f"Creating AdsPower profile: {profile_name}, group: {group_name}")
    group_id = get_group_id(group_name, api_port)  # Fetch the group_id
    if group_id is None:
        print(f"Failed to retrieve group ID for {group_name}")
        return None

    create_url = f'http://local.adspower.net:{api_port}/api/v1/user/create'
    proxy_config = config['proxy_groups'].get(group_name, {})
    
    profile_data = {
        'name': profile_name,
        'group_id': group_id,  # Include group_id in the request
        'user_proxy_config': proxy_config,
        'os': config['default_profile_settings']['os'],
        'browser': config['default_profile_settings']['browser']
    }
    print("Profile data being sent:", profile_data)
    response = requests.post(create_url, json=profile_data)
    print(response.json())  # Print response for debugging
    if response.status_code == 200:
        print("Profile created successfully.")
        return response.json()
    else:
        print(f"Failed to create profile. Status code: {response.status_code}")
        return None

# Create browser profiles
def create_browser_profiles(profile_count, names, groups, api_port):
    created_profiles = []
    for i in range(profile_count):
        if not names:  # Check if lists are empty
            break

        profile_name = random.choice(names)
        names.remove(profile_name)  # Remove the selected name from the list

        group_name = groups[i % len(groups)]
        profile = create_ads_power_profile(profile_name, group_name, api_port)
        if profile:
            created_profiles.append(profile)
            time.sleep(1)  # Sleep for 1 second between API requests
    return created_profiles

def calculate_needed_profiles(current_group_counts, max_browsers, config):
    max_limit_per_group = max_browsers // len(config['proxy_groups'])
    total_needed_profiles = 0
    needed_profiles = {}

    # Sort groups by the number of existing profiles (ascending)
    sorted_groups = sorted(current_group_counts, key=current_group_counts.get)

    for group_name in sorted_groups:
        if total_needed_profiles >= max_browsers:
            needed_profiles[group_name] = 0
            continue

        current_count = current_group_counts[group_name]
        additional_needed = max_limit_per_group - current_count

        if additional_needed > 0:
            if total_needed_profiles + additional_needed > max_browsers:
                additional_needed = max_browsers - total_needed_profiles
            needed_profiles[group_name] = additional_needed
            total_needed_profiles += additional_needed
        else:
            needed_profiles[group_name] = 0

    return needed_profiles

# Scheduling
def calculate_next_run_time(config):
    start_time = datetime.strptime(config['schedule']['start_time'], "%H:%M")
    now = datetime.now()
    runs_per_day = config['schedule']['runs_per_day']
    interval = 24 / runs_per_day

    next_run = datetime(now.year, now.month, now.day, start_time.hour, start_time.minute)
    while now >= next_run:
        next_run += timedelta(hours=interval)

    return next_run

def format_timedelta(td):
    days, seconds = td.days, td.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds"

# Main execution flow
def run_main_tasks(config):
    sheets_service = setup_google_sheets_api(config['service_account'])
    adspower_api_port = config['adspower_api']['port']
    max_browsers = config['max_browsers']

    # Get current group counts
    current_group_counts = get_current_browser_counts_per_group(adspower_api_port, config['proxy_groups'])
    print("Current Group Counts:", current_group_counts)

    # Calculate needed profiles with the current group counts
    needed_profiles = calculate_needed_profiles(current_group_counts, max_browsers, config)
    print("Needed Profiles:", needed_profiles)

    total_needed_profiles = sum(needed_profiles.values())
    if total_needed_profiles == 0:
        print("No additional profiles needed.")
        return

    # Fetch names from Google Sheets
    name_sheet_info = config['google_sheets']['spreadsheets'][0]['name_sheet_names'][0]
    spreadsheet_id = config['google_sheets']['spreadsheets'][0]['name_id']
    male_names = fetch_data_from_google_sheets(sheets_service, spreadsheet_id, f'{name_sheet_info}!A:A')
    female_names = fetch_data_from_google_sheets(sheets_service, spreadsheet_id, f'{name_sheet_info}!B:B')
    last_names = fetch_data_from_google_sheets(sheets_service, spreadsheet_id, f'{name_sheet_info}!C:C')

    # Generate profile names
    profile_names = generate_profile_names(male_names, female_names, last_names, total_needed_profiles)

    # Create profiles for each group based on needed profiles
    for group_name in config['proxy_groups']:
        num_profiles_to_create = needed_profiles.get(group_name, 0)
        for _ in range(num_profiles_to_create):
            if not profile_names:
                break
            profile_name = profile_names.pop(0)
            create_ads_power_profile(profile_name, group_name, adspower_api_port, config)
            time.sleep(1)  # Sleep for 1 second between API requests

def main():
    config = read_config("config.json")

    # Run the main tasks immediately when the script starts
    print("Starting initial run...")
    run_main_tasks(config)
    print("Initial run completed.")

    # Then start the scheduling loop
    while True:
        next_run = calculate_next_run_time(config)

        while True:
            current_time = datetime.now()
            time_to_next_run = next_run - current_time

            if time_to_next_run.total_seconds() <= 0:
                break

            # Print time remaining every hour
            print(f"Time until next run: {format_timedelta(time_to_next_run)}")
            sleep_time = min(3600, time_to_next_run.total_seconds())  # Sleep for up to an hour
            time.sleep(sleep_time)

            if sleep_time < 3600:
                break  # Exit the inner loop if it's time for the next run

        # Run the main tasks again at the scheduled time
        print("Starting scheduled task...")
        run_main_tasks(config)
        print("Scheduled task completed. Waiting for the next schedule.")

if __name__ == "__main__":
    main()