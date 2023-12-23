import requests
import time
import random
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import json
import requests
from datetime import datetime, timedelta


### Google Sheet Remark Updater
class GoogleSheetsUpdater:
    def __init__(self, config):
        self.config = config
        self.setup_google_sheets()
        user_agent_email = self.config.get('geolocation', {}).get('user_agent_email', 'default_email@example.com')
        self.geolocator = Nominatim(user_agent=user_agent_email)

    def setup_google_sheets(self):
        credentials = Credentials.from_service_account_info(
            self.config['service_account'], 
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        self.service = build('sheets', 'v4', credentials=credentials)

    def check_api_availability(self):
        api_base_url = f"http://{self.config['adspower']['host']}:{self.config['adspower']['port']}/status"
        response = requests.get(api_base_url)
        print(f"API Status Check: URL: {api_base_url}, Response Code: {response.status_code}, Response Body: {response.text}")
        if response.status_code == 200 and response.json().get('code') == 0:
            print("Adspower API is available.")
            return True
        else:
            print(f"Adspower API is not available: {response.text}")
            return False

    def run(self):
        if not self.check_api_availability():
            return

        spreadsheet_id = self.config['google_sheets']['spreadsheets'][0]['id']
        sheet_names = self.config['google_sheets']['spreadsheets'][0]['sheet_names']

        for sheet_name in sheet_names:
            print(f"Starting to process sheet: {sheet_name}")
            sheet_info = self.service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            total_rows = sheet_info['sheets'][0]['properties']['gridProperties']['rowCount']

            empty_row_count = 0

            for i in range(4, total_rows + 1):
                try:
                    print(f"Processing row {i} in sheet '{sheet_name}'")
                    range_name = f'{sheet_name}!A{i}:AA{i}'
                    time.sleep(1)
                    row_values = self.service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id, range=range_name).execute().get('values', [[]])[0]
                    print(f"Row {i} data: {row_values}")

                    if not row_values[14].strip():
                        empty_row_count += 1
                        if empty_row_count >= 5:
                            print(f"Encountered 5 empty rows in a row in sheet '{sheet_name}', moving to next sheet.")
                            break
                        continue
                    else:
                        empty_row_count = 0

                    username = row_values[0] if len(row_values) > 0 else None # Column A
                    password = row_values[1] if len(row_values) > 1 else None # Column B
                    address = row_values[3] if len(row_values) > 3 else None # Column D
                    fakey = row_values[7] if len(row_values) > 7 else None # Column H
                    user_id = row_values[14] if len(row_values) > 14 else None # Column O
                    gmail_status = row_values[25] if len(row_values) > 25 else None # Column Z

                    if not (user_id and gmail_status):
                        continue

                    if address.strip():  # Checks if there is an address
                        lat, lon = self.attempt_geolocation(address)
                        if lat is not None and lon is not None:
                            gmail_status += '\nLocation Added'
                        else:
                            gmail_status += '\nError Processing Location'
                    else:
                        gmail_status += '\nLocation Info Missing'

                    if fakey:
                        gmail_status += '\n2FA Added'
                    else:
                        gmail_status += '\n2FA Info Missing'

                    if username and password:
                        gmail_status += '\nAccount Added'
                    else:
                        gmail_status += '\nAccount Info Missing'

                    self.update_adspower_remark(user_id, gmail_status, lat, lon, username, password, fakey)

                except Exception as e:
                    print(f"Error processing row {i} in sheet '{sheet_name}': {e}, Type: {type(e)}")
                    print(f"Exception occurred while updating user ID {user_id}: {e}, Exception Type: {type(e).__name__}")
                    time.sleep(1)

            print(f"Finished processing sheet: {sheet_name}")

    def attempt_geolocation(self, address):
        try:
            geocode_url = f"https://geocode.maps.co/search?q={address}"
            response = requests.get(geocode_url)
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    lat = data[0]['lat']
                    lon = data[0]['lon']
                    return lat, lon
        except Exception as e:
            print(f"Geocoding error: {e}")
        
        return None, None

    def update_adspower_remark(self, user_id, gmail_status, lat, lon, username, password, fakey):
        print(f"Preparing to update Adspower API for user ID: {user_id}")
        print(f"Updating with: Latitude - {lat}, Longitude - {lon}, Username - {username}, Password - {password}, 2FA - {fakey}")
        api_base_url = f"http://{self.config['adspower']['host']}:{self.config['adspower']['port']}/"
        headers = {'Authorization': self.config['adspower']['api_key']}
        domain_name = 'accounts.google.com' if username and password else None
        lat = round(float(lat), 6) if lat is not None else None
        lon = round(float(lon), 6) if lon is not None else None
        accuracy = random.randint(10, 5000)
        update_data = {
        "user_id": user_id,
        "remark": gmail_status,
        "username": username,
        "password": password,
        "fakey": fakey,
        "domain_name": domain_name,
        "fingerprint_config": {
            "location_switch": "0" if lat is not None and lon is not None else "1",
            "longitude": str(round(float(lon), 6)) if lon is not None else "",
            "latitude": str(round(float(lat), 6)) if lat is not None else "",
            "accuracy": str(accuracy) if lat is not None and lon is not None else ""
            }
        }

        # Log the request details
        print(f"Sending request to Adspower API at {api_base_url}api/v1/user/update")
        print(f"Request Headers: {headers}")
        print(f"Request Body: {update_data}")

        response = requests.post(api_base_url + "api/v1/user/update", json=update_data, headers=headers)

        # Debugging prints
        print(f"API Request sent for user_id: {user_id}")
        print(f"API Response Status Code: {response.status_code}")
        print(f"API Response: {response.text}")

        # Check for error messages in the response body
        if response.status_code != 200 or 'error' in response.text.lower():
            print(f"Error in API response for user ID {user_id}: {response.text}")
        else:
            print(f"Successfully updated Adspower API for user ID: {user_id}")
        time.sleep(1)

### Google Sheet Profile Update - Last Opened
class GSheetLastOpen:

    def __init__(self, config):
        self.config = config
        self.setup_google_sheets(config)

    # Google Sheets setup
    def setup_google_sheets(self, config):
        self.config = config
        credentials = Credentials.from_service_account_info(
            self.config['service_account'], 
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        self.service = build('sheets', 'v4', credentials=credentials)

    def get_group_id(self, group_name):
        api_base_url = f"http://{self.config['adspower']['host']}:{self.config['adspower']['port']}/"
        headers = {'Authorization': self.config['adspower']['api_key']}
        params = {"group_name": group_name}
        try:
            response = requests.get(f"{api_base_url}api/v1/group/list", params=params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if data["code"] == 0:
                    return next((group["group_id"] for group in data["data"]["list"] if group["group_name"].lower() == group_name.lower()), None)
                else:
                    print(f"Error retrieving group ID: {data['msg']}")
            else:
                print(f"HTTP Error {response.status_code} while retrieving group ID")
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")

    def get_user_ids_by_group(self, group_id):
        api_base_url = f"http://{self.config['adspower']['host']}:{self.config['adspower']['port']}/"
        headers = {'Authorization': self.config['adspower']['api_key']}
        user_ids = []
        page = 1
        page_size = 100

        while True:
            params = {"group_id": group_id, "page": page, "page_size": page_size}
            response = requests.get(api_base_url + "api/v1/user/list", params, headers=headers)

            if response.status_code != 200:
                print(f"Failed to fetch data for page {page}: Status code {response.status_code}")
                break

            response_data = response.json()
            if 'code' not in response_data or response_data['code'] != 0:
                print(f"API error for page {page}: {response_data}")
                break

            data = response_data.get('data', {}).get('list', [])
            print(f"Fetched {len(data)} user IDs for page {page}")

            if not data:
                print(f"No more data available after page {page}")
                break

            for user in data:
                print(f"Adding user ID: {user['user_id']}")
                user_ids.append(user['user_id'])

            if len(data) < page_size:
                print(f"Reached last page: {page}")
                break

            page += 1
            time.sleep(1)

        return user_ids

    def get_user_info(self, user_id):
        api_base_url = f"http://{self.config['adspower']['host']}:{self.config['adspower']['port']}/"
        params = {"user_id": user_id}
        headers = {'Authorization': self.config['adspower']['api_key']}
        
        try:
            response = requests.get(api_base_url + 'api/v1/user/list', params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'list' in data['data']:
                    return data["data"]["list"][0]
                else:
                    return None
            else:
                return None
            time.sleep(1)
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def update_sheet(self, user_id, group_name, remark, last_open_time, output_sheets):
        # Extracting the first line of the remark
        first_line_remark = remark.split('\n')[0] if remark else ''

        formatted_last_open_time = GSheetLastOpen.format_last_open_time(last_open_time)
        current_time = datetime.now().strftime('%d/%m %H:%M')
        spreadsheet_id = self.config['google_sheets']['spreadsheets'][0]['id']

        # Retrieve the current data from the sheet
        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=f'{output_sheets}').execute()
        values = result.get('values', [])

        # Initialize row_index for appending a new row
        row_index = len(values) + 1

        # Check if user_id exists and find its row index
        for index, row in enumerate(values):
            if row and row[0] == user_id:
                row_index = index + 1
                break

        # Prepare the data to be written, including only the first line of the remark
        data_to_write = [user_id, group_name, first_line_remark, formatted_last_open_time, current_time]
        update_range = f'{output_sheets}!A{row_index}:E{row_index}'
        body = {'values': [data_to_write]}

        # Write the data to the sheet
        sheet.values().update(spreadsheetId=spreadsheet_id, range=update_range, valueInputOption='USER_ENTERED', body=body).execute()

        # Log the operation
        if row_index <= len(values):
            print(f"Updated existing data for user ID {user_id} at row {row_index}.")
        else:
            print(f"Appended new data for user ID {user_id} at row {row_index}.")

    def process_groups_by_name(group_names,self):
        for name in group_names:
            group_id = self.get_group_id(name)
            if group_id:
                self.process_group(group_id)
            else:
                print(f"Could not process group: {name}")
            time.sleep(1)

    def process_group(group_id,self):
        user_ids = self.get_user_ids_by_group(group_id)
        processed_user_ids = []

        for user_id in user_ids:
            user_info = self.get_user_info(user_id,self)
            if user_info:
                self.update_sheet(user_id, user_info.get('group_name', ''), user_info.get('remark', ''), user_info.get('last_open_time', ''))
                processed_user_ids.append(user_id)
            time.sleep(1)

        # Check for missed user IDs
        missed_user_ids = [uid for uid in user_ids if uid not in processed_user_ids]
        if missed_user_ids:
            print(f"Reprocessing missed user IDs for group {group_id}: {missed_user_ids}")
            for user_id in missed_user_ids:
                user_info = self.get_user_info(user_id,self)
                if user_info:
                    self.update_sheet(user_id, user_info.get('group_name', ''), user_info.get('remark', ''), user_info.get('last_open_time', ''))
                time.sleep(1)

        print(f"Completed processing group {group_id}")

    def format_last_open_time(timestamp):
        if not timestamp or timestamp == "0":
            return "Never Opened"
        try:
            # Convert Unix timestamp to datetime object
            utc_time = datetime.fromtimestamp(int(timestamp))
            # Check if the date is 01 Jan 1970, which corresponds to a timestamp of 0
            if utc_time.year == 1970 and utc_time.month == 1 and utc_time.day == 1:
                return "Never Opened"
            # Format the time
            return utc_time.strftime('%d %b %y %I:%M %p')
        except (ValueError, OSError):
            return "Never Opened"
 ################   

    def run(self, output_sheets):
        print(f"Processing sheet: {output_sheets}")
        # Extract group names from the config
        group_names = self.config['groups']

        # Iterate over each sheet name specified in the config
        for output_sheets in self.config['google_sheets']['spreadsheets'][0]['output_sheets']:
            # Process each group for the current sheet
            for group_name in group_names:
                print(f"Processing group '{group_name}' in sheet '{output_sheets}'")
                self.process_group_for_sheet(group_name, output_sheets)

    def process_group_for_sheet(self, group_name, output_sheets):
        print(f"Fetching group ID for '{group_name}'")
        # Get the group ID for the current group
        group_id = self.get_group_id(group_name)
        time.sleep(1)
        if not group_id:
            print(f"Group ID not found for {group_name}")
            return

        # Get user IDs for the group
        print(f"Retrieving user IDs for group '{group_name}' (Group ID: {group_id})")
        user_ids = self.get_user_ids_by_group(group_id)
        time.sleep(1)
        for user_id in user_ids:
            # Get user info for each user ID
            print(f"Fetching user info for User ID: {user_id}")
            user_info = self.get_user_info(user_id)
            if not user_info:
                print(f"No user info found for user ID {user_id}")
                continue

            # Extract data from user_info and update the sheet
            print(f"Updating sheet '{output_sheets}' for User ID {user_id}")
            remark = user_info.get('remark', '')
            last_open_time = user_info.get('last_open_time', '')
            self.update_sheet(user_id, group_name, remark, last_open_time, output_sheets)

 ###########   
    
def calculate_next_run_time(config):
    start_time = datetime.strptime(config['schedule']['start_time'], "%H:%M")
    now = datetime.now()
    runs_per_day = config['schedule']['runs_per_day']
    interval = 24 // runs_per_day

    next_run = datetime(now.year, now.month, now.day, start_time.hour, start_time.minute)
    while now >= next_run:
        next_run = next_run + timedelta(hours=interval)

    return next_run

def format_timedelta(td, next_run_time):
    days, seconds = td.days, td.seconds
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    next_run_str = next_run_time.strftime('%H:%M')
    return f"{days} days, {hours} hours, {minutes} minutes, {seconds} seconds until next scheduled run at {next_run_str}"


if __name__ == "__main__":
    print("Starting script execution...")
    with open('config.json') as f:
        config = json.load(f)
        print("Configuration loaded successfully.")

    # Run the Google Sheets Updater script
    print("Running Google Sheets Updater...")
    google_sheets_updater = GoogleSheetsUpdater(config)
    google_sheets_updater.run()
    print("Google Sheets Updater completed.")

    # Run the GSheetLastOpen script for each output sheet
    gsheet_last_open = GSheetLastOpen(config)
    for output_sheet in config['google_sheets']['spreadsheets'][0]['output_sheets']:
        print(f"Processing output sheet: {output_sheet}")
        gsheet_last_open.run(output_sheet)
    print("GSheetLastOpen processing completed for all output sheets.")

    # Start the scheduling loop
    print("Entering scheduling loop...")
    while True:
        next_run = calculate_next_run_time(config)
        while True:
            current_time = datetime.now()
            time_to_next_run = next_run - current_time
            if time_to_next_run.total_seconds() <= 0:
                break
            sleep_time = min(3600, time_to_next_run.total_seconds())  # Sleep for 60 minutes or until the next run time
            print(format_timedelta(time_to_next_run, next_run))  # Pass the actual time remaining
            time.sleep(sleep_time)
            if sleep_time < 3600:
                break  # Exit the inner loop if it's time for the next run

        # Run the scripts again at the scheduled time
        print("Waking up for scheduled run.")
        google_sheets_updater.run()
        for output_sheet in config['google_sheets']['spreadsheets'][0]['output_sheets']:
            gsheet_last_open.run(output_sheet)
        print("Scheduled run completed. Re-entering sleep until next schedule.")
