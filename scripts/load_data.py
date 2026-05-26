import csv
import requests
import os

API_URL = "http://127.0.0.1:8000/api/cities/"
CSV_FILE_PATH = "CountryCode-City.csv"

def load_data():
    """
    Reads the CSV file and sends a POST request for each row to the API.
    """
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: Could not find '{CSV_FILE_PATH}' in the project root.")
        return

    print("Starting data ingestion...")
    success_count = 0
    error_count = 0

    with open(CSV_FILE_PATH, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            city_name = row.get('city')
            country_code = row.get('countyCode')

            if not city_name or not country_code:
                continue

            payload = {
                "name": city_name.strip(),
                "country_code": country_code.strip()
            }

            try:
                response = requests.post(API_URL, json=payload)
                if response.status_code == 200:
                    success_count += 1
                else:
                    error_count += 1
                    print(f"Failed to insert {city_name}: {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"Network error for {city_name}: {e}")
                error_count += 1

            total_processed = success_count + error_count
            if total_processed % 1000 == 0:
                print(f"Processed {total_processed} records...")

    print(f"Ingestion completed. Success: {success_count}, Errors: {error_count}")

if __name__ == "__main__":
    load_data()