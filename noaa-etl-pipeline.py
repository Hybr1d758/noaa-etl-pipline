import requests
from dotenv import load_dotenv, find_dotenv
import os
import datetime
import boto3
import json
from botocore.exceptions import NoCredentialsError

def load_token():
    print("Current working directory:", os.getcwd())
    dotenv_path = find_dotenv('.env')
    print("Found .env at:", dotenv_path)
    load_dotenv(dotenv_path)
    token = os.getenv("NOAA_TOKEN")
    print("NOAA_TOKEN loaded:", token)
    if not token:
        raise ValueError("NOAA_TOKEN not found in .env file.")
    return token

def build_headers(token):
    return {
        "token": token,
        "Accept": "application/json"
    }

def fetch_data(url, headers, params=None):
    try:
        print("Requesting:", url)
        print("Headers:", headers)
        print("Params:", params)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err} - {response.text}")
    except Exception as err:
        print(f"Other error occurred: {err}")
    return None

def test_datasets_endpoint(headers):
    url = "https://www.ncei.noaa.gov/cdo-web/api/v2/datasets"
    try:
        print("Requesting:", url)
        print("Headers:", headers)
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print("/datasets endpoint response:", response.json())
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err} - {response.text}")
    except Exception as err:
        print(f"Other error occurred: {err}")

def upload_to_s3(data, bucket, key):
    """
    Uploads data (as JSON) to the specified S3 bucket/key.
    """
    s3 = boto3.client('s3')
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        print(f"Successfully uploaded data to s3://{bucket}/{key}")
    except NoCredentialsError:
        print("AWS credentials not found. Please configure them.")
    except Exception as e:
        print(f"Failed to upload to S3: {e}")

def main():
    token = load_token()
    headers = build_headers(token)
    test_datasets_endpoint(headers)
    url = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"  # Example API endpoint
    # Use recent date range: last 7 days
    today = datetime.date.today()
    seven_days_ago = today - datetime.timedelta(days=7)
    params = {
        "datasetid": "GHCND",
        "locationid": "CITY:US390029",
        "startdate": seven_days_ago.strftime("%Y-%m-%d"),
        "enddate": today.strftime("%Y-%m-%d"),
        "limit": 10
    }
    data = fetch_data(url, headers, params)
    if data:
        # Save raw data to S3
        bucket = "your-bucket-name"  # <-- Replace with your actual bucket name
        key = f"noaa_raw/{datetime.date.today()}.json"
        upload_to_s3(data, bucket, key)
        # ...then continue with transformation and Postgres steps
        print(data)
    else:
        print("Failed to fetch data.")

if __name__ == "__main__":
    main()
