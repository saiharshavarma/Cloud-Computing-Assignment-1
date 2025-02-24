import requests
import time
import json
import boto3
from datetime import datetime
from decimal import Decimal

API_KEY = "fBuRBceMhZoWbPXwbFaTwBn2ajFOxNGtc6fhiA6yNYpsU5Y3IkLVId5moX5wzVauv1tVt2Penk6jNGbHPSZKVT0uaBcSBLM89tV0xbjf4GdHpGxJ96NznfOSjNG7Z3Yx"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}"
}
BASE_URL = "https://api.yelp.com/v3/businesses/search"

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

cuisines = ["Chinese", "Italian", "Japanese", "Mexican", "Indian"]

def convert_floats_to_decimal(item):
    if isinstance(item, list):
        return [convert_floats_to_decimal(i) for i in item]
    elif isinstance(item, dict):
        return {k: convert_floats_to_decimal(v) for k, v in item.items()}
    elif isinstance(item, float):
        return Decimal(str(item))
    else:
        return item

def search_yelp(cuisine, offset=0, limit=50):
    params = {
        "term": f"{cuisine} restaurants",
        "location": "Manhattan, New York",
        "limit": limit,
        "offset": offset,
    }
    response = requests.get(BASE_URL, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error:", response.status_code, response.text)
        return None

def process_and_store(cuisine, businesses):
    for business in businesses:
        business_id = business.get("id")
        name = business.get("name")
        address = ", ".join(business.get("location", {}).get("display_address", []))
        coordinates = business.get("coordinates", {})
        coordinates = convert_floats_to_decimal(coordinates)
        
        review_count = business.get("review_count")
        rating = business.get("rating")
        zip_code = business.get("location", {}).get("zip_code")
        inserted_at = datetime.utcnow().isoformat()

        item = {
            "BusinessId": business_id,
            "Name": name,
            "Address": address,
            "Coordinates": coordinates,
            "ReviewCount": review_count,
            "Rating": rating,
            "ZipCode": zip_code,
            "Cuisine": cuisine,
            "InsertedAtTimestamp": inserted_at
        }
        
        item = convert_floats_to_decimal(item)
        
        try:
            table.put_item(Item=item)
            print(f"Stored: {business_id}")
        except Exception as e:
            print(f"Error storing {business_id}: {e}")

def main():
    for cuisine in cuisines:
        print(f"Processing cuisine: {cuisine}")
        offset = 0
        while offset < 1000:
            data = search_yelp(cuisine, offset=offset)
            if data is None or "businesses" not in data:
                break

            businesses = data["businesses"]
            if not businesses:
                break

            process_and_store(cuisine, businesses)
            offset += len(businesses)
            print(f"Fetched {offset} results for {cuisine}")
            time.sleep(1)

if __name__ == "__main__":
    main()