import json
import boto3
import urllib.request
import urllib.error
from datetime import datetime
import base64
import os

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
ses = boto3.client('ses', region_name='us-east-1')
user_state_table = dynamodb.Table('UserSearchState')
yelp_table = dynamodb.Table('yelp-restaurants')
ES_ENDPOINT = "https://search-restaurants-search-oun4on4o3zee65phdemmgxyb4y.aos.us-east-1.on.aws"
ES_USERNAME = "saiharshavarma"
ES_PASSWORD = "ss18851@NYU"
SENDER_EMAIL = "saiharshavarma.sangaraju@gmail.com" 

def send_email(recipient, subject, body_text):
    try:
        response = ses.send_email(
            Source=SENDER_EMAIL,
            Destination={
                "ToAddresses": [recipient]
            },
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Text": {"Data": body_text}
                }
            }
        )
        print("Email sent, MessageId:", response.get("MessageId"))
    except Exception as e:
        print("Error sending email:", e)

def get_random_restaurant(cuisine):
    query = {
      "size": 1,
      "query": {
        "function_score": {
          "query": {
            "match": {
              "Cuisine": cuisine.capitalize()
            }
          },
          "random_score": {}
        }
      }
    }
    url = f"{ES_ENDPOINT}/restaurants/_search"
    headers = {"Content-Type": "application/json"}
    credentials = f"{ES_USERNAME}:{ES_PASSWORD}"
    encoded_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
    headers["Authorization"] = f"Basic {encoded_credentials}"
    data = json.dumps(query).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            hits = result.get("hits", {}).get("hits", [])
            if hits:
                return hits[0]["_source"]
    except Exception as e:
        print("Error querying OpenSearch:", e)
    return None

def lambda_handler(event, context):
    print("Recommendation event:", json.dumps(event))
    body = event.get("body")
    if body is None:
        try:
            body = json.loads(event)
        except Exception as e:
            body = {}
    user_email = event.get("email") or body.get("email")
    if not user_email:
        return {"statusCode": 400, "body": json.dumps({"error": "Missing email in request"})}
    try:
        response = user_state_table.get_item(Key={"UserEmail": user_email})
        state = response.get("Item")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": "Error retrieving user state", "details": str(e)})}
    if not state:
        return {"statusCode": 200, "body": json.dumps({"message": "No previous search state found for this user."})}
    last_location = state.get("LastLocation")
    last_cuisine = state.get("LastCuisine")
    if not last_cuisine:
        return {"statusCode": 200, "body": json.dumps({"message": "No cuisine data found in your previous search."})}
    restaurant = get_random_restaurant(last_cuisine)
    if not restaurant:
        return {"statusCode": 200, "body": json.dumps({"message": "No restaurant found based on your previous search."})}
    restaurant_id = restaurant.get("RestaurantId")
    if not restaurant_id:
        return {"statusCode": 200, "body": json.dumps({"message": "No RestaurantId found in search result."})}
    try:
        dynamo_response = yelp_table.get_item(Key={"BusinessId": restaurant_id})
        details = dynamo_response.get("Item", {})
    except Exception as e:
        details = {}
    name = details.get("Name", "Unknown Restaurant")
    address = details.get("Address", "Address not available")
    rating = details.get("Rating", "N/A")
    review_count = details.get("ReviewCount", "N/A")
    recommendation2 = f"Based on your last search in {last_location} for {last_cuisine.capitalize()} cuisine, you will be notified via email once I have some restaurant suggestions."
    recommendation = f"Based on your last search in {last_location} for {last_cuisine.capitalize()} cuisine, here's a recommendation: {name}, located at {address}. It has a rating of {rating} based on {review_count} reviews."
    response_payload = {"message": recommendation, "timestamp": datetime.utcnow().isoformat()}
    send_email(user_email, "Restaurant Recommendation", recommendation)
    return recommendation2