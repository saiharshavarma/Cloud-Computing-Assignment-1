import json
import boto3
import urllib.request
import urllib.error
from decimal import Decimal
from datetime import datetime
import base64

username = "saiharshavarma"
password = "ss18851@NYU"
credentials = f"{username}:{password}"
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

sqs = boto3.client('sqs')
ses = boto3.client('ses', region_name='us-east-1')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/329599653658/DiningRequestsQueue"
ES_ENDPOINT = "https://search-restaurants-search-oun4on4o3zee65phdemmgxyb4y.aos.us-east-1.on.aws"
SENDER_EMAIL = "saiharshavarma.sangaraju@gmail.com" 

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
    headers["Authorization"] = f"Basic {encoded_credentials}"
    data = json.dumps(query).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers)
    
    try:
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            hits = result.get("hits", {}).get("hits", [])
            if hits:
                return hits[0]["_source"]
            else:
                print("No hits found in ES for cuisine:", cuisine)
    except urllib.error.HTTPError as e:
        print("HTTPError querying ES:", e.code, e.reason)
    except urllib.error.URLError as e:
        print("URLError querying ES:", e.reason)
    
    return None

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

def lambda_handler(event, context):
    print("LF2 event:", json.dumps(event))
    
    records = event.get("Records", [])
    if not records:
        print("No records in event")
        return {"status": "No messages"}
    
    for record in records:
        receipt_handle = record.get("receiptHandle")
        try:
            body = json.loads(record.get("body", "{}"))
        except Exception as e:
            print("Error parsing message body:", e)
            continue
        
        cuisine = body.get("Cuisine")
        email = body.get("Email")
        
        if not cuisine or not email:
            print("Missing required parameters in message.")
            continue
        
        restaurant = get_random_restaurant(cuisine)
        if not restaurant:
            print("No restaurant found for cuisine:", cuisine)
            continue
        
        restaurant_id = restaurant.get("RestaurantId")
        if not restaurant_id:
            print("No RestaurantId found in ES document")
            continue
        
        try:
            dynamo_response = table.get_item(Key={"BusinessId": restaurant_id})
            details = dynamo_response.get("Item", {})
        except Exception as e:
            print("Error fetching details from DynamoDB:", e)
            details = {}
        
        name = details.get("Name", "Unknown Restaurant")
        address = details.get("Address", "Address not available")
        rating = details.get("Rating", "N/A")
        review_count = details.get("ReviewCount", "N/A")
        
        email_subject = f"Your {cuisine} restaurant suggestion"
        email_body = (
            f"Hello,\n\n"
            f"Based on your dining request, here is a restaurant suggestion for {cuisine} cuisine:\n\n"
            f"Name: {name}\n"
            f"Address: {address}\n"
            f"Rating: {rating}\n"
            f"Review Count: {review_count}\n\n"
            f"Enjoy your meal!\n\n"
            f"Best regards,\n"
            f"Dining Concierge"
        )
        
        send_email(email, email_subject, email_body)
        print("Processed message for email:", email)
    
    return {"status": "Processed messages"}