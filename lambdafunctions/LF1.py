import json
import boto3
from datetime import datetime

sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
user_state_table = dynamodb.Table('UserSearchState')
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/329599653658/DiningRequestsQueue"

def lambda_handler(event, context):
    print("Lex event:", json.dumps(event))
    session_state = event.get("sessionState", {})
    intent = session_state.get("intent", {})
    intent_name = intent.get("name")
    if intent_name == "GreetingIntent":
        response_text = "Hi there, how can I help you today?"
    elif intent_name == "ThankYouIntent":
        response_text = "You're welcome!"
    elif intent_name == "DiningSuggestionsIntent":
        slots = intent.get("slots", {})
        location = slots.get("Location", {}).get("value", {}).get("interpretedValue")
        cuisine = slots.get("Cuisine", {}).get("value", {}).get("interpretedValue")
        dining_time = slots.get("DiningTime", {}).get("value", {}).get("interpretedValue")
        party_size = slots.get("PartySize", {}).get("value", {}).get("interpretedValue")
        email = slots.get("Email", {}).get("value", {}).get("interpretedValue")
        message = {
            "Location": location,
            "Cuisine": cuisine,
            "DiningTime": dining_time,
            "PartySize": party_size,
            "Email": email,
            "RequestId": context.aws_request_id
        }
        try:
            response = sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=json.dumps(message))
            print("Message sent to SQS, MessageId:", response.get('MessageId'))
        except Exception as e:
            print("Error sending message to SQS:", e)
        if email and location and cuisine:
            try:
                user_state_table.put_item(Item={
                    "UserEmail": email,
                    "LastLocation": location,
                    "LastCuisine": cuisine,
                    "Timestamp": datetime.utcnow().isoformat()
                })
                print("Saved user state for", email)
            except Exception as e:
                print("Error saving user state:", e)
        response_text = ("Got it! I've received your dining request. You will be notified via email once I have some restaurant suggestions.")
    else:
        response_text = "Sorry, I didn't understand that."
    lex_response = {
        "sessionState": {
            "sessionAttributes": session_state.get("sessionAttributes", {}),
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": intent_name,
                "state": "Fulfilled",
                "slots": intent.get("slots", {})
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": response_text
            }
        ]
    }
    return lex_response