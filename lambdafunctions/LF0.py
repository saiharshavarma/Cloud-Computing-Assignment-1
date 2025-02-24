import json
import boto3
import datetime

lex_client = boto3.client('lexv2-runtime')
lambda_client = boto3.client('lambda')
BOT_ID = "EI7RZNZ2YB"
BOT_ALIAS_ID = "TSTALIASID"
LOCALE_ID = "en_US"

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    try:
        body = event.get("body", {})
        if isinstance(body, str):
            body = json.loads(body)
    except Exception as e:
        return {
            "statusCode": 400,
            "headers": {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"},
            "body": json.dumps({"error": "Sorry, we couldn't understand your request. Please try again."})
        }
    session_id = body.get("sessionId") or context.aws_request_id
    messages = body.get("messages", [])
    if not messages:
        return {
            "statusCode": 400,
            "headers": {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"},
            "body": json.dumps({"error": "No message was provided. Please type your message."})
        }
    user_message = messages[0].get("unstructured", {}).get("text", "")
    if not user_message:
        return {
            "statusCode": 400,
            "headers": {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"},
            "body": json.dumps({"error": "It looks like your message is empty. Please enter your request."})
        }
    if user_message.strip().lower() == "recommend a restaurant":
        try:
            payload = json.dumps({"sessionId": session_id, "email": body.get("email")})
            lf3_response = lambda_client.invoke(FunctionName="LF3", InvocationType="RequestResponse", Payload=payload)
            response_payload = json.load(lf3_response["Payload"])
        except Exception as e:
            return {
                "statusCode": 500,
                "headers": {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"},
                "body": json.dumps({"error": "We encountered an issue fetching your recommendation. Please try again later."})
            }
        response_message = {
            "type": "unstructured",
            "unstructured": {
                "id": session_id,
                "text": response_payload,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        }
        response_body = {"messages": [response_message]}
        return {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"},
            "body": json.dumps(response_body)
        }
    try:
        lex_response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text=user_message
        )
        print("Lex response:", json.dumps(lex_response))
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"},
            "body": json.dumps({"error": "We are experiencing technical difficulties contacting our assistant. Please try again later."})
        }
    response_messages = []
    for msg in lex_response.get("messages", []):
        response_messages.append({
            "type": "unstructured",
            "unstructured": {
                "id": session_id,
                "text": msg.get("content", ""),
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        })
    if not response_messages:
        response_messages.append({
            "type": "unstructured",
            "unstructured": {
                "id": session_id,
                "text": "Sorry, we couldn't understand your request. Please try again.",
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        })
    bot_response = {"messages": response_messages}
    return {
        "statusCode": 200,
        "headers": {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"},
        "body": json.dumps(bot_response)
    }