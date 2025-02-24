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
        body = {}
    session_id = body.get("sessionId")
    if not session_id:
        session_id = context.aws_request_id
    messages = body.get("messages", [])
    if not messages:
        return {"statusCode": 400, "body": json.dumps({"error": "No message provided"})}
    user_message = messages[0].get("unstructured", {}).get("text", "")
    if not user_message:
        return {"statusCode": 400, "body": json.dumps({"error": "Message text is empty"})}
    if user_message.strip().lower() == "recommend a restaurant":
        payload = json.dumps({"sessionId": session_id, "email": body.get("email")})
        lf3_response = lambda_client.invoke(FunctionName="LF3", InvocationType="RequestResponse", Payload=payload)
        print('Invoking LF3')
        response_payload = json.load(lf3_response["Payload"])
        print("response_payload", response_payload)
        #body_dict = json.loads(response_payload['body'])
        response_message = {
        "type": "unstructured",
        "unstructured": {
            "id": session_id,
            "text": response_payload,
            "timestamp": datetime.datetime.utcnow().isoformat()
            }
        }
    
        response_body = {
            "messages": [response_message]
        }
        return {"statusCode": 200, "headers": {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}, "body": json.dumps(response_body)}
    try:
        lex_response = lex_client.recognize_text(botId=BOT_ID, botAliasId=BOT_ALIAS_ID, localeId=LOCALE_ID, sessionId=session_id, text=user_message)
        print("Lex response:", json.dumps(lex_response))
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": "Error calling Lex", "details": str(e)})}
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
                "text": "I'm still under development. Please come back later.",
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
        })
    bot_response = {"messages": response_messages}
    return {"statusCode": 200, "headers": {"Access-Control-Allow-Origin": "*", "Content-Type": "application/json"}, "body": json.dumps(bot_response)}