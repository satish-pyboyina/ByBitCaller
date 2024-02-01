import config, json
import boto3
import os


# def lambda_handler(event, context):
#     print(event)
#     print("*****")
#     print(context)
#     print("*****")
#     return {
#         'statusCode': 200,
#         'body': json.dumps('WebSocket Default Lambda Triggered')
#     }


s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Log the entire event for debugging
        print("WebSocket Event:", json.dumps(event, indent=2))
        
        #save event in signal variable
        signal = json.loads(json.dumps(event, indent=2))
        
        #fetch trigger-time from signal
        print(f"Signal: {signal['trigger_time']}")
        
        trigger_time = signal['trigger_time']

        # Save message to S3 bucket (ensure bucket_name and object_key are set in config.py)
        bucket_name = config.bucket_name
        object_key = config.object_key

        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(event),
            ContentType='application/json'
        )

        response = {
            "statusCode": 200,
            "body": json.dumps({"message": "Message saved to S3 successfully"})
        }
    except Exception as e:
        response = {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

    return response