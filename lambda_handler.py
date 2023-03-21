import base64
import json
import boto3
from decimal import Decimal

print('Loading function')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    dynamodb_client = boto3.resource('dynamodb',region_name='us-east-1')
    table = dynamodb_client.Table('cloud_table')
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        std_payload = json.loads(payload)
        std_payload['52Weekhigh'] = str(Decimal(round(std_payload['52Weekhigh'],2)))
        std_payload['52WeekLow'] = str(Decimal(round(std_payload['52WeekLow'],2)))
        std_payload['POI'] = str(Decimal(round(std_payload['POI'],2)))
        table.put_item(Item=std_payload)
        print("Decoded payload: " + payload)
        client = boto3.client('sns', region_name='us-east-1')
        topic_arn = "arn:aws:sns:us-east-1:279147770544:anamoly"

        try:
            client.publish(TopicArn=topic_arn, Message=f"POI - {std_payload}", Subject="Anamoly")
            result = 1
        except Exception:
            result = 0
        
    return 'Successfully processed {} records.'.format(len(event['Records']))