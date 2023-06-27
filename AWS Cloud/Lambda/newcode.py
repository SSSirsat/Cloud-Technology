import json

def lambda_handler(event, context):
    print(event)
    output_records = []
    
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf-8')
        modified_payload = convert_to_single_line_json(payload)
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': modified_payload
        }
        
        output_records.append(output_record)
    
    return {'records': output_records}

def convert_to_single_line_json(payload):
    parsed_payload = json.loads(payload)
    single_line_payload = json.dumps(parsed_payload)
    return single_line_payload
