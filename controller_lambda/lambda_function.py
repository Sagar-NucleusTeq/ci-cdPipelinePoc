def lambda_handler(event, context):
    response = {
        'statusCode': 200,
        'body': 'This is Hello World #2222',
        'headers': {
            'Content-Type': 'application/json'
        }
    }
    return response