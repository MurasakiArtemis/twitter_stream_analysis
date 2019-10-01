import os

CONSTANTS = {
    'PORT': os.environ.get('PORT', 3001),
    # 'AWS': {
    #     'aws_access_key_id': os.environ['aws_access_key_id'],
    #     'aws_secret_access_key': os.environ['aws_secret_access_key'],
    #     'region_name': os.environ['aws_region_name'],
    # },
    'TWITTER': {
        'consumer_key': os.environ['twitter_consumer_key'],
        'consumer_secret': os.environ['twitter_consumer_secret'],
        'access_token_key': os.environ['twitter_access_token'],
        'access_token_secret': os.environ['twitter_access_token_secret'],
    },
    'HTTP_STATUS': {
        '404_NOT_FOUND': 404,
        '200_OK': 200,
    },
    'ENDPOINT': {
        'START': '/api/start/',
        'STOP': '/api/stop/',
        'REPORT': '/api/report/',
    },
}
