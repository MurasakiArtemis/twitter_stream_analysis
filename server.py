import flask
import argparse
from constants import CONSTANTS
import twitter_hook
import dateutil
import TwitterAPI as twitter
import boto3
from logging.config import dictConfig


dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})

app = flask.Flask(__name__)
kinesis = boto3.client(
    'kinesis',
    aws_access_key_id=CONSTANTS.get('AWS').get('aws_access_key_id'),
    aws_secret_access_key=CONSTANTS.get('AWS').get('aws_secret_access_key'),
    region_name=CONSTANTS.get('AWS').get('region_name'),
)
twitter_client = twitter.TwitterAPI(
    CONSTANTS.get('TWITTER').get('consumer_key'),
    CONSTANTS.get('TWITTER').get('consumer_secret'),
    CONSTANTS.get('TWITTER').get('access_token_key'),
    CONSTANTS.get('TWITTER').get('access_token_secret'),
)
topics = ['trans', 'transgender']
hook = twitter_hook.TwitterDataStream(
    kinesis,
    twitter_client,
    logger=app.logger,
    topics=topics
)


@app.route('/')
def index():
    if hook.is_alive():
        latest_tweet = twitter_hook.TwitterDataStream.latest_tweet
        json_response = flask.jsonify({
            'name': latest_tweet.get('user').get('screen_name'),
            'time': dateutil.parser.parse(
                latest_tweet.get('created_at')
            ).strftime("%B %d, %Y – %H:%M %z"),
            'tweet': latest_tweet,
        })
        status = CONSTANTS.get('HTTP_STATUS').get('200_OK')
    else:
        json_response = flask.jsonify({'error': 'Service not started'})
        status = CONSTANTS.get('HTTP_STATUS').get('404_NOT_FOUND')
    return flask.make_response(
        json_response,
        status,
    )


@app.route(CONSTANTS.get('ENDPOINT').get('START'))
def start():
    global hook
    if not hook.is_alive():
        hook = twitter_hook.TwitterDataStream(
            kinesis,
            twitter_client,
            logger=app.logger,
            topics=topics
        )
        hook.start()
    json_response = flask.jsonify({'info': 'Service has started'})
    return flask.make_response(
        json_response,
        CONSTANTS.get('HTTP_STATUS').get('200_OK'),
    )


@app.route(CONSTANTS.get('ENDPOINT').get('STOP'))
def stop():
    if hook.is_alive():
        hook.join()
    json_response = flask.jsonify({'info': 'Service has stopped'})
    return flask.make_response(
        json_response,
        CONSTANTS.get('HTTP_STATUS').get('200_OK'),
    )


@app.route(
    CONSTANTS.get('ENDPOINT').get('REPORT'),
    defaults={'tweet_id': None}
)
@app.route(CONSTANTS.get('ENDPOINT').get('REPORT') + '<int:tweet_id>')
def report(tweet_id):
    if hook.is_alive():
        thread_status = hook.status()
        json_response = flask.jsonify(thread_status)
        status = CONSTANTS.get('HTTP_STATUS').get('200_OK')
        if tweet_id is not None:
            count = thread_status.get('count')
            elapsed_time = thread_status.get('elapsed_time')
            separator = ', '
            topics_list = separator.join(topics)
            twitter_client.request(
                'statuses/update',
                {
                    'status': f'{count} tweets processed '
                              f'during {elapsed_time} '
                              f'keywords: {topics_list}',
                    'in_reply_to_status_id': tweet_id,
                },
            )
    else:
        json_response = flask.jsonify({'error': 'Service not started'})
        status = CONSTANTS.get('HTTP_STATUS').get('404_NOT_FOUND')
    return flask.make_response(
        json_response,
        status,
    )


@app.errorhandler(404)
def page_not_found(error):
    json_response = flask.jsonify({'error': 'Page not found'})
    return flask.make_response(
        json_response,
        CONSTANTS.get('HTTP_STATUS').get('404_NOT_FOUND'),
    )


if __name__ == '__main__':
    hook.start()
    app.run(port=CONSTANTS['PORT'])
