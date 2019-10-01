import threading
from constants import CONSTANTS
import logging
import typing
import TwitterAPI as twitter_api
import time
import json


class TwitterDataStream(threading.Thread):
    data_lock = threading.Lock()
    latest_tweet = {}

    def __init__(
        self,
        kinesis,
        twitter,
        name: str='TwitterDataStream',
        logger: logging.StreamHandler=None,
        topics: typing.List[str]=[],
    ):
        self._stopevent = threading.Event()
        self._sleepperiod = 0
        threading.Thread.__init__(self, name=name)
        self.daemon = True
        self._logger = logger
        self.topics = topics
        self.kinesis = kinesis
        self.twitter = twitter
        self._count = 0
        self._start_time = 0

    def status(self):
        elapsed_time_ns = time.time_ns() - self._start_time
        seconds = int(elapsed_time_ns/1000000000)
        hours = int(seconds/3600)
        seconds = seconds % 3600
        minutes = int(seconds/60)
        seconds = seconds % 60
        return {
            'count': self._count,
            'start_time': self._start_time,
            'elapsed_time_ns': elapsed_time_ns,
            'elapsed_time': f'{hours}h {minutes}m {seconds}s',
        }

    def run(self):
        self._start_time = time.time_ns()
        self._logger and self._logger.info(
            f'Starting {self.name} at {self._start_time}'
        )
        while not self._stopevent.isSet():
            try:
                params = {
                    'track': self.topics,
                }
                stream = self.twitter.request('statuses/filter', params)
                for item in stream:
                    if self._stopevent.isSet():
                        break
                    with self.data_lock:
                        self.latest_tweet = item
                    self.kinesis.put_record(
                        StreamName='twitter_stream',
                        Data=json.dumps(item),
                        PartitionKey=item.get('id_str'),
                    )
                    self._count += 1
                    if self._logger is not None:
                        info = item.get('user').get('screen_name')
                        text = item.get('text')
                        self._logger.info(
                            f'<{self._count}> @{info}: {text}'
                        )
                    self._stopevent.wait(self._sleepperiod)
                    if 'disconnect' in item:
                        event = item.get('disconnect')
                        if event.get('code') in [2, 5, 6, 7]:
                            self._logger.error(event['reason'])
                            raise Exception(event['reason'])
                        else:
                            self._logger.error(event['reason'])
                            break
            except twitter_api.TwitterRequestError as e:
                if e.status_code < 500:
                    raise
                else:
                    pass
            except twitter_api.TwitterConnectionError:
                pass
        elapsed_time = self.status().get('elapsed_time')
        self._logger and self._logger.info(
            f'{self._count} tweets processed during {elapsed_time}'
        )
        self._logger and self._logger.info(f'{self.getName()} ends')

    def join(self, timeout=None):
        """ Stop the thread and wait for it to end. """
        self._stopevent.set()
        super().join(timeout)
