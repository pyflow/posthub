

import psycopg2
import json
import psycopg2.extensions
from basepy.log import logger
import select

class QueueListener:
    def __init__(self, db_url, queue_name=None):
        self.channels = []
        if queue_name:
            self.listen_to_queue(queue_name)
        self.db_url = db_url
        self._conn = None

    @property
    def conn(self):
        if not self._conn:
            self._conn = psycopg2.connect(self.db_url)
            self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        return self._conn

    def listen_to_queue(self, queue_name):
        channel_name = f'posthub_queue_{queue_name}'
        if channel_name not in self.channels:
            self.channels.append(channel_name)
        cur = self.conn.cursor()
        cur.execute('LISTEN {};'.format(channel_name))
        self.conn.commit()


    def listen_to(self, queue_name=None, queues=None):
        if queue_name:
            self.listen_to_queue(queue_name)
        for queue_name in queues:
            self.listen_to_queue(queue_name)


    def read_once(self):
        conn = self.conn
        listened = []
        if select.select([conn],[],[],5) == ([],[],[]):
            return []
        else:
            conn.poll()
            while conn.notifies:
                for notify in conn.notifies:
                    listened.append({'channel':notify.channel, 'payload':notify.payload})
            return listened


class Queue:
    def __init__(self, db_url, queue_name):
        self.queue_name = queue_name
        self.db_url = db_url
        self._conn = None

    @property
    def conn(self):
        if not self._conn:
            self._conn = psycopg2.connect(self.db_url)
            self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        return self._conn