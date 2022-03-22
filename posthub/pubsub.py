
import psycopg2
import json
import psycopg2.extensions
from basepy.log import logger
import select

class PubSub:
    def __init__(self, channel_name, db_url):
        self.channel_name = channel_name
        self.db_url = db_url
        self._conn = None
        self._callback = None
        self.more_channels = []

    @property
    def conn(self):
        if not self._conn:
            self._conn = psycopg2.connect(self.db_url)
            self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        return self._conn

    def publish(self, message, channel=None):
        if type(message) in (dict,):
            dumped = json.dumps(message)
        else:
            dumped = json.dumps({'message': message})
        dumped_bytes = dumped.encode('utf-8')
        if len(dumped_bytes) > 8000:
            raise Exception('publish message size must less then 8kb, got {}'.format(len(dumped_bytes)))
        cur = self.conn.cursor()
        channel_name = channel or self.channel_name
        cur.execute("NOTIFY {}, '{}';".format(channel_name, dumped))
        self.conn.commit()


    def subscribe(self, callback = None, more_channels=None):
        if callback:
            if not callable(callback):
                raise Exception('callback {} must be callable.'.format(callback))
            if self._callback:
                raise Exception('callback already been setted.')
            self._callback = callback
        if more_channels:
            for channel in more_channels:
                self.more_channels.append(channel)
        cur = self.conn.cursor()
        cur.execute('LISTEN {};'.format(self.channel_name))
        for channel_name in self.more_channels:
            cur.execute('LISTEN {};'.format(channel_name))
        self.conn.commit()


    def listen(self, once=False):
        conn = self.conn
        while True:
            if select.select([conn],[],[],5) == ([],[],[]):
                pass
            else:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    listened = {'channel':notify.channel, 'payload':notify.payload}
                    if once:
                        yield listened
                        return
                    else:
                        yield listened

    def run_loop(self, max_got = -1):
        num = 0
        while True:
            for listened in self.listen():
                try:
                    self._callback(listened)
                    num += 1
                except Exception as ex:
                    logger.error(str(ex))

                if max_got > 0 and num >= max_got:
                    return
