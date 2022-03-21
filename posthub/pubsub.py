
import psycopg2
import json
import psycopg2.extensions
from basepy.log import logger

class PubSubChannel:
    def __init__(self, channel_name, db_url):
        self.channel_name = channel_name
        self.db_url = db_url
        self._conn = None
        self._callback = None

    @property
    def conn(self):
        if not self._conn:
            self._conn = psycopg2.connect(self.db_url)
            self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        return self._conn

    def publish(self, message):
        if type(message) in (dict,):
            dumped = json.dumps(message)
        else:
            dumped = json.dumps({'message': message})
        dumped = dumped.encode('utf-8')
        if len(dumped) > 8000:
            raise Exception('publish message size must less then 8kb, got {}'.format(len(dumped)))
        cur = self.conn.cursor()
        cur.execute(b'NOTIFY {}, {};'.format(self.channel_name.encode('utf-8'), dumped))
        cur.commit()


    def subscribe(self, callback):
        if not callable(callback):
            raise Exception('callback {} must be callable.'.format(callback))
        if self._callback:
            raise Exception('callback already been setted.')
        self._callback = callback


    def listen(self, once=True):
        cur = self.conn.cursor()
        cur.execute('LISTEN {};'.format(self.channel_name))
        conn = self.conn
        while True:
            if select.select([conn],[],[],5) == ([],[],[]):
                pass
            else:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)
                    listened = {'channel':notify.channel, 'payload':notify.payload}
                    if once:
                        return listened
                    else:
                        yield listened

    def run_loop(self):
        while True:
            listened = self.listen()
            try:
                self._callback(listened)
            except Exception as ex:
                logger.error(str(ex))
