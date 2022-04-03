import importlib.resources
import psycopg2
import json
import psycopg2.extensions
import os

class PosthubSchema:
    def __init__(self, db_url):
        self.db_url = db_url
        self._conn = None
        with importlib.resources.path('posthub', '__init__.py') as f:
            self._sql_dir = os.path.join(os.path.dirname(f), 'sql')
            self._schema_sql = os.path.join(self._sql_dir, 'schema.sql')
            self._drop_sql = os.path.join(self._sql_dir, 'drop.sql')

    @property
    def conn(self):
        if not self._conn:
            self._conn = psycopg2.connect(self.db_url)
            self._conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        return self._conn


    def create(self):
        sql = ''
        with open(self._schema_sql, 'rb') as f:
            sql = f.read().decode('utf-8')
        conn = self.conn
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


    def delete(self):
        sql = ''
        with open(self._drop_sql, 'rb') as f:
            sql = f.read().decode('utf-8')
        conn = self.conn
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()


def cli_schema():
    import sys
    argv = sys.argv[1:]
    help_text = 'python -m posthub create/drop  postgres://user:pass@127.0.0.1/db'
    if len(argv) < 2:
        print(help_text)
        sys.exit(-1)
    cmd, db_url = argv[0].strip().lower(), argv[1]
    if cmd not in ['create', 'drop']:
        print('command only support create/drop.')
        sys.exit(-1)
    schema = PosthubSchema(db_url=db_url)
    if cmd == 'create':
        schema.create()
    elif cmd == 'drop':
        schema.drop()