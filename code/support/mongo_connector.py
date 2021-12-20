''' Based on: https://gist.github.com/mangangreg/f84d8899e961c48a8539b813e746eac6
'''
import os
import sys
import time
from pathlib import Path

from pymongo import MongoClient
from dotenv import load_dotenv

HERE = Path(__file__).parent

class SCALESMongo:
    def __init__(self, user=None, password=None, host=None, port=None, database=None, env_file=HERE/'.mongo.env'):

        # Load the env file
        load_dotenv(env_file)

        self.user = user or os.getenv('MONGO_USER')
        self.password = password or os.getenv('MONGO_PASSWORD')
        self.host = host or os.getenv('MONGO_HOST') or 'localhost'
        self.port = port or os.getenv('MONGO_PORT') or 27017
        self.database = database or os.getenv('MONGO_DATABASE')

        # Build the URI
        self.URI = self._constructURI()

        # Initialise connection and db
        self.connection = None
        self.db = None

    def _constructURI(self):
        return f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}"

    def connect(self):
        self.connection = MongoClient(self.URI)
        self.db = self.connection[self.database]

class SaneResult:
    ''' A sane/readable Pymongo result object '''

    def __init__(self, res):
        self.res = res
        self.counts = self.build_counts(res)
        self.counts_string = " ".join(f"{k}={v}" for k,v in self.counts.items()).rstrip()

    def __repr__(self):
        if not self.res:
            return ''
        class_str = str(self.res.__class__).strip('<> ')
        return f"<{class_str} acknowledged={self.res.acknowledged} {self.counts_string}>"

    def build_counts(self, res):
        ''' Find the attributes that contain insert/update count numbers '''

        counts = {}
        for k in dir(res):
            if k.endswith('count'):
                counts.update({k: res.__getattribute__(k)})
            elif k.endswith('_ids') and not k.startswith('_'):
                counts.update({k.split('_ids',maxsplit=1)[0]: len(res.__getattribute__(k))})

        return counts


