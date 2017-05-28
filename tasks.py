# Filename: run_luigi.py
import luigi
from luigi.s3 import S3Target, S3Client
from luigi.util import inherits
import json
import sys
from datetime import date, timedelta
from functions import *

class spotify_local_file(luigi.ExternalTask):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget("json/spotify_tracks_%s.json" % self.date.strftime('%Y-%m-%d'))

@inherits(spotify_local_file)
class spotify_clean_aws(luigi.Task):
    date = luigi.DateParameter(default = date.today()-timedelta(1))

    def requires(self):
            return self.clone(spotify_local_file)

    def output(self):
        client = S3Client(host = 's3.us-east-2.amazonaws.com')
        return S3Target('s3://myspotifydata/spotify_test_%s.json' % self.date.strftime('%Y-%m-%d'), client=client)

    def run(self):
        data = parse_json("json/spotify_tracks_%s.json" % self.date.strftime('%Y-%m-%d'))
        with self.output().open('w') as out_file:
            json.dump(data, out_file)
            
@inherits(spotify_local_file)
class spotify_clean_local(luigi.Task):
    date = luigi.DateParameter(default = date.today()-timedelta(1))

    def requires(self):
            return self.clone(spotify_local_file)

    def output(self):
        return luigi.LocalTarget("clean_json/spotify_tracks_%s.json" % self.date.strftime('%Y-%m-%d') )
 
    def run(self):
        data = parse_json2("json/spotify_tracks_%s.json" % self.date.strftime('%Y-%m-%d'))
        with self.output().open('w') as out_file:
            json.dump(data, out_file)
            
class spotify_clean_merge(luigi.Task):
    date = luigi.DateParameter(default = date.today()-timedelta(1))

    #def requires(self):
    #        return self.clone(spotify_local_file)

    def output(self):
        client = S3Client(host = 's3.us-east-2.amazonaws.com')
        return S3Target('s3://myspotifydata/spotify_test_%s.json' % self.date.strftime('%Y-%m-%d'), client=client)

    def run(self):
        data = merge_jsons('/Users/tamas/Documents/luigi/spotify/clean_json')
        with self.output().open('w') as out_file:
            json.dump(data, out_file)

if __name__ == '__main__':
    luigi.run()