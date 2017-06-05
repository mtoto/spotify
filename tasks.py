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
class spotify_clean_local(luigi.Task):
    date = luigi.DateParameter(default = date.today()-timedelta(1))

    def requires(self):
            return self.clone(spotify_local_file)

    def output(self):
        return luigi.LocalTarget("clean_json/spotify_tracks_%s.json" % self.date.strftime('%Y-%m-%d') )
 
    def run(self):
        data = parse_json("json/spotify_tracks_%s.json" % self.date.strftime('%Y-%m-%d'))
        with self.output().open('w') as out_file:
            json.dump(data, out_file)
            
class spotify_merge_aws(luigi.ExternalTask):
    date = luigi.DateParameter(default = date.today()-timedelta(1)) 

    def output(self):
        client = S3Client(host = 's3.us-east-2.amazonaws.com')
        return S3Target('s3://myspotifydata/spotify_full_%s.json' % self.date.strftime('%Y-%m-%d'), client=client)

    def run(self):
        data = merge_jsons('/home/pi/home_iot/spotify/clean_json')
        with self.output().open('w') as out_file:
            json.dump(data, out_file)
            
class spotify_get_var(luigi.Task):
    date = luigi.DateParameter(default = date.today()-timedelta(1))
    type_of_var = luigi.Parameter(default = 'artist')
    
    def requires(self):
            return spotify_merge_aws()
        
    def run(self):
        arts = get_var_wrapper('spotify_full_%s.json' % self.date.strftime('%Y-%m-%d'),
                               self.type_of_var)
        with self.output().open('w') as out_file:
            json.dump(arts, out_file)

    def output(self):
        client = S3Client(host = 's3.us-east-2.amazonaws.com')
        return S3Target('s3://myspotifydata/spotify_%s.json' % self.date.strftime('%Y-%m-%d') + '_%t' % self.type_of_var,
                        client=client)

if __name__ == '__main__':
    luigi.run()
