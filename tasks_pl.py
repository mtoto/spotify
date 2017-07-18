# Luigi task to write clean files to aws
import luigi
import json
from luigi.s3 import S3Target, S3Client
from datetime import date, timedelta
from functions_pl import *
import pandas as pd

class local_raw_json(luigi.ExternalTask):
    date = luigi.DateParameter(default = date.today()-timedelta(1)) 

    def output(self):
        return luigi.LocalTarget('json/spotify_tracks_%s.json' % 
                                 self.date.strftime('%Y-%m-%d'))
    
# Luigi task to write clean files to aws
class spotify_clean_aws(luigi.Task):
    date = luigi.DateParameter(default = date.today()-timedelta(1)) 
    
    def requires(self):
        return self.clone(local_raw_json)

    def output(self):
        client = S3Client(host = 's3.us-east-2.amazonaws.com')
        return S3Target('s3://myspotifydata/spotify_tracks_%s.json' % 
                        self.date.strftime('%Y-%m-%d'), 
                        client=client)

    def run(self):   
        with self.input().open('r') as in_file:
            data = json_parser(in_file)
            
        with self.output().open('w') as out_file:
            json.dump(data, out_file)
            
# Task to parse relevant fields and merge a week of data
class spotify_merge_weekly_aws(luigi.Task):
    date = luigi.DateParameter(default = (date.today()-timedelta(8)))
    daterange = luigi.IntParameter(7)

    def requires(self):
        return [spotify_clean_aws(i) for i in [self.date + timedelta(x) for x in range(self.daterange)]]
        
    def output(self):
        client = S3Client(host = 's3.us-east-2.amazonaws.com')
        return S3Target('s3://myspotifydata/spotify_week_%s.json' % 
                        (self.date.strftime('%Y-%m-%d') + '_' + str(self.daterange)), 
                        client=client)
    
    def run(self):
        results = []
        for file in self.input():
            
            with file.open('r') as in_file:
                data = json.load(in_file)
                parsed = parse_json(data)
                
            results.extend(parsed)
            
        result = {v['played_at']:v for v in results}.values()
        
        with self.output().open('w') as out_file:
            json.dump(result, out_file)
            
# Task to aggregate weekly data and create playlist
class spotify_morning_playlist(luigi.Task):
    date = luigi.DateParameter(default = (date.today()-timedelta(8)))
    daterange = luigi.IntParameter(7)

    def requires(self):
        return self.clone(spotify_merge_weekly_aws)
    
    def output(self):
        client = S3Client(host = 's3.us-east-2.amazonaws.com')
        return S3Target('s3://myspotifydata/spotify_top10_%s.json' % 
                        (self.date.strftime('%Y-%m-%d') + '_' + str(self.daterange)), 
                        client=client)
    
    def run(self):
        
        with self.input().open('r') as in_file:
            res_code = create_playlist(in_file, self.date)
              
        # write to file if succesful
        if (res_code == 201):
            with self.output().open('w') as out_file:
                json.dump(res_code, out_file)
        
           
                 
