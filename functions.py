import requests
import datetime
import json
import collections
import os
import boto3
import math

from spotify_creds import *

""" Get acces token """
def access_token():
    
    grant_type = 'refresh_token'

    body_params = {'grant_type' : grant_type,
                   'refresh_token' : refresh_token}

    url = 'https://accounts.spotify.com/api/token'
    response = requests.post(url, data = body_params, auth = (client_id, client_secret))
    response_dict = json.loads(response.content)
    accessToken = response_dict.get('access_token')

    return accessToken
    
""" Get recent songs """
def download_data():

    current_time = datetime.datetime.now().strftime('%Y-%m-%d')
    filename = '/home/pi/home_iot/spotify/json/spotify_tracks_%s.json' % current_time
    
    accesToken = access_token()
    headers = {'Authorization': 'Bearer ' + accesToken }
    payload = {'limit': 50}

    url = 'https://api.spotify.com/v1/me/player/recently-played'
    response = requests.get(url, headers = headers,
                            params = payload)
    data = response.json()

    with open(filename, 'a') as f:
        json.dump(data['items'], f)
        f.write('\n')
    
""" Convert json to a list of dicts"""
def json_parser(file):
    result =[]
    with open(file) as f:
        for line in f:
            data = json.loads(line)
            result.extend(data)
    return(result)

"""Parse json so it can easily be converted to a dataframe"""
def parse_json(file): 
    
    dict_list = json_parser(file)

    results = []
    track_cols = ['name','uri','explicit','preview_url',
                  'track_number','disc_number','href',
                  'duration_ms','type','id']

    for item in dict_list:
        
        d_time = {'played_at' : item['played_at'] }
        
        if item['context'] is not None:
            
            d_context = item['context'] 
            d_context['spotify_external_url'] = item['context']['external_urls']['spotify']
            d_context['playlist_href'] = item['context']['href'] # deal with other duplicate names
            d_context.pop('external_urls', None)
            
        else:
            d_context = {'href' : None, 'spotify_external_url': None,
                         'type' : None, 'uri' : None, 'playlist_href': None}
        
        for key in item.keys():
            
            if (key == 'track'):
              
                track = item[key]
                d_arts = collections.defaultdict(list)
                
                for i in track['artists']: 
                    for k, v in i.items():
                        
                        if (k != 'external_urls'):
                            d_arts[k].append(v)
                            
                        elif (k == 'external_urls'):
                            d_arts['artist_urls'].append(v['spotify'])
                                    
                d_arts['artist_id'] = d_arts.pop('id')
                d_arts['artist_name'] = d_arts.pop('name')
                
                track_sub = { k: track[k] for k in track_cols }
                track_sub['track_name'] = track_sub.pop('name')
                track_sub['track_id'] = track_sub.pop('id')
            
        d = dict(track_sub, **d_arts)
        d.update(d_time)
        d.update(d_context)

        results.append(d)
        
        result = {v['played_at']:v for v in results}.values()
             
    return(result)

""" Merge json files into one list of dicts from directory"""
def merge_jsons(dir):
    results = []
    
    for filename in os.listdir(dir):
        if filename.endswith('.json'):
            parsed = json_parser(dir+'/'+filename)
            results.extend(parsed)
            
    result = {v['played_at']:v for v in results}.values()
            
    return result

""" Merge json files into one list of dicts from list of filenames"""
def update_json(list_of_files):
    results = []
    
    for i in list_of_files:
            parsed = json_parser(i)
            results.extend(parsed)
          
    result = {v['played_at']:v for v in results}.values()

    return result

""" Get unique values for key from file in s3.
    This will be come in handy creating dicts for
    variables such as artist genre. """
def get_unique_vals(bucket,filename,key):
    
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket, filename)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)

    t=[d[key] for d in json_content]
    l=[item for sublist in t for item in sublist]
    
    return(l)
    
def get_artists(list_of_artists):

    accesToken = access_token()
    headers = {'Authorization': 'Bearer ' + accesToken }
    payload = {'limit': 50, 'ids': ','.join(list_of_artists) }
    
    response = requests.get("https://api.spotify.com/v1/artists", 
                            headers = headers,
                            params = payload)
    data = response.json()

    return data

# thanks! https://stackoverflow.com/a/26945303/4964651
def slice_per(source, limit):
    step = int(math.floor(len(source) / limit))
    return [source[i::step] for i in range(step)]

def process_range(source, store=None):
    
    nested_arts = slice_per(source, 49)
    store = []
    
    for ids in nested_arts:
        store.append(get_artists(ids))
        
    return store

def parse_artists(arts_resp):
    
    result = []
    for item in arts_resp:
        for a in item['artists']:
            arts_dict = { k: a[k] for k in ['id','name','genres','popularity'] }
            arts_dict['followers'] = a['followers']['total']
            result.append(arts_dict)
            
    return(result)

def artist_wrapper(file_s3):
    
    date_today = datetime.date.today() - datetime.timedelta(1)
    list_of_artists = get_unique_vals('myspotifydata', 
                                       file_s3,
                                      'artist_id')
    list_of_arts_resp = process_range(list_of_artists)
    return parse_artists(list_of_arts_resp)

          


