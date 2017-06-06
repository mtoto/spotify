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
            d_context['playlist_url'] = item['context']['external_urls']['spotify']
            d_context['playlist_href'] = item['context']['href'] # deal with other duplicate names
            d_context.pop('external_urls', None)
            d_context.pop('href', None)
   
        else:
            d_context = {'spotify_external_url': None, 'type' : None, 
                         'uri' : None, 'playlist_href': None}
        
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
                d_arts['artist_href'] = d_arts.pop('href')

                
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

"""General functions"""

def get_var_wrapper(file_s3, type_of_var='artist'):
    
    if (type_of_var=='album'):
        key, typeof1, typeof2, parse_function = ("uri","string","albums",parse_albums)
    
    elif (type_of_var=='artist'): 
        key, typeof1, typeof2, parse_function = ("artist_id","list","arts",parse_artists)
    
    elif (type_of_var=='playlist'): 
        key, typeof1, typeof2, parse_function = ("playlist_href","url","playlist",parse_playlists)
    
    date_today = datetime.date.today() - datetime.timedelta(1)
    list_of_var = get_unique_vals('myspotifydata', 
                                  filename=file_s3,
                                  key=key,
                                  typeof=typeof1)
    list_of_var_resp = process_range(list_of_var,typeof=typeof2)
    return parse_function(list_of_var_resp)

def get_unique_vals(bucket,filename,key, typeof = 'list'):
    
    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket, filename)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    
    if (typeof=='list'):
        t=[d[key] for d in json_content]
        l=[item for sublist in t for item in sublist]
        l2=[x for x in set(l) if x is not None]
        result = l2
        
    elif(typeof=='url'):
        t=[d[key] for d in json_content]
        l=[x for x in set(t) if x is not None]
        result=[x for x in l if 'null' not in x]
        
    elif(typeof=='string'):
        t=[d[key] for d in json_content]
        l=[x for x in set(t) if x is not None]
        l2=[x for x in l if 'album' in x]
        result=[s.partition('album:')[2] for s in l2 if 'album' in s]
        
    return(result)

# thanks! https://stackoverflow.com/a/26945303/4964651
def slice_per(source, limit):
    step = int(math.floor(len(source) / limit))
    return [source[i::step] for i in range(step)]

def process_range(source, typeof = 'arts'):
    
    store = []

    if (typeof=='arts'):
        nested_arts = slice_per(source, 49)
        for ids in nested_arts:
            store.append(get_artists(ids))
            
    elif (typeof=='playlist'):
        for ids in source:
            store.append(get_playlist(ids))
            
    elif (typeof=='albums'):
        nested_albs = slice_per(source, 19)
        for ids in nested_albs:
            store.append(get_albums(ids))
        
    return store

""" Playlist functions"""
def get_playlist(href):

    accesToken = access_token()
    headers = {'Authorization': 'Bearer ' + accesToken }
    params = {'fields': 'name,description,followers,href'}

    response = requests.get(href,headers = headers,
                                 params = params) 
    data = response.json()

    return data

def parse_playlists(playlist_resps):
    
    result = []
    for a in playlist_resps:
        if 'error' not in a.keys():
            play_dict = { k: a[k] for k in ['description','href','name'] }
            
            play_dict['playlist_followers'] = a['followers']['total']
            play_dict['playlist_descr'] = play_dict.pop('description')
            play_dict['playlist_href'] = play_dict.pop('href')
            play_dict['playlist_name'] = play_dict.pop('name')

            result.append(play_dict)
            
    return(result)

""" Artists funcs"""
def get_artists(list_of_artists):

    accesToken = access_token()
    headers = {'Authorization': 'Bearer ' + accesToken }
    payload = {'limit': 50, 'ids': ','.join(list_of_artists) }
    
    response = requests.get("https://api.spotify.com/v1/artists", 
                            headers = headers,
                            params = payload)
    data = response.json()

    return data

def parse_artists(arts_resp):
    
    result = []
    for item in arts_resp:
        for a in item['artists']:
            arts_dict = { k: a[k] for k in ['id','name','genres','popularity'] }
            
            arts_dict['artist_followers'] = a['followers']['total']
            arts_dict['artist_id'] = arts_dict.pop('id')
            arts_dict['artist_name'] = arts_dict.pop('name')
            arts_dict['artist_genres'] = arts_dict.pop('genres')
            arts_dict['artist_popularity'] = arts_dict.pop('popularity')

            result.append(arts_dict)
            
    return(result)

"""Album funcs"""
def get_albums(list_of_albums):

    accesToken = access_token()
    headers = {'Authorization': 'Bearer ' + accesToken }
    payload = {'limit': 50, 'ids': ','.join(list_of_albums) }
    
    response = requests.get("https://api.spotify.com/v1/albums", 
                            headers = headers,
                            params = payload)
    data = response.json()

    return data

def parse_albums(albs_resp):
    
    result = []
    for item in albs_resp:
        for a in item['albums']:
            albs_dict = { k: a[k] for k in ['id','genres','name','popularity','release_date'] }
            
            albs_dict['album_genres'] = albs_dict.pop('genres')
            albs_dict['album_id'] = albs_dict.pop('id')
            albs_dict['album_name'] = albs_dict.pop('name')
            albs_dict['album_popularity'] = albs_dict.pop('popularity')
            albs_dict['album_release_date'] = albs_dict.pop('release_date')

            d_arts = collections.defaultdict(list)
            for i in a['artists']: 
                    for k, v in i.items():
                        
                        if (k not in ('uri','type','external_urls')):
                            d_arts[k].append(v)
                            
                        elif (k == 'external_urls'):
                            d_arts['artist_album_urls'].append(v['spotify'])
                                    
            d_arts['album_artist_id'] = d_arts.pop('id')
            d_arts['album_artist_name'] = d_arts.pop('name')
            d_arts['album_artist_href'] = d_arts.pop('href')
                
            d = dict(albs_dict, **d_arts)
    
            result.append(d)
            
    return(result)
          


