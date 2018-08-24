import requests
import datetime
import json
import collections
import os
import boto3
import math
from spotify_creds import *
import pandas as pd

# Get access token
def access_token():
    
    body_params = {'grant_type' : 'refresh_token',
                'refresh_token' : refresh_token}

    url = 'https://accounts.spotify.com/api/token'
    response = requests.post(url, 
                             data = body_params, 
                             auth = (client_id, client_secret))
    
    response_dict = json.loads(response.content)
    accessToken = response_dict.get('access_token')

    return accessToken
    
# Get most recent songs and append the response
# to a new json file every day
def download_data():

    current_time = datetime.datetime.now().strftime('%Y-%m-%d')
    filename = '/spotify/json/spotify_tracks_%s.json' % current_time
    
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
        
# This function takes a list of track uri's 
# to replace songs in my morning playlist
# and returns the status code of the put request.
def replace_tracks(tracks, url):
    
    #url = 'https://api.spotify.com/v1/users/1170891844/playlists/6a2QBfOgCqFQLN08FUxpj3/tracks'
    accesToken = access_token()
    headers = {'Authorization': 'Bearer ' + accesToken,
               'Content-Type':'application/json'}
    data = {"uris": ','.join(tracks)}

    response = requests.put(url, headers = headers,
                            params = data)
                            
    return response.status_code
        
# Cleaner function to get rid of redundancy
def deduplicate(file):
    result =[]
    
    for line in file:
        data = json.loads(line)
        result.extend(data)
    
    result = {i['played_at']:i for i in result}.values()
    return result

#Parse json so it can easily be converted to a dataframe
def parse_json(file): 
    results = []
    track_cols = ['name','uri','explicit','duration_ms','type','id']

    for item in file:
        
        d_time = {'played_at' : item['played_at']}
        
        for key in item.keys(): 
            if (key == 'track'):
                track = item[key]
                d_arts = collections.defaultdict(list)
                
                for i in track['artists']: 
                    for k, v in i.items(): 
                        if (k in ['id','name']):
                            d_arts['artist_' + k].append(v)
                            
                track_sub = { k: track[k] for k in track_cols }
                
                for k,v in track_sub.items():
                    if (k in ['id','name']):
                        track_sub['track_' + k] = track_sub.pop(k)
            
        d = dict(track_sub, **d_arts)
        d.update(d_time)

        results.append(d)
                     
    return results

# Retrieve all of my playlists, based on
# spotify id (string)
def my_playlists(spotify_id):
    my_playlists = list()
    
    url = 'https://api.spotify.com/v1/me/playlists'  
    accesToken = access_token()
    headers = {'Authorization': 'Bearer ' + accesToken }
    response = requests.get(url, 
                            headers = headers,
                            params = {'limit':50})
    data = response.json()
    
    for i in data['items']:
        if i['owner']['id'] == spotify_id:
            my_playlists.append(i['href'])
            
    return(my_playlists)

# Get relevant contents of the playlists
def get_playlist_tracks(href):

    accesToken = access_token()
    headers = {'Authorization': 'Bearer ' + accesToken }
    #params = {'fields': 'id,name,description,followers'}
    
    response = requests.get(href,headers = headers)
    data = response.json()
    
    return data

# Return all tracks together with the date added
# from all of my playlists.
def all_playlist_tracks(playlists):
    
    songs = list()
    tracks = [get_playlist_tracks(i + '/tracks') for i in playlists]
    
    for t in range(len(tracks)):
            for i in range(len(tracks[t]["items"])):
                try:
                    songs.append((tracks[t]["items"][i]["added_at"],
                                  tracks[t]["items"][i]["track"]["uri"]))
                    pass
                except:
                    continue
                
    return songs

# This function updates my recently discovered 
# tracks playlist with the last 30 songs I added.
def create_new_tracks_playlist(dataset):
    
    df = pd.DataFrame(dataset, columns=['date', 'uri'])
    df["date"] = pd.to_datetime(df["date"])
    
    df = df.sort_values('date', ascending = False)
    songs = df["uri"].unique().tolist()[0:30]
                
    # make api call
    res_code = replace_tracks(songs, 'https://api.spotify.com/v1/users/1170891844/playlists/7BLadDfw4fPCVY4bgfTIOI/tracks')
    return res_code

# This function reads in the weekly dataset 
# as a pandas dataframe, outputs the list of 
# top ten tracks and feeds them to replace_tracks()
def create_playlist(dataset, date):
    
    data = pd.read_json(dataset)          
    data['played_at'] = pd.to_datetime(data['played_at'])
    
    data = data.set_index('played_at') \
               .between_time('7:00','12:00')
        
    data = data[data.index > str(date)]
    # aggregate data
    songs = data['uri'].value_counts()\
                       .nlargest(10) \
                       .index \
                       .get_values() \
                       .tolist()
    # make api call
    res_code = replace_tracks(songs,               'https://api.spotify.com/v1/users/1170891844/playlists/6a2QBfOgCqFQLN08FUxpj3/tracks')
    
    return res_code