import requests
import datetime
import json
import collections

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
                                    
                track_sub = { k: track[k] for k in track_cols }     
                track_sub['track_name'] = track_sub.pop('name')
            

        d = dict(track_sub, **d_arts)
        d.update(d_time)
        d.update(d_context)

        results.append(d)
        
        result = {v['played_at']:v for v in results}.values()

                
    return(result)



