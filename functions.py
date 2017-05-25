import requests
import datetime
import json
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
    


