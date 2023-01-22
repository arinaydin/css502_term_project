import json
from requests import post, get
import base64
import csv
import pandas as pd
import os

client_id = '12c3f4d03c5843fc92566438eb6b1581'
client_secret = 'd8b41d8b794a4960bb26dc724bc0eeb9'

def getToken():
    auth_string = client_id + ':' + client_secret
    auth_bytes = auth_string.encode('utf-8')
    auth_base64 = str(base64.b64encode(auth_bytes),'utf-8')
    url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization': 'Basic ' + auth_base64,
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = {'grant_type': 'client_credentials'}
    result = post(url, headers=headers, data=data)
    json_results = json.loads(result.content)
    token = json_results['access_token']
    return token

def getAuthHeader(token):
    return {'Authorization': 'Bearer ' + token}

def searchForSong(token, song_name):
    url = f'https://api.spotify.com/v1/search?q={song_name}&type=track&imit=1'
    headers = getAuthHeader(token)
    result = get(url, headers = headers)
    json_result = json.loads(result.content)['tracks']['items']
    song_dict = json_result[0]
    return song_dict['id']

def getAudioFeatures(token, song_name):
    song_id = searchForSong(token, song_name)
    url = f'https://api.spotify.com/v1/audio-features/{song_id}'
    headers = getAuthHeader(token)
    result = get(url, headers=headers)
    json_result = json.loads(result.content)
    return json_result

def chartmetricToAnalysis():
    analysis_df = pd.DataFrame(columns=['Date', 'Rank', 'Change', 'Track', 'Internal Link', 'External Link', 'Artist', 'Album', 'Label', 'Release Date', 'Peak Date', '7-Day Velocity', 'Total Days on Chart', 'ISRC'])
    file_count = 1
    for filename in os.listdir('Top Charts Data/'):
        with open(os.path.join('Top Charts Data/', filename)) as file:
            if filename[33:36] == 'csv':
                date = filename[21:32]
                csvreader = csv.reader(file)
                header = next(csvreader)
                header.insert(0,'Date')
                for row in csvreader:
                    row.insert(0,date)
                    analysis_df.loc[len(analysis_df.index)] = row
                print(f'{filename} completed.{file_count}')
                file_count += 1
    songs = analysis_df.drop_duplicates(subset='Track', keep='first', ignore_index=True)
    songs = songs.loc[:,'Track']
    print(f'Total number of songs for feature analysis is {len(songs)}')
    return analysis_df, songs

def finalDfCreator(token, songs, analysis_df):
    try:
        audiofeatures_df = pd.read_csv('audiofeatures.csv')
        audiofeatures_dict = audiofeatures_df.to_dict()
        print((audiofeatures_df))
        print('audiofeatures.csv is used as dict')
    except:
        audiofeatures_dict = {}
    failed_songs = []
    song_success = 1
    song_fail = 1

    for song in songs:
        try:
            result = getAudioFeatures(token, song)
            audiofeatures_dict[song] = list(result.values())
            features = list(result.keys())
            audiofeatures_df = pd.DataFrame.from_dict(audiofeatures_dict, orient='index', columns=features)
            audiofeatures_df['Track'] = list(audiofeatures_df.index)
            audiofeatures_df.to_csv('audiofeatures.csv')
            print(f'{song} SUCCESS! ({song_success})')
            song_success += 1
        except:
            failed_songs.append(song)
            print(f'{song} FAILED! ({song_fail})')
            song_fail += 1

    print(f'!!!Song analysis completed with {song_success} successful and {song_fail} failed attempts!!!')

    failed_songs = pd.Series(failed_songs)
    failed_songs.to_csv('failed_songs.csv')
    print('audiofeatures_df is created')
    audiofeatures_df.to_csv('audiofeatures.csv')
    print('analysis_df & audiofeatures_df merge started')
    final_df = pd.merge(analysis_df, audiofeatures_df, on='Track',how='outer')
    final_df.to_csv('final.csv')

token = getToken()
analysis_df, songs = chartmetricToAnalysis()
analysis_df.to_csv('analysis_df.csv')
songs_series = pd.Series(songs)
songs_series.to_csv('songs.csv')

analysis_df = pd.read_csv('analysis_df.csv')
songs_series = pd.read_csv('songs.csv')
songs = list(songs_series['Track'])

finalDfCreator(token, songs, analysis_df)

failed_songs = pd.read_csv('failed_songs.csv')
failed_songs = list(failed_songs['0'])
finalDfCreator(token, failed_songs, analysis_df)
