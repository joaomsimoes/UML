from keys import YOUTUBE
import requests
from youtube_transcript_api import YouTubeTranscriptApi
from youtube_transcript_api.formatters import TextFormatter
from datetime import datetime
from db_conn import query
import logging
import time
import boto3


logging.basicConfig(level=logging.DEBUG)

session = boto3.Session(
    aws_access_key_id='AKIASXCUIXAKONE3HCGW',
    aws_secret_access_key='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'
)

s3 = session.resource('s3')

logging.debug(datetime.now().strftime('%H:%M:%S') + " ; " + "Connected to S3" + "\n")


def youtube_check_new_video(channel_id=None):
    try:
        to_filter = ['UUngIhBkikUe6e7tZTjpKK7Q', 'UUviqt5aaucA1jP3qFmorZLQ',
                     'UUgY66N1YS_G9lYMvCQko6yw', 'UUz28r8vkhJD9WhP5wmjefSw']

        links_db = query('query_video_id', [])
        links_db = [i[0] for i in links_db]

        url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId={channel_id}&maxResults=15&" \
              f"key={YOUTUBE}"

        response = requests.request("GET", url)
        data = response.json()['items']
        logging.debug(datetime.now().strftime('%H:%M:%S') + " ; " + str(response) + "\n")

        new_videos = []

        for video in data:
            video_id = video['snippet']['resourceId']['videoId']
            video_title = str(video['snippet']['title'])
            if channel_id in to_filter:
                if 'bitcoin' in video_title.lower():
                    if video_id not in links_db:
                        new_videos.append(video_id)
                        query('save_new_video_id', [video['snippet']['publishedAt'], channel_id, video_id])

            else:
                if video_id not in links_db:
                    new_videos.append(video_id)
                    query('save_new_video_id', [video['snippet']['publishedAt'], channel_id, video_id])

        return new_videos

    except Exception as e:
        logging.exception(datetime.now().strftime('%H:%M:%S') + " ; " + str(e) + "\n")


def youtube_subs(url):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(url)
        formatter = TextFormatter()
        text_formatted = formatter.format_transcript(transcript)

        object = s3.Object("youtube-joao-crypto", f'{url}.txt')
        result = object.put(Body=text_formatted)
        res = result.get('ResponseMetadata')

        if res.get('HTTPStatusCode') == 200:
            logging.debug(datetime.now().strftime('%H:%M:%S') + " ; " + 'File Uploaded Successfully' + "\n")
        else:
            logging.debug(datetime.now().strftime('%H:%M:%S') + " ; " + f'File Not Uploaded - {url}' + "\n")

    except Exception as e:
        logging.exception(datetime.now().strftime('%H:%M:%S') + " ; " + str(e) + "\n")


if __name__ == '__main__':
    channel_list = {'CryptoShorts': 'UUybasP-2D2b5kTLAb_kvhWQ', 'MoreCryptoOnline': 'UUngIhBkikUe6e7tZTjpKK7Q',
                    'Andy': 'UUNHz224-6zLSFgMUju__wwQ', 'Nat': 'UUGadbHPeZP_mQGNOtabaGAg',
                    'Jebb': 'UUviqt5aaucA1jP3qFmorZLQ', 'CryptoWorld': 'UUgY66N1YS_G9lYMvCQko6yw',
                    'Benjamin': 'UURvqjQPSeaWn-uEx-w0XOIg', 'CowboyTrades': 'UUz28r8vkhJD9WhP5wmjefSw',
                    'WolvesOfCrypto': 'UUTt7JL7E3DfSClhyidGLJtw'}
    while True:
        for _, channel_id in channel_list.items():
            new_videos_url = youtube_check_new_video(channel_id)
            for url in new_videos_url:
                youtube_subs(url)

        time.sleep(43200)
