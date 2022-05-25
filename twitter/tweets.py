import requests
from keys import bearer_token
from db_conn import query


def create_url_user(user_id):
    return f"https://api.twitter.com/2/users/{user_id}/tweets"


def create_url_tweets(t_id):
    return "https://api.twitter.com/2/tweets?ids=1529432090911064065"


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r


def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def main_tweet(user_id=None):
    links_db = query('query_tweets', [])
    links_db = [i[0] for i in links_db]

    url = create_url_tweets(1528708541925806083)
    json_response = connect_to_endpoint(url)
    print(json_response)
    # for tweet in json_response['data']:
    #     if tweet['id'] not in links_db:
    #         query('save_info_tweets', [tweet['id'], tweet['text'], tweet['created_at']])


if __name__ == '__main__':
    main_tweet()
