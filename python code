import json
import tweepy
api_key = your_key
api_secret = your_api_secret
access_token=your_token
access_token_secret=your_token_secret
#auth = tweepy.OAuthHandler(api_key,api_secret)
#auth.set_access_token(access_token,access_token_secret)
from tweepy import API


# In[2]:


import mysql.connector
from mysql.connector import errorcode
import time
from dateutil import parser
from tweepy.streaming import StreamListener


# In[26]:


class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = tweepy.OAuthHandler(api_key, api_secret)
        auth.set_access_token(access_token, access_token_secret)
        return auth


class MyStreamListener(tweepy.StreamListener):
    
    def __init__ (self,conn):
        self.conn = conn
     
    def on_data(self,data):
        all_data = json.loads(data)
        self.conn = conn
        cursor=conn.cursor()
        if 'text' in all_data:
            id = all_data['id_str']
            text = all_data['text']
            createdAt = parser.parse(all_data['created_at'])
            truncated = all_data['truncated']
            source = all_data['source']
            if all_data['in_reply_to_status_id'] is None:
                inReplyToStatusId = 'NULL'
            else:
                inReplyToStatusId = all_data['in_reply_to_status_id_str']
            if all_data['coordinates'] is None:
                coordinatesLon = 'NULL'
                coordinatesLat = 'NULL'
            else: 
                coordinatesLon = all_data['coordinates']['coordinates'][0]
                coordinatesLat = all_data['coordinates']['coordinates'][1]
            if all_data['place'] is None:
                placeName='NULL'
            else:
                placeName = all_data['place']["full_name"]
            replyCount = all_data['reply_count']
            retweetCount = all_data['retweet_count']
            if all_data['favorite_count'] is None:
                favoriteCount = 'NULL'
            else:
                favoriteCount = all_data['favorite_count']
            if all_data['lang'] is None:
                lang='NULL'
            else:
                lang = all_data['lang']
            userId = all_data['user']['id_str']
            userNameScreen = all_data['user']['screen_name']
            if all_data["user"]["location"] is None:
                userLocation='NULL'
            else:
                userLocation= all_data["user"]["location"]
            verified = all_data['user']['verified']
            followersCount = all_data['user']['followers_count']
            friendsCount = all_data['user']['friends_count']
            listedCount = all_data['user']['listed_count']
            statusesCount = all_data['user']['statuses_count']
            userSince = parser.parse(all_data['user']['created_at'])
            if all_data['user']['profile_image_url_https'] is None:
                profileImageUrl = 'NULL'
            else:
                profileImageUrl= all_data['user']['profile_image_url_https']                         
            
            cursor.execute('INSERT INTO tweets(id,text,truncated,createdAt,source,inReplyToStatusId,coordinatesLon,coordinatesLat,placeName,replyCount,retweetCount,favoriteCount,userId,lang) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                           (id,text,truncated,createdAt,source,inReplyToStatusId,coordinatesLon,coordinatesLat,placeName,replyCount,retweetCount,favoriteCount,userId,lang))
            conn.commit()
            cursor.execute('INSERT INTO users(userId,userNameScreen,userLocation,verified,followersCount,friendsCount,listedCount,statusesCount,userSince,profileImageUrl) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                           (userId,userNameScreen,userLocation,verified,followersCount,friendsCount,listedCount,statusesCount,userSince,profileImageUrl))
            conn.commit()
            return True
        else:
            return True

    def on_error(self, status):   
        if status == 420:
            return False   
        else:
            print(status) 
            
class TwitterStreamer():

    def __init__(self):
        self.twitter_autenticator = TwitterAuthenticator()    

    def stream_tweets(self,listener,polygon):
        # This line filter Twitter Streams to capture data by the keywords: 
        listener = MyStreamListener(conn)
        auth = self.twitter_autenticator.authenticate_twitter_app()
        stream = tweepy.Stream(auth, listener)
        stream.filter(locations=polygon)

class Twitter_user():
    
    def __init__ (self, conn,select):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth)
        self.conn = conn
        self.select = select
        
    def get_user_id (self, select):
        cursor = self.conn.cursor()
        userId=cursor.execute(select)
        id=cursor.fetchall()
        user_id = [int(x[0]) for x in id]
        return user_id
    
   
    def get_follower_list(self, users):
        auth = TwitterAuthenticator().authenticate_twitter_app()
        api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)
        for i in users:
            try:
                c = tweepy.Cursor(api.followers_ids, id = int(i), count=1000)
                ids = []
                for page in c.pages():
                    ids.extend(page)
                    time.sleep(62)
                    for followers in ids:
                        yield (i,followers)
            except tweepy.TweepError:
                pass
                    
    def insert_to_database(self,generator):
        conn = self.conn
        cursor=self.conn.cursor()
        follower_list = list(generator)
        print (follower_list)
        sql = 'INSERT INTO followers (userId, followerId) VALUES (%s, %s)'
        cursor.executemany(sql,follower_list)
        conn.commit()


# In[27]:


conn = mysql.connector.connect(user='root', password=pw,
                              host='localhost',
                              database='Twitter',
                              charset = 'utf8mb4')
select = 'select userId from users'


# In[15]:


polygon=[left point,top point,right point,bottom point]
streamer = TwitterStreamer()


# In[16]:


streamer.stream_tweets(listener,polygon)


# In[28]:


#id = Twitter_user(conn,select)


# In[29]:


#if __name__ == '__main__':
    userId=id.get_user_id(select)
    followers = id.get_follower_list(userId)
    id.insert_to_database(followers)


# In[55]:








