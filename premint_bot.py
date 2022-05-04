import time
import traceback

import tweepy

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Integer, String, Text, DateTime, ForeignKey, Float
from sqlalchemy.orm import sessionmaker
from config import global_config
import datetime

engine = create_engine(global_config.getRaw('db', 'premint_bot_url'))
Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session()

auth = tweepy.OAuthHandler(global_config.getRaw('twitter2_premint', 'consumer_key'),
                           global_config.getRaw('twitter2_premint', 'consumer_secret'))
auth.set_access_token(global_config.getRaw('twitter2_premint', 'key'),
                      global_config.getRaw('twitter2_premint', 'secret'))

api = tweepy.API(auth)


class Premint(Base):
    __tablename__ = "premint"  # 数据库中保存的表名字

    id = Column(Integer, index=True, primary_key=True)
    url = Column(String(200), index=True, nullable=False)
    updated_at = Column(DateTime, default=datetime.datetime.now)


class IDPrinter(tweepy.Stream):

    def on_status(self, status):
        tweet = ''
        url = ''
        if hasattr(status, 'retweeted_status'):
            try:
                tweet = status.retweeted_status.extended_tweet["full_text"]
                if len(status.retweeted_status.extended_tweet["entities"]["urls"]) > 0:
                    url = status.retweeted_status.extended_tweet["entities"]["urls"][0]['expanded_url']
            except:
                tweet = status.retweeted_status.text
                if len(status.entities["urls"]) > 0:
                    url = status.entities["urls"][0]['expanded_url']

        else:
            try:
                tweet = status.extended_tweet["full_text"]
                if len(status.extended_tweet["entities"]["urls"]) > 0:
                    url = status.extended_tweet["entities"]["urls"][0]['expanded_url']
            except AttributeError:
                tweet = status.text
                if len(status.entities["urls"]) > 0:
                    url = status.entities["urls"][0]['expanded_url']
        print('----------------------------')
        print(url)
        if url.find('www.premint.xyz') > 0:
            _url = url[0:url.rindex('/')]
            print(_url)

            if session.query(Premint).filter(Premint.url == _url).count() > 0:
                print(_url + "已存在。")
            else:
                try:
                    message = 'Premint Alert!!!  \n\n' \
                              'Good luck! #Premint #Whitelist \n\n' \
                              '#NFTCommmunity #premint #NFTs #NFT #NFTGiveaway \n\n' \
                              + _url
                    print("message: {}".format(message))
                    re = api.update_status(message)
                    print("转推结果：" + str(re.id_str))
                    _premint = Premint(url=_url)
                    session.add(_premint)
                    session.commit()
                    print("sleep 300s")
                    time.sleep(300)

                # except pymysql.err.DataError as e1:
                #     print(e1)
                except Exception as e:
                    traceback.print_exc()
                    session.rollback()
                    print("sleep 300s")
                    time.sleep(300)

        print(tweet)
        print('----------------------------')

    def on_connection_error(self):
        session.close()
        self.disconnect()


printer = IDPrinter(
    global_config.getRaw('twitter2_premint', 'consumer_key'), global_config.getRaw('twitter2_premint', 'consumer_secret'),
    global_config.getRaw('twitter2_premint', 'key'), global_config.getRaw('twitter2_premint', 'secret')
)

# printer.filter(follow=["1427221745971335168"])


i = 0
while True:
    i = i + 1
    print('第{}次'.format(i))
    printer.filter(track=['@PREMINT_NFT'])
    # printer.sample()
