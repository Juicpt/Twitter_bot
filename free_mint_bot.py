import os
import time
import datetime
import traceback

import requests
import tweepy
import sqlalchemy
from sqlalchemy import Column, Integer, DateTime, String, DECIMAL
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from svglib.svglib import svg2rlg
from reportlab.graphics import renderPM
from util import get_random_useragent
from config import global_config
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import sys
sys.setrecursionlimit(10000000)


engine = create_engine(global_config.getRaw('db', 'free_mint_bot_url'))
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

auth = tweepy.OAuthHandler(global_config.getRaw('twitter2', 'consumer_key'),
                           global_config.getRaw('twitter2', 'consumer_secret'))
auth.set_access_token(global_config.getRaw('twitter2', 'key'),
                      global_config.getRaw('twitter2', 'secret'))

api = tweepy.API(auth)


def get_latest_block():
    block = ""
    try:
        tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y/%m/%d").replace('/', '%2F')
        req_session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        req_session.mount('http://', adapter)
        req_session.mount('https://', adapter)

        url = 'https://deep-index.moralis.io/api/v2/dateToBlock?chain=eth&date={}'.format(tomorrow)
        res = req_session.get(url, headers={'accept': "application/json",
                                         'X-API-Key': global_config.getRaw('moralis', 'key')})

        block = res.json()['block']
        timestamp = res.json()['timestamp']

        print(block)
        print(timestamp)

        return block
    except Exception as e:
        traceback.print_exc()
        print("retry------get_moralis_by_block. block: " + block)
        time.sleep(30)
        return get_latest_block()






class Transaction(Base):
    __tablename__ = "transaction"  # 数据库中保存的表名字
    id = Column(Integer, index=True, primary_key=True)
    transaction_hash = Column(String(100), nullable=True)
    token_id = Column(String(100), nullable=True)
    value = Column(DECIMAL, nullable=True)
    token_address = Column(String(50), nullable=True)
    block_number = Column(Integer, nullable=True)
    amount = Column(Integer, nullable=True)
    block_timestamp = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)


class Contract(Base):
    __tablename__ = "contract"  # 数据库中保存的表名字
    id = Column(Integer, index=True, primary_key=True)
    name = Column(String(50), nullable=True)
    token_address = Column(String(50), nullable=True)
    slug = Column(String(50), nullable=True)
    image_url = Column(String(200), nullable=True)
    contract_image_url = Column(String(200), nullable=True)
    external_url = Column(String(200), nullable=True)
    block_number = Column(Integer, nullable=True)
    opensea_collection_url = Column(String(200), nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)


class BlockInfo(Base):
    __tablename__ = "block_info"  # 数据库中保存的表名字
    id = Column(Integer, index=True, primary_key=True)
    block = Column(Integer, nullable=True)
    timestamp = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now)


def get_extension(url):
    array = url.split('.')
    return array[len(array) - 1]


def replace_extension_name(url):
    extension = get_extension(url)
    return 'temp.{}'.format(extension)



def convert_svg_to_png(filename):
    if filename[-4:] == '.svg':
        drawing = svg2rlg(filename)
        png_path = filename.replace('svg', 'png')
        renderPM.drawToFile(drawing, png_path, fmt="PNG")
        return png_path
    return filename


# '获取文件的大小,结果保留两位小数，单位为MB'''
def get_FileSize(filePath):
    # filePath = unicode(filePath, 'utf8')
    fsize = os.path.getsize(filePath)
    fsize = fsize / float(1024 * 1024)
    return round(fsize, 2)


def tweet_image(url, message):
    if get_extension(url) == 'webm':
        filename = 'temp.mp4'
    else:
        filename = 'temp.jpg'
    requests.DEFAULT_RETRIES = 5
    s = requests.session()
    s.keep_alive = False

    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    request = session.get(url, stream=True, headers={'User-Agent': get_random_useragent()})

    if request.status_code == 200:
        with open(filename, 'wb') as image:
            for chunk in request:
                image.write(chunk)
        extension = get_extension(filename)
        if extension == 'svg':
            filename = convert_svg_to_png(filename)

        if extension == 'svg+xml':
            os.rename(filename, filename.replace('.svg+xml', '.svg'))
            filename = filename.replace('.svg+xml', '.svg')
        print("filename:{};type:{}".format(str(filename), type(filename)))

        size = get_FileSize(filename)
        print("filesize:{}MB".format(size))

        if 4 > size > 0:

            if extension in('mp4', 'webm'):
                upload_result = api.media_upload(filename)
                re = api.update_status(status=message, media_ids=[upload_result.media_id_string])
            else:
                re = api.update_status_with_media(status=message, filename=filename)
        else:

            message = message + '\n\nImage:' + url
            re = api.update_status(message)
        print("sleep 300s")
        time.sleep(300)

        print("转推结果：" + re.id_str)
        os.remove(filename)
    else:
        print("Unable to download image")

    time.sleep(1)
    return True


def get_transaction_by_block(block):
    # block = 14554209
    print("block:{}".format(block))
    time.sleep(2)
    json = get_moralis_by_block(block)
    # print("json:{}".format(json))
    if json:
        # print("json:{}".format(json))
        print("json:")
    else:
        return False
    if json is None:
        return False
    total = json['total']
    if total == 0:
        return False
    results = json['result']

    for result in results:
        # time.sleep(2)
        transaction_hash = result['transaction_hash']
        block_timestamp = result['block_timestamp']
        token_id = result['token_id']
        value = result['value']
        token_address = result['token_address']
        verified = result['verified']
        block_number = result['block_number']
        amount = result['amount']
        from_address = result['from_address']

        # if from_address == '0x0000000000000000000000000000000000000000':
        #     print('mint')
        if amount is None:
            return False
        if value is None:
            return False
        if value != '0':
            continue

        session.commit()
        if session.query(Transaction).filter(Transaction.transaction_hash == transaction_hash).count() == 0:
            if len(amount) > 5:
                amount = 1
            if from_address == '0x0000000000000000000000000000000000000000':
                entry = Transaction(
                    transaction_hash=transaction_hash,
                    token_id=token_id,
                    value=int(value)/1000000000000000000,
                    token_address=token_address,
                    block_number=block_number,
                    amount=amount,
                    block_timestamp=datetime.datetime.strptime(block_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                )
                print("transaction_hash:{} saved".format(transaction_hash))
                session.add(entry)
                session.commit()

            else:
                continue



    having_results = session.query(Transaction.token_address, sqlalchemy.func.count(Transaction.amount)) \
        .filter(Transaction.block_number > block - 3) \
        .group_by(Transaction.token_address) \
        .having(sqlalchemy.func.count(Transaction.amount) > 5) \
        .all()

    print(having_results)

    for having_result in having_results:
        transaction_token_address = having_result[0]
        transaction_count = having_result[1]
        print("having_results:token_address:"+transaction_token_address)
        print("having_results:count:"+str(transaction_count))

        tran = session.query(Transaction).filter(Transaction.token_address == transaction_token_address) \
            .order_by(Transaction.block_timestamp).first()

        tran_token_id = tran.token_id
        tran_block_number = tran.block_number


        print(having_result)
        session.commit()
        if session.query(Contract).filter(Contract.token_address == transaction_token_address).count() == 0:
            try:
                time.sleep(1)
                asset_json = get_asset_from_opensea(transaction_token_address, tran_token_id)

                # image_url = asset_json['image_url']
                image_preview_url = asset_json['image_preview_url']
                animation_url = asset_json['animation_url']

                name = asset_json['collection']['name']
                slug = asset_json['collection']['slug']
                contract_image_url = asset_json['asset_contract']['image_url']

                external_url = asset_json['collection']['external_url']

                opensea_collection_url = 'https://opensea.io/collection/' + slug
                # nftport_json = get_nftinfo_from_nftport(token_address, token_id)
                # send_tweet_use_nftport(nftport_json, result)

                if external_url is None:
                    external_url = ''


                message = 'Free mint alert\n\n ' \
                          'NFT : ' + name + ' \n\n ' \
                          'Website :' + external_url + ' \n\n ' \
                          'Opensea :' + opensea_collection_url + ' \n\n ' \
                          'Etherscan: https://etherscan.io/address/' + transaction_token_address + '#writeContract\n\n ' \
                          '#FreeNFTs #NFTs #NFTGiveaway'
                if image_preview_url:
                    tweet_result = tweet_image(image_preview_url, message)
                else:
                    tweet_result = tweet_image(contract_image_url, message)

                if tweet_result:
                    entry = Contract(
                        name=name,
                        slug=slug,
                        token_address=transaction_token_address,
                        block_number=tran_block_number,
                        image_url=image_preview_url,
                        contract_image_url=contract_image_url,
                        external_url=external_url,
                        opensea_collection_url=opensea_collection_url
                    )
                    print("token_address:{} saved".format(transaction_token_address))
                    session.add(entry)
                    session.commit()
            except Exception as e:
                traceback.print_exc()
                session.rollback()
                print("sleep 3s")
                time.sleep(3)
                continue

    return True


def send_tweet_use_nftport(nftport_json, result):
    response = nftport_json['response']
    if response != 'OK':
        return False

    transaction_hash = result['transaction_hash']
    block_timestamp = result['block_timestamp']
    token_id = result['token_id']
    value = result['value']
    token_address = result['token_address']
    verified = result['verified']

    contract_name = nftport_json['contract']['name']
    cached_file_url = nftport_json['nft']['cached_file_url']
    if cached_file_url is None:
        return False
    print("contract_name:{}".format(contract_name))
    print("cached_file_url:{}".format(cached_file_url))

    message = '我是兔子铃铛机器人\n\n ' \
              '发现免费mint的nft.\n\n ' \
              'NFT : ' + contract_name + ' \n\n ' \
                                         'ID : ' + token_id + ' \n\n ' \
                                                              'https://opensea.io/assets?search[query]=' + contract_name.replace(
        ' ', '%20') + '\n\n' \
                      'URL:https://etherscan.io/tx/' + transaction_hash

    tweet_result = tweet_image(cached_file_url, message)

    if tweet_result:
        entry = Transaction(
            transaction_hash=transaction_hash,
            token_id=token_id,
            value=value,
            token_address=token_address,

        )

        session.add(entry)
        session.commit()


def get_moralis_by_block(block):
    # block = 14554209
    time.sleep(1)

    try:
        req_session = requests.Session()
        retry = Retry(connect=3, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)
        req_session.mount('http://', adapter)
        req_session.mount('https://', adapter)
        url = 'https://deep-index.moralis.io/api/v2/block/{}/nft/transfers?chain=eth&limit=500'.format(block)

        res = req_session.get(url, headers={'accept': "application/json",
                                         'X-API-Key': global_config.getRaw('moralis', 'key')})
        return res.json()
    except Exception as e:
        traceback.print_exc()
        print("retry------get_moralis_by_block. block: " + str(block))

        get_moralis_by_block(block)


def get_nftinfo_from_nftport(token_address, token_id):
    nftport_url = "https://api.nftport.xyz/v0/nfts/{}/{}".format(token_address, token_id)

    querystring = {"chain": "ethereum"}

    headers = {
        'Content-Type': "application/json",
        'Authorization': "4ea8892d-30a6-416b-b3d4-7650f7bc731c"
    }

    response = requests.request("GET", nftport_url, headers=headers, params=querystring)
    return response.json()


def get_asset_from_opensea(address, token_id):

    req_session = requests.Session()
    retry = Retry(connect=3, backoff_factor=5)
    adapter = HTTPAdapter(max_retries=retry)
    req_session.mount('http://', adapter)
    req_session.mount('https://', adapter)

    url = "https://api.opensea.io/api/v1/asset/{}/{}/?include_orders=false".format(address, token_id)
    res = req_session.get(url, headers={'accept': "application/json",
                                     'X-API-KEY': global_config.getRaw('opensea', 'key')})
    json = res.json()
    print(json)
    return json


def get_collection_stats(slug):
    url = "https://api.opensea.io/api/v1/collection/{}/stats".format(slug)
    res = requests.get(url, headers={'accept': "application/json",
                                     'X-API-KEY': global_config.getRaw('opensea', 'key')})
    json = res.json()
    print(json)
    return json


# -----------------------------------------------------------------

def start(from_block, to_block):


    if session.query(BlockInfo).filter().count() == 0:

        to_block = get_latest_block()
        from_block = to_block
        entry = BlockInfo(
            block=to_block)

        session.add(entry)
        session.commit()
    else:
        from_block = session.query(BlockInfo).first().block
        to_block = get_latest_block()

        print("from_block={}".format(from_block))
        print("to_block={}".format(to_block))
        if from_block >= to_block:
            time.sleep(30)
            latest_block = get_latest_block()
            start(from_block, latest_block)


    for block in range(from_block, to_block):
        print("block={}".format(block))
        # block = 14580279
        if get_transaction_by_block(block) == False:
            time.sleep(1)
            get_transaction_by_block(block)
        else:
            _block = session.query(BlockInfo).update({BlockInfo.block: block})
            print(_block)
            session.commit()

        print("block:from={}".format(from_block))
        print("block:    ={}".format(block))
        print("block:  to={}".format(to_block))

        if block == to_block - 1:
            session.query(BlockInfo).update({BlockInfo.block: to_block})
            session.commit()
            to_block = get_latest_block()

            start(block, to_block)

start(None, None)