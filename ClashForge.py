# -*- coding: utf-8 -*-
# !/usr/bin/env python3
import base64
import subprocess
import threading
import time
import urllib.parse
import json
import glob
import re
import yaml
import random
import string
import httpx
import asyncio
from itertools import chain
from typing import Dict, List, Optional
import sys
import requests
import zipfile
import gzip
import shutil
import platform
import os
from datetime import datetime
from asyncio import Semaphore
from concurrent.futures import ThreadPoolExecutor, as_completed
import ssl

ssl._create_default_https_context = ssl._create_unverified_context
import warnings

warnings.filterwarnings('ignore')
# from requests_html import HTMLSession  # REMOVED for performance
import psutil

try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    pass  # nest_asyncio未安装时忽略，js_render中将使用子进程替代


# ========== v5: 增量缓存 + 超时保护 ==========
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.clashforge_cache')
SUB_CACHE_FILE = os.path.join(CACHE_DIR, 'sub_cache.json')
DELAY_CACHE_FILE = os.path.join(CACHE_DIR, 'delay_cache.json')
CHECKPOINT_FILE = os.path.join(CACHE_DIR, 'checkpoint.json')
CACHE_EXPIRE_HOURS = 6
BATCH_SIZE = 5000
USE_CACHE = True
FORCE_REFRESH = False

def ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)

def load_json_cache(filepath):
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def save_json_cache(filepath, data):
    ensure_cache_dir()
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        print(f'cache write error: {e}')

def is_cache_valid(cache_entry, expire_hours=CACHE_EXPIRE_HOURS):
    if not cache_entry or 'timestamp' not in cache_entry:
        return False
    cached_time = cache_entry.get('timestamp', 0)
    return (time.time() - cached_time) < (expire_hours * 3600)

def load_checkpoint():
    cp = load_json_cache(CHECKPOINT_FILE)
    if cp and is_cache_valid(cp, expire_hours=24):
        return cp
    return {}

def save_checkpoint(stage, data):
    cp = load_checkpoint()
    cp['stage'] = stage
    cp['data'] = data
    cp['timestamp'] = time.time()
    save_json_cache(CHECKPOINT_FILE, cp)

def clear_cache():
    for f in [SUB_CACHE_FILE, DELAY_CACHE_FILE, CHECKPOINT_FILE]:
        if os.path.exists(f):
            os.remove(f)
    print('cache cleared')

def _cache_sub_result(url, result_type, data):
    sub_cache = load_json_cache(SUB_CACHE_FILE)
    url_hash = hashlib.md5(url.encode()).hexdigest()
    if result_type == 'proxy':
        return
    sub_cache[url_hash] = {'type': result_type, 'data': data, 'timestamp': time.time(), 'url': url}
    save_json_cache(SUB_CACHE_FILE, sub_cache)

def _flush_delay_cache(batch_results, delay_cache):
    for r in batch_results:
        cache_key = hashlib.md5(r.name.encode()).hexdigest()
        delay_cache[cache_key] = {'name': r.name, 'delay': r.delay if r.is_valid else None, 'timestamp': time.time()}
    save_json_cache(DELAY_CACHE_FILE, delay_cache)


def safe_decode(data, encodings=None):
    """安全解码字节数据，依次尝试多种编码，最终用replace兜底"""
    if isinstance(data, str):
        return data
    if encodings is None:
        encodings = ['utf-8', 'gbk', 'gb2312', 'gb18030', 'big5', 'latin-1']
    for enc in encodings:
        try:
            return data.decode(enc)
        except (UnicodeDecodeError, AttributeError):
            continue
    # 最终兜底：utf-8 with replace
    return data.decode('utf-8', errors='replace')

# TEST_URL = "http://www.gstatic.com/generate_204"
TEST_URL = "http://www.pinterest.com"
CLASH_API_PORTS = [9090]
CLASH_API_HOST = "127.0.0.1"
CLASH_API_SECRET = ""
TIMEOUT = 3
# 存储所有节点的速度测试结果
SPEED_TEST = False
SPEED_TEST_LIMIT = 5  # 只测试前30个节点的下行速度，每个节点测试5秒
results_speed = []
MAX_CONCURRENT_TESTS = 100
MAX_CONCURRENT_SUBS = 50  # 并发下载订阅源数量
LIMIT = 10000  # 最多保留LIMIT个节点
CONFIG_FILE = 'clash_config.yaml'
INPUT = "input"  # 从文件中加载代理节点，支持yaml/yml、txt(每条代理链接占一行)
BAN = ["中国", "China", "CN", "电信", "移动", "联通"]
headers = {
    'Accept-Charset': 'utf-8',
    'Accept': 'text/html,application/x-yaml,*/*',
    'User-Agent': 'Clash Verge/1.7.7'
}

# Clash 配置文件的基础结构
clash_config_template = {
    "port": 7890,
    "socks-port": 7891,
    "redir-port": 7892,
    "allow-lan": True,
    "mode": "rule",
    "log-level": "info",
    "external-controller": "127.0.0.1:9090",
    "geodata-mode": True,
    'geox-url': {'geoip': 'https://raw.githubusercontent.com/Loyalsoldier/geoip/release/geoip.dat',
                 'mmdb': 'https://raw.githubusercontent.com/Loyalsoldier/geoip/release/GeoLite2-Country.mmdb'},
    "dns": {
        "enable": True,
        "ipv6": False,
        "default-nameserver": [
            "223.5.5.5",
            "119.29.29.29"
        ],
        "enhanced-mode": "fake-ip",
        "fake-ip-range": "198.18.0.1/16",
        "use-hosts": True,
        "nameserver": [
            "https://doh.pub/dns-query",
            "https://dns.alidns.com/dns-query"
        ],
        "fallback": [
            "https://doh.dns.sb/dns-query",
            "https://dns.cloudflare.com/dns-query",
            "https://dns.twnic.tw/dns-query",
            "tls://8.8.4.4:853"
        ],
        "fallback-filter": {
            "geoip": True,
            "ipcidr": [
                "240.0.0.0/4",
                "0.0.0.0/32"
            ]
        }
    },
    "proxies": [],
    "proxy-groups": [
        {
            "name": "节点选择",
            "type": "select",
            "proxies": [
                "自动选择",
                "故障转移",
                "DIRECT",
                "手动选择"
            ]
        },
        {
            "name": "自动选择",
            "type": "url-test",
            "exclude-filter": "(?i)中国|China|CN|电信|移动|联通",
            "proxies": [],
            # "url": "http://www.gstatic.com/generate_204",
            "url": "http://www.pinterest.com",
            "interval": 300,
            "tolerance": 50
        },
        {
            "name": "故障转移",
            "type": "fallback",
            "exclude-filter": "(?i)中国|China|CN|电信|移动|联通",
            "proxies": [],
            "url": "http://www.gstatic.com/generate_204",
            "interval": 300
        },
        {
            "name": "手动选择",
            "type": "select",
            "proxies": []
        },
    ],
    "rules": [
        "DOMAIN,app.adjust.com,DIRECT",
        "DOMAIN,bdtj.tagtic.cn,DIRECT",
        "DOMAIN,log.mmstat.com,DIRECT",
        "DOMAIN,sycm.mmstat.com,DIRECT",
        "DOMAIN-SUFFIX,blog.google,DIRECT",
        "DOMAIN-SUFFIX,googletraveladservices.com,DIRECT",
        "DOMAIN,dl.google.com,DIRECT",
        "DOMAIN,dl.l.google.com,DIRECT",
        "DOMAIN,fonts.googleapis.com,DIRECT",
        "DOMAIN,fonts.gstatic.com,DIRECT",
        "DOMAIN,mtalk.google.com,DIRECT",
        "DOMAIN,alt1-mtalk.google.com,DIRECT",
        "DOMAIN,alt2-mtalk.google.com,DIRECT",
        "DOMAIN,alt3-mtalk.google.com,DIRECT",
        "DOMAIN,alt4-mtalk.google.com,DIRECT",
        "DOMAIN,alt5-mtalk.google.com,DIRECT",
        "DOMAIN,alt6-mtalk.google.com,DIRECT",
        "DOMAIN,alt7-mtalk.google.com,DIRECT",
        "DOMAIN,alt8-mtalk.google.com,DIRECT",
        "DOMAIN,fairplay.l.qq.com,DIRECT",
        "DOMAIN,livew.l.qq.com,DIRECT",
        "DOMAIN,vd.l.qq.com,DIRECT",
        "DOMAIN,analytics.strava.com,DIRECT",
        "DOMAIN,msg.umeng.com,DIRECT",
        "DOMAIN,msg.umengcloud.com,DIRECT",
        "PROCESS-NAME,com.ximalaya.ting.himalaya,节点选择",
        "DOMAIN-SUFFIX,himalaya.com,节点选择",
        "PROCESS-NAME,deezer.android.app,节点选择",
        "DOMAIN-SUFFIX,deezer.com,节点选择",
        "DOMAIN-SUFFIX,dzcdn.net,节点选择",
        "PROCESS-NAME,com.tencent.ibg.joox,节点选择",
        "PROCESS-NAME,com.tencent.ibg.jooxtv,节点选择",
        "DOMAIN-SUFFIX,joox.com,节点选择",
        "DOMAIN-KEYWORD,jooxweb-api,节点选择",
        "PROCESS-NAME,com.skysoft.kkbox.android,节点选择",
        "DOMAIN-SUFFIX,kkbox.com,节点选择",
        "DOMAIN-SUFFIX,kkbox.com.tw,节点选择",
        "DOMAIN-SUFFIX,kfs.io,节点选择",
        "PROCESS-NAME,com.pandora.android,节点选择",
        "DOMAIN-SUFFIX,pandora.com,节点选择",
        "PROCESS-NAME,com.soundcloud.android,节点选择",
        "DOMAIN-SUFFIX,p-cdn.us,节点选择",
        "DOMAIN-SUFFIX,sndcdn.com,节点选择",
        "DOMAIN-SUFFIX,soundcloud.com,节点选择",
        "PROCESS-NAME,com.spotify.music,节点选择",
        "DOMAIN-SUFFIX,pscdn.co,节点选择",
        "DOMAIN-SUFFIX,scdn.co,节点选择",
        "DOMAIN-SUFFIX,spotify.com,节点选择",
        "DOMAIN-SUFFIX,spoti.fi,节点选择",
        "DOMAIN-KEYWORD,spotify.com,节点选择",
        "DOMAIN-KEYWORD,-spotify-com,节点选择",
        "PROCESS-NAME,com.aspiro.tidal,节点选择",
        "DOMAIN-SUFFIX,tidal.com,节点选择",
        "PROCESS-NAME,com.google.android.apps.youtube.music,节点选择",
        "PROCESS-NAME,com.google.android.youtube.tvmusic,节点选择",
        "PROCESS-NAME,tv.abema,节点选择",
        "DOMAIN-SUFFIX,abema.io,节点选择",
        "DOMAIN-SUFFIX,abema.tv,节点选择",
        "DOMAIN-SUFFIX,ameba.jp,节点选择",
        "DOMAIN-SUFFIX,hayabusa.io,节点选择",
        "DOMAIN-KEYWORD,abematv.akamaized.net,节点选择",
        "PROCESS-NAME,com.channel4.ondemand,节点选择",
        "DOMAIN-SUFFIX,c4assets.com,节点选择",
        "DOMAIN-SUFFIX,channel4.com,节点选择",
        "PROCESS-NAME,com.amazon.avod.thirdp,节点选择",
        "DOMAIN-SUFFIX,aiv-cdn.net,节点选择",
        "DOMAIN-SUFFIX,aiv-delivery.net,节点选择",
        "DOMAIN-SUFFIX,amazonvideo.com,节点选择",
        "DOMAIN-SUFFIX,primevideo.com,节点选择",
        "DOMAIN-SUFFIX,media-amazon.com,节点选择",
        "DOMAIN,atv-ps.amazon.com,节点选择",
        "DOMAIN,fls-na.amazon.com,节点选择",
        "DOMAIN,avodmp4s3ww-a.akamaihd.net,节点选择",
        "DOMAIN,d25xi40x97liuc.cloudfront.net,节点选择",
        "DOMAIN,dmqdd6hw24ucf.cloudfront.net,节点选择",
        "DOMAIN,dmqdd6hw24ucf.cloudfront.net,节点选择",
        "DOMAIN,d22qjgkvxw22r6.cloudfront.net,节点选择",
        "DOMAIN,d1v5ir2lpwr8os.cloudfront.net,节点选择",
        "DOMAIN,d27xxe7juh1us6.cloudfront.net,节点选择",
        "DOMAIN-KEYWORD,avoddashs,节点选择",
        "DOMAIN,linear.tv.apple.com,节点选择",
        "DOMAIN,play-edge.itunes.apple.com,节点选择",
        "PROCESS-NAME,tw.com.gamer.android.animad,节点选择",
        "DOMAIN-SUFFIX,bahamut.com.tw,节点选择",
        "DOMAIN-SUFFIX,gamer.com.tw,节点选择",
        "DOMAIN,gamer-cds.cdn.hinet.net,节点选择",
        "DOMAIN,gamer2-cds.cdn.hinet.net,节点选择",
        "PROCESS-NAME,bbc.iplayer.android,节点选择",
        "DOMAIN-SUFFIX,bbc.co.uk,节点选择",
        "DOMAIN-SUFFIX,bbci.co.uk,节点选择",
        "DOMAIN-KEYWORD,bbcfmt,节点选择",
        "DOMAIN-KEYWORD,uk-live,节点选择",
        "PROCESS-NAME,com.dazn,节点选择",
        "DOMAIN-SUFFIX,dazn.com,节点选择",
        "DOMAIN-SUFFIX,dazn-api.com,节点选择",
        "DOMAIN,d151l6v8er5bdm.cloudfront.net,节点选择",
        "DOMAIN-KEYWORD,voddazn,节点选择",
        "PROCESS-NAME,com.disney.disneyplus,节点选择",
        "DOMAIN-SUFFIX,bamgrid.com,节点选择",
        "DOMAIN-SUFFIX,disneyplus.com,节点选择",
        "DOMAIN-SUFFIX,disney-plus.net,节点选择",
        "DOMAIN-SUFFIX,disney自动选择.com,节点选择",
        "DOMAIN-SUFFIX,dssott.com,节点选择",
        "DOMAIN,cdn.registerdisney.go.com,节点选择",
        "PROCESS-NAME,com.dmm.app.movieplayer,节点选择",
        "DOMAIN-SUFFIX,dmm.co.jp,节点选择",
        "DOMAIN-SUFFIX,dmm.com,节点选择",
        "DOMAIN-SUFFIX,dmm-extension.com,节点选择",
        "PROCESS-NAME,com.tvbusa.encore,节点选择",
        "DOMAIN-SUFFIX,encoretvb.com,节点选择",
        "DOMAIN,edge.api.brightcove.com,节点选择",
        "DOMAIN,bcbolt446c5271-a.akamaihd.net,节点选择",
        "PROCESS-NAME,com.fox.now,节点选择",
        "DOMAIN-SUFFIX,fox.com,节点选择",
        "DOMAIN-SUFFIX,foxdcg.com,节点选择",
        "DOMAIN-SUFFIX,theplatform.com,节点选择",
        "DOMAIN-SUFFIX,uplynk.com,节点选择",
        "DOMAIN-SUFFIX,foxplus.com,节点选择",
        "DOMAIN,cdn-fox-networks-group-green.akamaized.net,节点选择",
        "DOMAIN,d3cv4a9a9wh0bt.cloudfront.net,节点选择",
        "DOMAIN,foxsports01-i.akamaihd.net,节点选择",
        "DOMAIN,foxsports02-i.akamaihd.net,节点选择",
        "DOMAIN,foxsports03-i.akamaihd.net,节点选择",
        "DOMAIN,staticasiafox.akamaized.net,节点选择",
        "PROCESS-NAME,com.hbo.hbonow,节点选择",
        "DOMAIN-SUFFIX,hbo.com,节点选择",
        "DOMAIN-SUFFIX,hbogo.com,节点选择",
        "DOMAIN-SUFFIX,hbonow.com,节点选择",
        "DOMAIN-SUFFIX,hbomax.com,节点选择",
        "PROCESS-NAME,hk.hbo.hbogo,节点选择",
        "DOMAIN-SUFFIX,hbogoasia.com,节点选择",
        "DOMAIN-SUFFIX,hbogoasia.hk,节点选择",
        "DOMAIN,bcbolthboa-a.akamaihd.net,节点选择",
        "DOMAIN,players.brightcove.net,节点选择",
        "DOMAIN,s3-ap-southeast-1.amazonaws.com,节点选择",
        "DOMAIN,dai3fd1oh325y.cloudfront.net,节点选择",
        "DOMAIN,44wilhpljf.execute-api.ap-southeast-1.amazonaws.com,节点选择",
        "DOMAIN,hboasia1-i.akamaihd.net,节点选择",
        "DOMAIN,hboasia2-i.akamaihd.net,节点选择",
        "DOMAIN,hboasia3-i.akamaihd.net,节点选择",
        "DOMAIN,hboasia4-i.akamaihd.net,节点选择",
        "DOMAIN,hboasia5-i.akamaihd.net,节点选择",
        "DOMAIN,cf-images.ap-southeast-1.prod.boltdns.net,节点选择",
        "DOMAIN-SUFFIX,5itv.tv,节点选择",
        "DOMAIN-SUFFIX,ocnttv.com,节点选择",
        "PROCESS-NAME,com.hulu.plus,节点选择",
        "DOMAIN-SUFFIX,hulu.com,节点选择",
        "DOMAIN-SUFFIX,huluim.com,节点选择",
        "DOMAIN-SUFFIX,hulustream.com,节点选择",
        "PROCESS-NAME,jp.happyon.android,节点选择",
        "DOMAIN-SUFFIX,happyon.jp,节点选择",
        "DOMAIN-SUFFIX,hjholdings.jp,节点选择",
        "DOMAIN-SUFFIX,hulu.jp,节点选择",
        "PROCESS-NAME,air.ITVMobilePlayer,节点选择",
        "DOMAIN-SUFFIX,itv.com,节点选择",
        "DOMAIN-SUFFIX,itvstatic.com,节点选择",
        "DOMAIN,itvpnpmobile-a.akamaihd.net,节点选择",
        "PROCESS-NAME,com.kktv.kktv,节点选择",
        "DOMAIN-SUFFIX,kktv.com.tw,节点选择",
        "DOMAIN-SUFFIX,kktv.me,节点选择",
        "DOMAIN,kktv-theater.kk.stream,节点选择",
        "PROCESS-NAME,com.linecorp.linetv,节点选择",
        "DOMAIN-SUFFIX,linetv.tw,节点选择",
        "DOMAIN,d3c7rimkq79yfu.cloudfront.net,节点选择",
        "PROCESS-NAME,com.litv.mobile.gp.litv,节点选择",
        "DOMAIN-SUFFIX,litv.tv,节点选择",
        "DOMAIN,litvfreemobile-hichannel.cdn.hinet.net,节点选择",
        "PROCESS-NAME,com.mobileiq.demand5,节点选择",
        "DOMAIN-SUFFIX,channel5.com,节点选择",
        "DOMAIN-SUFFIX,my5.tv,节点选择",
        "DOMAIN,d349g9zuie06uo.cloudfront.net,节点选择",
        "PROCESS-NAME,com.tvb.mytvsuper,节点选择",
        "DOMAIN-SUFFIX,mytvsuper.com,节点选择",
        "DOMAIN-SUFFIX,tvb.com,节点选择",
        "PROCESS-NAME,com.netflix.mediaclient,节点选择",
        "DOMAIN-SUFFIX,netflix.com,节点选择",
        "DOMAIN-SUFFIX,netflix.net,节点选择",
        "DOMAIN-SUFFIX,nflxext.com,节点选择",
        "DOMAIN-SUFFIX,nflximg.com,节点选择",
        "DOMAIN-SUFFIX,nflximg.net,节点选择",
        "DOMAIN-SUFFIX,nflxso.net,节点选择",
        "DOMAIN-SUFFIX,nflxvideo.net,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest0.com,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest1.com,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest2.com,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest3.com,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest4.com,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest5.com,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest6.com,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest7.com,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest8.com,节点选择",
        "DOMAIN-SUFFIX,netflixdnstest9.com,节点选择",
        "DOMAIN-KEYWORD,dualstack.api自动选择-device-prod-nlb-,节点选择",
        "DOMAIN-KEYWORD,dualstack.ichnaea-web-,节点选择",
        "IP-CIDR,23.246.0.0/18,节点选择,no-resolve",
        "IP-CIDR,37.77.184.0/21,节点选择,no-resolve",
        "IP-CIDR,45.57.0.0/17,节点选择,no-resolve",
        "IP-CIDR,64.120.128.0/17,节点选择,no-resolve",
        "IP-CIDR,66.197.128.0/17,节点选择,no-resolve",
        "IP-CIDR,108.175.32.0/20,节点选择,no-resolve",
        "IP-CIDR,192.173.64.0/18,节点选择,no-resolve",
        "IP-CIDR,198.38.96.0/19,节点选择,no-resolve",
        "IP-CIDR,198.45.48.0/20,节点选择,no-resolve",
        "IP-CIDR,34.210.42.111/32,节点选择,no-resolve",
        "IP-CIDR,52.89.124.203/32,节点选择,no-resolve",
        "IP-CIDR,54.148.37.5/32,节点选择,no-resolve",
        "PROCESS-NAME,jp.nicovideo.android,节点选择",
        "DOMAIN-SUFFIX,dmc.nico,节点选择",
        "DOMAIN-SUFFIX,nicovideo.jp,节点选择",
        "DOMAIN-SUFFIX,nimg.jp,节点选择",
        "PROCESS-NAME,com.pccw.nowemobile,节点选择",
        "DOMAIN-SUFFIX,nowe.com,节点选择",
        "DOMAIN-SUFFIX,nowestatic.com,节点选择",
        "PROCESS-NAME,com.pbs.video,节点选择",
        "DOMAIN-SUFFIX,pbs.org,节点选择",
        "DOMAIN-SUFFIX,phncdn.com,节点选择",
        "DOMAIN-SUFFIX,phprcdn.com,节点选择",
        "DOMAIN-SUFFIX,pornhub.com,节点选择",
        "DOMAIN-SUFFIX,pornhubpremium.com,节点选择",
        "PROCESS-NAME,com.twgood.android,节点选择",
        "DOMAIN-SUFFIX,skyking.com.tw,节点选择",
        "DOMAIN,hamifans.emome.net,节点选择",
        "PROCESS-NAME,com.ss.android.ugc.trill,节点选择",
        "DOMAIN-SUFFIX,byteoversea.com,节点选择",
        "DOMAIN-SUFFIX,ibytedtos.com,节点选择",
        "DOMAIN-SUFFIX,muscdn.com,节点选择",
        "DOMAIN-SUFFIX,musical.ly,节点选择",
        "DOMAIN-SUFFIX,tiktok.com,节点选择",
        "DOMAIN-SUFFIX,tik-tokapi.com,节点选择",
        "DOMAIN-SUFFIX,tiktokcdn.com,节点选择",
        "DOMAIN-SUFFIX,tiktokv.com,节点选择",
        "DOMAIN-KEYWORD,-tiktokcdn-com,节点选择",
        "PROCESS-NAME,tv.twitch.android.app,节点选择",
        "DOMAIN-SUFFIX,jtvnw.net,节点选择",
        "DOMAIN-SUFFIX,ttvnw.net,节点选择",
        "DOMAIN-SUFFIX,twitch.tv,节点选择",
        "DOMAIN-SUFFIX,twitchcdn.net,节点选择",
        "PROCESS-NAME,com.hktve.viutv,节点选择",
        "DOMAIN-SUFFIX,viu.com,节点选择",
        "DOMAIN-SUFFIX,viu.tv,节点选择",
        "DOMAIN,api.viu.now.com,节点选择",
        "DOMAIN,d1k2us671qcoau.cloudfront.net,节点选择",
        "DOMAIN,d2anahhhmp1ffz.cloudfront.net,节点选择",
        "DOMAIN,dfp6rglgjqszk.cloudfront.net,节点选择",
        "PROCESS-NAME,com.google.android.youtube,节点选择",
        "PROCESS-NAME,com.google.android.youtube.tv,节点选择",
        "DOMAIN-SUFFIX,googlevideo.com,节点选择",
        "DOMAIN-SUFFIX,youtube.com,节点选择",
        "DOMAIN,youtubei.googleapis.com,节点选择",
        "DOMAIN-SUFFIX,biliapi.net,节点选择",
        "DOMAIN-SUFFIX,bilibili.com,节点选择",
        "DOMAIN,upos-hz-mirrorakam.akamaized.net,节点选择",
        "DOMAIN-SUFFIX,iq.com,节点选择",
        "DOMAIN,cache.video.iqiyi.com,节点选择",
        "DOMAIN,cache-video.iq.com,节点选择",
        "DOMAIN,intl.iqiyi.com,节点选择",
        "DOMAIN,intl-rcd.iqiyi.com,节点选择",
        "DOMAIN,intl-subscription.iqiyi.com,节点选择",
        "DOMAIN-KEYWORD,oversea-tw.inter.iqiyi.com,节点选择",
        "DOMAIN-KEYWORD,oversea-tw.inter.ptqy.gitv.tv,节点选择",
        "IP-CIDR,103.44.56.0/22,节点选择,no-resolve",
        "IP-CIDR,118.26.32.0/23,节点选择,no-resolve",
        "IP-CIDR,118.26.120.0/24,节点选择,no-resolve",
        "IP-CIDR,223.119.62.225/28,节点选择,no-resolve",
        "IP-CIDR,23.40.242.10/32,节点选择,no-resolve",
        "IP-CIDR,23.40.241.251/32,节点选择,no-resolve",
        "DOMAIN-SUFFIX,api.mgtv.com,节点选择",
        "DOMAIN-SUFFIX,wetv.vip,节点选择",
        "DOMAIN-SUFFIX,wetvinfo.com,节点选择",
        "DOMAIN,testflight.apple.com,节点选择",
        "DOMAIN-SUFFIX,appspot.com,节点选择",
        "DOMAIN-SUFFIX,blogger.com,节点选择",
        "DOMAIN-SUFFIX,getoutline.org,节点选择",
        "DOMAIN-SUFFIX,gvt0.com,节点选择",
        "DOMAIN-SUFFIX,gvt3.com,节点选择",
        "DOMAIN-SUFFIX,xn--ngstr-lra8j.com,节点选择",
        "DOMAIN-SUFFIX,ytimg.com,节点选择",
        "DOMAIN-KEYWORD,google,节点选择",
        "DOMAIN-KEYWORD,.blogspot.,节点选择",
        "DOMAIN-SUFFIX,aka.ms,节点选择",
        "DOMAIN-SUFFIX,onedrive.live.com,节点选择",
        "DOMAIN,az416426.vo.msecnd.net,节点选择",
        "DOMAIN,az668014.vo.msecnd.net,节点选择",
        "DOMAIN-SUFFIX,cdninstagram.com,节点选择",
        "DOMAIN-SUFFIX,facebook.com,节点选择",
        "DOMAIN-SUFFIX,facebook.net,节点选择",
        "DOMAIN-SUFFIX,fb.com,节点选择",
        "DOMAIN-SUFFIX,fb.me,节点选择",
        "DOMAIN-SUFFIX,fbaddins.com,节点选择",
        "DOMAIN-SUFFIX,fbcdn.net,节点选择",
        "DOMAIN-SUFFIX,fbsbx.com,节点选择",
        "DOMAIN-SUFFIX,fbworkmail.com,节点选择",
        "DOMAIN-SUFFIX,instagram.com,节点选择",
        "DOMAIN-SUFFIX,m.me,节点选择",
        "DOMAIN-SUFFIX,messenger.com,节点选择",
        "DOMAIN-SUFFIX,oculus.com,节点选择",
        "DOMAIN-SUFFIX,oculuscdn.com,节点选择",
        "DOMAIN-SUFFIX,rocksdb.org,节点选择",
        "DOMAIN-SUFFIX,whatsapp.com,节点选择",
        "DOMAIN-SUFFIX,whatsapp.net,节点选择",
        "DOMAIN-SUFFIX,pscp.tv,节点选择",
        "DOMAIN-SUFFIX,periscope.tv,节点选择",
        "DOMAIN-SUFFIX,t.co,节点选择",
        "DOMAIN-SUFFIX,twimg.co,节点选择",
        "DOMAIN-SUFFIX,twimg.com,节点选择",
        "DOMAIN-SUFFIX,twitpic.com,节点选择",
        "DOMAIN-SUFFIX,twitter.com,节点选择",
        "DOMAIN-SUFFIX,x.com,节点选择",
        "DOMAIN-SUFFIX,vine.co,节点选择",
        "DOMAIN-SUFFIX,telegra.ph,节点选择",
        "DOMAIN-SUFFIX,telegram.org,节点选择",
        "IP-CIDR,91.108.4.0/22,节点选择,no-resolve",
        "IP-CIDR,91.108.8.0/22,节点选择,no-resolve",
        "IP-CIDR,91.108.12.0/22,节点选择,no-resolve",
        "IP-CIDR,91.108.16.0/22,节点选择,no-resolve",
        "IP-CIDR,91.108.20.0/22,节点选择,no-resolve",
        "IP-CIDR,91.108.56.0/22,节点选择,no-resolve",
        "IP-CIDR,149.154.160.0/20,节点选择,no-resolve",
        "IP-CIDR,2001:b28:f23d::/48,节点选择,no-resolve",
        "IP-CIDR,2001:b28:f23f::/48,节点选择,no-resolve",
        "IP-CIDR,2001:67c:4e8::/48,节点选择,no-resolve",
        "DOMAIN-SUFFIX,line.me,节点选择",
        "DOMAIN-SUFFIX,line-apps.com,节点选择",
        "DOMAIN-SUFFIX,line-scdn.net,节点选择",
        "DOMAIN-SUFFIX,naver.jp,节点选择",
        "IP-CIDR,103.2.30.0/23,节点选择,no-resolve",
        "IP-CIDR,125.209.208.0/20,节点选择,no-resolve",
        "IP-CIDR,147.92.128.0/17,节点选择,no-resolve",
        "IP-CIDR,203.104.144.0/21,节点选择,no-resolve",
        "DOMAIN-SUFFIX,amazon.co.jp,节点选择",
        "DOMAIN,d3c33hcgiwev3.cloudfront.net,节点选择",
        "DOMAIN,payments-jp.amazon.com,节点选择",
        "DOMAIN,s3-ap-northeast-1.amazonaws.com,节点选择",
        "DOMAIN,s3-ap-southeast-2.amazonaws.com,节点选择",
        "DOMAIN,a248.e.akamai.net,节点选择",
        "DOMAIN,a771.dscq.akamai.net,节点选择",
        "DOMAIN-SUFFIX,4shared.com,节点选择",
        "DOMAIN-SUFFIX,9cache.com,节点选择",
        "DOMAIN-SUFFIX,9gag.com,节点选择",
        "DOMAIN-SUFFIX,abc.com,节点选择",
        "DOMAIN-SUFFIX,abc.net.au,节点选择",
        "DOMAIN-SUFFIX,abebooks.com,节点选择",
        "DOMAIN-SUFFIX,ao3.org,节点选择",
        "DOMAIN-SUFFIX,apigee.com,节点选择",
        "DOMAIN-SUFFIX,apkcombo.com,节点选择",
        "DOMAIN-SUFFIX,apk-dl.com,节点选择",
        "DOMAIN-SUFFIX,apkfind.com,节点选择",
        "DOMAIN-SUFFIX,apkmirror.com,节点选择",
        "DOMAIN-SUFFIX,apkmonk.com,节点选择",
        "DOMAIN-SUFFIX,apkpure.com,节点选择",
        "DOMAIN-SUFFIX,aptoide.com,节点选择",
        "DOMAIN-SUFFIX,archive.is,节点选择",
        "DOMAIN-SUFFIX,archive.org,节点选择",
        "DOMAIN-SUFFIX,archiveofourown.com,节点选择",
        "DOMAIN-SUFFIX,archiveofourown.org,节点选择",
        "DOMAIN-SUFFIX,arte.tv,节点选择",
        "DOMAIN-SUFFIX,artstation.com,节点选择",
        "DOMAIN-SUFFIX,arukas.io,节点选择",
        "DOMAIN-SUFFIX,ask.com,节点选择",
        "DOMAIN-SUFFIX,avg.com,节点选择",
        "DOMAIN-SUFFIX,avgle.com,节点选择",
        "DOMAIN-SUFFIX,badoo.com,节点选择",
        "DOMAIN-SUFFIX,bandwagonhost.com,节点选择",
        "DOMAIN-SUFFIX,bangkokpost.com,节点选择",
        "DOMAIN-SUFFIX,bbc.com,节点选择",
        "DOMAIN-SUFFIX,behance.net,节点选择",
        "DOMAIN-SUFFIX,bibox.com,节点选择",
        "DOMAIN-SUFFIX,biggo.com.tw,节点选择",
        "DOMAIN-SUFFIX,binance.com,节点选择",
        "DOMAIN-SUFFIX,bit.ly,节点选择",
        "DOMAIN-SUFFIX,bitcointalk.org,节点选择",
        "DOMAIN-SUFFIX,bitfinex.com,节点选择",
        "DOMAIN-SUFFIX,bitmex.com,节点选择",
        "DOMAIN-SUFFIX,bit-z.com,节点选择",
        "DOMAIN-SUFFIX,bloglovin.com,节点选择",
        "DOMAIN-SUFFIX,bloomberg.cn,节点选择",
        "DOMAIN-SUFFIX,bloomberg.com,节点选择",
        "DOMAIN-SUFFIX,blubrry.com,节点选择",
        "DOMAIN-SUFFIX,book.com.tw,节点选择",
        "DOMAIN-SUFFIX,booklive.jp,节点选择",
        "DOMAIN-SUFFIX,books.com.tw,节点选择",
        "DOMAIN-SUFFIX,boslife.net,节点选择",
        "DOMAIN-SUFFIX,box.com,节点选择",
        "DOMAIN-SUFFIX,brave.com,节点选择",
        "DOMAIN-SUFFIX,businessinsider.com,节点选择",
        "DOMAIN-SUFFIX,buzzfeed.com,节点选择",
        "DOMAIN-SUFFIX,bwh1.net,节点选择",
        "DOMAIN-SUFFIX,castbox.fm,节点选择",
        "DOMAIN-SUFFIX,cbc.ca,节点选择",
        "DOMAIN-SUFFIX,cdw.com,节点选择",
        "DOMAIN-SUFFIX,change.org,节点选择",
        "DOMAIN-SUFFIX,channelnewsasia.com,节点选择",
        "DOMAIN-SUFFIX,ck101.com,节点选择",
        "DOMAIN-SUFFIX,clarionproject.org,节点选择",
        "DOMAIN-SUFFIX,cloudcone.com,节点选择",
        "DOMAIN-SUFFIX,clyp.it,节点选择",
        "DOMAIN-SUFFIX,cna.com.tw,节点选择",
        "DOMAIN-SUFFIX,comparitech.com,节点选择",
        "DOMAIN-SUFFIX,conoha.jp,节点选择",
        "DOMAIN-SUFFIX,crucial.com,节点选择",
        "DOMAIN-SUFFIX,cts.com.tw,节点选择",
        "DOMAIN-SUFFIX,cw.com.tw,节点选择",
        "DOMAIN-SUFFIX,cyberctm.com,节点选择",
        "DOMAIN-SUFFIX,dailymotion.com,节点选择",
        "DOMAIN-SUFFIX,dailyview.tw,节点选择",
        "DOMAIN-SUFFIX,daum.net,节点选择",
        "DOMAIN-SUFFIX,daumcdn.net,节点选择",
        "DOMAIN-SUFFIX,dcard.tw,节点选择",
        "DOMAIN-SUFFIX,deadline.com,节点选择",
        "DOMAIN-SUFFIX,deepdiscount.com,节点选择",
        "DOMAIN-SUFFIX,depositphotos.com,节点选择",
        "DOMAIN-SUFFIX,deviantart.com,节点选择",
        "DOMAIN-SUFFIX,disconnect.me,节点选择",
        "DOMAIN-SUFFIX,discordapp.com,节点选择",
        "DOMAIN-SUFFIX,discordapp.net,节点选择",
        "DOMAIN-SUFFIX,disqus.com,节点选择",
        "DOMAIN-SUFFIX,dlercloud.com,节点选择",
        "DOMAIN-SUFFIX,dmhy.org,节点选择",
        "DOMAIN-SUFFIX,dns2go.com,节点选择",
        "DOMAIN-SUFFIX,dowjones.com,节点选择",
        "DOMAIN-SUFFIX,dropbox.com,节点选择",
        "DOMAIN-SUFFIX,dropboxapi.com,节点选择",
        "DOMAIN-SUFFIX,dropboxusercontent.com,节点选择",
        "DOMAIN-SUFFIX,duckduckgo.com,节点选择",
        "DOMAIN-SUFFIX,duyaoss.com,节点选择",
        "DOMAIN-SUFFIX,dw.com,节点选择",
        "DOMAIN-SUFFIX,dynu.com,节点选择",
        "DOMAIN-SUFFIX,earthcam.com,节点选择",
        "DOMAIN-SUFFIX,ebookservice.tw,节点选择",
        "DOMAIN-SUFFIX,economist.com,节点选择",
        "DOMAIN-SUFFIX,edgecastcdn.net,节点选择",
        "DOMAIN-SUFFIX,edx-cdn.org,节点选择",
        "DOMAIN-SUFFIX,elpais.com,节点选择",
        "DOMAIN-SUFFIX,enanyang.my,节点选择",
        "DOMAIN-SUFFIX,encyclopedia.com,节点选择",
        "DOMAIN-SUFFIX,esoir.be,节点选择",
        "DOMAIN-SUFFIX,etherscan.io,节点选择",
        "DOMAIN-SUFFIX,euronews.com,节点选择",
        "DOMAIN-SUFFIX,evozi.com,节点选择",
        "DOMAIN-SUFFIX,exblog.jp,节点选择",
        "DOMAIN-SUFFIX,feeder.co,节点选择",
        "DOMAIN-SUFFIX,feedly.com,节点选择",
        "DOMAIN-SUFFIX,feedx.net,节点选择",
        "DOMAIN-SUFFIX,firech.at,节点选择",
        "DOMAIN-SUFFIX,flickr.com,节点选择",
        "DOMAIN-SUFFIX,flipboard.com,节点选择",
        "DOMAIN-SUFFIX,flitto.com,节点选择",
        "DOMAIN-SUFFIX,foreignpolicy.com,节点选择",
        "DOMAIN-SUFFIX,fortawesome.com,节点选择",
        "DOMAIN-SUFFIX,freetls.fastly.net,节点选择",
        "DOMAIN-SUFFIX,friday.tw,节点选择",
        "DOMAIN-SUFFIX,ft.com,节点选择",
        "DOMAIN-SUFFIX,ftchinese.com,节点选择",
        "DOMAIN-SUFFIX,ftimg.net,节点选择",
        "DOMAIN-SUFFIX,gate.io,节点选择",
        "DOMAIN-SUFFIX,genius.com,节点选择",
        "DOMAIN-SUFFIX,getlantern.org,节点选择",
        "DOMAIN-SUFFIX,getsync.com,节点选择",
        "DOMAIN-SUFFIX,github.com,节点选择",
        "DOMAIN-SUFFIX,github.io,节点选择",
        "DOMAIN-SUFFIX,githubusercontent.com,节点选择",
        "DOMAIN-SUFFIX,globalvoices.org,节点选择",
        "DOMAIN-SUFFIX,goo.ne.jp,节点选择",
        "DOMAIN-SUFFIX,goodreads.com,节点选择",
        "DOMAIN-SUFFIX,gov.tw,节点选择",
        "DOMAIN-SUFFIX,greatfire.org,节点选择",
        "DOMAIN-SUFFIX,gumroad.com,节点选择",
        "DOMAIN-SUFFIX,hbg.com,节点选择",
        "DOMAIN-SUFFIX,heroku.com,节点选择",
        "DOMAIN-SUFFIX,hightail.com,节点选择",
        "DOMAIN-SUFFIX,hk01.com,节点选择",
        "DOMAIN-SUFFIX,hkbf.org,节点选择",
        "DOMAIN-SUFFIX,hkbookcity.com,节点选择",
        "DOMAIN-SUFFIX,hkej.com,节点选择",
        "DOMAIN-SUFFIX,hket.com,节点选择",
        "DOMAIN-SUFFIX,hootsuite.com,节点选择",
        "DOMAIN-SUFFIX,hudson.org,节点选择",
        "DOMAIN-SUFFIX,huffpost.com,节点选择",
        "DOMAIN-SUFFIX,hyread.com.tw,节点选择",
        "DOMAIN-SUFFIX,ibtimes.com,节点选择",
        "DOMAIN-SUFFIX,i-cable.com,节点选择",
        "DOMAIN-SUFFIX,icij.org,节点选择",
        "DOMAIN-SUFFIX,icoco.com,节点选择",
        "DOMAIN-SUFFIX,imgur.com,节点选择",
        "DOMAIN-SUFFIX,independent.co.uk,节点选择",
        "DOMAIN-SUFFIX,initiummall.com,节点选择",
        "DOMAIN-SUFFIX,inoreader.com,节点选择",
        "DOMAIN-SUFFIX,insecam.org,节点选择",
        "DOMAIN-SUFFIX,ipfs.io,节点选择",
        "DOMAIN-SUFFIX,issuu.com,节点选择",
        "DOMAIN-SUFFIX,istockphoto.com,节点选择",
        "DOMAIN-SUFFIX,japantimes.co.jp,节点选择",
        "DOMAIN-SUFFIX,jiji.com,节点选择",
        "DOMAIN-SUFFIX,jinx.com,节点选择",
        "DOMAIN-SUFFIX,jkforum.net,节点选择",
        "DOMAIN-SUFFIX,joinmastodon.org,节点选择",
        "DOMAIN-SUFFIX,justmysocks.net,节点选择",
        "DOMAIN-SUFFIX,justpaste.it,节点选择",
        "DOMAIN-SUFFIX,kadokawa.co.jp,节点选择",
        "DOMAIN-SUFFIX,kakao.com,节点选择",
        "DOMAIN-SUFFIX,kakaocorp.com,节点选择",
        "DOMAIN-SUFFIX,kik.com,节点选择",
        "DOMAIN-SUFFIX,kingkong.com.tw,节点选择",
        "DOMAIN-SUFFIX,knowyourmeme.com,节点选择",
        "DOMAIN-SUFFIX,kobo.com,节点选择",
        "DOMAIN-SUFFIX,kobobooks.com,节点选择",
        "DOMAIN-SUFFIX,kodingen.com,节点选择",
        "DOMAIN-SUFFIX,lemonde.fr,节点选择",
        "DOMAIN-SUFFIX,lepoint.fr,节点选择",
        "DOMAIN-SUFFIX,lihkg.com,节点选择",
        "DOMAIN-SUFFIX,linkedin.com,节点选择",
        "DOMAIN-SUFFIX,limbopro.xyz,节点选择",
        "DOMAIN-SUFFIX,listennotes.com,节点选择",
        "DOMAIN-SUFFIX,livestream.com,节点选择",
        "DOMAIN-SUFFIX,logimg.jp,节点选择",
        "DOMAIN-SUFFIX,logmein.com,节点选择",
        "DOMAIN-SUFFIX,mail.ru,节点选择",
        "DOMAIN-SUFFIX,mailchimp.com,节点选择",
        "DOMAIN-SUFFIX,marc.info,节点选择",
        "DOMAIN-SUFFIX,matters.news,节点选择",
        "DOMAIN-SUFFIX,maying.co,节点选择",
        "DOMAIN-SUFFIX,medium.com,节点选择",
        "DOMAIN-SUFFIX,mega.nz,节点选择",
        "DOMAIN-SUFFIX,mergersandinquisitions.com,节点选择",
        "DOMAIN-SUFFIX,mingpao.com,节点选择",
        "DOMAIN-SUFFIX,mixi.jp,节点选择",
        "DOMAIN-SUFFIX,mobile01.com,节点选择",
        "DOMAIN-SUFFIX,mubi.com,节点选择",
        "DOMAIN-SUFFIX,myspace.com,节点选择",
        "DOMAIN-SUFFIX,myspacecdn.com,节点选择",
        "DOMAIN-SUFFIX,nanyang.com,节点选择",
        "DOMAIN-SUFFIX,nationalinterest.org,节点选择",
        "DOMAIN-SUFFIX,naver.com,节点选择",
        "DOMAIN-SUFFIX,nbcnews.com,节点选择",
        "DOMAIN-SUFFIX,ndr.de,节点选择",
        "DOMAIN-SUFFIX,neowin.net,节点选择",
        "DOMAIN-SUFFIX,newstapa.org,节点选择",
        "DOMAIN-SUFFIX,nexitally.com,节点选择",
        "DOMAIN-SUFFIX,nhk.or.jp,节点选择",
        "DOMAIN-SUFFIX,nii.ac.jp,节点选择",
        "DOMAIN-SUFFIX,nikkei.com,节点选择",
        "DOMAIN-SUFFIX,nitter.net,节点选择",
        "DOMAIN-SUFFIX,nofile.io,节点选择",
        "DOMAIN-SUFFIX,notion.so,节点选择",
        "DOMAIN-SUFFIX,now.com,节点选择",
        "DOMAIN-SUFFIX,nrk.no,节点选择",
        "DOMAIN-SUFFIX,nuget.org,节点选择",
        "DOMAIN-SUFFIX,nyaa.si,节点选择",
        "DOMAIN-SUFFIX,nyt.com,节点选择",
        "DOMAIN-SUFFIX,nytchina.com,节点选择",
        "DOMAIN-SUFFIX,nytcn.me,节点选择",
        "DOMAIN-SUFFIX,nytco.com,节点选择",
        "DOMAIN-SUFFIX,nytimes.com,节点选择",
        "DOMAIN-SUFFIX,nytimg.com,节点选择",
        "DOMAIN-SUFFIX,nytlog.com,节点选择",
        "DOMAIN-SUFFIX,nytstyle.com,节点选择",
        "DOMAIN-SUFFIX,ok.ru,节点选择",
        "DOMAIN-SUFFIX,okex.com,节点选择",
        "DOMAIN-SUFFIX,on.cc,节点选择",
        "DOMAIN-SUFFIX,orientaldaily.com.my,节点选择",
        "DOMAIN-SUFFIX,overcast.fm,节点选择",
        "DOMAIN-SUFFIX,paltalk.com,节点选择",
        "DOMAIN-SUFFIX,parsevideo.com,节点选择",
        "DOMAIN-SUFFIX,pawoo.net,节点选择",
        "DOMAIN-SUFFIX,pbxes.com,节点选择",
        "DOMAIN-SUFFIX,pcdvd.com.tw,节点选择",
        "DOMAIN-SUFFIX,pchome.com.tw,节点选择",
        "DOMAIN-SUFFIX,pcloud.com,节点选择",
        "DOMAIN-SUFFIX,peing.net,节点选择",
        "DOMAIN-SUFFIX,picacomic.com,节点选择",
        "DOMAIN-SUFFIX,pinimg.com,节点选择",
        "DOMAIN-SUFFIX,pixiv.net,节点选择",
        "DOMAIN-SUFFIX,player.fm,节点选择",
        "DOMAIN-SUFFIX,plurk.com,节点选择",
        "DOMAIN-SUFFIX,po18.tw,节点选择",
        "DOMAIN-SUFFIX,potato.im,节点选择",
        "DOMAIN-SUFFIX,potatso.com,节点选择",
        "DOMAIN-SUFFIX,prism-break.org,节点选择",
        "DOMAIN-SUFFIX,proxifier.com,节点选择",
        "DOMAIN-SUFFIX,pt.im,节点选择",
        "DOMAIN-SUFFIX,pts.org.tw,节点选择",
        "DOMAIN-SUFFIX,pubu.com.tw,节点选择",
        "DOMAIN-SUFFIX,pubu.tw,节点选择",
        "DOMAIN-SUFFIX,pureapk.com,节点选择",
        "DOMAIN-SUFFIX,quora.com,节点选择",
        "DOMAIN-SUFFIX,quoracdn.net,节点选择",
        "DOMAIN-SUFFIX,qz.com,节点选择",
        "DOMAIN-SUFFIX,radio.garden,节点选择",
        "DOMAIN-SUFFIX,rakuten.co.jp,节点选择",
        "DOMAIN-SUFFIX,rarbgprx.org,节点选择",
        "DOMAIN-SUFFIX,reabble.com,节点选择",
        "DOMAIN-SUFFIX,readingtimes.com.tw,节点选择",
        "DOMAIN-SUFFIX,readmoo.com,节点选择",
        "DOMAIN-SUFFIX,redbubble.com,节点选择",
        "DOMAIN-SUFFIX,redd.it,节点选择",
        "DOMAIN-SUFFIX,reddit.com,节点选择",
        "DOMAIN-SUFFIX,redditmedia.com,节点选择",
        "DOMAIN-SUFFIX,resilio.com,节点选择",
        "DOMAIN-SUFFIX,reuters.com,节点选择",
        "DOMAIN-SUFFIX,reutersmedia.net,节点选择",
        "DOMAIN-SUFFIX,rfi.fr,节点选择",
        "DOMAIN-SUFFIX,rixcloud.com,节点选择",
        "DOMAIN-SUFFIX,roadshow.hk,节点选择",
        "DOMAIN-SUFFIX,rsshub.app,节点选择",
        "DOMAIN-SUFFIX,scmp.com,节点选择",
        "DOMAIN-SUFFIX,scribd.com,节点选择",
        "DOMAIN-SUFFIX,seatguru.com,节点选择",
        "DOMAIN-SUFFIX,shadowsocks.org,节点选择",
        "DOMAIN-SUFFIX,shindanmaker.com,节点选择",
        "DOMAIN-SUFFIX,shopee.tw,节点选择",
        "DOMAIN-SUFFIX,sina.com.hk,节点选择",
        "DOMAIN-SUFFIX,slideshare.net,节点选择",
        "DOMAIN-SUFFIX,softfamous.com,节点选择",
        "DOMAIN-SUFFIX,spiegel.de,节点选择",
        "DOMAIN-SUFFIX,ssrcloud.org,节点选择",
        "DOMAIN-SUFFIX,startpage.com,节点选择",
        "DOMAIN-SUFFIX,steamcommunity.com,节点选择",
        "DOMAIN-SUFFIX,steemit.com,节点选择",
        "DOMAIN-SUFFIX,steemitwallet.com,节点选择",
        "DOMAIN-SUFFIX,straitstimes.com,节点选择",
        "DOMAIN-SUFFIX,streamable.com,节点选择",
        "DOMAIN-SUFFIX,streema.com,节点选择",
        "DOMAIN-SUFFIX,t66y.com,节点选择",
        "DOMAIN-SUFFIX,tapatalk.com,节点选择",
        "DOMAIN-SUFFIX,teco-hk.org,节点选择",
        "DOMAIN-SUFFIX,teco-mo.org,节点选择",
        "DOMAIN-SUFFIX,teddysun.com,节点选择",
        "DOMAIN-SUFFIX,textnow.me,节点选择",
        "DOMAIN-SUFFIX,theguardian.com,节点选择",
        "DOMAIN-SUFFIX,theinitium.com,节点选择",
        "DOMAIN-SUFFIX,themoviedb.org,节点选择",
        "DOMAIN-SUFFIX,thetvdb.com,节点选择",
        "DOMAIN-SUFFIX,time.com,节点选择",
        "DOMAIN-SUFFIX,tineye.com,节点选择",
        "DOMAIN-SUFFIX,tiny.cc,节点选择",
        "DOMAIN-SUFFIX,tinyurl.com,节点选择",
        "DOMAIN-SUFFIX,torproject.org,节点选择",
        "DOMAIN-SUFFIX,tumblr.com,节点选择",
        "DOMAIN-SUFFIX,turbobit.net,节点选择",
        "DOMAIN-SUFFIX,tutanota.com,节点选择",
        "DOMAIN-SUFFIX,tvboxnow.com,节点选择",
        "DOMAIN-SUFFIX,udn.com,节点选择",
        "DOMAIN-SUFFIX,unseen.is,节点选择",
        "DOMAIN-SUFFIX,upmedia.mg,节点选择",
        "DOMAIN-SUFFIX,uptodown.com,节点选择",
        "DOMAIN-SUFFIX,urbandictionary.com,节点选择",
        "DOMAIN-SUFFIX,ustream.tv,节点选择",
        "DOMAIN-SUFFIX,uwants.com,节点选择",
        "DOMAIN-SUFFIX,v2fly.org,节点选择",
        "DOMAIN-SUFFIX,v2ray.com,节点选择",
        "DOMAIN-SUFFIX,viber.com,节点选择",
        "DOMAIN-SUFFIX,videopress.com,节点选择",
        "DOMAIN-SUFFIX,vimeo.com,节点选择",
        "DOMAIN-SUFFIX,voachinese.com,节点选择",
        "DOMAIN-SUFFIX,voanews.com,节点选择",
        "DOMAIN-SUFFIX,voxer.com,节点选择",
        "DOMAIN-SUFFIX,vzw.com,节点选择",
        "DOMAIN-SUFFIX,w3schools.com,节点选择",
        "DOMAIN-SUFFIX,washingtonpost.com,节点选择",
        "DOMAIN-SUFFIX,wattpad.com,节点选择",
        "DOMAIN-SUFFIX,whoer.net,节点选择",
        "DOMAIN-SUFFIX,wikileaks.org,节点选择",
        "DOMAIN-SUFFIX,wikimapia.org,节点选择",
        "DOMAIN-SUFFIX,wikimedia.org,节点选择",
        "DOMAIN-SUFFIX,wikinews.org,节点选择",
        "DOMAIN-SUFFIX,wikipedia.org,节点选择",
        "DOMAIN-SUFFIX,wikiquote.org,节点选择",
        "DOMAIN-SUFFIX,wikiwand.com,节点选择",
        "DOMAIN-SUFFIX,winudf.com,节点选择",
        "DOMAIN-SUFFIX,wire.com,节点选择",
        "DOMAIN-SUFFIX,wn.com,节点选择",
        "DOMAIN-SUFFIX,wordpress.com,节点选择",
        "DOMAIN-SUFFIX,workflow.is,节点选择",
        "DOMAIN-SUFFIX,worldcat.org,节点选择",
        "DOMAIN-SUFFIX,wsj.com,节点选择",
        "DOMAIN-SUFFIX,wsj.net,节点选择",
        "DOMAIN-SUFFIX,xhamster.com,节点选择",
        "DOMAIN-SUFFIX,xn--90wwvt03e.com,节点选择",
        "DOMAIN-SUFFIX,xn--i2ru8q2qg.com,节点选择",
        "DOMAIN-SUFFIX,xnxx.com,节点选择",
        "DOMAIN-SUFFIX,xvideos.com,节点选择",
        "DOMAIN-SUFFIX,yahoo.com,节点选择",
        "DOMAIN-SUFFIX,yandex.ru,节点选择",
        "DOMAIN-SUFFIX,ycombinator.com,节点选择",
        "DOMAIN-SUFFIX,yesasia.com,节点选择",
        "DOMAIN-SUFFIX,yes-news.com,节点选择",
        "DOMAIN-SUFFIX,yomiuri.co.jp,节点选择",
        "DOMAIN-SUFFIX,you-get.org,节点选择",
        "DOMAIN-SUFFIX,zaobao.com,节点选择",
        "DOMAIN-SUFFIX,zb.com,节点选择",
        "DOMAIN-SUFFIX,zello.com,节点选择",
        "DOMAIN-SUFFIX,zeronet.io,节点选择",
        "DOMAIN-SUFFIX,zoom.us,节点选择",
        "DOMAIN,cc.tvbs.com.tw,节点选择",
        "DOMAIN,ocsp.int-x3.letsencrypt.org,节点选择",
        "DOMAIN,search.avira.com,节点选择",
        "DOMAIN,us.weibo.com,节点选择",
        "DOMAIN-KEYWORD,.pinterest.,节点选择",
        "DOMAIN-SUFFIX,edu,节点选择",
        "DOMAIN-SUFFIX,gov,节点选择",
        "DOMAIN-SUFFIX,mil,节点选择",
        "DOMAIN-SUFFIX,google,节点选择",
        "DOMAIN-SUFFIX,abc.xyz,节点选择",
        "DOMAIN-SUFFIX,advertisercommunity.com,节点选择",
        "DOMAIN-SUFFIX,ampproject.org,节点选择",
        "DOMAIN-SUFFIX,android.com,节点选择",
        "DOMAIN-SUFFIX,androidify.com,节点选择",
        "DOMAIN-SUFFIX,autodraw.com,节点选择",
        "DOMAIN-SUFFIX,capitalg.com,节点选择",
        "DOMAIN-SUFFIX,certificate-transparency.org,节点选择",
        "DOMAIN-SUFFIX,chrome.com,节点选择",
        "DOMAIN-SUFFIX,chromeexperiments.com,节点选择",
        "DOMAIN-SUFFIX,chromestatus.com,节点选择",
        "DOMAIN-SUFFIX,chromium.org,节点选择",
        "DOMAIN-SUFFIX,creativelab5.com,节点选择",
        "DOMAIN-SUFFIX,debug.com,节点选择",
        "DOMAIN-SUFFIX,deepmind.com,节点选择",
        "DOMAIN-SUFFIX,dialogflow.com,节点选择",
        "DOMAIN-SUFFIX,firebaseio.com,节点选择",
        "DOMAIN-SUFFIX,getmdl.io,节点选择",
        "DOMAIN-SUFFIX,ggpht.com,节点选择",
        "DOMAIN-SUFFIX,googleapis.cn,节点选择",
        "DOMAIN-SUFFIX,gmail.com,节点选择",
        "DOMAIN-SUFFIX,gmodules.com,节点选择",
        "DOMAIN-SUFFIX,godoc.org,节点选择",
        "DOMAIN-SUFFIX,golang.org,节点选择",
        "DOMAIN-SUFFIX,gstatic.com,节点选择",
        "DOMAIN-SUFFIX,gv.com,节点选择",
        "DOMAIN-SUFFIX,gwtproject.org,节点选择",
        "DOMAIN-SUFFIX,itasoftware.com,节点选择",
        "DOMAIN-SUFFIX,madewithcode.com,节点选择",
        "DOMAIN-SUFFIX,material.io,节点选择",
        "DOMAIN-SUFFIX,page.link,节点选择",
        "DOMAIN-SUFFIX,polymer-project.org,节点选择",
        "DOMAIN-SUFFIX,recaptcha.net,节点选择",
        "DOMAIN-SUFFIX,shattered.io,节点选择",
        "DOMAIN-SUFFIX,synergyse.com,节点选择",
        "DOMAIN-SUFFIX,telephony.goog,节点选择",
        "DOMAIN-SUFFIX,tensorflow.org,节点选择",
        "DOMAIN-SUFFIX,tfhub.dev,节点选择",
        "DOMAIN-SUFFIX,tiltbrush.com,节点选择",
        "DOMAIN-SUFFIX,waveprotocol.org,节点选择",
        "DOMAIN-SUFFIX,waymo.com,节点选择",
        "DOMAIN-SUFFIX,webmproject.org,节点选择",
        "DOMAIN-SUFFIX,webrtc.org,节点选择",
        "DOMAIN-SUFFIX,whatbrowser.org,节点选择",
        "DOMAIN-SUFFIX,widevine.com,节点选择",
        "DOMAIN-SUFFIX,x.company,节点选择",
        "DOMAIN-SUFFIX,youtu.be,节点选择",
        "DOMAIN-SUFFIX,yt.be,节点选择",
        "DOMAIN-SUFFIX,ytimg.com,节点选择",
        "DOMAIN-SUFFIX,t.me,节点选择",
        "DOMAIN-SUFFIX,tdesktop.com,节点选择",
        "DOMAIN-SUFFIX,telegram.me,节点选择",
        "DOMAIN-SUFFIX,telesco.pe,节点选择",
        "DOMAIN-KEYWORD,.facebook.,节点选择",
        "DOMAIN-SUFFIX,facebookmail.com,节点选择",
        "DOMAIN-SUFFIX,noxinfluencer.com,节点选择",
        "DOMAIN-SUFFIX,smartmailcloud.com,节点选择",
        "DOMAIN-SUFFIX,weebly.com,节点选择",
        "DOMAIN-SUFFIX,twitter.jp,节点选择",
        "DOMAIN-SUFFIX,appsto.re,节点选择",
        "DOMAIN,books.itunes.apple.com,节点选择",
        "DOMAIN,apps.apple.com,节点选择",
        "DOMAIN,itunes.apple.com,节点选择",
        "DOMAIN,api-glb-sea.smoot.apple.com,节点选择",
        "DOMAIN-SUFFIX,smoot.apple.com,节点选择",
        "DOMAIN,lookup-api.apple.com,节点选择",
        "DOMAIN,beta.music.apple.com,节点选择",
        "DOMAIN-SUFFIX,bing.com,节点选择",
        "DOMAIN-SUFFIX,cccat.io,节点选择",
        "DOMAIN-SUFFIX,dubox.com,节点选择",
        "DOMAIN-SUFFIX,duboxcdn.com,节点选择",
        "DOMAIN-SUFFIX,ifixit.com,节点选择",
        "DOMAIN-SUFFIX,mangakakalot.com,节点选择",
        "DOMAIN-SUFFIX,shopeemobile.com,节点选择",
        "DOMAIN-SUFFIX,cloudcone.com.cn,节点选择",
        "DOMAIN-SUFFIX,inkbunny.net,节点选择",
        "DOMAIN-SUFFIX,metapix.net,节点选择",
        "DOMAIN-SUFFIX,s3.amazonaws.com,节点选择",
        "DOMAIN-SUFFIX,zaobao.com.sg,节点选择",
        "DOMAIN,international-gfe.download.nvidia.com,节点选择",
        "DOMAIN,ocsp.apple.com,节点选择",
        "DOMAIN,store-images.s-microsoft.com,节点选择",
        "DOMAIN-SUFFIX,qhres.com,DIRECT",
        "DOMAIN-SUFFIX,qhimg.com,DIRECT",
        "DOMAIN-SUFFIX,alibaba.com,DIRECT",
        "DOMAIN-SUFFIX,alibabausercontent.com,DIRECT",
        "DOMAIN-SUFFIX,alicdn.com,DIRECT",
        "DOMAIN-SUFFIX,alikunlun.com,DIRECT",
        "DOMAIN-SUFFIX,alipay.com,DIRECT",
        "DOMAIN-SUFFIX,amap.com,DIRECT",
        "DOMAIN-SUFFIX,autonavi.com,DIRECT",
        "DOMAIN-SUFFIX,dingtalk.com,DIRECT",
        "DOMAIN-SUFFIX,mxhichina.com,DIRECT",
        "DOMAIN-SUFFIX,soku.com,DIRECT",
        "DOMAIN-SUFFIX,taobao.com,DIRECT",
        "DOMAIN-SUFFIX,tmall.com,DIRECT",
        "DOMAIN-SUFFIX,tmall.hk,DIRECT",
        "DOMAIN-SUFFIX,ykimg.com,DIRECT",
        "DOMAIN-SUFFIX,youku.com,DIRECT",
        "DOMAIN-SUFFIX,xiami.com,DIRECT",
        "DOMAIN-SUFFIX,xiami.net,DIRECT",
        "DOMAIN-SUFFIX,aaplimg.com,DIRECT",
        "DOMAIN-SUFFIX,apple.co,DIRECT",
        "DOMAIN-SUFFIX,apple.com,DIRECT",
        "DOMAIN-SUFFIX,apple-cloudkit.com,DIRECT",
        "DOMAIN-SUFFIX,appstore.com,DIRECT",
        "DOMAIN-SUFFIX,cdn-apple.com,DIRECT",
        "DOMAIN-SUFFIX,icloud.com,DIRECT",
        "DOMAIN-SUFFIX,icloud-content.com,DIRECT",
        "DOMAIN-SUFFIX,me.com,DIRECT",
        "DOMAIN-SUFFIX,mzstatic.com,DIRECT",
        "DOMAIN-KEYWORD,apple.com.akadns.net,DIRECT",
        "DOMAIN-KEYWORD,icloud.com.akadns.net,DIRECT",
        "DOMAIN-SUFFIX,baidu.com,DIRECT",
        "DOMAIN-SUFFIX,baidubcr.com,DIRECT",
        "DOMAIN-SUFFIX,baidupan.com,DIRECT",
        "DOMAIN-SUFFIX,baidupcs.com,DIRECT",
        "DOMAIN-SUFFIX,bdimg.com,DIRECT",
        "DOMAIN-SUFFIX,bdstatic.com,DIRECT",
        "DOMAIN-SUFFIX,yunjiasu-cdn.net,DIRECT",
        "DOMAIN-SUFFIX,acgvideo.com,DIRECT",
        "DOMAIN-SUFFIX,biliapi.com,DIRECT",
        "DOMAIN-SUFFIX,biliapi.net,DIRECT",
        "DOMAIN-SUFFIX,bilibili.com,DIRECT",
        "DOMAIN-SUFFIX,bilibili.tv,DIRECT",
        "DOMAIN-SUFFIX,hdslb.com,DIRECT",
        "DOMAIN-SUFFIX,feiliao.com,DIRECT",
        "DOMAIN-SUFFIX,pstatp.com,DIRECT",
        "DOMAIN-SUFFIX,snssdk.com,DIRECT",
        "DOMAIN-SUFFIX,iesdouyin.com,DIRECT",
        "DOMAIN-SUFFIX,toutiao.com,DIRECT",
        "DOMAIN-SUFFIX,cctv.com,DIRECT",
        "DOMAIN-SUFFIX,cctvpic.com,DIRECT",
        "DOMAIN-SUFFIX,livechina.com,DIRECT",
        "DOMAIN-SUFFIX,didialift.com,DIRECT",
        "DOMAIN-SUFFIX,didiglobal.com,DIRECT",
        "DOMAIN-SUFFIX,udache.com,DIRECT",
        "DOMAIN-SUFFIX,21cn.com,DIRECT",
        "DOMAIN-SUFFIX,hitv.com,DIRECT",
        "DOMAIN-SUFFIX,mgtv.com,DIRECT",
        "DOMAIN-SUFFIX,iqiyi.com,DIRECT",
        "DOMAIN-SUFFIX,iqiyipic.com,DIRECT",
        "DOMAIN-SUFFIX,71.am,DIRECT",
        "DOMAIN-SUFFIX,jd.com,DIRECT",
        "DOMAIN-SUFFIX,jd.hk,DIRECT",
        "DOMAIN-SUFFIX,jdpay.com,DIRECT",
        "DOMAIN-SUFFIX,360buyimg.com,DIRECT",
        "DOMAIN-SUFFIX,iciba.com,DIRECT",
        "DOMAIN-SUFFIX,ksosoft.com,DIRECT",
        "DOMAIN-SUFFIX,meitu.com,DIRECT",
        "DOMAIN-SUFFIX,meitudata.com,DIRECT",
        "DOMAIN-SUFFIX,meitustat.com,DIRECT",
        "DOMAIN-SUFFIX,meipai.com,DIRECT",
        "DOMAIN-SUFFIX,dianping.com,DIRECT",
        "DOMAIN-SUFFIX,dpfile.com,DIRECT",
        "DOMAIN-SUFFIX,meituan.com,DIRECT",
        "DOMAIN-SUFFIX,meituan.net,DIRECT",
        "DOMAIN-SUFFIX,duokan.com,DIRECT",
        "DOMAIN-SUFFIX,mi.com,DIRECT",
        "DOMAIN-SUFFIX,mi-img.com,DIRECT",
        "DOMAIN-SUFFIX,miui.com,DIRECT",
        "DOMAIN-SUFFIX,miwifi.com,DIRECT",
        "DOMAIN-SUFFIX,xiaomi.com,DIRECT",
        "DOMAIN-SUFFIX,xiaomi.net,DIRECT",
        "DOMAIN-SUFFIX,hotmail.com,DIRECT",
        "DOMAIN-SUFFIX,microsoft.com,DIRECT",
        "DOMAIN-SUFFIX,msecnd.net,DIRECT",
        "DOMAIN-SUFFIX,office365.com,DIRECT",
        "DOMAIN-SUFFIX,outlook.com,DIRECT",
        "DOMAIN-SUFFIX,s-microsoft.com,DIRECT",
        "DOMAIN-SUFFIX,visualstudio.com,DIRECT",
        "DOMAIN-SUFFIX,windows.com,DIRECT",
        "DOMAIN-SUFFIX,windowsupdate.com,DIRECT",
        "DOMAIN-SUFFIX,163.com,DIRECT",
        "DOMAIN-SUFFIX,126.com,DIRECT",
        "DOMAIN-SUFFIX,126.net,DIRECT",
        "DOMAIN-SUFFIX,127.net,DIRECT",
        "DOMAIN-SUFFIX,163yun.com,DIRECT",
        "DOMAIN-SUFFIX,lofter.com,DIRECT",
        "DOMAIN-SUFFIX,netease.com,DIRECT",
        "DOMAIN-SUFFIX,ydstatic.com,DIRECT",
        "DOMAIN-SUFFIX,paypal.com,DIRECT",
        "DOMAIN-SUFFIX,paypal.me,DIRECT",
        "DOMAIN-SUFFIX,paypalobjects.com,DIRECT",
        "DOMAIN-SUFFIX,sina.com,DIRECT",
        "DOMAIN-SUFFIX,weibo.com,DIRECT",
        "DOMAIN-SUFFIX,weibocdn.com,DIRECT",
        "DOMAIN-SUFFIX,sohu.com,DIRECT",
        "DOMAIN-SUFFIX,sohucs.com,DIRECT",
        "DOMAIN-SUFFIX,sohu-inc.com,DIRECT",
        "DOMAIN-SUFFIX,v-56.com,DIRECT",
        "DOMAIN-SUFFIX,sogo.com,DIRECT",
        "DOMAIN-SUFFIX,sogou.com,DIRECT",
        "DOMAIN-SUFFIX,sogoucdn.com,DIRECT",
        "DOMAIN-SUFFIX,steamcontent.com,DIRECT",
        "DOMAIN-SUFFIX,steampowered.com,DIRECT",
        "DOMAIN-SUFFIX,steamstatic.com,DIRECT",
        "DOMAIN-SUFFIX,gtimg.com,DIRECT",
        "DOMAIN-SUFFIX,idqqimg.com,DIRECT",
        "DOMAIN-SUFFIX,igamecj.com,DIRECT",
        "DOMAIN-SUFFIX,myapp.com,DIRECT",
        "DOMAIN-SUFFIX,myqcloud.com,DIRECT",
        "DOMAIN-SUFFIX,qq.com,DIRECT",
        "DOMAIN-SUFFIX,qqmail.com,DIRECT",
        "DOMAIN-SUFFIX,servicewechat.com,DIRECT",
        "DOMAIN-SUFFIX,tencent.com,DIRECT",
        "DOMAIN-SUFFIX,tencent-cloud.net,DIRECT",
        "DOMAIN-SUFFIX,tenpay.com,DIRECT",
        "DOMAIN-SUFFIX,wechat.com,DIRECT",
        "DOMAIN,file-igamecj.akamaized.net,DIRECT",
        "DOMAIN-SUFFIX,ccgslb.com,DIRECT",
        "DOMAIN-SUFFIX,ccgslb.net,DIRECT",
        "DOMAIN-SUFFIX,chinanetcenter.com,DIRECT",
        "DOMAIN-SUFFIX,meixincdn.com,DIRECT",
        "DOMAIN-SUFFIX,ourdvs.com,DIRECT",
        "DOMAIN-SUFFIX,staticdn.net,DIRECT",
        "DOMAIN-SUFFIX,wangsu.com,DIRECT",
        "DOMAIN-SUFFIX,ipip.net,DIRECT",
        "DOMAIN-SUFFIX,ip.la,DIRECT",
        "DOMAIN-SUFFIX,ip.sb,DIRECT",
        "DOMAIN-SUFFIX,ip-cdn.com,DIRECT",
        "DOMAIN-SUFFIX,ipv6-test.com,DIRECT",
        "DOMAIN-SUFFIX,myip.la,DIRECT",
        "DOMAIN-SUFFIX,test-ipv6.com,DIRECT",
        "DOMAIN-SUFFIX,whatismyip.com,DIRECT",
        "DOMAIN,ip.istatmenus.app,DIRECT",
        "DOMAIN,sms.imagetasks.com,DIRECT",
        "DOMAIN-SUFFIX,netspeedtestmaster.com,DIRECT",
        "DOMAIN,speedtest.macpaw.com,DIRECT",
        "DOMAIN-SUFFIX,acg.rip,DIRECT",
        "DOMAIN-SUFFIX,animebytes.tv,DIRECT",
        "DOMAIN-SUFFIX,awesome-hd.me,DIRECT",
        "DOMAIN-SUFFIX,broadcasthe.net,DIRECT",
        "DOMAIN-SUFFIX,chdbits.co,DIRECT",
        "DOMAIN-SUFFIX,classix-unlimited.co.uk,DIRECT",
        "DOMAIN-SUFFIX,comicat.org,DIRECT",
        "DOMAIN-SUFFIX,empornium.me,DIRECT",
        "DOMAIN-SUFFIX,gazellegames.net,DIRECT",
        "DOMAIN-SUFFIX,hdbits.org,DIRECT",
        "DOMAIN-SUFFIX,hdchina.org,DIRECT",
        "DOMAIN-SUFFIX,hddolby.com,DIRECT",
        "DOMAIN-SUFFIX,hdhome.org,DIRECT",
        "DOMAIN-SUFFIX,hdsky.me,DIRECT",
        "DOMAIN-SUFFIX,icetorrent.org,DIRECT",
        "DOMAIN-SUFFIX,jpopsuki.eu,DIRECT",
        "DOMAIN-SUFFIX,keepfrds.com,DIRECT",
        "DOMAIN-SUFFIX,madsrevolution.net,DIRECT",
        "DOMAIN-SUFFIX,morethan.tv,DIRECT",
        "DOMAIN-SUFFIX,m-team.cc,DIRECT",
        "DOMAIN-SUFFIX,myanonamouse.net,DIRECT",
        "DOMAIN-SUFFIX,nanyangpt.com,DIRECT",
        "DOMAIN-SUFFIX,ncore.cc,DIRECT",
        "DOMAIN-SUFFIX,open.cd,DIRECT",
        "DOMAIN-SUFFIX,ourbits.club,DIRECT",
        "DOMAIN-SUFFIX,passthepopcorn.me,DIRECT",
        "DOMAIN-SUFFIX,privatehd.to,DIRECT",
        "DOMAIN-SUFFIX,pterclub.com,DIRECT",
        "DOMAIN-SUFFIX,redacted.ch,DIRECT",
        "DOMAIN-SUFFIX,springsunday.net,DIRECT",
        "DOMAIN-SUFFIX,tjupt.org,DIRECT",
        "DOMAIN-SUFFIX,totheglory.im,DIRECT",
        "DOMAIN-SUFFIX,cn,DIRECT",
        "DOMAIN-SUFFIX,115.com,DIRECT",
        "DOMAIN-SUFFIX,360in.com,DIRECT",
        "DOMAIN-SUFFIX,51ym.me,DIRECT",
        "DOMAIN-SUFFIX,8686c.com,DIRECT",
        "DOMAIN-SUFFIX,99.com,DIRECT",
        "DOMAIN-SUFFIX,abchina.com,DIRECT",
        "DOMAIN-SUFFIX,accuweather.com,DIRECT",
        "DOMAIN-SUFFIX,aicoinstorge.com,DIRECT",
        "DOMAIN-SUFFIX,air-matters.com,DIRECT",
        "DOMAIN-SUFFIX,air-matters.io,DIRECT",
        "DOMAIN-SUFFIX,aixifan.com,DIRECT",
        "DOMAIN-SUFFIX,amd.com,DIRECT",
        "DOMAIN-SUFFIX,b612.net,DIRECT",
        "DOMAIN-SUFFIX,bdatu.com,DIRECT",
        "DOMAIN-SUFFIX,beitaichufang.com,DIRECT",
        "DOMAIN-SUFFIX,booking.com,DIRECT",
        "DOMAIN-SUFFIX,bstatic.com,DIRECT",
        "DOMAIN-SUFFIX,cailianpress.com,DIRECT",
        "DOMAIN-SUFFIX,camera360.com,DIRECT",
        "DOMAIN-SUFFIX,chaoxing.com,DIRECT",
        "DOMAIN-SUFFIX,chaoxing.com,DIRECT",
        "DOMAIN-SUFFIX,chinaso.com,DIRECT",
        "DOMAIN-SUFFIX,chuimg.com,DIRECT",
        "DOMAIN-SUFFIX,chunyu.mobi,DIRECT",
        "DOMAIN-SUFFIX,cmbchina.com,DIRECT",
        "DOMAIN-SUFFIX,cmbimg.com,DIRECT",
        "DOMAIN-SUFFIX,ctrip.com,DIRECT",
        "DOMAIN-SUFFIX,dfcfw.com,DIRECT",
        "DOMAIN-SUFFIX,dji.net,DIRECT",
        "DOMAIN-SUFFIX,docschina.org,DIRECT",
        "DOMAIN-SUFFIX,douban.com,DIRECT",
        "DOMAIN-SUFFIX,doubanio.com,DIRECT",
        "DOMAIN-SUFFIX,douyu.com,DIRECT",
        "DOMAIN-SUFFIX,dxycdn.com,DIRECT",
        "DOMAIN-SUFFIX,dytt8.net,DIRECT",
        "DOMAIN-SUFFIX,eastmoney.com,DIRECT",
        "DOMAIN-SUFFIX,eudic.net,DIRECT",
        "DOMAIN-SUFFIX,feng.com,DIRECT",
        "DOMAIN-SUFFIX,fengkongcloud.com,DIRECT",
        "DOMAIN-SUFFIX,frdic.com,DIRECT",
        "DOMAIN-SUFFIX,futu5.com,DIRECT",
        "DOMAIN-SUFFIX,futunn.com,DIRECT",
        "DOMAIN-SUFFIX,gandi.net,DIRECT",
        "DOMAIN-SUFFIX,gcores.com,DIRECT",
        "DOMAIN-SUFFIX,geilicdn.com,DIRECT",
        "DOMAIN-SUFFIX,getpricetag.com,DIRECT",
        "DOMAIN-SUFFIX,gifshow.com,DIRECT",
        "DOMAIN-SUFFIX,godic.net,DIRECT",
        "DOMAIN-SUFFIX,hicloud.com,DIRECT",
        "DOMAIN-SUFFIX,hongxiu.com,DIRECT",
        "DOMAIN-SUFFIX,hostbuf.com,DIRECT",
        "DOMAIN-SUFFIX,huxiucdn.com,DIRECT",
        "DOMAIN-SUFFIX,huya.com,DIRECT",
        "DOMAIN-SUFFIX,ibm.com,DIRECT",
        "DOMAIN-SUFFIX,infinitynewtab.com,DIRECT",
        "DOMAIN-SUFFIX,ithome.com,DIRECT",
        "DOMAIN-SUFFIX,java.com,DIRECT",
        "DOMAIN-SUFFIX,jianguoyun.com,DIRECT",
        "DOMAIN-SUFFIX,jianshu.com,DIRECT",
        "DOMAIN-SUFFIX,jianshu.io,DIRECT",
        "DOMAIN-SUFFIX,jidian.im,DIRECT",
        "DOMAIN-SUFFIX,kaiyanapp.com,DIRECT",
        "DOMAIN-SUFFIX,kaspersky-labs.com,DIRECT",
        "DOMAIN-SUFFIX,keepcdn.com,DIRECT",
        "DOMAIN-SUFFIX,kkmh.com,DIRECT",
        "DOMAIN-SUFFIX,lanzous.com,DIRECT",
        "DOMAIN-SUFFIX,licdn.com,DIRECT",
        "DOMAIN-SUFFIX,luojilab.com,DIRECT",
        "DOMAIN-SUFFIX,maoyan.com,DIRECT",
        "DOMAIN-SUFFIX,maoyun.tv,DIRECT",
        "DOMAIN-SUFFIX,mls-cdn.com,DIRECT",
        "DOMAIN-SUFFIX,mobike.com,DIRECT",
        "DOMAIN-SUFFIX,moke.com,DIRECT",
        "DOMAIN-SUFFIX,mubu.com,DIRECT",
        "DOMAIN-SUFFIX,myzaker.com,DIRECT",
        "DOMAIN-SUFFIX,nim-lang-cn.org,DIRECT",
        "DOMAIN-SUFFIX,nvidia.com,DIRECT",
        "DOMAIN-SUFFIX,oracle.com,DIRECT",
        "DOMAIN-SUFFIX,originlab.com,DIRECT",
        "DOMAIN-SUFFIX,qdaily.com,DIRECT",
        "DOMAIN-SUFFIX,qidian.com,DIRECT",
        "DOMAIN-SUFFIX,qyer.com,DIRECT",
        "DOMAIN-SUFFIX,qyerstatic.com,DIRECT",
        "DOMAIN-SUFFIX,raychase.net,DIRECT",
        "DOMAIN-SUFFIX,ronghub.com,DIRECT",
        "DOMAIN-SUFFIX,ruguoapp.com,DIRECT",
        "DOMAIN-SUFFIX,sankuai.com,DIRECT",
        "DOMAIN-SUFFIX,scomper.me,DIRECT",
        "DOMAIN-SUFFIX,seafile.com,DIRECT",
        "DOMAIN-SUFFIX,sm.ms,DIRECT",
        "DOMAIN-SUFFIX,smzdm.com,DIRECT",
        "DOMAIN-SUFFIX,snapdrop.net,DIRECT",
        "DOMAIN-SUFFIX,snwx.com,DIRECT",
        "DOMAIN-SUFFIX,s-reader.com,DIRECT",
        "DOMAIN-SUFFIX,sspai.com,DIRECT",
        "DOMAIN-SUFFIX,subhd.tv,DIRECT",
        "DOMAIN-SUFFIX,takungpao.com,DIRECT",
        "DOMAIN-SUFFIX,teamviewer.com,DIRECT",
        "DOMAIN-SUFFIX,tianyancha.com,DIRECT",
        "DOMAIN-SUFFIX,tophub.today,DIRECT",
        "DOMAIN-SUFFIX,udacity.com,DIRECT",
        "DOMAIN-SUFFIX,uning.com,DIRECT",
        "DOMAIN-SUFFIX,weather.com,DIRECT",
        "DOMAIN-SUFFIX,weico.cc,DIRECT",
        "DOMAIN-SUFFIX,weidian.com,DIRECT",
        "DOMAIN-SUFFIX,xiachufang.com,DIRECT",
        "DOMAIN-SUFFIX,xiaoka.tv,DIRECT",
        "DOMAIN-SUFFIX,ximalaya.com,DIRECT",
        "DOMAIN-SUFFIX,xinhuanet.com,DIRECT",
        "DOMAIN-SUFFIX,xmcdn.com,DIRECT",
        "DOMAIN-SUFFIX,yangkeduo.com,DIRECT",
        "DOMAIN-SUFFIX,yizhibo.com,DIRECT",
        "DOMAIN-SUFFIX,zhangzishi.cc,DIRECT",
        "DOMAIN-SUFFIX,zhihu.com,DIRECT",
        "DOMAIN-SUFFIX,zhihuishu.com,DIRECT",
        "DOMAIN-SUFFIX,zhimg.com,DIRECT",
        "DOMAIN-SUFFIX,zhuihd.com,DIRECT",
        "DOMAIN,download.jetbrains.com,DIRECT",
        "DOMAIN,images-cn.ssl-images-amazon.com,DIRECT",
        "DOMAIN-SUFFIX,local,DIRECT",
        "IP-CIDR,192.168.0.0/16,DIRECT,no-resolve",
        "IP-CIDR,10.0.0.0/8,DIRECT,no-resolve",
        "IP-CIDR,172.16.0.0/12,DIRECT,no-resolve",
        "IP-CIDR,127.0.0.0/8,DIRECT,no-resolve",
        "IP-CIDR,100.64.0.0/10,DIRECT,no-resolve",
        "IP-CIDR6,::1/128,DIRECT,no-resolve",
        "IP-CIDR6,fc00::/7,DIRECT,no-resolve",
        "IP-CIDR6,fe80::/10,DIRECT,no-resolve",
        "IP-CIDR6,fd00::/8,DIRECT,no-resolve",
        "GEOIP,CN,DIRECT",
        "MATCH,节点选择"
    ]
}


# 解析 TUIC 链接
# tuic://uuid:password@server:port?params#name
def parse_tuic_link(link):
    try:
        # 去掉 tuic:// 前缀
        link = link[8:]
        config_part, name = link.split('#', 1) if '#' in link else (link, "tuic")
        name = urllib.parse.unquote(name)

        # 分离 userinfo 和 host:port
        if '@' in config_part:
            user_info, host_info = config_part.split('@', 1)
        else:
            return None

        # 解析 uuid:password 或 uuid
        if ':' in user_info:
            uuid, password = user_info.split(':', 1)
        else:
            uuid, password = user_info, ""

        # 解析 host:port?query
        if '?' in host_info:
            host_port, query = host_info.split('?', 1)
        else:
            host_port, query = host_info, ""

        # 解析 host 和 port
        if ':' in host_port:
            server = host_port.rsplit(':', 1)[0]
            port_str = host_port.rsplit(':', 1)[1].split('/')[0].strip()
            port = int(port_str)
        else:
            server = host_port
            port = 443

        params = urllib.parse.parse_qs(query)

        result = {
            "name": name,
            "type": "tuic",
            "server": server,
            "port": port,
            "uuid": uuid,
            "password": password,
            "sni": params.get("sni", [""])[0],
            "skip-cert-verify": params.get("skip-cert-verify", ["false"])[0] == "true",
            "reduce-comment": True,
            "client-fingerprint": params.get("fp", ["chrome"])[0],
        }

        # congestion-controller
        cc = params.get("congestion_control", params.get("congestion-controller", ["cubic"]))
        if cc:
            result["congestion-controller"] = cc[0]

        # alpn
        alpn = params.get("alpn", [])
        if alpn:
            result["alpn"] = alpn[0].split(",") if alpn[0] else []

        # disable-sni
        if params.get("disable-sni", ["false"])[0] == "true":
            result["disable-sni"] = True

        return result
    except Exception as e:
        print(f"解析TUIC链接失败: {e}")
        return None


# 解析 WireGuard (WARP) 链接
# wg://private_key@server:port?params#name
# warp://private_key@server:port?params#name
def parse_warp_link(link):
    try:
        # 去掉 wg:// 或 warp:// 前缀
        if link.startswith("warp://"):
            link = link[7:]
        elif link.startswith("wg://"):
            link = link[5:]

        config_part, name = link.split('#', 1) if '#' in link else (link, "warp")
        name = urllib.parse.unquote(name)

        # 分离 private_key 和 host:port
        if '@' in config_part:
            private_key, host_info = config_part.split('@', 1)
        else:
            return None

        # 解析 host:port?query
        if '?' in host_info:
            host_port, query = host_info.split('?', 1)
        else:
            host_port, query = host_info, ""

        # 解析 host 和 port
        if ':' in host_port:
            server = host_port.rsplit(':', 1)[0]
            port_str = host_port.rsplit(':', 1)[1].split('/')[0].strip()
            port = int(port_str)
        else:
            server = host_port
            port = 2408

        params = urllib.parse.parse_qs(query)

        result = {
            "name": name,
            "type": "wireguard",
            "server": server,
            "port": port,
            "private-key": private_key,
            "ip": params.get("ip", ["172.16.0.2/32"])[0],
            "ipv6": params.get("ipv6", ["fd01:5ca1:ab1e::"])[0],
            "public-key": params.get("public_key", params.get("publickey", [""])[0] if "publickey" in params else params.get("public_key", [""])[0]),
            "reserved": [int(x) for x in params.get("reserved", ["0,0,0"])[0].split(",")] if "reserved" in params else [],
            "udp": True,
            "peers": [],
        }

        # mtu
        if "mtu" in params:
            result["mtu"] = int(params["mtu"][0])

        # dns
        if "dns" in params:
            result["dns"] = params["dns"][0].split(",")

        # keep-alive
        if "keep_alive" in params or "keepalive" in params:
            ka = params.get("keep_alive", params.get("keepalive", ["0"]))[0]
            result["keep-alive-interval"] = int(ka)

        # 如果有peers参数
        peer_pubkey = params.get("peer_public_key", params.get("publickey", [""])[0] if "publickey" in params else params.get("public_key", [""])[0])
        if peer_pubkey:
            result["peers"] = [{
                "public-key": peer_pubkey,
                "reserved": [int(x) for x in params.get("reserved", ["0,0,0"])[0].split(",")] if "reserved" in params else [],
                "endpoint": f"{server}:{port}" if server and port else "",
            }]

        return result
    except Exception as e:
        print(f"解析WARP/WireGuard链接失败: {e}")
        return None


# 解析 Hysteria2 链接
def parse_hysteria2_link(link):
    link = link[14:]
    parts = link.split('@')
    uuid = parts[0]
    server_info = parts[1].split('?')
    server = server_info[0].split(':')[0]
    port = int(server_info[0].split(':')[1].split('/')[0].strip())
    query_params = urllib.parse.parse_qs(server_info[1] if len(server_info) > 1 else '')
    insecure = '1' in query_params.get('insecure', ['0'])
    sni = query_params.get('sni', [''])[0]
    name = urllib.parse.unquote(link.split('#')[-1].strip())

    return {
        "name": f"{name}",
        "server": server,
        "port": port,
        "type": "hysteria2",
        "password": uuid,
        "auth": uuid,
        "sni": sni,
        "skip-cert-verify": not insecure,
        "client-fingerprint": "chrome"
    }


# 解析 Shadowsocks 链接
def parse_ss_link(link):
    link = link[5:]
    if "#" in link:
        config_part, name = link.split('#')
    else:
        config_part, name = link, ""
    decoded = safe_decode(base64.urlsafe_b64decode(config_part.split('@')[0] + '=' * (-len(config_part.split('@')[0]) % 4)))
    method_passwd = decoded.split(':')
    cipher, password = method_passwd if len(method_passwd) == 2 else (method_passwd[0], "")
    server_info = config_part.split('@')[1]
    server, port = server_info.split(':') if ":" in server_info else (server_info, "")

    return {
        "name": urllib.parse.unquote(name),
        "type": "ss",
        "server": server,
        "port": int(port),
        "cipher": cipher,
        "password": password,
        "udp": True
    }


# 解析 Trojan 链接
def parse_trojan_link(link):
    link = link[9:]
    config_part, name = link.split('#')
    user_info, host_info = config_part.split('@')
    username, password = user_info.split(':') if ":" in user_info else ("", user_info)
    host, port_and_query = host_info.split(':') if ":" in host_info else (host_info, "")
    port, query = port_and_query.split('?', 1) if '?' in port_and_query else (port_and_query, "")

    return {
        "name": urllib.parse.unquote(name),
        "type": "trojan",
        "server": host,
        "port": int(port),
        "password": password,
        "sni": urllib.parse.parse_qs(query).get("sni", [""])[0],
        "skip-cert-verify": urllib.parse.parse_qs(query).get("skip-cert-verify", ["false"])[0] == "true"
    }


# 解析 VLESS 链接（支持 ws/grpc/xhttp/reality 等）
def parse_vless_link(link):
    try:
        link = link[8:]
        if '#' in link:
            config_part, name = link.split('#', 1)
        else:
            config_part, name = link, "vless"
        name = urllib.parse.unquote(name)
        if '@' in config_part:
            user_info, host_info = config_part.split('@', 1)
        else:
            return None
        uuid = user_info
        host, query = host_info.split('?', 1) if '?' in host_info else (host_info, "")
        port = host.split(':')[-1] if ':' in host else ""
        server = host.split(':')[0] if ':' in host else ""

        params = urllib.parse.parse_qs(query)
        security = params.get("security", ["none"])[0]
        network_type = params.get("type", ["tcp"])[0]
        flow = params.get("flow", [""])[0]

        result = {
            "name": name,
            "type": "vless",
            "server": server,
            "port": int(port) if port else 443,
            "uuid": uuid,
            "tls": security in ("tls", "reality"),
            "skip-cert-verify": params.get("skip-cert-verify", ["false"])[0] == "true",
            "network": network_type,
        }

        # flow (xtls-rprx-vision 等)
        if flow:
            result["flow"] = flow

        # security 相关
        if security == "reality":
            result["reality-opts"] = {
                "public-key": params.get("pbk", [""])[0],
                "short-id": params.get("sid", [""])[0],
            }
            if params.get("fp", [""])[0]:
                result["client-fingerprint"] = params.get("fp", [""])[0]
        elif security == "tls":
            if params.get("fp", [""])[0]:
                result["client-fingerprint"] = params.get("fp", [""])[0]

        # sni
        sni = params.get("sni", [""])[0]
        if sni:
            result["sni"] = sni

        # alpn
        alpn = params.get("alpn", [""])[0]
        if alpn:
            result["alpn"] = alpn.split(",")

        # network opts
        if network_type == "ws":
            ws_opts = {}
            path = params.get("path", [""])[0]
            if path:
                ws_opts["path"] = path
            host_hdr = params.get("host", [""])[0]
            if host_hdr:
                ws_opts["headers"] = {"Host": host_hdr}
            if ws_opts:
                result["ws-opts"] = ws_opts
        elif network_type == "grpc":
            grpc_opts = {}
            service_name = params.get("serviceName", [""])[0]
            if service_name:
                grpc_opts["grpc-service-name"] = service_name
            mode = params.get("mode", [""])[0]
            if mode:
                grpc_opts["grpc-mode"] = mode
            if grpc_opts:
                result["grpc-opts"] = grpc_opts
        elif network_type == "xhttp":
            # Clash Meta (mihomo) xhttp 支持
            xhttp_opts = {}
            path = params.get("path", [""])[0]
            if path:
                xhttp_opts["path"] = path
            host_hdr = params.get("host", [""])[0]
            if host_hdr:
                xhttp_opts["headers"] = {"Host": host_hdr}
            mode = params.get("mode", ["auto"])[0]
            xhttp_opts["xhttp-mode"] = mode
            # xhttp 支持的额外参数
            extra = params.get("extra", [""])[0]
            if extra:
                xhttp_opts["xhttp-extra"] = extra
            result["xhttp-opts"] = xhttp_opts

        return result
    except Exception as e:
        return None


# 解析 VMESS 链接
def parse_vmess_link(link):
    link = link[8:]
    decoded_link = safe_decode(base64.urlsafe_b64decode(link + '=' * (-len(link) % 4)))
    vmess_info = json.loads(decoded_link)

    return {
        "name": urllib.parse.unquote(vmess_info.get("ps", "vmess")),
        "type": "vmess",
        "server": vmess_info["add"],
        "port": int(vmess_info["port"]),
        "uuid": vmess_info["id"],
        "alterId": int(vmess_info.get("aid", 0)),
        "cipher": "auto",
        "network": vmess_info.get("net", "tcp"),
        "tls": vmess_info.get("tls", "") == "tls",
        "sni": vmess_info.get("sni", ""),
        "ws-opts": {
            "path": vmess_info.get("path", ""),
            "headers": {
                "Host": vmess_info.get("host", "")
            }
        } if vmess_info.get("net", "tcp") == "ws" else {}
    }


# 解析ss订阅源
def parse_ss_sub(link):
    new_links = []
    try:
        # 发送请求并获取内容
        response = requests.get(link, headers=headers, verify=False, allow_redirects=True)
        if response.status_code == 200:
            data = response.json()
            new_links = [{"name": x['remarks'], "type": "ss", "server": x['server'], "port": x['server_port'],
                          "cipher": x['method'], "password": x['password'], "udp": True} for x in data]
            return new_links
    except requests.RequestException as e:
        print(f"请求错误: {e}")
        return new_links


def parse_md_link(link):
    try:
        # 发送请求并获取内容
        response = requests.get(link, timeout=15)
        response.raise_for_status()  # 检查请求是否成功
        content = response.text
        content = urllib.parse.unquote(content)
        # 定义正则表达式模式，匹配所需的协议链接
        pattern = r'(?:vless|vmess|trojan|hysteria2|hy2|hysteria|quic|snell|naive(?:\+https|\+quic)?|tuic|warp|wg|ss|ssr):\/\/[^#\s]*(?:#[^\s]*)?'

        # 使用re.findall()提取所有匹配的链接
        matches = re.findall(pattern, content)
        return matches

    except requests.RequestException as e:
        print(f"请求错误: {e}")
        return []


# js渲染页面
def js_render(url):
    """已禁用 - js渲染极慢(每次14秒)，改用httpx+timeout直接请求"""
    return None


# je_render返回的text没有缩进，通过正则表达式匹配proxies下的所有代理节点
def match_nodes(text):
    proxy_pattern = r"\{[^}]*name\s*:\s*['\"][^'\"]+['\"][^}]*server\s*:\s*[^,]+[^}]*\}"
    nodes = re.findall(proxy_pattern, text, re.DOTALL)

    # 将每个节点字符串转换为字典
    proxies_list = []
    for node in nodes:
        # 使用yaml.safe_load来加载每个节点
        node_dict = yaml.safe_load(node)
        proxies_list.append(node_dict)

    yaml_data = {"proxies": proxies_list}
    return yaml_data


# link非代理协议时(https)，请求url解析 - 优化版(无js_render，加timeout)
def process_url(url):
    isyaml = False
    try:
        response = requests.get(url, headers=headers, verify=False, allow_redirects=True, timeout=5)
        if response.status_code == 200:
            content_text = safe_decode(response.content)
            if 'proxies:' in content_text:
                if '</pre>' in content_text:
                    content_text = content_text.replace('<pre style="word-wrap: break-word; white-space: pre-wrap;">', '').replace('</pre>', '')
                try:
                    yaml_data = yaml.safe_load(content_text)
                    if yaml_data and 'proxies' in yaml_data:
                        isyaml = True
                        proxies = yaml_data.get('proxies') or []
                        return proxies, isyaml
                except Exception:
                    pass
            try:
                decoded_bytes = base64.b64decode(content_text)
                decoded_content = safe_decode(decoded_bytes)
                decoded_content = urllib.parse.unquote(decoded_content)
                return decoded_content.splitlines(), isyaml
            except Exception:
                return content_text.splitlines(), isyaml
        else:
            return [], isyaml
    except requests.RequestException as e:
        return [], isyaml


# 解析 QUIC 链接（Clash Meta 中 quic 即 hysteria1）
# quic://auth@server:port?sni=xxx&insecure=1&up=100&down=100#name
# 也支持 hysteria:// 格式
def parse_quic_link(link):
    try:
        # 去掉协议头
        if link.startswith("quic://"):
            link = link[7:]
        elif link.startswith("hysteria://"):
            link = link[11:]
        
        # 分离备注名
        if '#' in link:
            config_part, name = link.split('#', 1)
            name = urllib.parse.unquote(name)
        else:
            config_part, name = link, "quic"
        
        # 分离 auth 和 server 信息
        if '@' in config_part:
            auth, host_info = config_part.split('@', 1)
            auth = urllib.parse.unquote(auth)
        else:
            auth, host_info = "", config_part
        
        # 分离端口和查询参数
        if '?' in host_info:
            host_port, query_str = host_info.split('?', 1)
        else:
            host_port, query_str = host_info, ""
        
        # 解析 server:port
        host_port = host_port.strip('/')
        if ':' in host_port:
            server = host_port.rsplit(':', 1)[0]
            port_str = host_port.rsplit(':', 1)[1]
            # 端口可能包含路径
            port = int(port_str.split('/')[0])
        else:
            server = host_port
            port = 443
        
        # 解析查询参数
        params = urllib.parse.parse_qs(query_str)
        sni = params.get('sni', [''])[0]
        insecure = params.get('insecure', ['0'])[0] == '1'
        up = params.get('up', [''])[0]
        down = params.get('down', [''])[0]
        obfs = params.get('obfs', [''])[0]
        obfs_param = params.get('obfs-param', [''])[0] if obfs else ''
        alpn = params.get('alpn', [''])[0]
        
        result = {
            "name": name,
            "type": "hysteria",
            "server": server,
            "port": port,
            "auth": auth,
            "auth-str": auth,
            "sni": sni,
            "skip-cert-verify": insecure,
            "up": up,
            "down": down,
            "udp": True
        }
        
        # 可选字段
        if obfs:
            result["obfs"] = obfs
            if obfs_param:
                result["obfs-param"] = obfs_param
        if alpn:
            result["alpn"] = alpn.split(',')
        
        return result
    except Exception as e:
        print(f"解析QUIC链接失败: {e}")
        return None


# 解析 Snell 链接
# snell://psk@server:port?version=3&obfs=http&obfs-host=bing.com#name
def parse_snell_link(link):
    try:
        link = link[8:]  # 去掉 snell://
        
        # 分离备注名
        if '#' in link:
            config_part, name = link.split('#', 1)
            name = urllib.parse.unquote(name)
        else:
            config_part, name = link, "snell"
        
        # 分离 psk 和 server 信息
        if '@' in config_part:
            psk, host_info = config_part.split('@', 1)
            psk = urllib.parse.unquote(psk)
        else:
            psk, host_info = "", config_part
        
        # 分离端口和查询参数
        if '?' in host_info:
            host_port, query_str = host_info.split('?', 1)
        else:
            host_port, query_str = host_info, ""
        
        # 解析 server:port
        host_port = host_port.strip('/')
        if ':' in host_port:
            server = host_port.rsplit(':', 1)[0]
            port_str = host_port.rsplit(':', 1)[1]
            port = int(port_str.split('/')[0])
        else:
            server = host_port
            port = 443
        
        # 解析查询参数
        params = urllib.parse.parse_qs(query_str)
        version = int(params.get('version', ['1'])[0])
        obfs_mode = params.get('obfs', [''])[0]
        obfs_host = params.get('obfs-host', [''])[0]
        
        result = {
            "name": name,
            "type": "snell",
            "server": server,
            "port": port,
            "psk": psk,
            "version": version,
            "udp": True if version >= 3 else False
        }
        
        # 混淆设置
        if obfs_mode:
            result["obfs-opts"] = {
                "mode": obfs_mode
            }
            if obfs_host:
                result["obfs-opts"]["host"] = obfs_host
        
        return result
    except Exception as e:
        print(f"解析Snell链接失败: {e}")
        return None


# 解析 NaiveProxy 链接
# naive+https://user:pass@server:port?sni=xxx&insecure=0#name
# naive+quic://user:pass@server:port?sni=xxx&insecure=0#name  (即 naive+quic → network: quic)
# 也支持 naive:// 格式（默认https）
def parse_naive_link(link):
    try:
        # 判断网络类型
        if link.startswith("naive+quic://"):
            network = "quic"
            link = link[13:]
        elif link.startswith("naive+https://"):
            network = "https"
            link = link[14:]
        elif link.startswith("naive://"):
            network = "https"
            link = link[8:]
        else:
            network = "https"
        
        # 分离备注名
        if '#' in link:
            config_part, name = link.split('#', 1)
            name = urllib.parse.unquote(name)
        else:
            config_part, name = link, "naive"
        
        # 分离 user:pass 和 server 信息
        if '@' in config_part:
            userinfo, host_info = config_part.split('@', 1)
            # user:pass 需要用 base64 编码为 password 字段
            if ':' in userinfo:
                username, password = userinfo.split(':', 1)
            else:
                username, password = userinfo, ""
            # Clash Meta naive 类型使用 username + password
        else:
            username, password, host_info = "", "", config_part
        
        # 分离端口和查询参数
        if '?' in host_info:
            host_port, query_str = host_info.split('?', 1)
        else:
            host_port, query_str = host_info, ""
        
        # 解析 server:port
        host_port = host_port.strip('/')
        if ':' in host_port:
            server = host_port.rsplit(':', 1)[0]
            port_str = host_port.rsplit(':', 1)[1]
            port = int(port_str.split('/')[0])
        else:
            server = host_port
            port = 443
        
        # 解析查询参数
        params = urllib.parse.parse_qs(query_str)
        sni = params.get('sni', [''])[0]
        insecure = params.get('insecure', ['0'])[0] == '1'
        skip_cert_verify = params.get('skip-cert-verify', [''])[0] == 'true'
        
        result = {
            "name": name,
            "type": "naive",
            "server": server,
            "port": port,
            "username": urllib.parse.unquote(username),
            "password": urllib.parse.unquote(password),
            "network": network,
            "udp": True,
            "skip-cert-verify": insecure or skip_cert_verify
        }
        
        # 可选字段
        if sni:
            result["sni"] = sni
        
        return result
    except Exception as e:
        print(f"解析NaiveProxy链接失败: {e}")
        return None


# 解析 SSR 链接
# ssr://base64encoded_config
def parse_ssr_link(link):
    try:
        link = link[6:]  # 去掉 ssr://
        # SSR链接是base64编码的
        decoded = safe_decode(base64.urlsafe_b64decode(link + '=' * (-len(link) % 4)))
        # 格式: server:port:protocol:method:obfs:base64pass/?params#name
        parts = decoded.split('/?')
        main_part = parts[0]
        params_str = parts[1] if len(parts) > 1 else ''
        
        main_fields = main_part.split(':')
        if len(main_fields) < 6:
            return None
            
        server = main_fields[0]
        port = int(main_fields[1])
        protocol = main_fields[2]
        method = main_fields[3]
        obfs = main_fields[4]
        password_b64 = main_fields[5]
        password = safe_decode(base64.urlsafe_b64decode(password_b64 + '=' * (-len(password_b64) % 4)))
        
        # 解析备注名
        name = "ssr"
        params = urllib.parse.parse_qs(params_str)
        if 'remarks' in params:
            remarks_b64 = params['remarks'][0]
            name = safe_decode(base64.urlsafe_b64decode(remarks_b64 + '=' * (-len(remarks_b64) % 4)))
        
        return {
            "name": urllib.parse.unquote(name),
            "type": "ss",
            "server": server,
            "port": port,
            "cipher": method,
            "password": password,
            "udp": True
        }
    except Exception as e:
        print(f"解析SSR链接失败: {e}")
        return None


# 解析格不同的代理链接
def parse_proxy_link(link):
    try:
        if link.startswith("hysteria2://") or link.startswith("hy2://"):
            return parse_hysteria2_link(link)
        elif link.startswith("quic://") or link.startswith("hysteria://"):
            return parse_quic_link(link)
        elif link.startswith("snell://"):
            return parse_snell_link(link)
        elif link.startswith("naive+https://") or link.startswith("naive+quic://") or link.startswith("naive://"):
            return parse_naive_link(link)
        elif link.startswith("tuic://"):
            return parse_tuic_link(link)
        elif link.startswith("warp://") or link.startswith("wg://"):
            return parse_warp_link(link)
        elif link.startswith("trojan://"):
            return parse_trojan_link(link)
        elif link.startswith("ssr://"):
            return parse_ssr_link(link)
        elif link.startswith("ss://"):
            return parse_ss_link(link)
        elif link.startswith("vless://"):
            return parse_vless_link(link)
        elif link.startswith("vmess://"):
            return parse_vmess_link(link)
    except Exception as e:
        # print(e)
        return None




# 根据server和port共同约束去重
def deduplicate_proxies(proxies_list):
    unique_proxies = []
    seen = set()
    for proxy in proxies_list:
        key = (proxy['server'], proxy['port'], proxy['type'], proxy['password']) if proxy.get("password") else (
        proxy['server'], proxy['port'], proxy['type'])
        if key not in seen:
            seen.add(key)
            unique_proxies.append(proxy)
    return unique_proxies


# 出现节点name相同时，加上4位随机字符串
def add_random_suffix(name, existing_names):
    # 生成4位随机字符串
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=4))
    new_name = f"{name}-{suffix}"
    # 确保生成的新名字不在已存在的名字列表中
    while new_name in existing_names:
        suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=4))
        new_name = f"{name}-{suffix}"
    return new_name


# 从指定目录下的txt读取代理链接
def read_txt_files(folder_path):
    all_lines = []  # 用于存储所有文件的行

    # 使用 glob 获取指定文件夹下的所有 txt 文件
    txt_files = glob.glob(os.path.join(folder_path, '*.txt'))

    for file_path in txt_files:
        with open(file_path, 'r', encoding='utf-8') as file:
            # 读取文件内容并按行存入数组
            lines = file.readlines()
            all_lines.extend(line.strip() for line in lines)  # 去除每行的换行符并添加到数组中
    if all_lines:
        print(f'加载【{folder_path}】目录下所有txt中节点')
    return all_lines


# 从指定目录下的yaml/yml读取proxies
def read_yaml_files(folder_path):
    load_nodes = []
    # 使用 glob 获取指定文件夹下的所有 yaml/yml 文件
    yaml_files = glob.glob(os.path.join(folder_path, '*.yaml'))
    yaml_files.extend(glob.glob(os.path.join(folder_path, '*.yml')))

    for file_path in yaml_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                # 读取并解析yaml文件
                config = yaml.safe_load(file)
                # 如果存在proxies字段，添加到nodes列表
                if config and 'proxies' in config:
                    load_nodes.extend(config['proxies'])
        except Exception as e:
            print(f"Error reading {file_path}: {str(e)}")
    if load_nodes:
        print(f'加载【{folder_path}】目录下yaml/yml中所有节点')
    return load_nodes


# 进行type过滤
def filter_by_types_alt(allowed_types, nodes):
    # 进行过滤
    return [x for x in nodes if x.get('type') in allowed_types]


# 合并links列表
def merge_lists(*lists):
    return [item for item in chain.from_iterable(lists) if item != '']


def is_valid_utf8(s):
    """检查字符串是否为合法UTF-8，排除乱码"""
    if not s or not isinstance(s, str):
        return False
    # 检查是否包含过多替换字符（乱码标志）
    replace_count = s.count('\ufffd')
    if replace_count > 3 or (len(s) > 0 and replace_count / len(s) > 0.1):
        return False
    # 检查是否包含常见代理协议前缀
    valid_prefixes = ("hysteria2://", "hy2://", "quic://", "hysteria://", "snell://", "naive+https://", "naive+quic://", "naive://", "tuic://", "warp://", "wg://",
                    "trojan://", "ss://", "ssr://", "vless://", "vmess://")
    if s.strip().startswith(valid_prefixes):
        return True
    # 非代理协议的普通URL也放行
    if s.strip().startswith(("http://", "https://")):
        return True
    # 其他情况检查是否包含过多不可打印字符
    printable_ratio = sum(1 for c in s if c.isprintable() or c in '\t\n\r') / max(len(s), 1)
    return printable_ratio > 0.8


def handle_links(new_links, resolve_name_conflicts):
    try:
        skipped = 0
        for new_link in new_links:
            # 先检查链接合法性
            if not is_valid_utf8(new_link):
                skipped += 1
                continue
            link_stripped = new_link.strip()
            if link_stripped.startswith(("hysteria2://", "hy2://", "quic://", "hysteria://", "snell://", "naive+https://", "naive+quic://", "naive://", "tuic://", "warp://", "wg://", "trojan://", "ss://", "ssr://", "vless://", "vmess://")):
                node = parse_proxy_link(link_stripped)
                if node:
                    resolve_name_conflicts(node)
                else:
                    skipped += 1
            else:
                skipped += 1
        if skipped > 0:
            print(f"  跳过 {skipped} 条无效/乱码链接")
    except Exception as e:
        pass


# 并发下载单个订阅URL
def fetch_sub_url(url):
    """并发下载订阅URL，带超时保护+缓存"""
    # 缓存检查
    if USE_CACHE and not FORCE_REFRESH:
        sub_cache = load_json_cache(SUB_CACHE_FILE)
        url_hash = hashlib.md5(url.encode()).hexdigest()
        cached = sub_cache.get(url_hash)
        if cached and is_cache_valid(cached):
            return (cached.get("type", "error"), cached.get("data"), None)
    is_proxy_link = url.strip().startswith((
        "hysteria2://", "hy2://", "quic://", "hysteria://", "snell://",
        "naive+https://", "naive+quic://", "naive://", "tuic://",
        "warp://", "wg://", "trojan://", "ss://", "ssr://", "vless://", "vmess://"
    ))
    if is_proxy_link:
        return ("proxy", url, None)

    # 特殊标记链接
    if '|links' in url or '.md' in url:
        clean_url = url.replace('|links', '')
        try:
            new_links = parse_md_link(clean_url)
            return ("links", new_links, None)
        except Exception:
            return ("error", url, None)

    if '|ss' in url:
        clean_url = url.replace('|ss', '')
        try:
            new_links = parse_ss_sub(clean_url)
            return ("ss_sub", new_links, None)
        except Exception:
            return ("error", url, None)

    # 模板URL
    if '{' in url:
        url = resolve_template_url(url)

    # 普通订阅URL - 用超时保护
    try:
        new_links, isyaml = process_url(url)
        result = ("yaml" if isyaml else "links", new_links, None)
        if USE_CACHE and result[0] != "error" and new_links:
            _cache_sub_result(url, result[0], new_links)
        return result
    except requests.RequestException:
        return ("error", url, "timeout")
    except Exception as e:
        return ("error", url, str(e))


# 生成 Clash 配置文件（并发优化版）
def generate_clash_config(links, load_nodes):
    now = datetime.now()
    print(f"当前时间: {now}\n---")

    final_nodes = []
    existing_names = set()  # 存储所有节点名字以检查重复
    config = clash_config_template.copy()

    # 名称已存在的节点加随机后缀
    def resolve_name_conflicts(node):
        server = node.get("server")
        if not server:
            # print(f'不存在sever，非节点')
            return
        name = str(node["name"])
        if not_contains(name):
            if name in existing_names:
                name = add_random_suffix(name, existing_names)
            existing_names.add(name)
            node["name"] = name
            final_nodes.append(node)

    for node in load_nodes:
        resolve_name_conflicts(node)

    # ===== 并发下载所有订阅URL =====
    total_links = len(links)
    print(f'共 {total_links} 个订阅源，开始并发下载 (并发数: {MAX_CONCURRENT_SUBS})...')
    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SUBS) as executor:
        futures = {executor.submit(fetch_sub_url, link): link for link in links}
        # 总超时保护: 120秒内完成所有下载，超时则取消剩余任务
        try:
            for future in as_completed(futures, timeout=120):
                url = futures[future]
                completed += 1
                try:
                    result_type, result_data, error = future.result(timeout=6)
                    if result_type == "proxy":
                        node = parse_proxy_link(result_data)
                        if node:
                            resolve_name_conflicts(node)
                    elif result_type == "links":
                        handle_links(result_data, resolve_name_conflicts)
                    elif result_type == "ss_sub":
                        for node in result_data:
                            resolve_name_conflicts(node)
                    elif result_type == "yaml":
                        for node in result_data:
                            resolve_name_conflicts(node)
                    elif result_type == "error":
                        pass
                except Exception:
                    pass
                if completed % 10 == 0 or completed == total_links:
                    print(f"  progress: {completed}/{total_links} ({completed/total_links*100:.0f}%)")
        except TimeoutError:
            pending = [f for f in futures if not f.done()]
            print(f"  download timeout(120s), cancel {len(pending)} pending tasks")
            for f in pending:
                f.cancel()
    final_nodes = deduplicate_proxies(final_nodes)
    # 重置group中节点name
    config["proxy-groups"][1]["proxies"] = []
    for node in final_nodes:
        name = str(node["name"])
        if not_contains(name):
            # 0节点选择 1 自动选择 2故障转移 3手动选择
            config["proxy-groups"][1]["proxies"].append(name)
            proxies = list(set(config["proxy-groups"][1]["proxies"]))
            config["proxy-groups"][1]["proxies"] = proxies
            config["proxy-groups"][2]["proxies"] = proxies
            config["proxy-groups"][3]["proxies"] = proxies
    config["proxies"] = final_nodes

    if config["proxies"]:
        global CONFIG_FILE
        CONFIG_FILE = CONFIG_FILE[:-5] if CONFIG_FILE.endswith('.json') else CONFIG_FILE
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            yaml.dump(config, f, allow_unicode=True, default_flow_style=False)
        with open(f'{CONFIG_FILE}.json', "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False)
        print(f"已经生成Clash配置文件{CONFIG_FILE}|{CONFIG_FILE}.json")
    else:
        print('没有节点数据更新')


# 判断不包含
def not_contains(s):
    return not any(k in s for k in BAN)


# 自定义 Clash API 异常
class ClashAPIException(Exception):
    """自定义 Clash API 异常"""
    pass


# 代理测试结果类
class ProxyTestResult:
    """代理测试结果类"""

    def __init__(self, name: str, delay: Optional[float] = None):
        self.name = name
        self.delay = delay if delay is not None else float('inf')
        self.status = "ok" if delay is not None else "fail"
        self.tested_time = datetime.now()

    @property
    def is_valid(self) -> bool:
        return self.status == "ok"


def ensure_executable(file_path):
    """ 确保文件具有可执行权限（仅适用于 Linux 和 macOS） """
    if platform.system().lower() in ['linux', 'darwin']:
        os.chmod(file_path, 0o755)  # 设置文件为可执行


# 处理 Clash 配置错误，解析错误信息并更新配置文件
def handle_clash_error(error_message, config_file_path):
    start_time = time.time()
    config_file_path = f'{config_file_path}.json' if os.path.exists(f'{config_file_path}.json') else config_file_path

    proxy_index_match = re.search(r'proxy (\d+):', error_message)
    if not proxy_index_match:
        return False

    problem_index = int(proxy_index_match.group(1))

    try:
        # 读取配置文件
        with open(config_file_path, 'r', encoding='utf-8') as file:
            config = json.load(file)

        # 获取要删除的节点的name
        problem_proxy_name = config['proxies'][problem_index]['name']
        # 删除问题节点
        del config['proxies'][problem_index]

        # 从所有proxy-groups中删除该节点引用
        proxies = config['proxy-groups'][1]["proxies"]
        proxies.remove(problem_proxy_name)
        for group in config["proxy-groups"][1:]:
            group["proxies"] = proxies

        # 保存更新后的配置
        with open(config_file_path, 'w', encoding='utf-8') as file:
            file.write(json.dumps(config, ensure_ascii=False))

        print(
            f'配置异常：{error_message}修复配置异常，移除proxy[{problem_index}] {problem_proxy_name} 完毕，耗时{time.time() - start_time}s\n')
        return True

    except Exception as e:
        print(f"处理配置文件时出错: {str(e)}")
        return False


# 下载最新mihomo
def download_and_extract_latest_release():
    url = "https://api.github.com/repos/MetaCubeX/mihomo/releases/latest"
    response = requests.get(url, timeout=10)

    if response.status_code != 200:
        print("Failed to retrieve data")
        return

    data = response.json()
    assets = data.get("assets", [])
    os_type = platform.system().lower()
    targets = {
        "darwin": "mihomo-darwin-amd64-compatible",
        "linux": "mihomo-linux-amd64-compatible",
        "windows": "mihomo-windows-amd64-compatible"
    }

    # 确定下载链接和新名称
    download_url = None
    new_name = f"clash-{os_type}" if os_type != "windows" else "clash.exe"

    # 检查是否已存在二进制文件
    if os.path.exists(new_name):
        return

    for asset in assets:
        name = asset.get("name", "")
        # 根据操作系统确定下载文件的名称和后缀
        if os_type == "darwin" and targets["darwin"] in name and name.endswith('.gz'):
            download_url = asset["browser_download_url"]
            break
        elif os_type == "linux" and targets["linux"] in name and name.endswith('.gz'):
            download_url = asset["browser_download_url"]
            break
        elif os_type == "windows" and targets["windows"] in name and name.endswith('.zip'):
            download_url = asset["browser_download_url"]
            break

    if download_url:
        download_url = f"{download_url}"
        print(f"Downloading file from {download_url}")
        filename = download_url.split('/')[-1]
        response = requests.get(download_url)

        # 保存下载的文件
        with open(filename, 'wb') as f:
            f.write(response.content)

        # 解压文件并重命名
        extracted_files = []
        if filename.endswith('.zip'):
            with zipfile.ZipFile(filename, 'r') as zip_ref:
                zip_ref.extractall()
                extracted_files = zip_ref.namelist()
        elif filename.endswith('.gz'):
            with gzip.open(filename, 'rb') as f_in:
                output_filename = filename[:-3]
                with open(output_filename, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    extracted_files.append(output_filename)

        # 重命名并删除下载的文件
        for file_name in extracted_files:
            if os.path.exists(file_name):
                os.rename(file_name, new_name)
                break

        os.remove(filename)  # 删除下载的压缩文件
    else:
        print("No suitable release found for the current operating system.")


def read_output(pipe, output_lines):
    while True:
        line = pipe.readline()
        if line:
            output_lines.append(line)
        else:
            break


def kill_clash():
    """
    在 macOS、Linux 和 Windows 上强制杀掉 Clash 进程。
    支持配置文件：clash_config.yaml 和 clash_config.yaml.json
    """
    # 根据操作系统定义 Clash 进程名
    system = platform.system()
    clash_process_names = {
        "Windows": "clash.exe",
        "Linux": "clash-linux",
        "Darwin": "clash-darwin"  # macOS
    }
    config_files = ["clash_config.yaml", "clash_config.yaml.json"]

    # 检查是否支持当前操作系统
    if system not in clash_process_names:
        print("不支持的操作系统")
        return

    # 获取当前系统的 Clash 进程名
    process_name = clash_process_names[system]

    # 遍历所有进程，查找并终止 Clash 进程
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            # 如果进程名不匹配，跳过
            if proc.info['name'] != process_name:
                continue

            # 获取命令行参数并检查配置文件
            cmdline = proc.info['cmdline']
            if cmdline and len(cmdline) >= 3 and cmdline[1] == '-f' and cmdline[2] in config_files:
                # 强制终止进程
                proc.kill()
                # print(f"Clash 进程 (PID: {proc.pid}) 已终止 ({system})")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            # 忽略进程不存在、权限不足或僵尸进程的异常
            pass

    # print(f"未找到 Clash 进程 ({system})")


def start_clash():
    download_and_extract_latest_release()
    system_platform = platform.system().lower()

    if system_platform == 'windows':
        clash_binary = '.\\clash.exe'
    elif system_platform in ["linux", "darwin"]:
        clash_binary = f'./clash-{system_platform}'
        ensure_executable(clash_binary)
    else:
        raise OSError("Unsupported operating system.")

    not_started = True

    global CONFIG_FILE
    CONFIG_FILE = f'{CONFIG_FILE}.json' if os.path.exists(f'{CONFIG_FILE}.json') else CONFIG_FILE
    while not_started:
        # print(f'加载配置{CONFIG_FILE}')
        clash_process = subprocess.Popen(
            [clash_binary, '-f', CONFIG_FILE],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8'
        )

        output_lines = []

        # 启动线程来读取标准输出和标准错误
        stdout_thread = threading.Thread(target=read_output, args=(clash_process.stdout, output_lines))

        stdout_thread.start()

        timeout = 3
        start_time = time.time()
        while time.time() - start_time < timeout:
            stdout_thread.join(timeout=0.5)
            if output_lines:
                # 检查输出是否包含错误信息
                if 'GeoIP.dat' in output_lines[-1]:
                    print(output_lines[-1])
                    time.sleep(5)
                    if is_clash_api_running():
                        return clash_process

                if "Parse config error" in output_lines[-1]:
                    if handle_clash_error(output_lines[-1], CONFIG_FILE):
                        clash_process.kill()
                        output_lines = []
            if is_clash_api_running():
                return clash_process

        if not_started:
            clash_process.kill()
            continue
        return clash_process


def is_clash_api_running():
    try:
        url = f"http://{CLASH_API_HOST}:{CLASH_API_PORTS[0]}/configs"
        response = requests.get(url)
        # 检查响应状态码，200表示正常
        print(f'Clash API启动成功，开始批量检测')
        return response.status_code == 200
    except requests.exceptions.RequestException:
        # 捕获所有请求异常，包括连接错误等
        return False


# 切换到指定代理节点
def switch_proxy(proxy_name='DIRECT'):
    """
    切换 Clash 中策略组的代理节点。
    :param proxy_name: 要切换到的代理节点名称
    :return: 返回切换结果或错误信息
    """
    url = f"http://{CLASH_API_HOST}:{CLASH_API_PORTS[0]}/proxies/节点选择"
    data = {
        "name": proxy_name
    }

    try:
        response = requests.put(url, json=data)
        # 检查响应状态
        if response.status_code == 204:  # Clash API 切换成功返回 204 No Content
            print(f"切换到 '节点选择-{proxy_name}' successfully.")
            return {"status": "success", "message": f"Switched to proxy '{proxy_name}'."}
        else:
            return response.json()
    except Exception as e:
        print(f"Error occurred: {e}")
        return {"status": "error", "message": str(e)}


# 调用ClashAPI
class ClashAPI:
    def __init__(self, host: str, ports: List[int], secret: str = ""):
        self.host = host
        self.ports = ports
        self.base_url = None  # 将在连接检查时设置
        self.headers = {
            "Authorization": f"Bearer {secret}" if secret else "",
            "Content-Type": "application/json",
            'Accept-Charset': 'utf-8',
            'Accept': 'text/html,application/x-yaml,*/*',
            'User-Agent': 'Clash Verge/1.7.7'
        }
        self.client = httpx.AsyncClient(timeout=TIMEOUT)
        self.semaphore = Semaphore(MAX_CONCURRENT_TESTS)
        self._test_results_cache: Dict[str, ProxyTestResult] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.aclose()

    async def check_connection(self) -> bool:
        """检查与 Clash API 的连接状态，自动尝试不同端口"""
        for port in self.ports:
            try:
                test_url = f"http://{self.host}:{port}"
                response = await self.client.get(f"{test_url}/version")
                if response.status_code == 200:
                    version = response.json().get('version', 'unknown')
                    print(f"成功连接到 Clash API (端口 {port})，版本: {version}")
                    self.base_url = test_url
                    return True
            except httpx.RequestError:
                print(f"端口 {port} 连接失败，尝试下一个端口...")
                continue

        print("所有端口均连接失败")
        print(f"请确保 Clash 正在运行，并且 External Controller 已启用于以下端口之一: {', '.join(map(str, self.ports))}")
        return False

    async def get_proxies(self) -> Dict:
        """获取所有代理节点信息"""
        if not self.base_url:
            raise ClashAPIException("未建立与 Clash API 的连接")

        try:
            response = await self.client.get(
                f"{self.base_url}/proxies",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                print("认证失败，请检查 API Secret 是否正确")
            raise ClashAPIException(f"HTTP 错误: {e}")
        except httpx.RequestError as e:
            raise ClashAPIException(f"请求错误: {e}")

    async def test_proxy_delay(self, proxy_name: str) -> ProxyTestResult:
        """测试指定代理节点的延迟，使用缓存避免重复测试"""
        if not self.base_url:
            raise ClashAPIException("未建立与 Clash API 的连接")

        # 检查缓存
        if proxy_name in self._test_results_cache:
            cached_result = self._test_results_cache[proxy_name]
            # 如果测试结果不超过60秒，直接返回缓存的结果
            if (datetime.now() - cached_result.tested_time).total_seconds() < 60:
                return cached_result

        async with self.semaphore:
            try:
                response = await self.client.get(
                    f"{self.base_url}/proxies/{urllib.parse.quote(proxy_name, safe='')}/delay",
                    headers=self.headers,
                    params={"url": TEST_URL, "timeout": int(TIMEOUT * 1000)}
                )
                response.raise_for_status()
                delay = response.json().get("delay")
                result = ProxyTestResult(proxy_name, delay)
            except httpx.HTTPError:
                result = ProxyTestResult(proxy_name)
            except Exception as e:
                result = ProxyTestResult(proxy_name)
                # print(e)
            finally:
                # 更新缓存
                self._test_results_cache[proxy_name] = result
                return result


# 更新clash配置
class ClashConfig:
    """Clash 配置管理类"""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.proxy_groups = self._get_proxy_groups()

    def _load_config(self) -> dict:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"找不到配置文件: {self.config_path}")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"配置文件格式错误: {e}")
            sys.exit(1)

    def _get_proxy_groups(self) -> List[Dict]:
        """获取所有代理组信息"""
        return self.config.get("proxy-groups", [])

    def get_group_names(self) -> List[str]:
        """获取所有代理组名称"""
        return [group["name"] for group in self.proxy_groups]

    def get_group_proxies(self, group_name: str) -> List[str]:
        """获取指定组的所有代理"""
        for group in self.proxy_groups:
            if group["name"] == group_name:
                return group.get("proxies", [])
        return []

    def remove_invalid_proxies(self, results: List[ProxyTestResult]):
        """从配置中完全移除失效的节点"""
        # 获取所有失效节点名称
        invalid_proxies = {r.name for r in results if not r.is_valid}

        if not invalid_proxies:
            return

        # 从 proxies 部分移除失效节点
        valid_proxies = []
        if "proxies" in self.config:
            valid_proxies = [p for p in self.config["proxies"]
                             if p.get("name") not in invalid_proxies]
            self.config["proxies"] = valid_proxies

        # 从所有代理组中移除失效节点
        for group in self.proxy_groups:
            if "proxies" in group:
                group["proxies"] = [p for p in group["proxies"] if p not in invalid_proxies]
        global LIMIT
        left = LIMIT if len(self.config['proxies']) > LIMIT else len(self.config['proxies'])
        # LIMIT = LIMIT if len(self.config['proxies']) > LIMIT else len(self.config['proxies'])
        print(f"已从配置中移除 {len(invalid_proxies)} 个失效节点，最终保留{left}个延迟最小的节点")

    def keep_proxies_by_limit(self, proxy_names):
        if "proxies" in self.config:
            self.config["proxies"] = [p for p in self.config["proxies"] if p["name"] in proxy_names]

    def update_group_proxies(self, group_name: str, results: List[ProxyTestResult]):
        """更新指定组的代理列表，仅保留有效节点并按延迟排序"""
        # 移除失效节点
        self.remove_invalid_proxies(results)

        # 获取有效节点并按延迟排序
        valid_results = [r for r in results if r.is_valid]
        valid_results = list(set(valid_results))
        valid_results.sort(key=lambda x: x.delay)

        # 更新代理组
        proxy_names = [r.name for r in valid_results]
        for group in self.proxy_groups:
            if group["name"] == group_name:
                group["proxies"] = proxy_names
                break
        return proxy_names

    def save(self):
        """保存配置到文件"""
        try:
            # 保存新配置
            yaml_cfg = self.config_path.strip('.json') if self.config_path.endswith('.json') else self.config_path
            with open(yaml_cfg, 'w', encoding='utf-8') as f:
                yaml.dump(self.config, f, allow_unicode=True, sort_keys=False)
            # print(f"新配置已保存到: {yaml_cfg}")
            with open(f'{yaml_cfg}.json', "w", encoding="utf-8") as f:
                json.dump(self.config, f, ensure_ascii=False)
            # print(f'新配置已保存到: {yaml_cfg}.json')

        except Exception as e:
            print(f"保存配置文件失败: {e}")
            sys.exit(1)


# 打印测试结果摘要
def print_test_summary(group_name: str, results: List[ProxyTestResult]):
    """打印测试结果摘要"""
    valid_results = [r for r in results if r.is_valid]
    invalid_results = [r for r in results if not r.is_valid]
    total = len(results)
    valid = len(valid_results)
    invalid = len(invalid_results)

    print(f"\n策略组 '{group_name}' 测试结果:")
    print(f"总节点数: {total}")
    print(f"可用节点数: {valid}")
    print(f"失效节点数: {invalid}")

    delays = []

    if valid > 0:
        avg_delay = sum(r.delay for r in valid_results) / valid
        print(f"平均延迟: {avg_delay:.2f}ms")
        print("\n节点延迟统计:")
        sorted_results = sorted(valid_results, key=lambda x: x.delay)
        for i, result in enumerate(sorted_results[:LIMIT], 1):
            delays.append({"name": result.name, "Delay_ms": round(result.delay, 2)})
            print(f"{i}. {result.name}: {result.delay:.2f}ms")
    return delays


# 测试一组代理节点
async def test_group_proxies(clash_api: ClashAPI, proxies: List[str]) -> List[ProxyTestResult]:
    """测试一组代理节点"""
    print(f"开始测试 {len(proxies)} 个节点 (最大并发: {MAX_CONCURRENT_TESTS})")

    # 创建所有测试任务
    tasks = [clash_api.test_proxy_delay(proxy_name) for proxy_name in proxies]

    # 使用进度显示执行所有任务
    results = []
    for future in asyncio.as_completed(tasks):
        result = await future
        results.append(result)
        # 显示进度
        done = len(results)
        total = len(tasks)
        print(f"\r进度: {done}/{total} ({done / total * 100:.1f}%)", end="", flush=True)

    return results


async def proxy_clean():
    # 更新全局配置
    delays = []
    global MAX_CONCURRENT_TESTS, TIMEOUT, CLASH_API_SECRET, LIMIT, CONFIG_FILE
    CONFIG_FILE = f'{CONFIG_FILE}.json' if os.path.exists(f'{CONFIG_FILE}.json') else CONFIG_FILE
    print(f"===================节点批量检测基本信息======================")
    print(f"配置文件: {CONFIG_FILE}")
    print(f"API 端口: {CLASH_API_PORTS[0]}")
    print(f"并发数量: {MAX_CONCURRENT_TESTS}")
    print(f"超时时间: {TIMEOUT}秒")
    print(f"保留节点：最多保留{LIMIT}个延迟最小的有效节点")

    # 加载配置
    print(f'加载配置文件{CONFIG_FILE}')
    config = ClashConfig(CONFIG_FILE)
    available_groups = config.get_group_names()[1:]

    # 确定要测试的策略组
    groups_to_test = available_groups
    invalid_groups = set(groups_to_test) - set(available_groups)
    if invalid_groups:
        print(f"警告: 以下策略组不存在: {', '.join(invalid_groups)}")
        groups_to_test = list(set(groups_to_test) & set(available_groups))

    if not groups_to_test:
        print("错误: 没有找到要测试的有效策略组")
        print(f"可用的策略组: {', '.join(available_groups)}")
        return

    print(f"\n将测试以下策略组: {', '.join(groups_to_test)}")

    # 开始测试
    start_time = datetime.now()

    # 创建支持多端口的API实例
    async with ClashAPI(CLASH_API_HOST, CLASH_API_PORTS, CLASH_API_SECRET) as clash_api:
        if not await clash_api.check_connection():
            return

        try:
            all_test_results = []  # 收集所有测试结果

            # 测试策略组，只需要测试其中一个即可
            group_name = groups_to_test[0]
            print(f"\n======================== 开始测试策略组: {group_name} ====================")
            proxies = config.get_group_proxies(group_name)

            if not proxies:
                print(f"策略组 '{group_name}' 中没有代理节点")
            else:
                # 测试该组的所有节点
                results = await test_group_proxies(clash_api, proxies)
                all_test_results.extend(results)
                # 打印测试结果摘要
                delays = print_test_summary(group_name, results)

            print('\n===================移除失效节点并按延迟排序======================\n')
            # 一次性移除所有失效节点并更新配置
            config.remove_invalid_proxies(all_test_results)

            # 为每个组更新有效节点的顺序
            proxy_names = set()
            # 只对一个group的proxies排序即可
            group_proxies = config.get_group_proxies(group_name)
            group_results = [r for r in all_test_results if r.name in group_proxies]
            if LIMIT:
                group_results = group_results[:LIMIT]
            for r in group_results:
                proxy_names.add(r.name)

            for group_name in groups_to_test:
                proxy_names = config.update_group_proxies(group_name, group_results)
                print(f"'{group_name}'已按延迟大小重新排序")

            if LIMIT:
                config.keep_proxies_by_limit(proxy_names)

            # 保存更新后的配置
            config.save()

            if SPEED_TEST:
                # 测速
                print('\n===================检测节点速度======================\n')
                sorted_proxy_names = start_download_test(proxy_names)
                # 按测试重新排序
                new_list = sorted_proxy_names.copy()
                # 创建一个集合来跟踪已添加的元素
                added_elements = set(new_list)
                # 遍历 group_proxies，将不在 added_elements 中的元素添加到 new_list
                group_proxies = config.get_group_proxies(group_name)
                for item in group_proxies:
                    if item not in added_elements:
                        new_list.append(item)
                        added_elements.add(item)  # 将新添加的元素加入集合中
                # 排序好的节点名放入group-proxies
                for group_name in groups_to_test:
                    for group in config.proxy_groups:
                        if group["name"] == group_name:
                            group["proxies"] = new_list
                # 保存更新后的配置
                config.save()

            # 显示总耗时
            total_time = (datetime.now() - start_time).total_seconds()
            print(f"\n总耗时: {total_time:.2f} 秒")
            return delays
        except ClashAPIException as e:
            print(f"Clash API 错误: {e}")
        except Exception as e:
            print(f"发生错误: {e}")
            raise


# 获取当前时间的各个组成部分
def parse_datetime_variables():
    now = datetime.now()
    return {
        'Y': str(now.year),
        'm': str(now.month).zfill(2),
        'd': str(now.day).zfill(2),
        'H': str(now.hour).zfill(2),
        'M': str(now.minute).zfill(2),
        'S': str(now.second).zfill(2)
    }


# 移除URL中的代理前缀
def strip_proxy_prefix(url):
    proxy_pattern = r'^https?://[^/]+/https://'
    match = re.match(proxy_pattern, url)
    if match:
        real_url = re.sub(proxy_pattern, 'https://', url)
        proxy_prefix = url[:match.end() - 8]
        return real_url, proxy_prefix
    return url, None


# 判断是否为GitHub raw URL
def is_github_raw_url(url):
    return 'raw.githubusercontent.com' in url


# 从URL中提取文件模式，返回占位符前后的部分
def extract_file_pattern(url):
    # 查找形如 {x}<suffix> 的模式
    match = re.search(r'\{x\}(\.[a-zA-Z0-9]+)(?:/|$)', url)
    if match:
        return match.group(1)  # 返回文件后缀，如 '.yaml', '.txt', '.json'
    return None


# 从GitHub API获取匹配指定后缀的文件名
def get_github_filename(github_url, file_suffix):
    # 兼容两种URL格式:
    # 1) https://raw.githubusercontent.com/owner/repo/refs/heads/branch/path
    # 2) https://raw.githubusercontent.com/owner/repo/branch/path
    match = re.match(r'https://raw\.githubusercontent\.com/([^/]+)/([^/]+)/(?:refs/heads/)?([^/]+)/(.+)', github_url)
    if not match:
        print(f"无法从URL中提取owner和repo信息: {github_url}")
        return None

    owner = match.group(1)
    repo = match.group(2)
    branch = match.group(3)
    path_after_branch = match.group(4)

    # 移除 {x}<suffix> 部分来获取目录路径
    dir_path = re.sub(r'\{x\}' + re.escape(file_suffix) + '(?:/|$)', '', path_after_branch)
    # 如果目录路径包含文件名部分，取其目录
    if '/' in dir_path:
        dir_path = dir_path.rsplit('/', 1)[0] if not dir_path.endswith('/') else dir_path.rstrip('/')
    else:
        dir_path = ''

    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{dir_path}?ref={branch}"

    try:
        response = requests.get(api_url, headers=headers, timeout=10, verify=False)
        if response.status_code != 200:
            print(f"GitHub API请求失败({response.status_code}): {github_url}")
            return None

        files = response.json()
        if not isinstance(files, list):
            print(f"GitHub API返回非列表数据: {github_url}")
            return None

        matching_files = [f['name'] for f in files if isinstance(f, dict) and f.get('name', '').endswith(file_suffix)]

        if not matching_files:
            print(f"未找到匹配{file_suffix}的文件: {github_url}")
            return None

        return matching_files[0]
    except requests.RequestException as e:
        print(f"GitHub API请求异常: {e}")
        return None
    except Exception as e:
        print(f"获取GitHub文件名异常: {e}")
        return None


# 解析URL模板，支持任意组合的日期时间变量和分隔符
def parse_template(template_url, datetime_vars):
    def replace_template(match):
        """替换单个模板块的内容"""
        template_content = match.group(1)
        if template_content == 'x':
            return '{x}'  # 保持 {x} 不变，供后续处理

        result = ''
        # 用于临时存储当前字符
        current_char = ''

        # 遍历模板内容中的每个字符
        for char in template_content:
            if char in datetime_vars:
                # 如果是日期时间变量，替换为对应值
                if current_char:
                    # 添加之前累积的非变量字符
                    result += current_char
                    current_char = ''
                result += datetime_vars[char]
            else:
                # 如果是其他字符（分隔符），直接保留
                current_char += char

        # 添加最后可能剩余的非变量字符
        if current_char:
            result += current_char

        return result

    # 使用正则表达式查找并替换所有模板块
    return re.sub(r'\{([^}]+)\}', replace_template, template_url)


# 完整解析模板URL
def resolve_template_url(template_url):
    # 先处理代理前缀
    url, proxy_prefix = strip_proxy_prefix(template_url)

    # 获取日期时间变量
    datetime_vars = parse_datetime_variables()

    # 替换日期时间变量
    resolved_url = parse_template(url, datetime_vars)

    # 如果是GitHub URL且包含{x}，则处理文件名
    if is_github_raw_url(resolved_url) and '{x}' in resolved_url:
        # 提取文件后缀
        file_suffix = extract_file_pattern(resolved_url)
        if file_suffix:
            filename = get_github_filename(resolved_url, file_suffix)
            if filename:
                # 替换 {x}<suffix> 为实际文件名
                resolved_url = re.sub(r'\{x\}' + re.escape(file_suffix), filename, resolved_url)
            else:
                print(f"无法解析GitHub模板URL，跳过: {resolved_url}")
                return template_url  # 返回原始模板URL，不做替换

    # 如果有代理前缀，重新添加上
    if proxy_prefix:
        resolved_url = f"{proxy_prefix}{resolved_url}"

    return resolved_url


def start_download_test(proxy_names, speed_limit=0.1):
    """
    开始下载测试

    """
    # 第一步：测试所有节点的下载速度
    test_all_proxies(proxy_names[:SPEED_TEST_LIMIT])

    # 过滤出速度大于等于 speed_limit 的节点
    filtered_list = [item for item in results_speed if float(item[1]) >= float(f'{speed_limit}')]

    # 按下载速度从大到小排序
    sorted_proxy_names = []
    sorted_list = sorted(filtered_list, key=lambda x: float(x[1]), reverse=True)
    print(f'节点速度统计:')
    for i, result in enumerate(sorted_list[:LIMIT], 1):
        sorted_proxy_names.append(result[0])
        print(f"{i}. {result[0]}: {result[1]}Mb/s")

    return sorted_proxy_names


# 测试所有代理节点的下载速度（多线程并发版）
def test_all_proxies(proxy_names):
    try:
        i = 0
        lock = threading.Lock()
        def test_one(proxy_name):
            nonlocal i
            with lock:
                i += 1
                idx = i
            print(f"\r正在测速节点【{idx}/{len(proxy_names)}】: {proxy_name[:30]}", flush=True, end='')
            test_proxy_speed(proxy_name)

        with ThreadPoolExecutor(max_workers=3) as executor:
            list(executor.map(test_one, proxy_names))

        print("\r" + " " * 50 + "\r", end='') # 清空行并返回行首
    except Exception as e:
        print(f"测试节点速度时出错: {e}")


# 测试指定代理节点的下载速度（下载5秒后停止）
def test_proxy_speed(proxy_name):
    # 切换到该代理节点
    switch_proxy(proxy_name)
    # 设置代理
    proxies = {
        "http": 'http://127.0.0.1:7890',
        "https": 'http://127.0.0.1:7890',
    }

    # 开始下载并测量时间
    start_time = time.time()
    # 计算总下载量
    total_length = 0
    # 测试下载时间（秒）
    test_duration = 5  # 逐块下载，直到达到5秒钟为止

    # 不断发起请求直到达到时间限制
    while time.time() - start_time < test_duration:
        try:
            response = requests.get("http://speedtest.tele2.net/100MB.zip", stream=True, proxies=proxies,
                                    headers={'Cache-Control': 'no-cache'},
                                    timeout=test_duration)
            for data in response.iter_content(chunk_size=524288):
                total_length += len(data)
                if time.time() - start_time >= test_duration:
                    break
        except Exception as e:
            print(f"测试节点 {proxy_name} 下载失败: {e}")

    # 计算速度：Bps -> MB/s
    elapsed_time = time.time() - start_time
    speed = total_length / elapsed_time if elapsed_time > 0 else 0

    results_speed.append((proxy_name, f"{speed / 1024 / 1024:.2f}"))  # 记录速度测试结果
    return speed / 1024 / 1024  # 返回 MB/s


def upload_and_generate_urls(file_path=CONFIG_FILE):
    # api_url = "https://catbox.moe/user/api.php"
    # api_url = "https://f2.252035.xyz/user/api.php"
    api_url = "https://ade4e1d7-catbox.seczhcom.workers.dev/user/api.php"
    result = {"clash_url": None, "singbox_url": None}

    try:
        if not os.path.isfile(file_path):
            print(f"错误：文件 {file_path} 不存在。")
            return result
        if os.path.getsize(file_path) > 209715200:
            print("错误：文件大小超过 200MB 限制。")
            return result

        # Upload Clash config
        with open(file_path, 'rb') as file:
            response = requests.post(api_url, data={"reqtype": "fileupload"}, files={"fileToUpload": file}, timeout=15,
                                     verify=False)
            if response.status_code == 200:
                clash_url = response.text.strip()
                result["clash_url"] = clash_url
                print(f"Clash 配置文件上传成功！直链：{clash_url}")

                sb_full_url = f'https://url.v1.mk/sub?target=singbox&url={clash_url}&insert=false&config=https://raw.githubusercontent.com/ACL4SSR/ACL4SSR/master/Clash/config/ACL4SSR_Online_Full_NoAuto.ini&emoji=true&list=false&xudp=false&udp=false&tfo=false&expand=true&scv=false&fdn=false'
                encoded_url = base64.urlsafe_b64encode(sb_full_url.encode()).decode()
                response = requests.post("https://v1.mk/short", json={"longUrl": encoded_url})
                if response.status_code == 200:
                    data = response.json()
                    if data.get("Code") == 1:
                        singbox_url = data["ShortUrl"]
                        result["singbox_url"] = singbox_url
                        print(f"singbox 配置文件上传成功！直链：{singbox_url}")

    except Exception as e:
        print(f"发生错误：{e}")

    # 记录成功生成的链接到subs.json
    subs_file = "subs.json"
    if result["clash_url"] or result["singbox_url"]:
        try:
            # 初始化默认结构
            subs_data = {"clash": [], "singbox": []}

            # 尝试读取现有文件
            if os.path.exists(subs_file):
                try:
                    with open(subs_file, 'r', encoding='utf-8') as f:
                        subs_data = json.load(f)
                except:
                    pass  # 如果文件损坏，使用默认结构

            # 添加新链接到记录中(避免重复)
            if result["clash_url"] and result["clash_url"] not in subs_data.get("clash", []):
                if "clash" not in subs_data:
                    subs_data["clash"] = []
                subs_data["clash"].append(result["clash_url"])

            if result["singbox_url"] and result["singbox_url"] not in subs_data.get("singbox", []):
                if "singbox" not in subs_data:
                    subs_data["singbox"] = []
                subs_data["singbox"].append(result["singbox_url"])

            # 保存更新后的数据
            with open(subs_file, 'w', encoding='utf-8') as f:
                json.dump(subs_data, f, ensure_ascii=False, indent=2)

            print(f"已将订阅链接记录到 {subs_file}")
        except Exception as e:
            print(f"记录订阅链接失败: {str(e)}")

    return result


def work(links, check=False, allowed_types=[], only_check=False):
    # CLI args
    if "--clear-cache" in sys.argv:
        clear_cache()
        print("cache cleared")
    if "--force-refresh" in sys.argv:
        global FORCE_REFRESH
        FORCE_REFRESH = True
        print("force refresh mode")
    # checkpoint
    checkpoint = load_checkpoint() if USE_CACHE else {}
    if checkpoint:
        stage = checkpoint.get("stage", "")
        print(f"found checkpoint: {stage}")
    # cache stats
    if USE_CACHE:
        sub_cache = load_json_cache(SUB_CACHE_FILE)
        print(f"sub cache: {len(sub_cache)} entries")
    try:
        if not only_check:
            load_nodes = read_yaml_files(folder_path=INPUT)
            if allowed_types:
                load_nodes = filter_by_types_alt(allowed_types, nodes=load_nodes)
            links = merge_lists(read_txt_files(folder_path=INPUT), links)
            if links or load_nodes:
                generate_clash_config(links, load_nodes)

        if check or only_check:
            clash_process = None
            try:
                # 启动clash
                print(f"===================启动clash并初始化配置======================")
                clash_process = start_clash()
                # 切换节点到'节点选择-DIRECT'
                switch_proxy('DIRECT')
                asyncio.run(proxy_clean())
                print(f'批量检测完毕')
            except Exception as e:
                print("Error calling Clash API:", e)
            finally:
                print(f'关闭Clash API')
                if clash_process is not None:
                    clash_process.kill()

    except KeyboardInterrupt:
        print("\n用户中断执行")
        sys.exit(0)
    except Exception as e:
        print(f"程序执行失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    links = [
           "https://c7dabe95.proxy-978.pages.dev/767b6340-96dc-4aa0-8013-a8af7513d920?clash",
        "https://cdn.jsdelivr.net/gh/xiaoji235/airport-free/clash/naidounode.txt",
        "https://cdn.jsdelivr.net/gh/yangxiaoge/tvbox_cust@master/clash/Clash2.yml",
        "https://gy.xiaozi.us.kg/sub?token=lzj666",
        "https://igdux.top/5Hna",
        "https://mxlsub.me/newfull",
        "https://proxypool.link/ss/sub|ss",
        "https://proxypool.link/trojan/sub",
        "https://proxypool.link/vmess/sub",
        "https://raw.githubusercontent.com/Q3dlaXpoaQ/V2rayN_Clash_Node_Getter/refs/heads/main/APIs/sc0.yaml",
        "https://raw.githubusercontent.com/Q3dlaXpoaQ/V2rayN_Clash_Node_Getter/refs/heads/main/APIs/sc1.yaml",
        "https://raw.githubusercontent.com/Q3dlaXpoaQ/V2rayN_Clash_Node_Getter/refs/heads/main/APIs/sc2.yaml",
        "https://raw.githubusercontent.com/Q3dlaXpoaQ/V2rayN_Clash_Node_Getter/refs/heads/main/APIs/sc3.yaml",
        "https://raw.githubusercontent.com/Q3dlaXpoaQ/V2rayN_Clash_Node_Getter/refs/heads/main/APIs/sc4.yaml",
        "https://raw.githubusercontent.com/Roywaller/clash_subscription/refs/heads/main/clash_subscription.txt",
        "https://raw.githubusercontent.com/Ruk1ng001/freeSub/main/clash.yaml",
        "https://raw.githubusercontent.com/SoliSpirit/v2ray-configs/main/all_configs.txt",
        "https://raw.githubusercontent.com/a2470982985/getNode/main/clash.yaml",
        "https://raw.githubusercontent.com/aiboboxx/clashfree/refs/heads/main/clash.yml",
        "https://raw.githubusercontent.com/aiboboxx/v2rayfree/refs/heads/main/README.md",
        "https://raw.githubusercontent.com/anaer/Sub/refs/heads/main/clash.yaml",
        "https://raw.githubusercontent.com/chengaopan/AutoMergePublicNodes/master/list.yml",
        "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/clash.yml",
        "https://raw.githubusercontent.com/firefoxmmx2/v2rayshare_subcription/refs/heads/main/subscription/clash_sub.yaml",
        "https://raw.githubusercontent.com/free18/v2ray/refs/heads/main/c.yaml",
        "https://raw.githubusercontent.com/go4sharing/sub/main/sub.yaml",
        "https://raw.githubusercontent.com/leetomlee123/freenode/refs/heads/main/README.md",
        "https://raw.githubusercontent.com/ljlfct01/ljlfct01.github.io/refs/heads/main/节点",
        "https://raw.githubusercontent.com/mahdibland/SSAggregator/master/sub/sub_merge_yaml.yml",
        "https://raw.githubusercontent.com/mahdibland/ShadowsocksAggregator/master/Eternity.yml",
        "https://raw.githubusercontent.com/mahdibland/ShadowsocksAggregator/master/LogInfo.txt",
        "https://raw.githubusercontent.com/mai19950/clashgithub_com/refs/heads/main/site",
        "https://raw.githubusercontent.com/mfbpn/tg_mfbpn_sub/main/trial.yaml",
        "https://raw.githubusercontent.com/mfuu/v2ray/master/clash.yaml",
        "https://raw.githubusercontent.com/mgit0001/test_clash/refs/heads/main/heima.txt",
        "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.meta.yml",
        "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.yml",
        "https://raw.githubusercontent.com/Pawdroid/Free-servers/refs/heads/main/sub",
        "https://raw.githubusercontent.com/ripaojiedian/freenode/main/clash",
        "https://raw.githubusercontent.com/shahidbhutta/Clash/refs/heads/main/Router",
        "https://raw.githubusercontent.com/skka3134/Free-servers/refs/heads/main/README.md",
        "https://raw.githubusercontent.com/snakem982/proxypool/main/source/clash-meta.yaml",
        "https://raw.githubusercontent.com/vxiaov/free_proxies/main/clash/clash.provider.yaml",
        "https://raw.githubusercontent.com/wangyingbo/yb_clashgithub_sub/main/clash_sub.yml",
        "https://raw.githubusercontent.com/xiaoer8867785/jddy5/refs/heads/main/data/{Y_m_d}/{x}.yaml",
        "https://raw.githubusercontent.com/xiaoji235/airport-free/refs/heads/main/clash/naidounode.txt",
        "https://raw.githubusercontent.com/zhangkaiitugithub/passcro/main/speednodes.yaml",
        "https://raw.githubusercontent.com/aiboboxx/clashfree/refs/heads/main/clash.yml",
        "https://raw.githubusercontent.com/ljlfct01/ljlfct01.github.io/refs/heads/main/节点",
        "https://raw.githubusercontent.com/mahdibland/ShadowsocksAggregator/master/LogInfo.txt",
        "https://raw.githubusercontent.com/wangyingbo/yb_clashgithub_sub/main/clash_sub.yml",
        "https://SOS.CMLiussss.net/auto",
        "https://sub.fqzsnai.ggff.net/auto",
        "https://sub.mikeone.ggff.net/sub?token=6e300fe82f12874e439b76693aa179fb",
        "https://sub.reajason.eu.org/clash.yaml",
        "https://v1.mk/HuaplNe",
        "https://www.freeclashnode.com/uploads/{Y}/{m}/0-{Ymd}.yaml",
        "https://www.freeclashnode.com/uploads/{Y}/{m}/1-{Ymd}.yaml",
        "https://zrf.zrf.me/zrf",
        "https://ghfast.top/https://raw.githubusercontent.com/ovmvo/SubShare/refs/heads/main/sub/permanent/mihomo.yaml",
"https://gh-proxy.com/raw.githubusercontent.com/Ruk1ng001/freeSub/main/clash.yaml",
"https://subapi.kkhhyytt.cn/api/v1/client/subscribe?token=ae46ea78ad5e7b247417412b1fda1f17",
"https://ghfast.top/https://raw.githubusercontent.com/snakem982/proxypool/main/source/clash-meta-2.yaml",
"https://ghfast.top/https://raw.githubusercontent.com/ovmvo/SubShare/refs/heads/main/sub/permanent/mihomo.yaml",
"https://ghfast.top/https://raw.githubusercontent.com/srm2021lu/Clash-Subscription-with-AdBlock/refs/heads/10_Jan_2025/SSH.yaml",
"https://ghfast.top/https://raw.githubusercontent.com/ts-sf/fly/main/clash",
"https://52pokemon.xz61.cn/api/v1/client/subscribe?token=7dab13ea5a0349f1043df3d05299f5f9",
"https://clash.crossxx.com/sub/hysteria/1730865604",
"https://clash.crossxx.com/sub/ssr/1730808004",
"https://ghfast.top/https://raw.githubusercontent.com/MhdiTaheri/V2rayCollector_Py/refs/heads/main/sub/Hong%20Kong/config.txt",
"https://ghfast.top/https://raw.githubusercontent.com/MhdiTaheri/V2rayCollector_Py/refs/heads/main/sub/Singapore/config.txt",
"https://ghfast.top/https://raw.githubusercontent.com/Surfboardv2ray/Proxy-sorter/main/custom/udp.txt",
"https://ghfast.top/https://raw.githubusercontent.com/Surfboardv2ray/Proxy-sorter/main/ws_tls/proxies/wstls",
"https://ghfast.top/https://raw.githubusercontent.com/AzadNetCH/Clash/main/AzadNet.txt",
"https://ghfast.top/https://raw.githubusercontent.com/Surfboardv2ray/Proxy-sorter/main/custom/udp.txt" "http://raw.githubusercontent.com/Epodonios/bulk-xray-v2ray-vless-vmess-...-configs/refs/heads/main/sub/Hong%20Kong/config.txt" "http://raw.githubusercontent.com/Epodonios/bulk-xray-v2ray-vless-vmess-...-configs/refs/heads/main/sub/Singapore/config.txt" "http://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.meta.yml" "http://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/countries/sg/mixed" "http://raw.githubusercontent.com/soroushmirzaei/telegram-configs-collector/main/countries/hk/mixed" "http://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/clashmeta.yaml" "http://raw.githubusercontent.com/misersun/config003/main/config_all.yaml" "http://raw.githubusercontent.com/anaer/Sub/refs/heads/main/clash.yaml",
"https://clash.crossxx.com/sub/vmess/1730808004",
"https://ghfast.top/https://raw.githubusercontent.com/srm2021lu/Clash-Subscription-with-AdBlock/refs/heads/main/Clash_Subscription_with_AdsBlock.yaml",
"https://raw.githubusercontent.com/anaer/Sub/refs/heads/main/proxies.yaml",
"https://raw.githubusercontent.com/atomhb/Auto_Update_Sub/refs/heads/main/sub.yaml",
"https://raw.githubusercontent.com/wenxig/free-nodes-sub/refs/heads/main/data/clash.yml",
"https://raw.githubusercontent.com/tbbatbb/Proxy/master/dist/clash.config.yaml",
"https://raw.githubusercontent.com/Alvin9999/pac2/master/clash/1/config.yaml",
"https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/clash.yml",
"https://raw.githubusercontent.com/ermaozi01/free_clash_vpn/main/subscribe/clash.yml",
"https://raw.githubusercontent.com/zhangkaiitugithub/passcro/main/speednodes.yaml",
"https://raw.githubusercontent.com/vveg26/get_proxy/main/dist/clash.config.yaml",
"https://raw.githubusercontent.com/vveg26/chromego_merge/main/sub/merged_proxies.yaml",
"https://raw.githubusercontent.com/lcx12901/v2ray-/master/sspool.herokuapp.com/yzcloud.yaml",
"https://raw.githubusercontent.com/lcx12901/v2ray-/master/sspool.herokuapp.com/yzcloud2.yaml",
"https://raw.githubusercontent.com/ronghuaxueleng/get_v2/main/pub/changfengoss.yaml",
"https://raw.githubusercontent.com/ronghuaxueleng/get_v2/main/pub/combine.yaml",
"https://raw.githubusercontent.com/bingoYB/node_processing/main/dist/all.yaml",
"https://raw.githubusercontent.com/itxve/fetch-clash-node/main/node/ClashNode.yaml",
"https://raw.githubusercontent.com/YasserDivaR/pr0xy/main/winformClash.yaml",
"https://raw.githubusercontent.com/sh3d0ww02f/sh3d0ww02f.github.io/main/clash1.yaml",
"https://raw.githubusercontent.com/obscure1990/freeVM/master/snippets/nodes.yml",
"https://raw.githubusercontent.com/sun9426/sun9426.github.io/main/subscribe/Clash.yaml",
"https://raw.githubusercontent.com/itxve/fetch-clash-node/main/node/ClashNode.yaml",
"https://raw.githubusercontent.com/sh3d0ww02f/sh3d0ww02f.github.io/main/clash1.yaml",
"https://raw.githubusercontent.com/igeekshare/GeekshareFreeNode/main/clash/Geekshare.yaml",
"https://raw.githubusercontent.com/learnhard-cn/free_proxy_ss/main/clash/clash.provider.yaml",
"https://raw.githubusercontent.com/learnhard-cn/free_proxy_ss/main/clash/config.yaml",
"https://raw.githubusercontent.com/jw853355718/clash_233/master/config.yml",
"https://raw.githubusercontent.com/BUTUbird/ClashPoint/main/application.yaml",
"https://raw.githubusercontent.com/vpei/free-node-1/main/o/proxies.txt",
"https://raw.githubusercontent.com/du5/free/master/file/0909/Clash.yaml",
"https://raw.githubusercontent.com/oslook/clash-freenode/main/clash.yaml",
"https://raw.githubusercontent.com/jw853355718/clash_233/master/config.yml",
"https://raw.githubusercontent.com/kevin-wud/v2ray-node/main/clash.yaml",
"https://raw.githubusercontent.com/nasheep/FreeNode/main/clash/PlayLab",
"https://raw.githubusercontent.com/tony0392/clash/main/clash.yaml",
"https://raw.githubusercontent.com/zhlx2835/freefq/main/clash.yaml",
"https://raw.githubusercontent.com/Junely/clash/main/template3.yaml",
"https://raw.githubusercontent.com/misersun/config003/main/config_all_quest.yaml",
"https://raw.githubusercontent.com/misersun/config003/main/config_all.yaml",
"https://raw.githubusercontent.com/renyige1314/CLASH/main/CLASH",
"https://raw.githubusercontent.com/69z1zfw2fly/fly/main/2.yaml",
"https://raw.githubusercontent.com/Flik6/getNode/main/clash.yaml",
"https://raw.githubusercontent.com/free18/v2ray/main/Clash.yaml",
"https://raw.githubusercontent.com/anaer/Sub/main/clash.yaml",
"https://raw.githubusercontent.com/baip01/clash/main/clash",
"https://raw.githubusercontent.com/ts-sf/fly/main/clash",
"https://raw.githubusercontent.com/parkerpa/zypjj/main/clash",
"https://raw.githubusercontent.com/baip01/clash/main/clash",
"https://raw.githubusercontent.com/shbioc/clash/main/aaa01.yaml",
"https://raw.githubusercontent.com/ssrsub/ssr/master/Clash.yml",
"https://raw.githubusercontent.com/9Fork/openit/main/Clash.yaml",
"https://raw.githubusercontent.com/chfchf0306/clash/main/clash",
"https://raw.githubusercontent.com/chongdong1230/dxz/main/clash",
"https://raw.githubusercontent.com/aiboboxx/clashfree/main/clash.yml",
"https://raw.githubusercontent.com/hkaa0/permalink/main/proxy/clash",
"https://raw.githubusercontent.com/rxsweet/proxies/main/sub/rx.yaml",
"https://raw.githubusercontent.com/rxsweet/proxies/main/sub/srx.yaml",
"https://raw.githubusercontent.com/rxsweet/proxies/main/sub/free.yaml",
"https://raw.githubusercontent.com/rxsweet/proxies/main/sub/sources/dynamicAll.yaml",
"https://raw.githubusercontent.com/rxsweet/proxies/main/sub/sources/miningAll.yaml",
"https://raw.githubusercontent.com/imboys/proxyForClash/refs/heads/master/free%20proxy.yml",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/yudou66.yaml",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/wenode.yaml",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/nodefree.yaml",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/nodev2ray.yaml",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/ndnode.yaml",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/clashmeta.yaml",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/v2rayshare.yaml",
"https://raw.githubusercontent.com/cxr9912/cxr2022/refs/heads/main/free.yaml",
"https://raw.githubusercontent.com/cxr9912/cxr2022/refs/heads/main/aaaaaaaa.yaml",
"https://raw.githubusercontent.com/cxr9912/cxr2022/refs/heads/main/18cj.json",
"https://raw.githubusercontent.com/cxr9912/cxr2022/main/ss.yaml",
"https://raw.githubusercontent.com/cxr9912/cxr2022/main/ssr.yaml",
"https://raw.githubusercontent.com/cxr9912/cxr2022/main/free.yaml",
"https://raw.githubusercontent.com/cxr9912/cxr2022/main/vmess.yaml",
"https://raw.githubusercontent.com/cxr9912/cxr2022/main/mix.yaml",
"https://raw.githubusercontent.com/moneyfly1/sublist/main/clash.yml",
"https://raw.githubusercontent.com/NiceVPN123/NiceVPN/main/Clash.yaml",
"https://raw.githubusercontent.com/shbioc/clash/main/aaa01.yaml",
"https://raw.githubusercontent.com/chongdong1230/dxz/main/clash",
"https://raw.githubusercontent.com/pojiezhiyuanjun/2023/master/0804clash.yml",
"https://raw.githubusercontent.com/freenodes/freenodes/main/clash.yaml",
"https://raw.githubusercontent.com/moneyfly1/sublist/main/clash.yml",
"https://raw.githubusercontent.com/freebaipiao/freebaipiao/main/jiassweetoy3.yaml",
"https://raw.githubusercontent.com/gooooooooooooogle/Clash-Config/main/Clash.yaml",
"https://raw.githubusercontent.com/chongdong1230/dxz/main/clash",
"https://raw.githubusercontent.com/itsyebekhe/PSG/main/subscriptions/clash/mix",
"https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.yml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/ainita.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/amin_o__o_bitplatform.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/hamedp-71/Sub_Checker_Creator_final.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/hamedp-71/Trojan_hp.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/MatinGhanbari_v2ray-configs-super-sub.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/namira.dev.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/yebekhe/vpn-fail.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/gheychiamoozesh.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/the3rf_com_sub_php.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/F0rc3Run_XX.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/shatakvpn.yaml4_Sub.txt",
"https://raw.githubusercontent.com/10ium/free-config/refs/heads/main/HighSpeed.txt",
"https://raw.githubusercontent.com/lagzian/SS-Collector/main/mix_clash.yaml",
"https://raw.githubusercontent.com/ALIILAPRO/v2rayNG-Config/main/sub.txt",
"https://raw.githubusercontent.com/MrMohebi/xray-proxy-grabber-telegram/master/collected-proxies/clash-meta/all.yaml",
"https://raw.githubusercontent.com/10ium/free-config/refs/heads/main/free-mihomo-sub/MahsaNetConfigTopic.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/FreedomGuard/Finder_configs.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/ebrasha/lite.yaml",
"https://raw.githubusercontent.com/Misaka-blog/chromego_merge/main/sub/merged_proxies_new.yaml",
"https://raw.githubusercontent.com/mlabalabala/v2ray-node/main/nodefree4clash.txt",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/66.42.50.118.yaml",
"https://raw.githubusercontent.com/10ium/MihomoSaz/main/Sublist/Barabama/clashmeta.yaml",
"https://raw.githubusercontent.com/10ium/free-config/refs/heads/main/dnsforgame/shecan.yml",
"https://bitbucket.org/huwo1/proxy_nodes/raw/f31ca9ec67b84071515729ff45b011b6b09c10f2/clash.yaml",
"https://github.com/MrMohebi/xray-proxy-grabber-telegram/raw/master/collected-proxies/clash-meta/all.yaml",
"https://github.com/mahdibland/V2RayAggregator/raw/master/sub/sub_merge_yaml.yml",
"https://github.com/vxiaov/free_proxy_ss/raw/main/clash/clash.provider.yaml",
"https://github.com/NiREvil/vless/blob/main/sub/clash-meta.yml",
"https://github.com/LonUp/NodeList/raw/main/Clash/Node/Latest.yaml",
"https://gitlab.com/univstar1/v2ray/-/raw/main/data/clash/general.yaml",
"https://freevpnspy.githubrowcontent.com/2024/08/20240802_novless.yaml",
"https://freevpnspy.githubrowcontent.com/2024/08/20240802_vless.yaml",
"https://raw.githubusercontent.com/firefoxmmx2/v2rayshare_subcription/main/subscription/clash_sub.yaml",
"https://raw.githubusercontent.com/ts-sf/fly/main/clash ",
"https://raw.githubusercontent.com/MatinGhanbari/v2ray-configs/main/subscriptions/filtered/subs/ss.txt ",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/nodefree.yaml",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/v2rayshare.yaml",
"https://raw.githubusercontent.com/Barabama/FreeNodes/refs/heads/main/nodes/wenode.yaml",
"https://raw.githubusercontent.com/4n0nymou3/multi-proxy-config-fetcher/refs/heads/main/configs/proxy_configs_tested.txt",
"https://raw.githubusercontent.com/peweza/PublicPewezaSub/refs/heads/main/SUBPewezaVPN",
"https://gist.githubusercontent.com/shuaidaoya/9e5cf2749c0ce79932dd9229d9b4162b/raw/all.yaml",
"https://raw.githubusercontent.com/Argh94/Proxy-List/refs/heads/main/All_Config.txt",
"https://raw.githubusercontent.com/chengaopan/AutoMergePublicNodes/master/list.yml",
"https://raw.githubusercontent.com/Samive/clash-sub/refs/heads/main/clash.yaml",
"https://raw.githubusercontent.com/mingko3/socks5-clash-proxy/refs/heads/main/proxy.yaml",
"https://raw.githubusercontent.com/chengaopan/AutoMergePublicNodes/refs/heads/master/list.yml",
"https://raw.githubusercontent.com/vluma/free-vpn2clash/refs/heads/main/output/free-VPN.yaml",
"https://raw.githubusercontent.com/twj0/subseek/refs/heads/master/data/sub_github.txt",
"https://xsg2025.xsgsvip.dpdns.org/xsg?sub=zrf.zrf.me&proxyip=ProxyIP.HK.CMLiussss.net",
"https://xsg241220.cnxskj.dpdns.org/xs?sub=sub.keaeye.icu&proxyip=ProxyIP.HK.CMLiussss.net",
"https://xsgg25.xsgsvip.dpdns.org/xsgg?sub=VLESS.fxxk.dedyn.io&proxyip=ProxyIP.JP.CMLiussss.net",
"https://xs250408.xskj2000.dpdns.org/xs?sub=sub.keaeye.icu&proxyip=ProxyIP.JP.CMLiussss.net",
"https://xscm250711.xn--eywz0c.dpdns.org/xs?sub=owo.o00o.ooo&proxyip=sjc.o00o.ooo",
"https://xysxwk250428.3344550.xyz/sub/full-normal/b2399c08-88a1-4b1a-b763-1334a31033ee?app=clash#%F0%9F%92%A6%20BPB%20Full%20Normal",
"https://xycc24424.xskj.dedyn.io/xysx?sub=sub.mot.cloudns.biz&proxyip=ProxyIP.SG.CMLiussss.net",
"https://6xvzd.no-mad-world.club/link/8qdpMT5zIZTrFZNF?clash=3",
"https://gldhm.no-mad-world.club/link/cIaAF9T1a7qgsa0E?clash=3",
        "https://raw.githubusercontent.com/PuddinCat/BestClash/refs/heads/main/proxies.yaml",
"https://raw.githubusercontent.com/snakem982/proxypool/main/source/clash-meta.yaml",
"https://raw.githubusercontent.com/Jsnzkpg/Jsnzkpg/Jsnzkpg/Jsnzkpg",
"https://mihomonode.github.io/uploads/{Y}/{m}/0-{Ymd}.yaml",
"https://mihomonode.github.io/uploads/{Y}/{m}/1-{Ymd}.yaml",
"https://mihomonode.github.io/uploads/{Y}/{m}/2-{Ymd}.yaml",
"https://mihomonode.github.io/uploads/{Y}/{m}/3-{Ymd}.yaml",
"https://mihomonode.github.io/uploads/{Y}/{m}/4-{Ymd}.yaml",
"https://clashmihomo.github.io/uploads/{Y}/{m}/0-{Ymd}.yaml",
"https://clashmihomo.github.io/uploads/{Y}/{m}/1-{Ymd}.yaml",
"https://clashmihomo.github.io/uploads/{Y}/{m}/2-{Ymd}.yaml",
"https://clashmihomo.github.io/uploads/{Y}/{m}/3-{Ymd}.yaml",
"https://clashmihomo.github.io/uploads/{Y}/{m}/4-{Ymd}.yaml",
"https://raw.githubusercontent.com/hello-world-1989/cn-news/refs/heads/main/clash.yaml",
"https://raw.githubusercontent.com/NZNL31/node/refs/heads/main/clash.yaml",
"https://raw.githubusercontent.com/danmaifu/mianfeijiedian/main/feed/v2ray-{Ymd}.txt",
"https://raw.githubusercontent.com/TheCrowCreature/v2rayExtractor/refs/heads/main/mix/sub.html",
"https://raw.githubusercontent.com/maimengmeng/mysub/refs/heads/main/valid_content_all.txt",
"https://raw.githubusercontent.com/Pawdroid/Free-servers/refs/heads/main/sub",
"https://raw.githubusercontent.com/ts-sf/fly/refs/heads/main/clash",
"https://raw.githubusercontent.com/free18/v2ray/refs/heads/main/c.yaml",
"https://raw.githubusercontent.com/peasoft/NoMoreWalls/refs/heads/master/list.yml",
"https://raw.githubusercontent.com/shaoyouvip/free/refs/heads/main/all.yaml",
"https://raw.githubusercontent.com/twj0/subseek/refs/heads/master/data/sub_platform.txt",
"https://raw.githubusercontent.com/twj0/subseek/refs/heads/master/data/sub_github.txt",
        "https://raw.githubusercontent.com/YFTree/ClashNodes/refs/heads/main/Clash/0.yaml",
"https://raw.githubusercontent.com/YFTree/ClashNodes/refs/heads/main/Clash/1.yaml",
"https://raw.githubusercontent.com/YFTree/ClashNodes/refs/heads/main/Clash/2.yaml",
"https://raw.githubusercontent.com/YFTree/ClashNodes/refs/heads/main/Clash/3.yaml",
"https://raw.githubusercontent.com/YFTree/ClashNodes/refs/heads/main/Clash/4.yaml",
"https://raw.githubusercontent.com/RYSF13/project-wallbreaker/refs/heads/main/subscribe/v2ray.txt",
        "https://raw.githubusercontent.com/Leon406/SubCrawler/master/sub/share/vless",
"https://raw.githubusercontent.com/Leon406/SubCrawler/master/sub/share/hysteria2",
"https://raw.githubusercontent.com/Leon406/SubCrawler/main/sub/share/a11",
"https://github.com/kismetpro/NodeSuber/raw/refs/heads/main/out/All_Configs_Sub.txt",
"https://fn10.sp1230.top/s/55f99a6fd538ab28c6719d300f9dfe2a",
"https://fn10.sp1230.top/s/e114c5e941a0fffe48a1895522afb65b",
"https://fn10.sp1230.top/s/af14226dc006218ae7868ca04c862591",
"https://fn10.sp1230.top/s/bd25fac90095fc0b0f8e44b8aeec2972",
"https://fn10.sp1230.top/s/cd54be82cc5dc0eeaf5a7b3c77b9ca52",
"https://fn10.sp1230.top/s/2f2eef189e3155b3efbd14e5483300ac",
"https://fn10.sp1230.top/s/13f6928748922f6fa2d4054cd18a888c",
"https://fn10.sp1230.top/s/62e7ad7084f42f553f2ffec9cbf248c9",
"https://fn10.sp1230.top/s/c5a3c81f4bcb8f70902b4f5e340a8072",
"https://fn10.sp1230.top/s/36262acf9a355d06aa15065448227305",
"https://fn10.sp1230.top/s/69b736aad25f01495e6b349b46a63555",
"https://fn10.sp1230.top/s/0f710d743a9cd97d7e4c5eeac15e5daa",
"https://fn10.sp1230.top/s/acb315d043bf63cd3fb84c405cc3f40c",
"https://fn10.sp1230.top/s/3a13b8e8a822ee7e84db2c709262e7b9",
"https://fn10.sp1230.top/s/82b7bcee736cb11f1abe63843413a16d",
"https://fn10.sp1230.top/s/90f1a9ecaebaf9c30df2c410b80b77b5",
"https://raw.githubusercontent.com/xyfqzy/free-nodes/main/nodes/clash.yaml",
"https://sscap4.github.io/uploads/{Y}/{m}/0-{Ymd}.yaml",
"https://sscap4.github.io/uploads/{Y}/{m}/1-{Ymd}.yaml",
"https://sscap4.github.io/uploads/{Y}/{m}/2-{Ymd}.yaml",
"https://sscap4.github.io/uploads/{Y}/{m}/3-{Ymd}.yaml",
"https://sscap4.github.io/uploads/{Y}/{m}/4-{Ymd}.yaml",
"https://raw.githubusercontent.com/shichongzheng/v2rayfree/main/v2rayfree",
"https://clashdaily.github.io/uploads/{Y}/{m}/4-{Ymd}.yaml",
"https://clashdaily.github.io/uploads/{Y}/{m}/4-{Ymd}.yaml",
"https://clashdaily.github.io/uploads/{Y}/{m}/4-{Ymd}.yaml",
"https://clashdaily.github.io/uploads/{Y}/{m}/4-{Ymd}.yaml",
"https://clashdaily.github.io/uploads/{Y}/{m}/4-{Ymd}.yaml",
"https://raw.githubusercontent.com/RYSF13/project-wallbreaker/main/subscribe/v2ray.txt",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/moneyfly1_merged_proxies_new.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/moneyfly1_merged_proxies_new.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/moneyfly1_merged_proxies_new.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/moneyfly1_merged_proxies_new.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/trojanvmess.pages.dev/cmcm_b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/trojanvmess.pages.dev/cmcm_b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/mahdibland/SSAggregator/sub/sub_merge_base64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahdibland/SSAggregator/sub/sub_merge_base64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahdibland/SSAggregator/sub/sub_merge_yaml.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/mahdibland/SSAggregator/sub/sub_merge_yaml.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Epodonios/v2ray-configs/All_Configs_base64_Sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/ndsphonemy/_my.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/trojanvmess.pages.dev/cmcm_b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/trojanvmess.pages.dev/cmcm_b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/mahdibland/SSAggregator/sub/sub_merge_yaml.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/mahdibland/SSAggregator/sub/sub_merge_base64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/encoded/10ium_mixed_iran.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/AzadNetCH/Clash/AzadNet.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/AzadNet/-t.me.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/MatinGhanbari/v2ray-configs/subscriptions/filtered/subs/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/MatinGhanbari/v2ray-configs/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MatinGhanbari/v2ray-configs/subscriptions/filtered/subs/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MatinGhanbari/v2ray-configs/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/v2ray-configs/subscriptions/filtered/subs/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/v2ray-configs/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/MatinGhanbari/v2ray-configs/vmess.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/MatinGhanbari/v2ray-configs/subscriptions/filtered/subs/vmess.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MatinGhanbari/v2ray-configs/vmess.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MatinGhanbari/v2ray-configs/subscriptions/filtered/subs/vmess.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/v2ray-configs/vmess.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/v2ray-configs/subscriptions/filtered/subs/vmess.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/AzadNetCH/Clash/AzadNet.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/AzadNet/-t.me.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/rb360full_Reza-2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/V2Hub3/merged_base64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/ndsphonemy/_default.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/10ium_V2ray_Config_All_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/miladtahanian_config.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Epodonios/v2ray-configs/All_Configs_base64_Sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/10ium_V2ray_Config_vless_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/ndsphonemy_my.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/ndsphonemy/_my.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/rb360full_Reza-Collection.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/MirrorMan/v2nodes.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Surfboardv2ray/TGParse/splitted/mixed.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Surfboardv2ray/TGParse/mixed.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/NiREvil_SSTime.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/NiREvil_SSTime.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/NiREvil_SSTime.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/NiREvil_SSTime.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/robin.victoriacross.ir.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/ResistalProxy_server.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/maimengmeng/_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/_vmess_iran.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Surfboardv2ray/TGParse/splitted/vless.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/free18.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/free18.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/free18.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/AzadNetCH/Clash/AzadNet.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/AzadNet/-t.me.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium_vmess_iran.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/66_42_50_118.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/66_42_50_118.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/66_42_50_118.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/ndsphonemy_default.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/ndsphonemy/_default.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Ruk1ng001.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/v2nodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/shatakvpn.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/MirrorMan/v2nodes.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/66_42_50_118.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/MirrorMan/hamedp-71_Trojan_hp.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/itsyebekhe/_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/ndsphonemy/_my.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/_trojan_iran.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/_ss_iran.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/ndsphonemy/_default.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Mosifree/-Reality.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium_ss_iran.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/ALIILAPRO/v2rayNG-Config/sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/hamedp-71/_Trojan_hp.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/maimengmeng.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/FreedomGuard/_Finder_configs.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/lagzian/_meta.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/robin.victoriacross.ir.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/itsyebekhe_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/rb360full_Reza-2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_ss_iran.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/10ium_ss_iran.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/rb360full_Reza-2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/10ium_ss_iran.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/10ium_ss_iran.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/MirrorMan/Danialsamadi_v2go_custom.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Epodonios/v2ray-configs/All_Configs_base64_Sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_vmess_iran.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_trojan_iran.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/10ium_trojan_iran.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/V2Hub3/merged_base64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/10ium_trojan_iran.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/maimengmeng.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Epodonios/v2ray-configs/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Epodonios/v2ray-configs/Splitted-By-Protocol/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Epodonios/v2ray-configs/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Epodonios/v2ray-configs/Splitted-By-Protocol/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Epodonios/v2ray-configs/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Epodonios/v2ray-configs/Splitted-By-Protocol/ss.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/maimengmeng_500.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/maimengmeng.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/liketolivefree.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/liketolivefree_sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/NiREvil_SSTime.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/NiREvil_SSTime.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/vpnclashfa-backup/MirrorMan/v2nodes.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/hiddify/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/base64/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Epodonios/v2ray-configs/All_Configs_base64_Sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/tristan-deng_MyNodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Danialsamadi_v2go_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Epodonios/v2ray-configs/trojan.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Epodonios/v2ray-configs/Splitted-By-Protocol/trojan.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Epodonios/v2ray-configs/trojan.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Epodonios/v2ray-configs/Splitted-By-Protocol/trojan.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/MirrorMan/Danialsamadi_v2go_custom.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/rb360full_Reza-Collection.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/rb360full_Reza-Collection.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/wudongdefeng_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/wudongdefeng_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Ruk1ng001.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/Danialsamadi_v2go_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/_V2Hub3_shadowsocks.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/wudongdefeng_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/wudongdefeng_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/vpnclashfa-backup/SubConfigShuffler/maimengmeng.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/Ruk1ng001.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/MirrorMan/hamedp-71_Sub_Checker_Creator_final.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/hamedp-71/_Sub_Checker_Creator_final.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/hamedp-71_hp.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium_V2Hub3_shadowsocks.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/10ium_telegram_configs_collector_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/maimengmeng_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/itsyebekhe_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/_V2Hub3_vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/wudongdefeng_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/wudongdefeng_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/maimengmeng/_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/hiddify/vless.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/base64/vless.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/source/base64/ar14n24b.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/robin.victoriacross.ir.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/ndsphonemy/_default.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/miladtahanian_config.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/miladtahanian_config.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/rb360full_Reza-2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/v2ray-configs/vless.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/v2ray-configs/subscriptions/filtered/subs/vless.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/ResistalProxy_server.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/ResistalProxy_server.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_V2Hub_shadowsocks.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_V2Hub3_shadowsocks.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/AzadNetCH/workers/AzadNet.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/AzadNetCH/Clash/AzadNet.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/V2Hub3/shadowsocks.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/V2Hub3/merged_base64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/V2Hub3/shadowsocks.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/V2Hub3/shadowsocks.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/10ium_vmess_iran.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/10ium_vmess_iran.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/10ium_vmess_iran.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium_V2Hub3_vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Mosifree_SS.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Mosifree/_SS.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/ndsphonemy_default.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Ruk1ng001.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/tristan-deng_MyNodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_V2RayAggregator-Eternity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_Aggregator.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/mahdibland/ShadowsocksAggregator/Eternity.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/_V2RayAggregator-Eternity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/V2RayAggregator/Eternity.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahdibland/ShadowsocksAggregator/Eternity.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/V2RayAggregator/Eternity.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/mahdibland/ShadowsocksAggregator/Eternity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/MirrorMan/MatinGhanbari_v2ray-configs-super-sub.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahdibland/ShadowsocksAggregator/Eternity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/v2ray-configs/super-sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/v2ray-configs/subscriptions/v2ray/super-sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/_v2ray-configs-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/MatinGhanbari/-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/hamedp-71_Trojan_hp.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Leon406/SubCrawler/sub/share/a11.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Leon406/SubCrawler/sub/share/a11.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/Surfboardv2ray/_IR.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Surfboardv2ray/TGParse/splitted/mixed.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Surfboardv2ray/TGParse/mixed.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/lagzian_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/lagzian/IranConfigCollector/Base64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/mfuu_v2ray.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mfuu_v2ray.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/hamedp-71_hp.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/lagzian/_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/lagzian/_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/lagzian/IranConfigCollector/Base64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/MirrorMan/hamedp-71_Sub_Checker_Creator_final.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/hamedp-71/_Sub_Checker_Creator_final.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/hamedp-71_hp.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/FreedomGuard/_Finder_configs.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/v2nodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/shatakvpn.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/10ium_V2ray_Config_All_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/ResistalProxy_server.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/MirrorMan/MatinGhanbari_v2ray-configs-super-sub.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MatinGhanbari/v2ray-configs/super-sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MatinGhanbari/v2ray-configs/subscriptions/v2ray/super-sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MatinGhanbari/_v2ray-configs-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MatinGhanbari/-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/rb360full_Reza-Collection.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/MatinGhanbari_v2ray-configs-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/maimengmeng/_500.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/maimengmeng/000.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/FreedomGuard_Finder_configs.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/vpnclashfa-backup/MirrorMan/hamedp-71_Sub_Checker_Creator_final.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/hamedp-71/_Sub_Checker_Creator_final.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/hamedp-71_hp.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/lagzian_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/lagzian/_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/V2RayAggregator/Eternity.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/mahdibland/ShadowsocksAggregator/Eternity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/10ium_V2ray_Config_trojan_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/10ium_V2ray_Config_trojan_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/Mosifree_Vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Mosifree_Vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Mosifree/_Vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/10ium_CollectorLite_Config_mixed_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/MirrorMan/hamedp-71_Trojan_hp.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/lagzian_trinity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/lagzian_trinity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/lagzian/_trinity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/lagzian/_trinity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/lagzian/_trinity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/Surfboardv2ray/_mahsa.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/hamedp-71_hp.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/hamedp-71_Sub_Checker_Creator_final.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/FreedomGuard/_Finder_configs.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/roosterkid/openproxylist/V2RAY_BASE64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/FreedomGuard/_Finder_configs.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/roosterkid/_V2RAY_RAW.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/V2Hub3/reality.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/shabane/_merged.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/tristan-deng_MyNodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/lagzian/IranConfigCollector/Base64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/10ium_Collector_mixed_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/hamedp-71_hp.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/hamedp-71_Sub_Checker_Creator_final.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/ebrasha/_lite.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/itsyebekhe_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/shabane_merged.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/shabane/_merged.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/_V2Hub3_trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/rb360full_Reza-2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_V2Hub_trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_V2Hub3_trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/rb360full_Reza-2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/V2Hub3/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/10ium_V2Hub_merged_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/V2Hub3/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/v2nodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/shatakvpn.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/mfuu_v2ray.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/Mosifree_SS.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/vpnclashfa-backup/MirrorMan/Danialsamadi_v2go_custom.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/ALIILAPRO/v2rayNG-Config/sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium_V2RayAggregator-Eternity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Leon406/SubCrawler/sub/share/a11.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/miladtahanian_config.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Danialsamadi_v2go_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/ResistalProxy_server.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Danialsamadi_v2go_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/rb360full_Reza-Collection.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/hamedp-71_openproxylist.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/tristan-deng_MyNodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/roosterkid_V2RAY_BASE64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/roosterkid.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/maimengmeng_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/shabane_ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/roosterkid_V2RAY_RAW.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/roosterkid_v2ray.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/roosterkid/openproxylist/V2RAY_BASE64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/shabane/_merged.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/shabane/_ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/shabane/_ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/roosterkid/_V2RAY_RAW.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/shabane/_ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/Surfboardv2ray/_US.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/v2nodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/shatakvpn.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/maimengmeng/_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/yebekhe_vpn-fail.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/v2ray_hidify.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Surfboardv2ray/TGParse/splitted/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Surfboardv2ray/TGParse/splitted/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/yebekhe_vpn-fail.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/vpnclashfa-backup/MirrorMan/Danialsamadi_v2go_custom.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/ebrasha_lite.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/yebekhe_vpn-fail.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/v2ray_hidify.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/yebekhe_vpn-fail.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/maimengmeng/_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/maimengmeng_custom.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/ebrasha/_lite.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/_hin-vpn-mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Barabama_clashmeta.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/robin.victoriacross.ir.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/MatinGhanbari_v2ray-configs-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/MatinGhanbari/v2ray-configs/super-sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/MatinGhanbari/v2ray-configs/subscriptions/v2ray/super-sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/MatinGhanbari/_v2ray-configs-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/MatinGhanbari/-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/MahsaNetConfigTopic.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium_V2RayAggregator-Eternity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/MahsaNetConfigTopic.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/AzadNet/-hysteria.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/tristan-deng_MyNodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_hin-vpn-mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_HiN-VPN.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/proxy_kafee.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/rb360full_Reza-Collection.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/vpnclashfa-backup/MirrorMan/MatinGhanbari_v2ray-configs-super-sub.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/ALIILAPRO/v2rayNG-Config/sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/HiN-VPN/subscription/hiddify/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/HiN-VPN/subscription/base64/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/proxy_kafee.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/vpnclashfa-backup/MirrorMan/hamedp-71_Trojan_hp.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/MatinGhanbari/v2ray-configs/super-sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/MatinGhanbari/v2ray-configs/subscriptions/v2ray/super-sub.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/MatinGhanbari/_v2ray-configs-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/MatinGhanbari/-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/FreedomGuard_Finder_configs.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/vpnclashfa-backup/MirrorMan/MatinGhanbari_v2ray-configs-super-sub.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/lagzian_meta.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/roosterkid_v2ray.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/itsyebekhe_PSG_mix_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/azadnet05.pages.dev/sub/4d794980-54c0-4fcb-8def-c2beaecadbad.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/freedomnet25500_free.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/ebrasha/_lite.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/lagzian/_meta.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Leon406/SubCrawler/sub/share/a11.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/MatinGhanbari_v2ray-configs-super-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/roosterkid/_V2RAY_RAW.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/freedomnet25500_free.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Surfboardv2ray/TGParse/splitted/ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Surfboardv2ray/TGParse/splitted/mixed.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/Surfboardv2ray/TGParse/mixed.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/roosterkid/_V2RAY_BASE64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/roosterkid.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/roosterkid-V2RAY_BASE64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Surfboardv2ray/TGParse/splitted/ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Surfboardv2ray/TGParse/splitted/ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/HiN-VPN/subscription/source/base64/ar14n24b.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/rayan_proxy.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Rayan-Config_H-I.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Ruk1ng001.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/mahdibland/ShadowsocksAggregator/EternityAir.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahdibland/ShadowsocksAggregator/EternityAir.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/maimengmeng_500.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/maimengmeng.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/ebrasha_lite.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/ebrasha/_lite.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/hfarahani_pr.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_fetcher.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/vpnclashfa-backup/MirrorMan/v2nodes.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/hamedp-71_openproxylist.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/roosterkid/_V2RAY_RAW.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/SnapdragonLee_clash_config_extra_US.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/roosterkid/openproxylist/V2RAY_BASE64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/SnapdragonLee_clash_config_extra_US.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/amirparsaxs_xsfilternet.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/hamedp-71_openproxylist.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/SnapdragonLee_clash_config_extra_US.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/ResistalProxy_server.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/vpnclashfa-backup/MirrorMan/hamedp-71_Trojan_hp.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/amirparsaxs_xsfilternet.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/rb360full_Reza-Collection.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/rayan/_proxy.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium_hin-vpn-mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/amirparsaxs_xsfilternet.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_V2Hub_vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/10ium_V2Hub3_vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/ResistalProxy_server.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/ResistalProxy_server.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/v2ray_hidify.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/V2Hub3/vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/V2Hub3/merged_base64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Rayan/-Config_H-I.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/V2Hub3/vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/lagzian/_reality.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/V2Hub3/vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/hiddify/ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/base64/ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/freedomnet25500_free.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/maimengmeng.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/free18.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/miladtahanian_config.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/ndsphonemy_lt-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/vpnclashfa-backup/SubConfigShuffler/roosterkid_v2ray.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/maimengmeng_500.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/rb360full_Reza-2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/itsyebekhe/PSG/subscriptions/clash/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/MirrorMan/gheychiamoozesh.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/itsyebekhe/PSG/subscriptions/clash/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/chromego-sub.netlify.app/sub/merged_proxies_new.yaml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/ndsphonemy_lt-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/ndsphonemy/_lt-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/HiN-VPN/subscription/hiddify/ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/HiN-VPN/subscription/hiddify/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/HiN-VPN/subscription/base64/ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/HiN-VPN/subscription/base64/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/ndsphonemy/_lt-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/ndsphonemy/_lt-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/HiN-VPN/subscription/hiddify/ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/HiN-VPN/subscription/base64/ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/ndsphonemy/_lt-sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/Rayan/-Config_WG.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/Surfboardv2ray_mahsa.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/lagzian/IranConfigCollector/Base64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/10ium_CollectorLite_Config_mixed_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MahsaNetConfigTopic.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/MahsaNet/ConfigTopic.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/FreedomGuard_Finder_configs.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/hiddify/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/base64/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/Surfboardv2ray/_mahsa.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/lagzian_vmess_tvc.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/HiN-VPN/subscription/source/base64/ar14n24b.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/azadnet05.pages.dev/sub/4d794980-54c0-4fcb-8def-c2beaecadbad.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/V2ray-Config/Splitted-By-Protocol/hysteria2.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/roosterkid_V2RAY_BASE64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/roosterkid.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/liketolivefree_sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/roosterkid_V2RAY_RAW.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/lagzian_vmess_tvc.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/lagzian_meta.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/lagzian/_vmess_tvc.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/lagzian/_meta.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/proxy_kafee.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/10ium_telegram_configs_collector_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/lagzian/_vmess_tvc.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/HiN-VPN/subscription/hiddify/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/HiN-VPN/subscription/base64/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/lagzian/_vmess_tvc.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/darkvpn.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/vpnclashfa-backup/SubConfigShuffler/10ium/V2ray/Config/All/cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/gheychiamoozesh.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/ndsphonemy/_my.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Surfboardv2ray/TGParse/splitted/mixed.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Surfboardv2ray/TGParse/mixed.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/Surfboardv2ray_bugfix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/mfuu_v2ray.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/shabane_trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/hamedp-71_openproxylist.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/MirrorMan/gheychiamoozesh.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/10ium_V2ray_HiNVPN_mix_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/peasoft_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/proxy_kafee.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Surfboardv2ray_bugfix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/66_42_50_118.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/Surfboardv2ray/_bugfix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/peasoft_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Surfboardv2ray/_bugfix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/Surfboardv2ray/_bugfix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/rayan_proxy.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahsanet/_mtn_sub_3.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahsanet/_mci_sub_3.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahsa-sub_3.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/Surfboardv2ray/_bugfix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/wudongdefeng_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/SnapdragonLee_clash_config_extra_US.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/wudongdefeng_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/shabane/_ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/shabane/_trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/shabane/_trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/vpnclashfa-backup/SubConfigShuffler/10ium/V2ray/Config/vmess/cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/peasoft_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/v2ray_hidify.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/10ium_V2ray_Config_vmess_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/10ium_V2ray_Config_vmess_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/source/base64/soskeynet.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/ebrasha_lite.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/lagzian_vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/MahsaNetConfigTopic.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/10ium_V2Hub_merged_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahsanet/_mci_sub_4.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahsa-sub_4.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/roosterkid/openproxylist/V2RAY_BASE64.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/shabane_ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/shabane_merged.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/itsyebekhe/PSG/lite/subscriptions/clash/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/peasoft_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/SubConfigShuffler/maimengmeng_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahsanet/_mtn_sub_4.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/itsyebekhe/PSG/lite/subscriptions/clash/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/firefoxmmx2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Barabama_v2rayshare.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Barabama_nodefree.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/lagzian_vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/lagzian_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/lagzian/_vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/lagzian/_mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/SnapdragonLee_clash_config_extra_US.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/rayan_proxy.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/roosterkid_V2RAY_BASE64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/roosterkid.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/lagzian/_vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/MahsaNetConfigTopic.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/lagzian/_vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/theGreatPeter_nodes.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/mahdibland/ShadowsocksAggregator/Eternity.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/mahdibland/ShadowsocksAggregator/Eternity.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/itsyebekhe/PSG/subscriptions/clash/vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/itsyebekhe/PSG/subscriptions/clash/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/roosterkid/_V2RAY_RAW.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/V2RayAggregator/Eternity.yml.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/darkvpn_xray_final.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/rayan_proxy.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/10ium_Collector_mixed_cloudflare.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/liketolivefree_sub.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/itsyebekhe/PSG/subscriptions/clash/vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/itsyebekhe/PSG/subscriptions/clash/vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/vpnclashfa-backup/SubConfigShuffler/rayan_proxy.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium_hin-vpn-mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/Surfboardv2ray/_mahsa.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/vpnclashfa-backup/SubConfigShuffler/roosterkid_v2ray.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/freedomnet25500_free.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/hamedp-71_openproxylist.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/muma16fx_netlify_app.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/muma16fx_netlify_app.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/vpnclashfa-backup/MirrorMan/the3rf_com_sub_php.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/muma16fx_netlify_app.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/muma16fx_netlify_app.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/shabane/_trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/itsyebekhe/PSG/subscriptions/clash/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Surfboardv2ray/_mahsa.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/itsyebekhe/PSG/subscriptions/clash/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/source/base64/movie10_oficial.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/itsyebekhe/PSG/subscriptions/clash/vmess_domain.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/moeinkey_ssh.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/darkvpn.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/roosterkid_V2RAY_RAW.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/ebrasha_lite.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/vpnclashfa-backup/SubConfigShuffler/MahsaNetConfigTopic.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/itsyebekhe/PSG/subscriptions/clash/vmess_domain.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/money.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahsanet/_mci_sub_1.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/itsyebekhe/PSG/subscriptions/clash/vmess_domain.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/hfarahani_pr.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/freedomnet25500_ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Surfboardv2ray/_ipv6.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/moneyfly1_merged_proxies.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Surfboardv2ray_ipv6.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/Barabama_ndnode.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/vpnclashfa-backup/SubConfigShuffler/MahsaNetConfigTopic.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/ndsphonemy_my.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/moeinkey_ssh.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/darkvpn.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/moeinkey_ssh.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/ndsphonemy/_hys-tuic.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/base64-encoder/moeinkey_ssh.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/mahsanet_mci_sub_1.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/freedomnet25500_ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/hfarahani_pr.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/freedomnet25500_ss.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/10ium/base64-encoder/hfarahani_pr.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/peasoft_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/firefoxmmx2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/hfarahani_pr.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/hfarahani_pr.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/itsyebekhe/PSG/subscriptions/clash/trojan_ipv4.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/Barabama_ndnode.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/itsyebekhe/PSG/subscriptions/clash/trojan_ipv4.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/Barabama_ndnode.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/itsyebekhe/PSG/lite/subscriptions/clash/vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/itsyebekhe/PSG/lite/subscriptions/clash/mix.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/mahsanet_mci_sub_2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/vpnclashfa-backup/MirrorMan/gheychiamoozesh.b64.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/mahsanet/_mci_sub_1.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/itsyebekhe/PSG/lite/subscriptions/clash/vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/itsyebekhe/PSG/lite/subscriptions/clash/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/itsyebekhe/PSG/lite/subscriptions/clash/vmess.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/itsyebekhe/PSG/lite/subscriptions/clash/trojan.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/vpnclashfa-backup/SubConfigShuffler/maimengmeng.txt.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/Surfboardv2ray_mahsa.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/10ium/base64-encoder/peasoft_list_raw.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/MahsaNetConfigTopic.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/10ium/base64-encoder/FreedomGuard/_Finder_configs.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/firefoxmmx2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/mahsanet/_mci_sub_2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/source/base64/configfa.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/source/base64/capoit.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/v2ray/itsyebekhe_IR.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/surfboard/Barabama_clashmeta.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/voken100g_recent.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/ss/voken100g/_recent.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/voken100g/_recent.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/mahsanet/_mci_sub_2.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/mixed/darkvpn/app_CloudflarePlus_proxy.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/voken100g/_recent.yaml",
"https://raw.githubusercontent.com/asgharkapk/Sub-Config-Extractor/refs/heads/main/output_configs/clash/10ium/HiN-VPN/subscription/source/base64/speeds_vpn1.yaml"
    ]
    work(links, check=True, only_check=False, allowed_types=["ss", "hysteria2", "hy2", "vless", "vmess", "trojan", "tuic", "wireguard", "hysteria", "snell", "naive"])
