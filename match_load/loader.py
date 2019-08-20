import json
import time
from json import JSONDecodeError
from threading import Thread, current_thread

import numpy as np
import urllib3
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


class MatchLoader():
    def __init__(self):
        self.db = MongoClient()['dota']
        self.pro_matches = self.db.pro_matches_full
        self.pro_matches_id = self.db.pro_matches_id
        self.user_agent = [{'User-Agent': 'Mozilla/6.0 (Windows NT 6.1; Win32; x32;en; rv:6.0) '
                                          'Gecko/20110621 Firefox/6.0'},
                           {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                          '(KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'},
                           {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                                          '(KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'},
                           {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 '
                                          '(KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36'},
                           ]
        self.STEAM_API_KEY = '28E3FBFDBF38C8BB4F89F08C6CEB2275'

    def get_working_proxy(self, top_n=15):
        user_agent = np.random.choice(self.user_agent, size=1)[0]
        proxy_list_site = 'https://free-proxy-list.net/'
        proxy_list_site2 = 'https://hidemy.name/en/proxy-list/'
        pm = urllib3.PoolManager(1, headers=user_agent, cert_reqs='CERT_REQUIRED')
        prx_req = pm.request('GET', proxy_list_site)
        prx_req2 = pm.request('GET', proxy_list_site2)
        prx_soup = BeautifulSoup(prx_req.data, 'html.parser')
        prx_soup2 = BeautifulSoup(prx_req2.data, 'html.parser')
        proxies_list = []

        for tr in prx_soup.find_all('tr')[:top_n]:
            tmp = []
            for td in tr.find_all('td')[:2]:
                tmp.append(td.text)
            if len(tmp) < 2:
                continue
            cur_prx = str(tmp[0]) + ':' + tmp[1]
            proxies_list.append(cur_prx)

        for i, elem in enumerate(prx_soup2.find_all(class_='d')[:top_n]):
            addr, port = [x.text for x in elem.find_all('td')[:2]]
            cur_full_prx = addr + ':' + port
            proxies_list.append(cur_full_prx)

        proxies_list = proxies_list[:top_n]
        np.random.shuffle(proxies_list)
        print('Getting working proxy...')
        for i, prx in enumerate(proxies_list):
            try:
                print(i)
                cur_prx_address = 'https://' + str(prx) + '/'
                prx_http = urllib3.ProxyManager(cur_prx_address,
                                                maxsize=1,
                                                headers=user_agent,
                                                cert_reqs='CERT_REQUIRED')
                r = prx_http.request('GET', 'https://stackoverflow.com/', timeout=0.7, )
                if r.status == 200:
                    print('Proxy found.')
                    return cur_prx_address
                time.sleep(0.3)
            except Exception as err:
                # print(err)
                continue

    def api_pro_matches_id(self, prx_address, start_id):
        url = 'https://api.opendota.com/api/proMatches?less_than_match_id='
        user_agent = np.random.choice(self.user_agent, size=1)[0]
        cur_url = url + str(start_id)
        prx_m = urllib3.ProxyManager(prx_address, headers=user_agent, cert_reqs='CERT_REQUIRED')
        r = prx_m.request('GET', cur_url, timeout=1)
        matches = json.loads(r.data)
        return matches

    def update_ids(self, last_id, n_first_pages):
        cur_prx = None
        while cur_prx is None:
            cur_prx = self.get_working_proxy()
        total_matches = 0
        for i in range(n_first_pages):
            if i % 50 == 0:
                total_matches = self.pro_matches_id.count()
            print(i, total_matches, last_id)
            try:
                cur_matches = self.api_pro_matches_id(cur_prx, start_id=last_id)
                last_id = int(np.min([x['match_id'] for x in cur_matches if 'match_id' in x]))
                if 'error' in cur_matches:
                    if cur_matches['error'] == 'rate limit exceeded':
                        while cur_prx is None:
                            cur_prx = self.get_working_proxy()
                        continue
                    continue
                if len(cur_matches) > 0:
                    for match in cur_matches:
                        try:
                            self.pro_matches_id.insert(match)
                            last_id = match['match_id']
                        except DuplicateKeyError as de:
                            continue
            except JSONDecodeError as jsnerr:
                cur_prx = None
                while cur_prx is None:
                    cur_prx = self.get_working_proxy()
                continue
            except Exception as err:
                print('update_ids: \n', err, sep='')
                cur_prx = None
                while cur_prx is None:
                    cur_prx = self.get_working_proxy()
            time.sleep(0.1)

    def api_get_match(self, prx_address, match_id):
        url = 'https://api.opendota.com/api/matches/'
        user_agent = np.random.choice(self.user_agent, size=1)[0]
        cur_url = url + str(match_id)
        prx_m = urllib3.ProxyManager(prx_address, headers=user_agent, cert_reqs='CERT_REQUIRED')
        r = prx_m.request('GET', cur_url, timeout=2)
        matches = json.loads(r.data)
        return matches

    def get_new_ids(self):
        ids_all = [x['match_id'] for x in self.pro_matches_id.find({}, {'match_id'})]
        ids_stored = [x['match_id'] for x in self.pro_matches.find({}, {'match_id'})]
        need_to_load_ids_bool = np.isin(ids_all, ids_stored, invert=True)
        need_to_load_ids = np.array(ids_all)[need_to_load_ids_bool]
        return need_to_load_ids

    def load_insert(self, matches_ids):
        prx_address = self.get_working_proxy()
        for i, cur_match_id in enumerate(matches_ids):
            print(current_thread().name, i, cur_match_id) if i % 10 == 0 else None
            try:
                cur_match = self.api_get_match(prx_address, cur_match_id)
                if 'error' in cur_match.keys():
                    print(cur_match)
                    prx_address = self.get_working_proxy()
                    continue
                if 'match_id' not in cur_match.keys():
                    print('havent match_id')
                    continue
                self.pro_matches.insert_one(cur_match)
            except DuplicateKeyError as de:
                print('DuplicateKeyError')
                continue
            except Exception as e:
                prx_address = self.get_working_proxy()
                print('load_insert \n', e, cur_match_id)
            time.sleep(0.1)

    def load_new_matches(self, n_batches):
        new_ids = self.get_new_ids()
        print(len(new_ids), 'New ids found')
        if len(new_ids) > 0:
            while len(new_ids) % n_batches != 0:
                new_ids = np.append(new_ids, 0)

            batches = np.split(new_ids, n_batches)
            threads = []
            print('threads appending')
            for batch in batches:
                threads.append(Thread(target=self.load_insert, args=(batch,)))

            print('threads starting')
            for t in threads:
                t.start()


loader = MatchLoader()
loader.update_ids(last_id=99999999999, n_first_pages=10)
loader.load_new_matches(n_batches=8)
