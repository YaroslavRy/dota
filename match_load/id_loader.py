import numpy as np
from bs4 import BeautifulSoup
import urllib3
from threading import Thread, current_thread
from multiprocessing.dummy import Pool as TPool
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import json
import time


class MatchLoader():
    def __init__(self):
        self.db = MongoClient()['dota']
        self.pro_matches = self.db.pro_matches_full
        self.pro_matches_id = self.db.pro_matches_id
        self.user_agent = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64;en; rv:5.0) \
                      Gecko/20110619 Firefox/5.0'}

    def get_working_proxy(self):
        user_agent = self.user_agent
        proxy_list_site = 'https://free-proxy-list.net/'
        pm = urllib3.PoolManager(1,
                                 headers=user_agent,
                                 cert_reqs='CERT_REQUIRED')
        prx_req = pm.request('GET', proxy_list_site)
        prx_soup = BeautifulSoup(prx_req.data, 'html.parser')
        proxies_list = []
        for tr in prx_soup.find_all('tr'):
            tmp = []
            for td in tr.find_all('td')[:2]:
                tmp.append(td.text)
            if len(tmp) < 2:
                continue
            proxies_list.append([str(tmp[0]) + ':' + tmp[1]])

        proxies_list = proxies_list[:100]
        np.random.shuffle(proxies_list)
        for i, prx in enumerate(proxies_list):
            try:
                print('Getting working proxy...')
                cur_prx_address = 'https://' + str(prx[0]) + '/'
                prx_http = urllib3.ProxyManager(cur_prx_address,
                                                maxsize=1,
                                                headers=user_agent,
                                                cert_reqs='CERT_REQUIRED')
                r = prx_http.request('GET', 'https://stackoverflow.com/', timeout=0.7)
                if r.status == 200:
                    print('Proxy found.')
                    return cur_prx_address
                time.sleep(0.1)
            except Exception as err:
                # print(err)
                continue

    def api_pro_matches_id(self, prx_address, start_id):
        url = 'https://api.opendota.com/api/proMatches?less_than_match_id='
        user_agent = self.user_agent
        cur_url = url + str(start_id)
        prx_m = urllib3.ProxyManager(prx_address, headers=user_agent, cert_reqs='CERT_REQUIRED')
        r = prx_m.request('GET', cur_url, timeout=1)
        matches = json.loads(r.data)
        return matches

    def update_ids(self, last_id):
        cur_prx = self.get_working_proxy()
        for i in range(1000):
            total_matches = self.pro_matches_id.count()
            print(i, total_matches, last_id)
            try:
                cur_matches = self.api_pro_matches_id(cur_prx, start_id=last_id)
                if 'error' in cur_matches and cur_matches['error'] == 'rate limit exceeded':
                    cur_prx = self.get_working_proxy()
                    continue
                else:
                    ids = [x['match_id'] for x in cur_matches if 'match_id' in x]
                    last_id = np.min(ids)
                    self.pro_matches_id.insert(cur_matches)
            except DuplicateKeyError as de:
                continue
            except Exception as err:
                print(err.__context__)
                print(err.args)
                if 'timeout' in str(err.__context__):
                    cur_prx = self.get_working_proxy()
                continue

    def api_get_match(self, prx_address, match_id):
        url = 'https://api.opendota.com/api/matches/'
        user_agent = self.user_agent
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
            except Exception as e:
                if 'timeout' in str(e.__context__):
                    prx_address = self.get_working_proxy()
                print(e, cur_match_id)
                continue
            time.sleep(0)

    def load_new_matches(self, n_batches):
        # get new ids
        new_ids = self.get_new_ids()
        print(len(new_ids))
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
# loader.update_ids(last_id=99999999999)
loader.load_new_matches(16)
