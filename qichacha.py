import json
import requests
from pyquery import PyQuery as pq
from config import *
from multiprocessing import Pool
import functools
import pymongo
from multiprocessing import Manager
import re
import time,random
next_url = 'https://www.qichacha.com/gongsi_industry.shtml?industryCode=A&p='
start_url = "https://www.qichacha.com/"
proxy = None
client = pymongo.MongoClient('localhost')
db = client['qichacha']
def get_proxy():
    response = requests.get('http://localhost:5555/random')
    if response.status_code == 200:
        return response.text
    return None

def get_index_url(url,headers):
    print("爬取:",url)
    global proxy
    try:
        if proxy:
            print("使用IP:", proxy)
            proxies = {
                "http": "http://" + proxy,
                'https': "http://" + proxy,
            }
            response = requests.get(url,headers=headers,proxies=proxies,timeout = 5)
        else:
            response = requests.get(url,headers=headers,timeout = 5)
        doc = re.compile("<title>([\S\s]*?)</title>")
        pqs = re.findall(doc,response.text)
        if response.status_code == 200 and pqs:
            return response.text
        else:
            print("重新请求")
            time.sleep(random.random())
            proxy = get_proxy()
            get_index_url(url,headers)
    except TimeoutError as e:
        print("重新请求",e)
        time.sleep(random.random())
        #proxy = get_proxy()
        get_index_url(url,headers)
def get_url(item):
    resp = re.compile('<span class="name">(.*?)</span>.*?</i>(.*?)<i class.*?></i>(.*?)<i class=.*?></i>(.*?)<i class=.*?></i>(.*?)</small>',re.S)
    items = re.findall(resp, item)
    for item in items:
        yield {
            '公司': item[0].strip(),
            '法人': item[1].strip(),
            '成立时间': item[2].strip(),
            '注册资金': item[3].strip(),
            '所属行业': item[4].strip()
        }
def save_messge(lock,messgea):
    with open("qichacha.txt",'a',encoding='utf-8') as f:
        lock.acquire()
        f.write(json.dumps(messgea,ensure_ascii=False)+'\n')
        lock.release()
def save_to_mongo(messgea):
    if db['qicha'].update({'公司':messgea['公司']},{'$set':messgea},True):
        print("数据存储到数据库成功",messgea['公司'])
    else:
        print("数据存储失败",messgea['公司'])
def main(lock,EnNum,offset):
    user_agent = user_angents()
    print("User_agent:",user_agent)
    headers = {
        #"Cookie":"UM_distinctid=165a402a57c34d-0aa76ebecdd88-43480420-1fa400-165a402a57d36f; _uab_collina=153605496551456619348542; zg_did=%7B%22did%22%3A%20%22165a402b893d37-057f0c42190a8b-43480420-1fa400-165a402b89457a%22%7D; acw_tc=74d3b7c915360550473342755e72b572cb3956a3c06fadddc7b5b1df18; PHPSESSID=cgjdph09lfmcebnq8i25b2vc57; Hm_lvt_3456bee468c83cc63fb5147f119f1075=1536054963,1536109060; hasShow=1; _umdata=6AF5B463492A874D7EC59D50A091C8C297508F0D07F6DD80623744F5D81F40EBCD050A6A39996A2CCD43AD3E795C914CE157BFA79BD89FBF8C6E9DD970C66497; CNZZDATA1254842228=1684257309-1536053867-https%253A%252F%252Fwww.baidu.com%252F%7C1536128106; zg_de1d1a35bfa24ce29bbf2c7eb17e6c4f=%7B%22sid%22%3A%201536126941850%2C%22updated%22%3A%201536131823383%2C%22info%22%3A%201536054966424%2C%22superProperty%22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22www.qichacha.com%22%7D; Hm_lpvt_3456bee468c83cc63fb5147f119f1075=1536131823",
        "User-Agent":user_agent,
        'Accept':'application / json, text / javascript, * / *; q = 0.01',
        'X - Requested - With':'XMLHttpRequest',
    }
    url = "https://www.qichacha.com/gongsi_industry.shtml?industryCode="+EnNum+"&p="+str(offset)
    item = get_index_url(url,headers)
    messges = get_url(item)
    for messgea in messges:
        if messgea and messgea.values():
            print("保存数据...")
            save_messge(lock,messgea)
            save_to_mongo(messgea)
            print("保存成功")

if __name__ == "__main__":
    manager = Manager()
    pool = Pool(3)
    Ens = ['A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T']
    lock = manager.Lock()
    cmain = functools.partial(main,lock)
    for EnNum in Ens:
        for offset in range(1, 501):
            time.sleep(random.random())
            resul = pool.apply_async(cmain,(EnNum,offset))
            resul.wait()
    print("爬取完成")
    pool.close()
    pool.join()

