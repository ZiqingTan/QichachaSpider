import json

import requests
from pyquery import PyQuery as pq
import re
from config import *
from multiprocessing import Pool
import functools
from multiprocessing import Manager
import time
next_url = 'https://www.qichacha.com/gongsi_industry.shtml?industryCode=A&p='
start_url = "https://www.qichacha.com/"
messgesa = []
def get_proxy():
    response = requests.get('http://localhost:5555/random')
    if response.status_code == 200:
        return response.text
    return None
def get_index_url(url,headers,proxy):
    print("爬取:",url)

    proxies = {
        "http":"http://"+proxy,
    }
    print("使用IP:", proxies)
    session = requests.Session()
    response = session.get(url,headers=headers,proxies=proxies)
    if response.status_code == 200:
        return response.text
    return None
def get_url(item):
    doc = pq(item)
    companys = doc(".container .row .col-md-12 .clear .name").items()
    messges = doc(".container .row .col-md-12 .clear .m-t-xs").items()
    for company in companys:
        company = company.text()
        for messge in messges:
            messge = messge.text()
            messge_split = messge.split()
            yield {'公司':company,
                    '法人':messge_split[0],
                    '成立时间':messge_split[1],
                    '注册资本':messge_split[2],
                    '所属行业':messge_split[3],
                    }
def save_messge(lock,messge):
    with open("qichacha.txt",'a') as f:
        lock.acquire()
        f.write(json.dumps(messge,ensure_ascii=False)+'\n')
        lock.release()
def main(lock,offset):
    user_agent = user_angents()
    print("User_agent:",user_agent)
    headers = {
        #"Cookie":"UM_distinctid=165a402a57c34d-0aa76ebecdd88-43480420-1fa400-165a402a57d36f; _uab_collina=153605496551456619348542; zg_did=%7B%22did%22%3A%20%22165a402b893d37-057f0c42190a8b-43480420-1fa400-165a402b89457a%22%7D; acw_tc=74d3b7c915360550473342755e72b572cb3956a3c06fadddc7b5b1df18; PHPSESSID=cgjdph09lfmcebnq8i25b2vc57; Hm_lvt_3456bee468c83cc63fb5147f119f1075=1536054963,1536109060; hasShow=1; _umdata=6AF5B463492A874D7EC59D50A091C8C297508F0D07F6DD80623744F5D81F40EBCD050A6A39996A2CCD43AD3E795C914CE157BFA79BD89FBF8C6E9DD970C66497; CNZZDATA1254842228=1684257309-1536053867-https%253A%252F%252Fwww.baidu.com%252F%7C1536128106; zg_de1d1a35bfa24ce29bbf2c7eb17e6c4f=%7B%22sid%22%3A%201536126941850%2C%22updated%22%3A%201536131823383%2C%22info%22%3A%201536054966424%2C%22superProperty%22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22www.qichacha.com%22%7D; Hm_lpvt_3456bee468c83cc63fb5147f119f1075=1536131823",
        "User-Agent":user_agent,
    }
    url = "https://www.qichacha.com/gongsi_industry.shtml?industryCode=A&p="+str(offset)
    proxy = get_proxy()
    item = get_index_url(url,headers,proxy)
    messges = get_url(item)
    for messgea in messges:
        if messgea:
            if messgea['公司'] not in messgesa:
                messgesa.append(messgea)
                print("保存数据...")
                save_messge(lock,messgea)
                print("保存成功")
if __name__ == "__main__":
    manager = Manager()
    lock = manager.Lock()
    cmain = functools.partial(main,lock)
    pool = Pool(3)
    for offset in range(1, 501):
        resul = pool.apply_async(cmain,(offset,))
    resul.wait()
    print("爬取完成")
    pool.close()
    pool.join()

