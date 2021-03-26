import requests 
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import urllib.request as ur
import json
import pandas as pd
import numpy as np
from time import sleep,time
import time
import os
from os import path
import math
import pyodbc
options = Options()
options.add_argument("--disable-notifications")
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.92 Safari/537.36',
}

########################################
##################美股##################
########################################

def us_crawler(us_url):
    resp = requests.get(us_url)
    js = resp.json()
    us_df = pd.DataFrame(js['ResultSet']['Result'])
    us_df = us_df.iloc[:,:2]
    browser = []
    iframe = []
    for i in us_df['V1'].index:
        ASPID = '{ASPID}'
        b = '}'
        a = us_df.iloc[i,0]
        browser.append("https://mmafund.sinopac.com/w/blank.asp?sUrl=$B2BRWDCOMMON$PROXY$SINOPAC$GOPAGE]XDJHTM{ASPID}SINOPAC!BID{b}{a}!API{b}1001".format(ASPID=ASPID,b=b,a=a))
        iframe.append('https://mmafund.sinopac.com/b2brwd/page/16005/basic/0001?sym={a}&symidxq={a}.US&symidbsr={a}.US&bid={a}'.format(a = a))
    us_df['browser'] = browser
    us_df['iframe'] = iframe
    us_df.columns = ['code', 'company', 'browser', 'iframe']
    us_df = us_df.drop_duplicates()
    us_df.to_excel('美股/美股url總表.xlsx')

########################################
##################ETF###################
########################################

def etf_preprocessing(etf):
    etf.encoding = "utf-8"
    soup = BeautifulSoup(etf.text, "html.parser")  
    rows = soup.select('td.article_title a')
    onclick = []
    for s in rows:
        onclick.append(s["onclick"])
    onclick = onclick
    onclick = pd.DataFrame(onclick)
    code = []
    company = []
    for j in onclick.index:
        x = [i for i in range(len(onclick.iloc[j,0])) if onclick.iloc[j,0].startswith("'", i)]
        code.append(onclick.iloc[j,0][x[0]+1:x[1]])
        company.append(onclick.iloc[j,0][x[2]+1:x[3]])
    onclick['code'] = code
    onclick['company'] = company
    onclick = onclick.iloc[:,1:]
    browser = []
    iframe = []
    for k in onclick.index:
        a = '}'
        b = onclick.iloc[k,0]
        browser.append('https://mmafund.sinopac.com/w/blank.asp?sUrl=$ETFWEB$HTML$ETFREPORT]DJHTM?|ETFID{a}{b}~{b}'.format(a = a, b = b))
        iframe.append('https://mmafund.sinopac.com/ETFWEB/HTML/ETFREPORT.DJHTM#ETFID={b}~{b}')
    onclick['browser'] = browser
    onclick['iframe'] = iframe
    onclick.columns = ['code','company','browser','iframe']
    return onclick


def etf_preprocessing_wd(etf):    
    etf.encoding = "utf-8"
    soup = BeautifulSoup(etf.page_source, "html.parser")  
    rows = soup.select('td.article_title a')
    onclick = []
    for s in rows:
        onclick.append(s["onclick"])
    onclick = onclick
    onclick = pd.DataFrame(onclick)
    code = []
    company = []
    for j in onclick.index:
        x = [i for i in range(len(onclick.iloc[j,0])) if onclick.iloc[j,0].startswith("'", i)]
        code.append(onclick.iloc[j,0][x[0]+1:x[1]])
        company.append(onclick.iloc[j,0][x[2]+1:x[3]])
    onclick['code'] = code
    onclick['company'] = company
    onclick = onclick.iloc[:,1:]
    browser = []
    iframe = []
    for k in onclick.index:
        a = '}'
        b = onclick.iloc[k,0]
        browser.append('https://mmafund.sinopac.com/w/blank.asp?sUrl=$ETFWEB$HTML$ETFREPORT]DJHTM?|ETFID{a}{b}~{b}'.format(a = a, b = b))
        iframe.append('https://mmafund.sinopac.com/ETFWEB/HTML/ETFREPORT.DJHTM#ETFID={b}~{b}')
    onclick['browser'] = browser
    onclick['iframe'] = iframe
    onclick.columns = ['code','company','browser','iframe']
    return onclick

def etf_wd_hot(chrome_url):
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_url)
    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)     
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'etf_hot_{i}': etf_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('ETF/etf_hot_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break
def etf_wd_active(chrome_url):
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_url)
    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)     
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'etf_active_{i}': etf_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('ETF/etf_active_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break


def etf_wd_interest(chrome_url):
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_url)
    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)     
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'etf_interest_{i}': etf_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('ETF/etf_interest_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break
#############################################
##################BOND#######################
#############################################
def bond_crawler(bond_url):
    resp = requests.get(bond_url)
    js = resp.json()
    bond_df = pd.DataFrame(js['BondList'])
    bond_df = bond_df[['BOND_ID','CH_ABBR_NAME','ISSUE_COMPANY_NAME1']]
    bond_df.to_excel('債券/bond_url.xlsx')

#########################################
#################Fund####################
#########################################

########single page#######
def fund_preprocessing(fund):    
    fund.encoding = "utf-8"
    soup = BeautifulSoup(fund.text, "html.parser")  
    rows = soup.select('td.article_title a')
    onclick = []
    for s in rows:
        onclick.append(s["onclick"])
    onclick = onclick
    onclick = pd.DataFrame(onclick)
    code = []
    company = []
    for j in onclick.index:
        x = [i for i in range(len(onclick.iloc[j,0])) if onclick.iloc[j,0].startswith("'", i)]
        code.append(onclick.iloc[j,0][x[0]+1:x[1]])
        company.append(onclick.iloc[j,0][x[2]+1:x[3]])
    onclick['code'] = code
    onclick['company'] = company
    onclick = onclick.iloc[:,1:]
    onclick.columns = ['code','company']
    return onclick

def fund_preprocessing_wd(fund):    
    fund.encoding = "utf-8"
    soup = BeautifulSoup(fund.page_source, "html.parser")  
    rows = soup.select('td.article_title a')
    onclick = []
    for s in rows:
        onclick.append(s["onclick"])
    onclick = onclick
    onclick = pd.DataFrame(onclick)
    code = []
    company = []
    for j in onclick.index:
        x = [i for i in range(len(onclick.iloc[j,0])) if onclick.iloc[j,0].startswith("'", i)]
        code.append(onclick.iloc[j,0][x[0]+1:x[1]])
        company.append(onclick.iloc[j,0][x[2]+1:x[3]])
    onclick['code'] = code
    onclick['company'] = company
    onclick = onclick.iloc[:,1:]
    onclick.columns = ['code','company']
    return onclick

########multiple page########
def fund_wd_first(chrome_url):
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_url)
    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)     
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'fund_first_{i}': fund_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('基金/fund_first_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break
def fund_wd_interest(chrome_url):
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_url)
    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)     
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'fund_interest_{i}': fund_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('基金/fund_interest_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break

def fund_wd_salary(chrome_url):
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_url)
    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)     
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'fund_salary_{i}': fund_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('基金/fund_salary_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break
def fund_wd_win(chrome_mstar, chrome_gold, chrome_smart ,chrome_lipper):
    ####mstar
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_mstar)

    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)     
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'fund_mstar_{i}': fund_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('fund_url/fund_mstar_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break
            
    ####gold
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_gold)

    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)
    chrome.find_element_by_link_text("台灣金鑽獎").click()
    time.sleep(5)
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'fund_gold_{i}': fund_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('fund_url/fund_gold_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break


    ####smart
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_smart)

    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)
    chrome.find_element_by_link_text("Smart智富台灣基金獎").click()
    time.sleep(5)
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'fund_smart_{i}': fund_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('fund_url/fund_smart_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break

    ####lipper    
    chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
    chrome.get(chrome_lipper)

    chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    time.sleep(5)
    chrome.find_element_by_link_text("理柏基金獎").click()
    time.sleep(5)
    soup = BeautifulSoup(chrome.page_source, 'html.parser')
    for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
        dfs = {f'fund_lipper_{i}': fund_preprocessing_wd(chrome)}
        pd.concat(dfs,axis = 0, ignore_index=True).to_csv('fund_url/fund_lipper_{i}.csv'.format(i = i))
        if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
            chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
            time.sleep(5)
            i += 1
        else:
            break
    chrome.close()
#####百元基金######
def fund_h_area():
    for i in range(1,7):
        chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
        chrome.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/new_fund_index.aspx?div=1'+'&tar='+str(i)+'&selectType=fund_tag')        
        chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        time.sleep(5) 
        soup = BeautifulSoup(chrome.page_source, 'html.parser')
        for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
            dfs = {f'fund_h_area_{i}': fund_preprocessing_wd(chrome)}
            pd.concat(dfs,axis = 0, ignore_index=True).to_csv('基金/fund_h_area_{i}.csv'.format(i = i))
            if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
                chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
                time.sleep(5)
                i += 1
            else:
                break


def fund_h_industry():
    for i in range(1,7):
        chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
        chrome.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/new_fund_index.aspx?div=2'+'&tar='+str(i)+'&selectType=fund_tag')        
        chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        time.sleep(5) 
        soup = BeautifulSoup(chrome.page_source, 'html.parser')
        for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
            dfs = {f'fund_h_industry_{i}': fund_preprocessing_wd(chrome)}
            pd.concat(dfs,axis = 0, ignore_index=True).to_csv('基金/fund_h_industry_{i}.csv'.format(i = i))
            if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
                chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
                time.sleep(5)
                i += 1
            else:
                break
        


def fund_h_invest():
    for i in range(1,4):        
        chrome = webdriver.Chrome('./chromedriver', chrome_options=options)
        chrome.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/new_fund_index.aspx?div=3&tar='+str(i)+'&selectType=fund_tag')
        chrome.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        time.sleep(5) 
        soup = BeautifulSoup(chrome.page_source, 'html.parser')
        for i in range(0,math.ceil(int(soup.find(id="HidtotalSize")["value"])/20)):    
            dfs = {f'fund_h_invest_{i}': fund_preprocessing_wd(chrome)}
            pd.concat(dfs,axis = 0, ignore_index=True).to_csv('test/fund_h_invest_{i}.csv'.format(i = i))
            if(len(chrome.find_elements_by_xpath("//a[contains(@class,'next')]")) > 0):
                chrome.find_element_by_xpath("//a[contains(@class,'next')]").click()
                time.sleep(5)
                i += 1
            else:
                break
            

if __name__ == "__main__":
    #####美股#####
    us_crawler('https://mmafund.sinopac.com/w/html/djjson/mmausstockranklist.djhtm?a=MARKET')
    #####ETF#####
    etf_1 = etf_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=ETF&PageNo=1&PageSize=20&hotSaletype=&fundType=&orderType=&boardType=default&_=1616467976062'))
    etf_2 = etf_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=ETF&PageNo=1&PageSize=20&hotSaletype=&fundType=&orderType=&boardType=performance&_=1616467976063'))
    etf_3 = etf_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=ETF&PageNo=1&PageSize=20&hotSaletype=&fundType=&orderType=&boardType=hotsale&_=1616467976064'))
    etf_4 = etf_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=ETF&PageNo=1&PageSize=20&hotSaletype=&fundType=&orderType=&boardType=default&_=1616480099969'))
    etf_5 = etf_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=ETF&PageNo=1&PageSize=20&hotSaletype=&fundType=&orderType=&boardType=performance&_=1616480099970'))
    etf_6 = etf_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=ETF&PageNo=1&PageSize=20&hotSaletype=&fundType=&orderType=&boardType=hotsale&_=1616480099971'))
    etf_7 = etf_preprocessing( requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/ResultList.aspx?PageNo=1&PageSize=20&keyword=&selectType=ETF&area=&industry=&category=&frequency=&currency=&TRACING_INDEX_CODE=&Beta=&Sharpe=&difference=&Interval=&percentage=&orderType=&CategoryPath=%2F%E9%A6%96%E9%81%B8%2F1&_=1616480274532'))
    etf = pd.concat([etf_1,etf_2,etf_3,etf_4,etf_5,etf_6,etf_7],axis = 0, ignore_index=True)
    etf = etf.drop_duplicates()
    etf.to_excel('ETF/etf.xlsx')
    etf_wd_hot('https://mma.sinopac.com/mma/SinopacFundSearch/search/new_ETF_index_popular.aspx?CategoryPath=scale&trans=C3')
    etf_wd_active('https://mma.sinopac.com/mma/SinopacFundSearch/search/new_ETF_index_active.aspx?CategoryPath=active&trans=C4')
    etf_wd_active('https://mma.sinopac.com/mma/SinopacFundSearch/search/new_ETF_index_active.aspx?CategoryPath=active&trans=C4')
    etf_wd_interest('https://mma.sinopac.com/mma/SinopacFundSearch/search/new_ETF_index_interest.aspx?CategoryPath=Y&trans=C6')
    #####債券######
    bond_crawler('https://mma.sinopac.com/ws/bond/bondquery/ws_bondInfo.ashx')
    #####基金######
    fund_1 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=fund&PageNo=1&PageSize=20&hotSaletype=&fundType=%E8%82%A1%E7%A5%A8%E5%9E%8B&orderType=&boardType=default&_=1616576213559'))
    fund_2 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=fund&PageNo=1&PageSize=20&hotSaletype=&fundType=%E5%B9%B3%E8%A1%A1%E5%9E%8B&orderType=&boardType=default&_=1616576213560'))
    fund_3 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=fund&PageNo=1&PageSize=20&hotSaletype=&fundType=%E5%82%B5%E5%88%B8%E5%9E%8B&orderType=&boardType=default&_=1616576213561'))
    fund_4 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=fund&PageNo=1&PageSize=20&hotSaletype=&fundType=%E8%82%A1%E7%A5%A8%E5%9E%8B&orderType=&boardType=performance&_=1616576213562'))
    fund_5 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=fund&PageNo=1&PageSize=20&hotSaletype=&fundType=%E5%B9%B3%E8%A1%A1%E5%9E%8B&orderType=&boardType=performance&_=1616576213563'))
    fund_6 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=fund&PageNo=1&PageSize=20&hotSaletype=&fundType=%E5%82%B5%E5%88%B8%E5%9E%8B&orderType=&boardType=performance&_=1616576213564'))
    fund_7 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=fund&PageNo=1&PageSize=20&hotSaletype=general&fundType=%E5%82%B5%E5%88%B8%E5%9E%8B&orderType=&boardType=hotsale&_=1616576213565'))
    fund_8 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=fund&PageNo=1&PageSize=20&hotSaletype=auto&fundType=%E5%82%B5%E5%88%B8%E5%9E%8B&orderType=&boardType=hotsale&_=1616576213566'))
    fund_index = pd.concat([fund_1,fund_2,fund_3,fund_4,fund_5,fund_6,fund_7,fund_8],axis = 0, ignore_index=True)
    fund_index = fund_index.drop_duplicates()
    fund_index.to_csv('基金/fund_index.csv')
    ####fund rank
    fund_r_1 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=&PageNo=1&PageSize=20&hotSaletype=&fundType=%E8%82%A1%E7%A5%A8%E5%9E%8B&orderType=&boardType=default&_=1616576944481'))
    fund_r_2 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=&PageNo=1&PageSize=20&hotSaletype=&fundType=%E5%B9%B3%E8%A1%A1%E5%9E%8B&orderType=&boardType=default&_=1616576944482'))
    fund_r_3 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=&PageNo=1&PageSize=20&hotSaletype=&fundType=%E5%82%B5%E5%88%B8%E5%9E%8B&orderType=&boardType=default&_=1616576944483'))
    fund_r_4 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=&PageNo=1&PageSize=20&hotSaletype=&fundType=%E8%82%A1%E7%A5%A8%E5%9E%8B&orderType=&boardType=performance&_=1616576944484'))
    fund_r_5 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=&PageNo=1&PageSize=20&hotSaletype=&fundType=%E5%B9%B3%E8%A1%A1%E5%9E%8B&orderType=&boardType=performance&_=1616576944485'))
    fund_r_6 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=&PageNo=1&PageSize=20&hotSaletype=&fundType=%E5%82%B5%E5%88%B8%E5%9E%8B&orderType=&boardType=performance&_=1616576944486'))
    fund_r_7 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=&PageNo=1&PageSize=20&hotSaletype=general&fundType=%E5%82%B5%E5%88%B8%E5%9E%8B&orderType=&boardType=hotsale&_=1616576944487'))
    fund_r_8 = fund_preprocessing(requests.get('https://mma.sinopac.com/mma/SinopacFundSearch/search/LeaderboardItem.aspx?selectType=&PageNo=1&PageSize=20&hotSaletype=auto&fundType=%E5%82%B5%E5%88%B8%E5%9E%8B&orderType=&boardType=hotsale&_=1616576944488'))
    fund_rank = pd.concat([fund_r_1,fund_r_2,fund_r_3,fund_r_4,fund_r_5,fund_r_6,fund_r_7,fund_r_8],axis = 0, ignore_index=True)
    fund_rank = fund_rank.drop_duplicates()
    fund_rank.to_csv('基金/fund_rank.csv')
    fund_wd_first('https://mma.sinopac.com/mma/SinopacFundSearch/search/new_fund_index_preferred.aspx?CategoryPath=1&trans=B4&selectType=fund_tag')            
    fund_wd_interest('https://mma.sinopac.com/mma/SinopacFundSearch/search/new_fund_index_interest.aspx?CategoryPath=Y&trans=B6&selectType=fund_tag')            
    fund_wd_salary('https://mma.sinopac.com/mma/SinopacFundSearch/search/Result.aspx?Keyword=%E8%96%AA%E8%BD%89&selectType=fund_tag')            
    chrome_mstar = 'https://mma.sinopac.com/mma/SinopacFundSearch/search/new_fund_index_winning.aspx?CategoryPath=%E6%99%A8%E6%98%9F%E5%9F%BA%E9%87%91%E7%8D%8E&trans=B5&selectType=fund_tag'                  
    chrome_smart = chrome_lipper = chrome_gold = chrome_mstar
    fund_wd_win(chrome_mstar, chrome_gold, chrome_smart ,chrome_lipper)        
    fund_h_area()
    fund_h_industry()
    fund_h_invest()          




