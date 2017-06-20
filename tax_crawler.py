from bs4 import BeautifulSoup
import requests
import json
import pandas as pd
import time, datetime

erie_county_url = "https://paytax.erie.gov/webprop/"
data = {'txtowner':'',
       'txtnum':'',
       'txtstreet':'',
       'Juris':'All'}
parcel_map = {'Parcel Status':'parcel_status', 'City\Town':'city_town', 'Village':'village',
        'S-B-L':'sbl', 'Owner':'owner', 'SWIS':'swis', 'Property Location':'property_location',
        'Mailing Address':'mailing_address', 'Property Class':'property_class', 'Line 2':'line_2',
        'Assessment':'assessment', 'Line 3':'line_3', 'Taxable':'taxable', 'Street':'street',
        'Desc':'desc', 'City/State':'city_state', 'Desc_2':'desc_2', 'Zip':'zip', 'Deed Book':'deed_book',
        'Deed Page':'deed_page', 'Frontage':'frontage', 'Depth':'depth', 'Acres':'acres',
        'Year Built':'year_built', 'Square Ft':'square_ft', 'Beds':'beds', 'Baths':'baths',
        'FirePlace':'fireplace', 'School':'school', 'Parcel History':'parcel_history_url', 'Parcel URL':'parcel_url'}

def mapkeys(dic):
    parcel_map = {'Parcel Status':'parcel_status', 'City\Town':'city_town', 'Village':'village',
        'S-B-L':'sbl', 'Owner':'owner', 'SWIS':'swis', 'Property Location':'property_location',
        'Mailing Address':'mailing_address', 'Property Class':'property_class', 'Line 2':'line_2',
        'Assessment':'assessment', 'Line 3':'line_3', 'Taxable':'taxable', 'Street':'street',
        'Desc':'desc', 'City/State':'city_state', 'Desc_2':'desc_2', 'Zip':'zip', 'Deed Book':'deed_book',
        'Deed Page':'deed_page', 'Frontage':'frontage', 'Depth':'depth', 'Acres':'acres',
        'Year Built':'year_built', 'Square Ft':'square_ft', 'Beds':'beds', 'Baths':'baths',
        'FirePlace':'fireplace', 'School':'school', 'Parcel History':'parcel_history_url', 'Parcel URL':'parcel_url'}
    new_dic = {}
    
    for k,v in parcel_map.items():
        new_dic[v] = dic[k]
    return new_dic

def getParcelHistoryURL(parcel_detail_table):
    parcel_history_url = parcel_detail_table.find_all('a', href=True)[0]['href']
    return parcel_history_url
    
def getParcelHistory(url):    
    parcel_history_page = requests.get(erie_county_url + parcel_history_url)
    parcel_history_soup = BeautifulSoup(parcel_history_page.text, 'lxml')
    parcel_history_table = parcel_history_soup.find(id='generic_site_table')
    
    for tr in parcel_history_table.find_all('tr'):
        for th,td in zip(tr.find_all('th'), tr.find_all('td')):
            print(th.text, ':', td.text)
        print()
        
def post_to_ds(df):
    for i in range(len(df)):
        pst = requests.post("http://buffalodataserver.com/parcels", json=mapkeys(json.loads(df.iloc[i].to_json())) )
        response = pst.json()
        if 'error' in response:
            print(response)
            
def end_key(soup):
    return "**** Last Record Found ****" in [r.text for r in soup.find_all('p')]

def crawl_to_df(parcel_detail_urls):
    df = pd.DataFrame([], columns=parcel_map.keys(), dtype='str')
    for n,url in enumerate(parcel_detail_urls):
        #print(erie_county_url+url)
        parcel_detail_page = requests.get(erie_county_url + url)
        parcel_detail_soup = BeautifulSoup(parcel_detail_page.text, 'lxml')
        parcel_detail_table = parcel_detail_soup.find(id='generic_site_table')

        parcel_dic = {}
        parcel_dic['Parcel URL'] = url
        for th,td in zip(parcel_detail_table.find_all('th'),parcel_detail_table.find_all('td')):
            parcel_dic[th.text] = td.text.strip()

        parcel_dic['Parcel History'] = getParcelHistoryURL(parcel_detail_table)
        df = df.append(parcel_dic, ignore_index=True)
        print(parcel_dic['Property Location'])
    return df

def crawl_parcels(txtsbl='', txtstreet=''):
    erie_county_url = "https://paytax.erie.gov/webprop/"
    data = {'txtowner':'',
           'txtnum':'',
           'txtstreet':txtstreet,
            'txtsbl':txtsbl,
           'Juris':'All'}

    s = requests.session()
    res = s.post(erie_county_url+"property_info_results.asp", data=data)
    soup = BeautifulSoup(res.text, 'lxml')

    while True:
        table = soup.find(id='generic_site_table')
        try:
            parcel_detail_urls = [r['href'] for r in table.find_all('a')]
        except AttributeError:
            print(res.text)
            res = s.get(erie_county_url+"property_info_results_next.asp")
            soup = BeautifulSoup(res.text, 'lxml')
            continue
        df_ext = crawl_to_df(parcel_detail_urls)
        print(len(df_ext))
        try:
            post_to_ds(df_ext)
        except:
            print("Error with response: " + txtsbl)
            
        if end_key(soup):
            break
        else:
            res = s.get(erie_county_url+"property_info_results_next.asp")
            soup = BeautifulSoup(res.text, 'lxml')
#res = s.get("https://paytax.erie.gov/webprop/property_info_details.asp?sbl=101.28-10-15&KEY=1430891012800010015000")

f = open('tax_crawl_log.txt','a')

for i in range(111, 1000):
    current_sbl = str(i)+'.'
    print("Starting SBL: " + current_sbl)
    f.write(str(datetime.datetime.now())+'\n')
    f.write(current_sbl+'\n')
    for j in range(3):
        crawl_parcels(txtsbl=current_sbl)
        try:
		break
        except:
            print("Error @ {}, trying the {} attempt".format(current_sbl, j))
            f.write("Error @ {}, trying the {} attempt\n".format(current_sbl, j))
            time.sleep(3**j)
    f.write('\n')
            
            
