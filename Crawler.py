#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import io
import time


# In[31]:


def crawler_url(url, filename):
    res = requests.get(url)
    # 是否訪問成功
    if res.status_code == 200:
        # 用pandas輸出成csv檔
        try:
            data = io.StringIO(res.text)
            df = pd.read_csv(data, sep=",")
            df.to_csv(f'{filename}_lvr_land_a.csv')
            
        # 看是否有例外出現
        except Exception as e:
            print(e)
            
        # Print done
        finally:
            print("Done")
    else:
        print('Failed')
        return 
    
if __name__ == "__main__":
    
    # 五個縣市用dict標記代表的英文字母
    city_dict = {"Taipei":"a", "New Taipei":"f", "Taoyuan":"h", "Taichung":"b", "Kaohsiung":"e"}
    
    # 跑爬蟲
    for city in city_dict:
        city_id = city_dict[city]
        url = f"https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName={city_id}_lvr_land_A.csv"
        crawler_url(url.format(city_id), city_dict[city])
        
        #休息
        time.sleep(2)

