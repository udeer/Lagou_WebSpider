import requests
import re
import time
import json
from lagou_webspider.handle_insert_datas import lagou_mysql
class HandLeLaGou(object):
    #使用session保存cookies信息
    def __init__(self):
        self.lagou_session=requests.session()
        self.header={
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'

        }
        self.city_list=''
    #获取全国所有城市列表方法
    def handle_city(self):
        city_search=re.compile(r'www\.lagou\.com\/.*\/">().*</a>')
        city_url='https://www.lagou.com/jobs/allCity.html'
        city_result=self.handle_request(method='GET',url=city_url)
        # 使用正则表达式获取城市列表
        self.city_list = city_search.findall(city_result)
        # 清除cookies的值
        self.lagou_session.cookies.clear()

    # 处理所有城市招聘方法
    def handle_city_job(self,city):
        first_request_url='https://www.lagou.com/jobs/list_python?city=%s&cl=false&fromSearch=true&labelWords=&suginput='%city
        first_response=self.handle_request(method='GET',url=first_request_url) #获取cookie信息和页码
        total_page_search=re.compile(r'class="span\stotalNum">(\d+)</span>')
        try:
            total_page = total_page_search.search(first_response).group(1)
            #没有岗位信息直接return
        except:
            return
        else:#构造页码
            for i in range(1,int(total_page)+1):
                data={
                    'pn':i,
                    'kd':'python'
                }
                page_url='https://www.lagou.com/jobs/positionAjax.json?city=%s&needAddtionalResult=false'%city
                referer_url ='https://www.lagou.com/jobs/list_python?city=%s&cl=false&fromSearch=true&labelWords=&suginput='%city
                self.header['referer']=referer_url.encode()  #在请求头中加入referer信息
                response=self.handle_request(method='POST',url=page_url,data=data,info=city)
                lagou_data=json.loads(response)
                job_list=lagou_data['content']['positionResult']['result']
                for job in job_list:
                    lagou_mysql.insert_item(job)



    #处理请求方法
    def handle_request(self,method,url,data=None,info=None):

        while True:
            if method == 'GET':
                response = self.lagou_session.get(url=url, headers=self.header)
            elif method == 'POST':
                response = self.lagou_session.post(url=url, headers=self.header, data=data)
            response.encoding = 'utf-8'
            if '频繁' in response.text:
                print('频繁')
                #清楚cookie信息
                self.lagou_session.cookies.clear()
                #重新发起get请求构造cookie信息
                first_request_url = 'https://www.lagou.com/jobs/list_python?city=%s&cl=false&fromSearch=true&labelWords=&suginput=' %info
                self.handle_request(method='GET', url=first_request_url)
                time.sleep(10)
                continue
            return response.text





if __name__=='__main__':
    lagou=HandLeLaGou()
    #获取所有城市方法
    lagou.handle_city()
    for city in lagou.city_list:
        print(city)
        lagou.handle_city_job(city)
        break









