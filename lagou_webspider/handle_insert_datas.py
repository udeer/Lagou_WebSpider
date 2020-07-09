from lagou_webspider.create_lagou_table import LagouTables
from lagou_webspider.create_lagou_table import session
from sqlalchemy import func
import time
from collections import Counter
class handlelagoudata(object):
    def __init__(self):
        #实例化session信息
        self.mysql_session=session()
        self.date = time.strftime('%Y-%m-%d', time.localtime())


    #数据的存储方法
    def insert_item(self,item):
        #抓取时间
        date=time.strftime('%Y-%m-%d',time.localtime())
        #存储数据的结构
        data=LagouTables(
            # 岗位ID
            positionid=item['positionId'],
            # 经度
            longitude=item['longitude'],
            # 纬度
            latitude=item['latitude'],
            # 岗位名称
            positionName=item['positionName'],
            # 工作年限
            workYear=item['workYear'],
            # 学历
            education=item['education'],
            # 岗位性质
            jobNature=item['jobNature'],
            # 公司类型
            financeStage=item['financeStage'],
            # 公司规模
            companySize=item['companySize'],
            # 业务方向
            industryField=item['industryField'],
            # 所在城市
            city=item['city'],
            # 岗位标签
            positionAdvantage=item['positionAdvantage'],
            # 公司简称
            companyShortName=item['companyShortName'],
            # 公司全称
            companyFullName=item['companyFullName'],
            # 公司所在区
            district=item['district'],
            # 公司福利标签
            companyLabelList=','.join(item['companyLabelList']),
            salary=item['salary'],
            # 抓取日期
            crawl_date=date
        )
        #在存储数据之前，需要现在表中查询下表是否存在
        query_result = self.mysql_session.query(LagouTables).filter(LagouTables.crawl_date == date,
                                                                    LagouTables.positionid == item[
                                                                        'positionId']).first()


        if query_result:
            print('该岗位信息已存在:%s%s%s'%(item['positionId'],item['city'],item['positionName']))
        else:
            #插入数据
            self.mysql_session.add(data)
            #提交数据
            self.mysql_session.commit()
            print('新增岗位信息%s'%item['positionId'])


    #查询行业信息方法
    def query_industryfield_result(self):
        info = {}
        # 查询今日抓取到的行业信息数据
        result = self.mysql_session.query(LagouTables.industryField).all()
        print(result)
        #处理原始数据
        result_list1=[x[0].split(',')[0] for x in result]
        #print(Counter(result_list1))
        #计数并返回
        result_list2=[x for x in Counter(result_list1).items() if x[1]>14]
        #print(result_list2)
        #填充series:里的Data数据
        data=[{'name':x[0],'value':x[1]} for x in result_list2]
        #print(data)
        #生成项目name
        name_list=[name['name'] for name in data]
        info['x_name']=name_list
        info['data']=data
        print(info)
        return info


    #查询薪资方法
    def query_salary_result(self):
        info = {}
        # 查询今日抓取到的薪资数据
        result = self.mysql_session.query(LagouTables.salary).filter(LagouTables.crawl_date==self.date).all()
        # 处理原始数据
        result_list1 = [x[0] for x in result]#列表生成式
        # 计数,并返回
        result_list2=[x for x in Counter(result_list1).items() if x[1]>16]
        result=[{'name':x[0],'value':x[1]} for x in result_list2]
        name_list=[name['name'] for name in result]
        info['x_name']=name_list
        info['data']=result
        print(info)
        return info


    #查询工作年限方法
    def query_workyear_result(self):
        info={}
        #取出数据
        result=self.mysql_session.query(LagouTables.workYear).filter(LagouTables.crawl_date==self.date).all()
        #处理原始数据
        result_1=[x[0] for x in result]
        #对数据进行count
        result_2=[x for x in Counter(result_1).items()]
        #生成data
        result=[{'name':x[0],'value':x[1]} for x in result_2 if x[1]>1]
        #生成x_name
        name_list=[name['name'] for name in result]
        info['x_name']=name_list
        info['data']=result
        print(info)
        return info


    #查询学历方法
    def query_education_result(self):
        info={}
        #查询今日抓取到的数据
        result=self.mysql_session.query(LagouTables.education).filter(LagouTables.crawl_date==self.date).all()
        #处理原始数据
        result_1=[x[0] for x in result]
        #计数并返回
        result_2=[x for x in Counter(result_1).items()]
        result=[{'name':x[0],'value':x[1]} for x in result_2]
        name_list=[name['name'] for name in result]
        info['x_name']=name_list
        info['data']=result
        print(info)
        return info


    #岗位发布数量,折线图
    def query_job_result(self):
        info={}
        #取出数据
        result=self.mysql_session.query(LagouTables.crawl_date,func.count('*').label('c')).filter(LagouTables.crawl_date==self.date).group_by(LagouTables.crawl_date).all()
        #处理原始数据
        result_1=[{'name':x[0],'value':x[1]} for x in result]
        #生成x_name
        name_list=[name['name'] for name in result_1]
        info['x_name'] =name_list
        info['data'] =result_1
        print(info)
        return info


    #查询城市count方法
    def query_city_result(self):
        info={}
        #取出数据
        result=self.mysql_session.query(LagouTables.city,func.count('*').label('c')).filter(LagouTables.crawl_date==self.date).group_by(LagouTables.city).all()
        #处理原始数据
        result_1=[{'name':x[0],'value':x[1]} for x in result]
        name_list=[name['name'] for name in result_1]
        info['x_name']=name_list
        info['data']=result_1
        print(info)
        return info


    #查询融资情况方法
    def query_financestage_result(self):
        info={}
        #取出数据
        result=self.mysql_session.query(LagouTables.financeStage).filter(LagouTables.crawl_date==self.date).all()
        #处理原始数据
        result_1=[x[0] for x in result]
        #计数并返回
        result_2=[x for x in Counter(result_1).items()]
        result=[{'name':x[0],'value':x[1]} for x in result_2]
        name_list=[name for name in result]
        info['x_name']=name_list
        info['data']=result
        print(info)
        return info


    #查询公司规模方法
    def query_companysize_result(self):
        info={}
        #取出数据
        result=self.mysql_session.query(LagouTables.companySize).filter(LagouTables.crawl_date==self.date).all()
        #处理原始数据
        result_1=[x[0] for x in result]
        #计数并返回
        result_2=[x for x in Counter(result_1).items()]
        result=[{'name':x[0],'value':x[1]} for x in  result_2]
        name_list=[name['name'] for name in result]
        info['x_name']=name_list
        info['data']=result
        print(info)
        return info


    #查询任职情况方法
    def query_jobNature_result(self):
        info={}
        #取出数据
        result=self.mysql_session.query(LagouTables.jobNature).filter(LagouTables.crawl_date==self.date).all()
        #处理原始数据
        result_1=[x[0] for x in result]
        #计数并返回
        result_2=[x for x in Counter(result_1).items()]
        result=[{'name':x[0],'value':x[1]} for x in result_2]
        name_list=[name['name'] for name in result]
        info['x_name']=name_list
        info['data']=result
        print(info)
        return info


    #抓取数量统计方法
    def count_resrult(self):
        info={}
        info['all_count']=self.mysql_session.query(LagouTables).count()
        info['today_count']=self.mysql_session.query(LagouTables).filter(LagouTables.crawl_date==self.date).count()
        print(info)
        return info



lagou_mysql=handlelagoudata()
lagou_mysql.query_industryfield_result()
lagou_mysql.query_salary_result()
lagou_mysql.query_education_result()
lagou_mysql.query_workyear_result()
lagou_mysql.query_job_result()
lagou_mysql.query_city_result()
lagou_mysql.query_financestage_result()
lagou_mysql.query_companysize_result()
lagou_mysql.query_jobNature_result()


