from flask import Flask,render_template,jsonify
from lagou_webspider.handle_insert_datas import lagou_mysql

#flask的实例化
app=Flask(__name__)

@app.route('/index')
def lagou():
    #库内抓取总数据
    result=lagou_mysql.count_resrult()
    return render_template('index.html',result=result) #传入info字典

@app.route('/get_echart_data')
def get_echart_data():
    info={}
    #行业发布数量分析
    info['echart_1']=lagou_mysql.query_industryfield_result()
    #薪资发布数量分析
    info['echart_2']=lagou_mysql.query_salary_result()
    #岗位数量分析 折线图
    info['echart_5']=lagou_mysql.query_workyear_result()
    # 学历情况分析
    info['echart_6'] = lagou_mysql.query_education_result()
    # 融资情况
    info['echart_31'] = lagou_mysql.query_financestage_result()
    # 公司规模
    info['echart_32'] = lagou_mysql.query_companysize_result()
    # 岗位要求
    info['echart_33'] = lagou_mysql.query_jobNature_result()
    # 各地区发布岗位数
    info['map'] = lagou_mysql.query_city_result()
    return jsonify(info)




if __name__=='__main__':
    app.run(debug=True,host='127.0.0.1',port=800)