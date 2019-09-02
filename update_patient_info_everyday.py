import pandas as pd
from sqlalchemy import create_engine
"""分别链接数据库appointment_micro_db和snbdx"""
try:
    engine=create_engine('mysql+pymysql://sn_bigdata:sn_bigdaata@2019@172.60.20.204:4000/appointment_micro_db?charset=utf8', encoding='utf-8', echo=True)
    engine1=create_engine('mysql+pymysql://sn_bigdata:sn_bigdaata@2019@172.60.20.204:4000/snbdx?charset=utf8', encoding='utf-8', echo=True)
    print("Database connected successfully")
except:
    print("Databsase connection failed")
import datetime
"""定义函数：通过身份证号获取年龄"""
def get_age(ID):
     """通过身份证号获取年龄"""
     birth_year = int(ID[6:10])
     birth_month = int(ID[10:12])
     birth_day = int(ID[12:14])
     now = (datetime.datetime.now() + datetime.timedelta(days=1))
     year = now.year
     month = now.month
     day = now.day
     if year == birth_year:
         return 0
     else:
         if birth_month > month or (birth_month == month and birth_day > day):
             return year - birth_year - 1
         else:
             return year - birth_year
"""定义函数：插入每一条记录"""
def insert_one(args):
    with engine1.connect() as conn:
        #插入一条新的记录
        sql="insert into dws_patient_info(patient_name,patient_id,patient_card_id,patient_phone,account_id,patient_id_card_no,patient_age) \
        values(%s,%s,%s,%s,%s,%s,%s);"
        conn.execute(sql,args)
"""查询出昨天一整天的预约病人，并更新到patient_info表中"""
def update_yesterday_info(yes_time):
    #筛选出昨天一整天的预约病人
    yesterday_data=pd.read_sql('select patient_id_card_no,patient_name,patient_id,patient_card_id,dept_id,patient_phone,account_id,dept_name,sch_date,created_date \
    from appointment where created_date like "%%'+str(yesterday)+'%%"',con=engine)
    yesterday_data.set_index('patient_id_card_no',inplace=True)#将病人的身份证号设置成索引列，便于提取信息
    patient_data=pd.read_sql('select patient_id_card_no,patient_name,patient_id from dws_patient_info',con=engine1)#获取已有病人的身份证号码
    p_list=patient_data["patient_id_card_no"].tolist()#获取当前病人信息表中，已经存在的用户信息。
    a,b,c=0,0,0
    for x in list(set(yesterday_data.index.values)):#遍历新一天预约表中所有的病人id
        if x not in p_list:#判断是否已经存在病人信息表中
            try:
                one=yesterday_data.loc[x,['patient_name','patient_id','patient_card_id','patient_phone','account_id']].tolist()#获取该条记录
                age = get_age(x)
                one.append(x)
                one.append(int(age))
                insert_one(one)
                print("insert successfully1")
                a+=1
            except:
                try:
                    ##当该病人在一天中多次就诊，则取第一条的记录，插入到病人信息表中。
                    temp=yesterday_data.loc[x,['patient_name','patient_id','patient_card_id','patient_phone','account_id']].head(1)
                    one=temp.loc[x,['patient_name','patient_id','patient_card_id','patient_phone','account_id']].tolist()
                    age=get_age(x)
                    one.append(x)
                    one.append(int(age))
                    insert_one(one)
                    print("insert successfully2")
                    a+=1
                except:
                    b+=1
                    print('insert failed')
        else:
            c+=1
            print('该用户已经存在')
    print("patient_info一共有：%d条记录" % len(patient_data))
    print("%s号一整天的挂号病人共有：%d" % (yes_time, len(yesterday_data)))
    print("插入成功的记录一共%d条；插入失败的记录一共%d条；已经存在的记录%d条。" % (a, b, c))
if __name__ == '__main__':
    now_time = datetime.datetime.now()#当前日期
   # yesterday = (now_time + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')#截取昨天的日期
    yesterday='2019-08-05'
    update_yesterday_info(str(yesterday))
