import pandas as pd
import datetime
from sqlalchemy import create_engine

rule_table=pd.read_excel(r'C:\Users\Administrator\Desktop\dept_name.xlsx',encoding='utf-8')#科室名称参照表
conn=create_engine('mysql+pymysql://sn_bigdata:sn_bigdaata@2019@172.60.20.204:4000/appointment_micro_db?charset=utf8',
                             encoding='utf-8', echo=True)
now_time = datetime.datetime.now()
yes_time = (now_time + datetime.timedelta(days=-2)).strftime('%Y-%m-%d')
time=(now_time + datetime.timedelta(days=-2)).strftime('%Y%m')
temp_data=pd.read_sql("select patient_id_card_no,dept_id ,dept_name,sch_date,created_date \
from appointment where created_date like '%s%%%%'"%(yes_time),con=conn)#20835

#将不同医院的相同的科室做相同的标记，解决不同医院相同科室名称不一致导致的无法统计总数的问题
data=pd.merge(temp_data,rule_table,how='left',left_on='dept_name',right_on='dept_name')#根据字段合并表
bf=data.groupby(['patient_id_card_no','dept_pyname']).agg({'dept_id':lambda x:x.count(),'created_date':lambda x:x.max(),
                                                               'sch_date':lambda x:x.max()})
#提取就诊人的预约科室
bf.loc[:,'dept_name']=[i[1]for i in bf.index]
conn1=create_engine('mysql+pymysql://sn_bigdata:sn_bigdaata@2019@172.60.20.204:4000/snbdx?charset=utf8',
                             encoding='utf-8', echo=True)
patient_label=pd.read_sql("select * from ads_patient_label_%s"%(time),con=conn1)
for i in bf.index:
    if bf.loc[i,'dept_name'] in list(patient_label.columns):
        if i[0] not in patient_label.loc[:,'patient_id_card_no'].tolist():
            b = len(patient_label)
            patient_label.loc[b, 'patient_id_card_no'] = i[0]
            patient_label.loc[b, bf.loc[i, 'dept_name']] = int(bf.loc[i, 'dept_id'])
            patient_label.loc[b, bf.loc[i, 'dept_name'] + '_order_date'] = str(bf.loc[i, 'sch_date'])
            patient_label.loc[b, bf.loc[i, 'dept_name'] + '_create_date'] =str(bf.loc[i, 'created_date'])
        else:
            c=patient_label[patient_label.loc[:,'patient_id_card_no']==i[0]].index
            if patient_label.loc[c, bf.loc[i, 'dept_na']: