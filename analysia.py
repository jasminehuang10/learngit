
import time, math, pymysql, pandas, datetime, numpy, xlsxwriter, os, psycopg2, dateutil, openpyxl, numpy, statistics
import pandas as pd
import os
from multiprocessing import Pool, TimeoutError, cpu_count
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import numpy as np
from scipy.stats.stats import pearsonr
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import cross_val_score, cross_val_predict
from sklearn.metrics import confusion_matrix, recall_score, precision_score, f1_score


def Connection_Initiation_Module():
    global connection1, connection2, connection3, connection4, connection5, server1, server2, server3, server4, server5, cursor1, cursor2, cursor3, cursor4, cursor5
    ###Connect to Lawson postgreSQL
    print(time.ctime(), 'Initiate Lawson postgreSQL connection!')
    connection1 = psycopg2.connect(host='120.26.164.117', port='5432', user="jen", password='limweijen',
                                   dbname='lawson')
    cursor1 = connection1.cursor()
    print(time.ctime(), 'Lawson postgreSQL connection successful!')


def Connection_Close_Module():
    connection1.close()
    print('Conenction close!')


Connection_Initiation_Module()

sql="""
SELECT 
b.user_id,
--性别
case when gender=1 then 1 else 0 end as f_male,
case when gender=2 then 1 else 0 end as f_female,
case when gender=0 then 1 else 0 end as f_genderless,
-- 年龄
case when age between 10 and 19 then 1 else 0 end as f_age_10_19,
case when age between 20 and 29 then 1 else 0 end as f_age_20_29,
case when age between 30 and 39 then 1 else 0 end as f_age_30_39,
case when age between 40 and 49 then 1 else 0 end as f_age_40_49,
case when age between 50 and 59 then 1 else 0 end as f_age_50_59,
case when age between 60 and 69 then 1 else 0 end as f_age_60_69,
case when age between 70 and 79 then 1 else 0 end as f_age_70_79,
case when (age not between 10 and 79) or (age is null) then 1 else 0 end as f_age_others,
-- 会员天数
'2019-08-02'-udtday as member_days,
-- pv,uv
coalesce(uv,0),  -- 将空值替换成其他值返回第一个非空值
coalesce(pv,0),
-- 购买总次数
count(distinct receipt_id) as trx_all,
-- 不同时间段购买次数占比
case when count(distinct receipt_id)=0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 6 and 9)::numeric/count(distinct receipt_id)::numeric,2) end as trx_6am_10am,
case when count(distinct receipt_id)=0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 10 and 13)::numeric/count(distinct receipt_id)::numeric,2) end  as trx_10am_2pm,
case when count(distinct receipt_id)=0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 14 and 17)::numeric/count(distinct receipt_id)::numeric,2) end  as trx_2pm_6pm,
case when count(distinct receipt_id)=0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 18 and 21)::numeric/count(distinct receipt_id)::numeric,2) end  as trx_6pm_10pm,
case when count(distinct receipt_id)=0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) in (22,23,0,1))::numeric/count(distinct receipt_id)::numeric,2) end  as trx_10pm_2am,
case when count(distinct receipt_id)=0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 2 and 5)::numeric/count(distinct receipt_id)::numeric,2) end  as trx_2am_6am,
-- 访问店数
count(distinct shop_id) as visited_shops,
-- 购买产品数
count(distinct product_id) as purchased_skus,
-- 客单价
 COALESCE(sum(discount_price*quantity)/count(distinct receipt_id),0) as amount_per_trx,
-- 是否购买了某分类/商品 如：茶
case when count(distinct receipt_id) filter(where medium_pid=1808) >0 then 1 else 0 end as buy_or_not

FROM 
user_tags b 
left join 
(select user_id,count(distinct user_id) as uv,count(user_id) as pv from telemetry
where timestamp between '2019-07-21 00:00:00' and '2019-07-31 23:59:59'
and region_id=1
group by 1) c on (b.user_id=c.user_id)

left join
(select * from receipts a
join receipt_item b using (receipt_id)
join shops c using (shop_id)
join product_id_sml_class d using (product_id)
where region_block_code='sh-lawson'
and user_id>0
and promotion_type=0
and receipt_timestamp between '2019-07-21 00:00:00' and '2019-07-31 23:59:59'
) part1 on (b.user_id=part1.user_id)

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
;"""

try:
    print(time.ctime(), 'sql_data Initiated!')
    cursor1.execute(sql)
    sql_data2 = cursor1.fetchall()
    print(time.ctime(), ' sql_data Successful!')
except:
    print(time.ctime(), 'Error!')
# return [time.ctime(),  '{}  completed!'.format(data)]

list1 = list(map(list, sql_data2))
df1 = pandas.DataFrame(list1, columns=['user_id', 'f_male', 'f_female', 'f_genderless', 'f_age_10_19',
                                       'f_age_20_29', 'f_age_30_39', 'f_age_40_49', 'f_age_50_59', 'f_age_60_69','f_age_70_79', 'f_age_others',
                                       'member_days', 'uv', 'pv',
                                       'trx_all',
                                       'trx_6am_10am', 'trx_10am_2pm', 'trx_2pm_6pm', 'trx_6pm_10pm','trx_10pm_2am','trx_2am_6am',
                                        'visited_shops','purchased_skus',
                                        '米饭', '调理面包', '调理面', '副食小吃', '油炸品', '熬点', '包子', '热盒饭',
                                       '现制其他', '甜品', '面包', '日配食品', '冷冻食品', '冰淇淋', '冷藏饮料', '常温饮料',
                                       '啤酒', '方便面', '嗜好品', '口袋零食', '休闲食品', '珍味', '米饼', '饼干', '进口零食',
                                        'amt_per_trx','buy_or_not'
                                       ])
x=df1[['f_male', 'f_female', 'f_genderless', 'f_age_10_19',
                                       'f_age_20_29', 'f_age_30_39', 'f_age_40_49', 'f_age_50_59', 'f_age_60_69','f_age_70_79', 'f_age_others',
                                       'member_days', 'uv', 'pv',
                                       'trx_all',
                                       'trx_6am_10am', 'trx_10am_2pm', 'trx_2pm_6pm', 'trx_6pm_10pm','trx_10pm_2am','trx_2am_6am',
                                        'visited_shops','purchased_skus',
                                        '米饭', '调理面包', '调理面', '副食小吃', '油炸品', '熬点', '包子', '热盒饭',
                                       '现制其他', '甜品', '面包', '日配食品', '冷冻食品', '冰淇淋', '冷藏饮料', '常温饮料',
                                       '啤酒', '方便面', '嗜好品', '口袋零食', '休闲食品', '珍味', '米饼', '饼干', '进口零食',
                                        'amt_per_trx']]
y=df1.buy_or_not

#随机森林
from sklearn.model_selection import train_test_split
from sklearn import ensemble
from sklearn import metrics

x_train,x_test,y_train,y_test=train_test_split(x,y,test_size=0.25)
RF_class=ensemble.RandomForestClassifier(n_estimators=200,random_state=1234)
RF_class.fit(x_train,y_train)
RFclass_pred=RF_class.predict(x_test)
metrics.accuracy_score(y_test,RFclass_pred)

#模型的保存
from sklearn.externals import joblib
joblib.dump(RF_class, 'RF_class_significance_var.pkl')

#变量的重要程度
import matplotlib.pyplot as plt
from pylab import *
mpl.rcParams['font.sans-serif'] = ['SimHei']
importance=RF_class.feature_importances_
Impt_series=pd.Series(importance,index=x_train.columns)
Impt_series.sort_values(ascending=True).plot('barh')
plt.show()

