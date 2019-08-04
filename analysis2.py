import time, math, pymysql, pandas, datetime, numpy, xlsxwriter, os, psycopg2, dateutil, openpyxl, numpy, statistics
import pandas as pd

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

sql_1="""select 
a.product_id,a.friendly_name,
part1.f_sugar,part1.small_vol_tag,
part2.shops_num,
part3.per_male, part3.per_female, 
part3.per_6am_10am, part3.per_10am_2pm, part3.per_2pm_6pm, part3.per_6pm_10pm,part3.per_10pm_2am,part3.per_2am_6am,
part3.per_weekday,part3.per_weekendday,
part3.min_max_price
from 
products a
left join product_id_sml_class b using(product_id)

right join (
select 
		distinct b.product_id,e.f_sugar,f.small_vol_tag
from receipts a
join receipt_item b using (receipt_id)
join shops c using (shop_id)
join product_id_sml_class d using (product_id)
left join external_drink_tag_suntory_data e using(product_id)  
left join product_tags f using(product_id)
--left join user_tags e using(user_id)
where 
region_block_code='sh-lawson'
and user_id>0
and promotion_type=0
and receipt_timestamp between '2019-07-01 00:00:00' and '2019-07-31 23:59:59'
and medium_pid=1808
) part1 using(product_id)

left join(

select 
		distinct b.product_id,count(distinct new_shop_id) as shops_num
from receipts a
join receipt_item b using (receipt_id)
join shops c using (shop_id)
join product_id_sml_class d using (product_id)
where region_block_code='sh-lawson'
and user_id>0
and promotion_type=0
and receipt_timestamp between '2019-07-01 00:00:00' and '2019-07-31 23:59:59'
and medium_pid=1808
group by 1 
)part2 using(product_id)

left join (

select 
		distinct b.product_id,
		round(count(distinct user_id) filter(where gender=1)::numeric/count(distinct user_id)::numeric,2) as per_male,
		round(count(distinct user_id) filter(where gender=2)::numeric/count(distinct user_id)::numeric,2) as per_female,
		case when count(distinct receipt_id) = 0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 6 and 9)::numeric/count(distinct receipt_id),2) end as per_6am_10am,
		case when count(distinct receipt_id) = 0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 10 and 13)::numeric/count(distinct receipt_id),2) end as per_10am_2pm, 
		case when count(distinct receipt_id) = 0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 14 and 17)::numeric/count(distinct receipt_id),2) end as per_2pm_6pm, 
		case when count(distinct receipt_id) = 0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 18 and 21)::numeric/count(distinct receipt_id),2) end as per_6pm_10pm, 
		case when count(distinct receipt_id) = 0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) in (22,23,0,1))::numeric/count(distinct receipt_id),2) end as per_10pm_2am, 
		case when count(distinct receipt_id) = 0 then 0 else round(count(distinct receipt_id) filter (where date_part('hour',receipt_timestamp) between 2 and 5)::numeric/count(distinct receipt_id),2) end as per_2am_6am, 
		case when count(distinct receipt_id) = 0 then 0 else round(count(distinct receipt_id) filter (where extract(DOW FROM receipt_timestamp) in (1,2,3,4,5))::numeric/count(distinct receipt_id),2) end as per_weekday, 
		case when count(distinct receipt_id) = 0 then 0 else round(count(distinct receipt_id) filter (where extract(DOW FROM receipt_timestamp) in (0,6))::numeric/count(distinct receipt_id),2) end as per_weekendday,
		--round(sum(discount_price)::numeric/sum(item_sell_price)::numeric,2) as promotion,
		round(min(discount_price)::numeric/max(discount_price)::numeric,2) as min_max_price
from receipts a
join receipt_item b using (receipt_id)
join shops c using (shop_id)
join product_id_sml_class d using (product_id)
left join user_tags e using(user_id)
where region_block_code='sh-lawson'
and user_id>0
and promotion_type=0
and receipt_timestamp between '2019-07-01 00:00:00' and '2019-07-31 23:59:59'
and medium_pid=1808
group by 1 
)part3 using(product_id)

where medium_pid=1808;"""

try:
    print(time.ctime(), 'sql_data Initiated!')
    cursor1.execute(sql_1)
    sql_data2 = cursor1.fetchall()
    print(time.ctime(), ' sql_data Successful!')
except:
    print(time.ctime(), 'Error!')
# return [time.ctime(),  '{}  completed!'.format(data)]

list1 = list(map(list, sql_data2))
df1 = pandas.DataFrame(list1, columns=['product_id','friendly_name','f_sugar','small_vol_tag','shops_num','per_male','per_female',
                                       'trx_6am_10am', 'trx_10am_2pm', 'trx_2pm_6pm', 'trx_6pm_10pm','trx_10pm_2am','trx_2am_6am',
                                       'per_weekday','per_weekendday','min_max_price'
                                       ])
data_new=df1[['f_sugar','small_vol_tag','shops_num','per_male','per_female',
                                       'trx_6am_10am', 'trx_10am_2pm', 'trx_2pm_6pm', 'trx_6pm_10pm','trx_10pm_2am','trx_2am_6am',
                                       'per_weekday','per_weekendday','min_max_price']]

data_new[['f_sugar_no','f_sugar_yes','small_vol_tag_large','small_vol_tag_small']]=pd.get_dummies(data_new[['f_sugar','small_vol_tag']])

data_new.shops_num=data_new.shops_num/max(data_new.shops_num)

data_new.drop(['f_sugar'],inplace=True,axis=1)

data_new.drop(['small_vol_tag'],inplace=True,axis=1)

import pandas as pd
import pymysql
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score  #轮廓系数

clf=KMeans(n_clusters=4,random_state=0)
clf=clf.fit(data_new)
#加标签
data_new['label']=clf.labels_
#查看每个类别中的用户数量
print(data_new.label.value_counts())
#评估模型
print(silhouette_score(data_new,clf.labels_))
#分别以中心点的RFM的中位数为标准，划分类别

#降维
from sklearn.decomposition import PCA
pca=PCA(2)
new_x=pca.fit_transform(data_new[['shops_num', 'per_male', 'per_female', 'trx_6am_10am', 'trx_10am_2pm',
       'trx_2pm_6pm', 'trx_6pm_10pm', 'trx_10pm_2am', 'trx_2am_6am',
       'per_weekday', 'per_weekendday', 'min_max_price', 'f_sugar_no',
       'f_sugar_yes', 'small_vol_tag_large', 'small_vol_tag_small']])
print(pca.explained_variance_ratio_) #打印贡献率

#对四个簇进行可视化
final=pd.merge(pd.DataFrame(new_x,columns=['pca_x1','pca_x2']),pd.DataFrame(data_new.label),left_index=True,right_index=True)
import matplotlib.pyplot as plt
import numpy as np
def plot_pca_scatter():
    colors = ['black', 'blue', 'purple', 'yellow', 'orange']
    for i in range(len(colors)):
        px = final.iloc[:, 0][final.label == i]
        py = final.iloc[:, 1][final.label == i]
        plt.scatter(px, py, c=colors[i],label=i)
    plt.legend(np.arange(0, 4).astype(str))
    plt.xlabel('First Principal Component')
    plt.ylabel('Second Principal Component')
    plt.legend(bbox_to_anchor=(1.05, 0), loc=3, borderaxespad=0)
    plt.show()
plot_pca_scatter()

#加上friendly_name进行可视化
final_2=pd.merge(pd.DataFrame(new_x,columns=['pca_x1','pca_x2']),pd.DataFrame(df1.friendly_name),left_index=True,right_index=True)
friendly_name=list(df1.friendly_name)
plt.rcParams['font.sans-serif'] = ['SimHei']    # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  #显示负号

plt.rcParams['savefig.dpi'] = 1000 #图片像素

plt.rcParams['figure.dpi'] = 1000 #分辨率
for i in range(len(friendly_name)):
    px2=final_2.iloc[:, 0][final_2.friendly_name == friendly_name[i]]
    py2=final_2.iloc[:, 1][final_2.friendly_name == friendly_name[i]]
    plt.scatter(px2, py2, c='red',label=friendly_name[i])
    plt.text(px2, py2+0.01,friendly_name[i],fontdict={'size':4,'color':'blue'})
fig=plt.gcf()
fig.set_size_inches(10.5, 10.5)
#plt.legend(np.arange(0, 8).astype(str))
plt.xlabel('First Principal Component')
plt.ylabel('Second Principal Component')
#plt.legend(bbox_to_anchor=(1.05, 0), loc=3, borderaxespad=0)
plt.show()