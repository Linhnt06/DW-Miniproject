#data we'll be using from Amazon S3
import pandas as pd
import numpy as np
# load the 2017 data
scripts = pd.read_csv('./dw-data/201701scripts_sample.csv.gz')
scripts.head()
col_names=[ 'code', 'name', 'addr_1', 'addr_2', 'borough', 'village', 'post_code']
practices = pd.read_csv('./dw-data/practices.csv.gz')
practices.columns = col_names
practices.head()
chem = pd.read_csv('./dw-data/chem.csv.gz')
chem.head()
#Question 1: summary_statistics
def summary_stat(a):
    summary =()
    scripts['a'].describe
summary_stats = [('items', (0,) * 6), ('quantity', (0,) * 6), ('nic', (0,) * 6), ('act_cost', (0,) * 6)]
#Question 2: most_common_item: Find the item with the highest total and return the result as [(bnf_name, total)]
items = scripts.groupby(['bnf_name'], sort=False)['items'].sum()
items_df = pd.DataFrame({'bnf_name':items.index, 'items':items.values})
a = items_df.loc[items_df['items'].idxmax()]
most_common_item=[]
emptytuple=()
emptytuple=a[0],a[1]
most_common_item.append(emptytuple)
print(most_common_item)
#Question 3: items_by_region: Find the most common item by post code. Results as a list of tuples (post code, item name, amount dispensed as % of total)
scripts.sort_values('bnf_name')
uniquepractice=(practices.sort_values('post_code')
                .groupby('code').first().reset_index())
joined = pd.merge(scripts, uniquepractice, how = 'inner', left_on = 'practice', right_on = 'code')

groups=joined.groupby(['post_code', 'bnf_name'])
sms=groups.sum()

g = sms['items'].groupby(level=0, group_keys=False)
res = g.nlargest(1)
res.head()

groups_post=joined.groupby(['post_code'])['items'].sum()

portion_of_total=0
lis=[]
final_list_grader=[]

count=0
for i, j in res.iteritems():
    if count<100:
        for ii, jj in groups_post.iteritems():
            if ii==i[0]:
                portion_of_total=j/jj
        emptytuple=()

        emptytuple=i[0],i[1],round(portion_of_total,10)
        lis.append(emptytuple)
    count+=1
final_list_grader=lis
#Question 4: script_anomalies. we've calculated this statistic, take the 100 practices with the largest z-score. Return your result as a list of tuples in the form (practice_code, practice_name, z-score, number_of_scripts)
import math
import gzip
import pandas as pd
from static_grader import grader


chem = pd.read_csv('./dw-data/chem.csv.gz', compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False)
chem.head()
chem.columns

with gzip.open ( './dw-data/201701scripts_sample.csv.gz', 'rb' ) as f:
    scripts = pd.read_csv ( f )

with gzip.open ( './dw-data/practices.csv.gz', 'rb' ) as f:
    practices = pd.read_csv ( f )



practices.columns = ['code', 'name', 'addr_1', 'addr_2', 'borough', 'village', 'post_code'] 
practices = practices[['code', 'name']].sort_values (by = ['name'], ascending = True) 
practices = practices [~practices.duplicated(['code'])] 
opioids = ['morphine', 'oxycodone', 'methadone', 'fentanyl', 'pethidine', 'buprenorphine', 'propoxyphene', 'codeine'] 


check = '|'.join(opioids) 
chem_df1 = chem 
chem_df1 [ 'test' ] = chem_df1 [ 'NAME' ].apply ( lambda x: any ( [ k in x.lower() for k in opioids ] ) ) 
key2 = chem_df1 [ "test" ] == True 
chem_df1 = chem_df1 [ key2 ]  
chem_sub = list (chem_df1['CHEM SUB']) 


scripts['opioid'] = scripts [ 'bnf_code' ].apply(lambda x: 1 if x in chem_sub else 0)
std_devn = scripts.opioid.std ()
overall_rate = scripts.opioid.mean()

scripts = scripts.merge (practices, left_on = 'practice', right_on = 'code')
scripts['cnt'] = 0


opioids_per_practice = scripts.groupby ( [ 'practice', 'name'], as_index = False ).agg ( { 'opioid': 'mean', 'cnt': 'count' } )
opioids_per_practice.drop_duplicates()

opioids_per_practice['opioid'] = opioids_per_practice ['opioid'] - overall_rate

opioids_per_practice['std_err'] = std_devn / opioids_per_practice['cnt'] ** 0.5
opioids_per_practice['z_score'] = opioids_per_practice['opioid'] / opioids_per_practice['std_err']

result = opioids_per_practice[['practice','name', 'z_score', 'cnt']]


result.sort_values(by = 'z_score', ascending = False, inplace = True)
anomalies = [(k[1], k[2], k[3], k[4]) for k in result.itertuples()][:100]
anomalies[:5]
#Question 5: script_growth. Load in beneficiary data from 6 months earlier, June 2016, and calculate the growth rate in prescription rate from June 2016 to January 2017 for each bnf_name. return the 50 items with largest growth and the 50 items with the largest shrinkage
scripts16 = pd.read_csv('./dw-data/201606scripts_sample.csv.gz', delimiter =",")
drugs_16 = scripts16[['bnf_name', 'items']]
drugs_16 = drugs_16.groupby('bnf_name').count().reset_index()
drugs_16.columns = ['bnf_name', 'count16']

drugs_17 = scripts[['bnf_name', 'items']]
drugs_17 = drugs_17.groupby('bnf_name').count().reset_index()
drugs_17.columns = ['bnf_name', 'count17']

drugs = drugs_16.merge(drugs_17, on = 'bnf_name', how ='inner')
drugs = drugs[drugs['count16']>=50]

drugs.is_copy = False
drugs['growth'] = ((drugs['count17']- drugs['count16'])/drugs['count16'])
drugs = drugs[['bnf_name', 'growth','count16']]
drugs.sort_values('growth', ascending = False, inplace = True)
script_growth =pd.concat([drugs.iloc[:50], drugs.iloc[-50:]], axis =0)
#Question 6: rare_scripts: identify practices whose costs disproportionately originate from rarely prescribed items.
cost_all = scripts[scripts['rare']]['act_cost'].sum() / scripts['act_cost'].sum()

relative_rare_cost_prop = rare_cost_prop - cost_all

standard_errors = relative_rare_cost_prop.std()  

z_score = relative_rare_cost_prop / standard_errors
z_score = pd.DataFrame(z_score.sort_values(ascending = False))    

z_score.reset_index(inplace = True)

z_score.columns = ['practice', 'z_score']

fin = (practices.groupby(['code'])[['code', 'name']]).head() # 

result = z_score.merge(fin, how = 'left', left_on = 'practice',right_on = 'code').drop('code', axis = 1)

df = result.groupby('practice').first().sort_values('z_score', ascending = False).reset_index()[:100] 

rare_scripts = list(zip(df.practice, df.name, df.z_score))

