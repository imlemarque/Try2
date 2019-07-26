# Import packages
import pandas as pd
import numpy as np
import re
from dateutil.parser import parse
import datetime
from datetime import timedelta
from ast import literal_eval

# Setting up and establishing connection with redshift

from sqlalchemy.engine import (
    url as sa_url,
    Engine as db_Engine,
    Connection as db_Connection,
)
from sqlalchemy import create_engine as sa_create_engine
from typing import Dict, List
import pandas as pd

 
def get_db_engine(config: Dict = {}) -> db_Engine:
    __DRIVERNAME = config.get("database_driver", "redshift+psycopg2")
    __PORT = config.get("PGPORT", 5555)
    __USER = config.get("PGUSER", None)
    __DATABASE = config.get("PGDATABASE", "dev")
    __HOST = config.get(
        "PGHOST", "xxxxxxxx.redshift.amazonaws.com"
    )
    __PASSWORD = config.get("PGPASSWORD", None)

    if not __USER or not __PASSWORD:
        raise ValueError("PGUSER/PGPASSWORD is not defined in config and environment.")

    db_url = sa_url.URL(
        drivername=__DRIVERNAME,
        username=__USER,
        password=__PASSWORD,
        host=__HOST,
        port=__PORT,
        database=__DATABASE,
    )
    db_engine = sa_create_engine(db_url, connect_args={"sslmode": "verify-full"})

    return db_engine

 
# Establish connection for user Manoj
engine = get_db_engine(config = {
    'PGUSER': 'xxxxxx',
    'PGPASSWORD': 'xxxxx'
})



def Extract_Claim_Status(row):
    if row['state'] == 3:
        val = "Closed"
    else:
        val = "Open"
    return val

def Extract_Lob(row):
    if row['claimnumber'][0] == 'H':
        val = "Home_Insurance"
    elif row['claimnumber'][0] == 'M':
        val = "Motor_Insurance"
    else:
        val = "Other"
    return val

def Extract_Report_Date(row):
    try:
        extract_date=re.findall(r"\d{4}-\d{1,2}-\d{1,2}",str(row['reporteddate']))
        return(parse(extract_date[0]))
    except:
        pass
    
def Extract_Close_Date(row):
    try:
        extract_date=re.findall(r"\d{4}-\d{1,2}-\d{1,2}",str(row['closedate']))
        return(parse(extract_date[0]))
    except:
        pass

def Create_Date(row):
    return(row['Extract_Report_Date']+timedelta(days=20))

def Diff_Date(row):
    try:
        return((parse(str(row['Extract_Close_Date'])) - row['Extract_Report_Date']).days)
    except:
        pass
   
def Extract_Tat_Date(row):
    try:
        extract_date=re.findall(r"\d{4}-\d{1,2}-\d{1,2}",str(row['Tat_Date']))
        return(parse(extract_date[0]))
    except:
        pass

# get the input data type info
def get_type(input_data):
    try:
        return type(literal_eval(input_data))
    except (ValueError, SyntaxError):
        return str
    
    
# Function to add business days ( excluding weekends and holidays)
import datetime as dtt
def date_plus_bus_days(from_date, add_days, holidays):
    business_days_to_add = add_days
    current_date = from_date
    while business_days_to_add > 0:
        current_date += dtt.timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5: # EXCLUDING SATURDAY & SUNDAY
            continue
        if current_date in holidays:
            continue
        business_days_to_add -= 1
    return current_date


# List of Holidays in Australia
Au_List_Holidays = [dtt.datetime(2018,1,1),
dtt.datetime(2018,1,26),
dtt.datetime(2018,3,30),
dtt.datetime(2018,3,31),
dtt.datetime(2018,4,1),
dtt.datetime(2018,4,2),
dtt.datetime(2018,4,25),
dtt.datetime(2018,5,7),
dtt.datetime(2018,8,15),
dtt.datetime(2018,10,1),
dtt.datetime(2018,12,25),
dtt.datetime(2018,12,26),
dtt.datetime(2019,1,1),
dtt.datetime(2019,1,28),
dtt.datetime(2019,4,19),
dtt.datetime(2019,4,20),
dtt.datetime(2019,4,21),
dtt.datetime(2019,4,22),
dtt.datetime(2019,4,25),
dtt.datetime(2019,6,10),
dtt.datetime(2019,8,5),
dtt.datetime(2019,10,7),
dtt.datetime(2019,12,25),
dtt.datetime(2019,12,26)]

    

query = "select * from sds.exlpoc_cc_claim;"
result = pd.read_sql(query, engine)

# Results table

result['Status'] = result.apply(Extract_Claim_Status, axis=1)
result['Lob_Name'] = result.apply(Extract_Lob, axis=1)
result['Extract_Report_Date'] = result.apply(Extract_Report_Date, axis=1)
result['Extract_Close_Date'] = result.apply(Extract_Close_Date, axis=1)
# result['Tat_Date'] = result.apply(Create_Date, axis=1)
# Add business days (exluding holidays and weekends)
result['Tat_Date'] = result['reporteddate'].apply(lambda x \
                                                  : date_plus_bus_days(x, 20, Au_List_Holidays))
result['Tat_Date'] = result.apply(Extract_Tat_Date, axis=1)
# Audit date column
result['Audit_Date'] = parse("2019-12-31").date()

# Adding difference between dates
result['Diff_Date'] = result.apply(Diff_Date, axis=1)
result['Flagged_Status']=np.where(result['Diff_Date']>20,1,0)





case = input('Please enter claim number : ')
#print('Claim ID :', case, ' :: Data Type is : ', get_type(case))
test_case=result[result['claimnumber'] == case]
display(test_case)
#print(test_case["id"].tolist()[0])
Adt_Date=(test_case["Audit_Date"].tolist()[0])
#print(type(Adt_Date))
report_date=(test_case["reporteddate"].tolist()[0])



# filtering with query method 
result[result['claimnumber'] == case]
# Function to check for valid user input
def valid_input(prompt):
    while True:
        try:
            value = input(prompt)
            if value.isdigit():
                print("Sorry, your response must not be only number.")
                continue
            if not value:
                raise ValueError('empty string')
            else:
                break
        except ValueError:
            print("Sorry, I didn't understand that.")
            continue
    return value

test_case=result[result['claimnumber'] == case]
test_case_id=test_case["id"].tolist()[0]
# Notes table
cc_history=("select * from sds.ccr_cc_history where claimid in (%s);" %test_case_id)
cc_history_df = pd.read_sql(cc_history, engine)
user_assigned_list=[]
group_assigned_list=[]
assigned_date_list=[]
def cc_history(text,date_ass):
    try:
        if "assigned to user" in text.lower():
            tokens=re.split(r"(\W)",text)
            user_index=tokens.index("user")
            in_index=tokens.index("in")
            user_name="".join(tokens[user_index+1:in_index]).strip()
            if len(user_assigned_list)>0:
                if user_name!=user_assigned_list[len(user_assigned_list)]:
                    user_assigned_list.append(user_name)
                    group_assigned_list.append("".join(tokens[in_index+4:]).strip())
                    assigned_date_list.append(str(date_ass.date()))
            else:
                user_assigned_list.append(user_name)
                group_assigned_list.append("".join(tokens[in_index+4:]).strip())
                assigned_date_list.append(str(date_ass.date()))
    except:pass
des_list=cc_history_df["description"].tolist()
ass_date_list=cc_history_df["eventtimestamp"].tolist()
for i in range(0,len(des_list)):
    cc_history(des_list[i],ass_date_list[i])
# Incident table
cc_incident=("select * from sds.ccr_cc_incident where claimid in (%s);" %test_case_id)
cc_incident = pd.read_sql(cc_incident, engine)
display(cc_incident)
cc_job=("select * from sds.ccr_ccx_sc_job where claimid in (%s);" %test_case_id)
cc_job_df = pd.read_sql(cc_job, engine)
print("Claim Center Job Table Query")
display(cc_job_df,)
#print(cc_job_df["contactid"])
cc_job_contactid_list=cc_job_df["contactid"].tolist()
cc_job_contactid_list=[x for x in cc_job_contactid_list if str(x)!='nan']
test_conactid=cc_job_contactid_list[0]
#print(test_conactid)
try:
    cc_contact=("select sc_tradingname from sds.ccr_cc_contact where id in (%s);" %test_conactid)
    cc_contact_df = pd.read_sql(cc_contact, engine)
    test_trading_name=cc_contact_df['sc_tradingname'][0]
    strategy_test=result['strategy'][0]
except:pass
query_one=("select id,personfirstnamedenorm,personlastnamedenorm from sds.ccr_cc_claimcontact where claimid in (%s);" %test_case_id)
cc_claimcontactid = pd.read_sql(query_one, engine)
#display(cc_claimcontactid)
test_id_list=cc_claimcontactid["id"].tolist()
test_firstname_list=cc_claimcontactid["personfirstnamedenorm"].tolist()
test_lastname_list=cc_claimcontactid["personlastnamedenorm"].tolist()
#print(test_id_list,test_firstname_list,test_lastname_list)
dum_list=[]
for elem in test_id_list:
    query=("select contactid from sds.ccr_cc_claimcontact where id in (%s);" %elem)
    df = pd.read_sql(query, engine)
    dum_list.append(df['contactid'][0])
test_role_list_comb=[]
auth_list=[]
for elem in test_id_list:
    query_two=("select claimcontactid,role from sds.ccr_cc_claimcontactrole  where claimcontactid in (%s);" %elem)
    cc_role = pd.read_sql(query_two, engine)
    query_three=("select id,sc_authparty from sds.ccr_cc_contact where id in (%s);" %elem)
    cc_auth = pd.read_sql(query_three, engine)
    
    query_four=("select fields from sds.ccr_cc_contact where id in (%s);" %elem) 
    cc_auth = pd.read_sql(query_three, engine)
    try:
        auth_list.append(cc_auth['sc_authparty'][0])
    except:
        auth_list.append("None")
        
    test_role_list=cc_role["role"].tolist()
    test_role_list_comb.append(test_role_list)
#print(test_role_list_comb)
#print(auth_list)
role_list=[3,1,4,10019,10040]
contact_id=[]
for i in range(0,len(auth_list)):
    element=auth_list[i]
    if element==None:
        for role in role_list:
            if role in test_role_list_comb[i]:
                contact_id.append(dum_list[i])
                break
#print(contact_id)
contact_id_list=[str(x) for x in contact_id]
query=("select name,lastnamedenorm,firstnamedenorm,cellphone,homephone,workphone,emailaddress1,emailaddress2 from sds.ccr_cc_contact where id in (%s);" 

%(",".join(contact_id_list)))
cc_contact_details = pd.read_sql(query, engine)
print("Authorized Parties Involved and their Contact Details:-")
display(cc_contact_details)
query_one=("select id,personfirstnamedenorm,personlastnamedenorm from sds.ccr_cc_claimcontact where claimid in (%s);" %test_case_id)
cc_claimcontactid = pd.read_sql(query_one, engine)
cc_ids=cc_claimcontactid["id"].tolist()
cc_ids=[str(x) for x in cc_ids]
query=("select contactid from sds.ccr_cc_claimcontact where id in (%s);" %(",".join(cc_ids)))
df2=pd.read_sql(query, engine)
def dif_join(df1,df2,col1,col2):
    df_joined=eval("(pd.merge(%s,%s, left_on='%s',right_on='%s', how='left'))" %(str(df1),str(df2),str(col1),str(col2)))
    df_joined=eval("df_joined.drop(['%s'], axis=1)" %(str(col2)))
    return(df_joined)
def unq_join(df1,df2,col1):
    df_joined=eval("(pd.merge(%s,%s,on='%s',how='left'))" %(str(df1),str(df2),str(col1)))
    return(df_joined)    
cc_claimcontactid["contactid"]=df2["contactid"].tolist()
#display(cc_claimcontactid)
query_one=("select claim_number,tiinteractiontypeid,nvcagentid,dtinteractionlocalstarttime from sds.snp_property_06_13;")
df = pd.read_sql(query_one, engine)
df2=df[df['claim_number']==case]
#display(df2)
agents_list=df2["nvcagentid"].tolist()
agents_list=[str(x) for x in agents_list]
query_one=("select * from sds.avaya_agent_id where nvcagentid in (%s);" % (",".join(agents_list)))
df_agent=pd.read_sql(query_one, engine)
q2="""
select c.claimid, a.employeenumber, a.firstname, a.lastname, c.authorid
from sds.ccr_cc_contact a, sds.ccr_cc_user as b,
sds.ccr_cc_note as c where a.id = b.contactid and b.id= c.authorid and c.claimid= %s;
""" %(test_case_id)
query_df = pd.read_sql(q2, engine)
print("Contact Meta data Fetching Query Results :-")
display(query_df)

# import relevant packages and methods
import fuzzywuzzy as fw
import datefinder as dtf
import pandas as pd
import re
# Notes table
notes_query="select * from sds.EXLPOC_Notes;"
## Test the connection
notes_df = pd.read_sql(notes_query, engine)
subject_keywords=["ibcc","obcc","repeat,call,summary","oboc,email,corro","customer,contact","ccr,ibcc"]
def subject_update(row):
    match_found=0
    try:
        text=row['subject'].lower()
        for keywords in subject_keywords:
            keyword_tokens=re.split(",",keywords)
            text_tokens=re.split(r"(\W)",text)
            if(len(set(keyword_tokens).intersection(set(text_tokens)))==len(keyword_tokens) or keywords.lower() in text.lower()):
                match_found=1
                break
            else:
                match_found=0
        return(match_found)
    except:
        return(match_found)
                

notes_df['Check_Update_subject'] = notes_df.apply(subject_update, axis=1)
display(notes_df.head(1))      
        


hit_list_one=['Customer,Contact',
'IBCC',
'OBCC',
'Repeat,Call,Summary',
'ID,confirmed'
]
hit_list_two=[
'IO',
'Relationship,Claim',
'called',
'customer',
'Received',
'Relationship,Claim',
'call,from'
]
hit_list_three=[
'adv,advised,IB,verified,IBC,Recd,Received,Advised,Called,Advd,Adv,Inbound,IBC,OBC,Caller,Spoke,Call,IB,informed',
'Authorized,Representative,Insured',
'IO,Io,insured,INS,customer',
'Received,Advised,Advd,Adv,Inbound,OBC,Caller,Call,called',
'IB',
'Insured',
'Insured'
]
def body_update(row):
    text=row['body']
    match_found=0
    try:
        for keywords in hit_list_one:
            keyword_tokens=re.split(",",keywords.lower())
            text_tokens=re.split(r"(\W)",text.lower())
            if(len(set(keyword_tokens).intersection(set(text_tokens)))==len(keyword_tokens) or keywords.lower() in text.lower() ):
                match_found=1
                break
            else:
                match_found=0
    except:pass
    try:
        if match_found==0:
            for i in range(0,len(hit_list_two)):
                keyword_tokens=re.split(",",hit_list_two[i].lower())
                text_tokens=re.split(r"(\W)",text.lower())
                text_tokens=[x for x in text_tokens if str(x)!=""]
                
               # print(keyword_tokens,text_tokens,len(set(keyword_tokens).intersection(set(text_tokens)))==len

                if(len(set(keyword_tokens).intersection(set(text_tokens)))==len(keyword_tokens)):
                    indices=[i for i,x in enumerate(text_tokens) if x==str(keyword_tokens[0])]
                    for pos in indices:
                        back_cutoff=max(0,pos-6)
                        forw_cutoff=min(len(text_tokens),pos+6)
                        tokens_sel=(text_tokens[back_cutoff:forw_cutoff])
                        keyword_tokens_one=re.split(",",hit_list_three[i].lower())
                        if(len(set(keyword_tokens_one).intersection(set(tokens_sel)))>0):
                            match_found=1
                            break
                        else:
                            pass

                else:
                    pass
    except:
        pass
            
    return(match_found)
                

notes_df['Check_update_body'] = notes_df.apply(body_update, axis=1)

notes_df['update']=notes_df['Check_update_body']+notes_df['Check_Update_subject']
notes_df['createdate']=notes_df.apply(lambda row: row.createtime.date(),axis=1)
#notes_df['createdate']=np.where(notes_df['update']>0,notes_df['createtime'].date,"dd")
updates_df=notes_df.loc[notes_df['update'] > 0]
updates_df=updates_df[updates_df['claimid'] == test_case_id]
display(updates_df,"dd")
db=updates_df
q2="""
select c.claimid, a.employeenumber, a.firstname, a.lastname, c.authorid
from sds.ccr_cc_contact a, sds.ccr_cc_user as b,
sds.ccr_cc_note as c where a.id = b.contactid and b.id= c.authorid and c.claimid= %s;
""" %(test_case_id)
query_df = pd.read_sql(q2, engine)
query_df=query_df.drop_duplicates()
#display(query_df)
def dif_join(df1,df2,col1,col2):
    df_joined=eval("(pd.merge(%s,%s, left_on='%s',right_on='%s', how='left'))" %(str(df1),str(df2),str(col1),str(col2)))
    df_joined=eval("df_joined.drop(['%s'], axis=1)" %(str(col2)))
    return(df_joined)

def unq_join(df1,df2,col1):
    df_joined=eval("(pd.merge(%s,%s,on='%s',how='left'))" %(str(df1),str(df2),str(col1)))
    return(df_joined)  
db1=db[["authorid","createdate","subject","body","update"]]
pp=unq_join("db1","query_df","authorid")
pp["name"]=pp["firstname"] +" " +pp["lastname"]
query_one=("select * from sds.avaya_agent_id")
df_agent=pd.read_sql(query_one, engine)
#display(df_agent.head(10))
ppp=unq_join("pp","df_agent","name")
import numpy as np
ppp["update_status"]=np.where(ppp['update']>0,"Updated","Not_Updated")
# Import packages
import pandas as pd
import numpy as np
import re
from dateutil.parser import parse
import datetime
from datetime import timedelta
from ast import literal_eval
query_one=("select claim_number,tiinteractiontypeid,nvcagentid,dtinteractionlocalstarttime from sds.snp_property_06_13;")
df = pd.read_sql(query_one, engine)
df2=df[df['claim_number']=="H027114042"]
df2["calldate"]=df2["dtinteractionlocalstarttime"].dt.date
display(df2)
import pandas as pd
ppp["createdate"]=pd.to_datetime(ppp["createdate"])
df2["calldate"]=pd.to_datetime(df2["calldate"])
df2=df2.drop(["dtinteractionlocalstarttime"],axis=1)
df2=df2.drop_duplicates()
display(df2)
dum=pd.merge(ppp,df2, left_on='createdate',right_on='calldate', how='left')
dum["call_check"]=np.where(dum['nvcagentid_x']==dum['nvcagentid_y'],"1","0")
dum=dum.drop(["nvcagentid_y"],axis=1)
display(dum)
    






final_dff=dum[(dum["call_check"]=='1') | (dum["update"]>0)]
dum=dum.sort_values(by="createdate")
#display(dum)
final_dff=final_dff.sort_values(by="createdate")
final_dff['Tat_Date'] = final_dff['createdate'].apply(lambda x \
                                                  : date_plus_bus_days(x, 20, Au_List_Holidays))
employee_ids=final_dff['employeenumber'].tolist()
employee_ids_string=",".join(employee_ids)
responsible_party_list=[]
for elem in employee_ids:
    query="""select name from sds.ccr_cc_group where id in (select groupid from sds.ccr_cc_groupuser where sc_isprimarygroup = 1 AND userid in (select id 

from sds.ccr_cc_user where contactid in (select id from sds.ccr_cc_contact where employeenumber LIKE '%s')));""" %(elem)
    responsible_party=pd.read_sql(query, engine)
    try:
        responsible_party_list.append(responsible_party["name"][0])
    except:responsible_party_list.append("None")

final_dff['responsible_party']=responsible_party_list
display(final_dff)



# import relevant packages and methods
import fuzzywuzzy as fw
import datefinder as dtf
import pandas as pd
import numpy as np
import re



def EngineFireUp():
    
    # Setting up and establishing connection with redshift

    from sqlalchemy.engine import (
        url as sa_url,
        Engine as db_Engine,
        Connection as db_Connection,
    )
    from sqlalchemy import create_engine as sa_create_engine
    from typing import Dict, List
    import pandas as pd

    def get_db_engine(config: Dict = {}) -> db_Engine:
        __DRIVERNAME = config.get("database_driver", "redshift+psycopg2")
        __PORT = config.get("PGPORT", 5555)
        __USER = config.get("PGUSER", None)
        __DATABASE = config.get("PGDATABASE", "dev")
        __HOST = config.get(
            "PGHOST", "xxxxxx.redshift.amazonaws.com"
        )
        __PASSWORD = config.get("PGPASSWORD", None)

        if not __USER or not __PASSWORD:
            raise ValueError("PGUSER/PGPASSWORD is not defined in config and environment.")

        db_url = sa_url.URL(
            drivername=__DRIVERNAME,
            username=__USER,
            password=__PASSWORD,
            host=__HOST,
            port=__PORT,
            database=__DATABASE,
        )
        db_engine = sa_create_engine(db_url, connect_args={"sslmode": "verify-full"})

        return db_engine


    # Establish connection for user Manoj
    engine = get_db_engine(config = {
        'PGUSER': 'xxxxx',
        'PGPASSWORD': 'xxxxxx'
    })
    
    return(engine)

engine = EngineFireUp()

sqlQ = """select createtime, claimid, body from sds.ccr_cc_note where body is not null and claimid in 
(14352779, 14363312, 14356809, 14406184, 14366658, 14435665, 14372972, 14459030, 14467757, 14458694, 14468782,
14472256, 14410756, 14468626, 14463351, 14468940, 14468048, 14475301, 14458272, 14481784, 14474674, 14482590,
14472244, 14475251, 14478443, 14453088, 14455079, 14464551, 14466435, 14465853, 14463837, 14471497, 14469350,
14468959, 14482043, 14476096, 14467434, 14478434, 14463618, 14477751, 14474705, 14465023, 14477865, 14479027,
14477982, 14476931, 14475486, 14475968, 14476260, 14479471, 14482546, 14467839, 14470084, 14470646, 14478235,
14440302, 14356439, 14346881, 14483578, 14372314, 14373385, 14427820, 14381135, 14431241, 14455025, 14454168,
14392356, 14454348, 14455385, 14458915, 14445328, 14461754, 14470064, 14472541, 14465020, 14470681, 14469234,
14473731, 14467822, 14475679, 14477141, 14477702, 14475687, 14463368, 14478437, 14477496, 14437160, 14483572,
14435664, 14478419, 14477458, 14470015, 14455916, 14456282, 14456166, 14466036, 14467824, 14470110, 14475705,
14476310, 14477425, 14475009, 14474814, 14476103, 14478429, 14480950, 14480954, 14348315, 14478138, 14465021,
14469275, 14483560, 14469679, 14477429, 14472085, 14481289, 14477293, 14470796, 14459510, 14346463, 14375074,
14393436, 14382005, 14352258, 14450875, 14369067, 14455168, 14445190, 14393407, 14461584, 14393605, 14470823,
14474786, 14476283, 14475401, 14476002, 14482323, 14463481, 14448004, 14438157, 14481124, 14456405, 14460693,
14459757, 14463850, 14467920, 14459726, 14466138, 14467429, 14459053, 14470314, 14463846, 14474802, 14475255,
14477732, 14458967, 14459447, 14481087, 14464027, 14480282, 14368756, 14475181, 14439029, 14475975, 14369417,
14477718, 14478126, 14372548, 14346598, 14397508, 14359599, 14453644, 14465637, 14475386, 14474795, 14464735,
14482245, 14476404, 14481593, 14478526, 14466651, 14444370, 14455345, 14443181, 14453788, 14464158, 14465333,
14468300, 14471643, 14474804, 14475469, 14475472, 14477333, 14477434, 14480624, 14482335, 14482217, 14482220,
14481516, 14476513, 14372277, 14483842, 14459070, 14478843) order by claimid, id"""


notes_df = pd.read_sql(sqlQ, engine)
## Clean the text

def CleanTxt(df, col):
    import pandas as pd
    
    # dictioanry for replacement
    remove_string = {"rn":" ", "rnr":"\n", "rnrn":"\n", "rnrnrn":"\n", "rn-":"\n","SummarynClaim":"Summary \nClaim", 
                     "ACTIONnRepairer":"ACTION\nRepairer", ".com.aun":".com.au", "nEmail:":"\nEmail:",
                    "nCorrespondence":"\nCorrespondence", "nFax:":"\nFax:", "nMail:":"\nMail:", "nClaim":"\nClaim",
                    "nAwait":"\nAwait", "nNEXT":"\nNEXT", "rn-           ": "\n", "rnLA": "LA", "trn":"t\n",
                     "FITrnXS$100":"FIT XS $100", "paidrnEst":"paid Est", "rnIf":"If", "bevlauedrnrnnext":"bevlaued next", 
                     "$1000rnReason":"$1000 \nReason", "srn":"s\n", "ACTIONnPayment":"ACTION\nPayment",
                     "18rnPolicy":"18 \nPolicy", "\(if yes,":"\n(if yes,", "SummarynName":"Summary\nName"}

    #sorting by keys for list of tuples 
    rem = sorted(remove_string.items(), key=lambda s: len(s[0]), reverse=True)

    df['cleaned_txt'] = df[col]

    for i, j in rem:
        df[i] = df['cleaned_txt'].str.extract('({})'.format(i)) 
        df['cleaned_txt'] = df['cleaned_txt'].str.replace(i, j)

    cols = list(remove_string.keys())
    df['Removed_string'] = (df[cols].notna().dot(pd.Index(cols) + ',')
                                            .str.strip(','))

    df = df.drop(remove_string, axis=1)
    
    return(df)



# function to find status on claim (Claim Finalization)

hit_list_two=[
"Claim",
"IO",
"Survey","complete"
]

hit_list_three=[
"finalised,closed,paid,happy,closing",
"happy",
"Customer,Service,complete",
"job, work, works"
]

def body_update(row):
    text=row['cleaned_txt']
    match_found=0
    try:
        if match_found==0:
            for i in range(0,len(hit_list_two)):
                keyword_tokens=re.split(",",hit_list_two[i].lower())
                text_tokens=re.split(r"(\W)",text.lower())
                text_tokens=[x for x in text_tokens if str(x)!=""]
                
               # print(keyword_tokens,text_tokens,len(set(keyword_tokens).intersection(set(text_tokens)))==len

                if(len(set(keyword_tokens).intersection(set(text_tokens)))==len(keyword_tokens)):
                    indices=[i for i,x in enumerate(text_tokens) if x==str(keyword_tokens[0])]
                    for pos in indices:
                        back_cutoff=max(0,pos-6)
                        forw_cutoff=min(len(text_tokens),pos+6)
                        tokens_sel=(text_tokens[back_cutoff:forw_cutoff])
                        keyword_tokens_one=re.split(",",hit_list_three[i].lower())
                        if(len(set(keyword_tokens_one).intersection(set(tokens_sel)))>0):
                            match_found=1
                            break
                        else:
                            pass
                else:
                    pass
    except:
        pass            
    return(match_found)



#notes_df = CleanTxt(notes_df, "body")
#notes_df['Check_claim_final'] = notes_df.apply(body_update, axis=1)
def ChkFindDate(df, id):
    df = CleanTxt(df, "body")
    df['Check_claim_final'] = df.apply(body_update, axis=1)
    df1 = df[df['Check_claim_final']==1]
    df2 = df1[df1['claimid'] == int(id)]
    df2 = df2.sort_values(by = 'createtime')
    date_vec=df2["createtime"].tolist()
    date_vec=[x.date() for x in date_vec]
    date_vec_sorted=sorted(date_vec)

    if df2.empty:
        return None
    else:
        return(date_vec_sorted[len(date_vec_sorted)-1])
    
final_date=ChkFindDate(notes_df,test_case_id)
print("Claim Finalized Date :-",final_date)
date_vec=final_dff['createdate'].tolist()
date_vec_tat=final_dff['Tat_Date'].tolist()
date_vec=sorted(date_vec)
date_vec_tat=sorted(date_vec_tat)
report_date_list=[]
report_date_list.append(report_date)
date_vec=report_date_list+date_vec
date_vec=[x.date() for x in date_vec]
date_vec.append(Adt_Date)
#print(date_vec,report_date)






result=0
stop=0
#print(final_date)
for i in range(1,len(date_vec)-1):
    if date_vec[i+1]<final_date:
        if((date_vec[i+1]-date_vec[i]).days>=20 and (date_vec[i+1]>date_vec_tat[i-1].date())):
            result=1
            #print("Condition Failed at :-",date_vec[i],date_vec[i+1],date_vec_tat[i-1].date(),"by:-",responsible_party_list[i-1])
            print("Condition Failed at :-",date_vec[i+1],"by:-",(date_vec[i+1]-date_vec[i]).days,"days by party :-",responsible_party_list[i-1])
        else:pass
            #print("Met")
    try:
        if(date_vec[i+1]>final_date):

            stop=1
    except:
        pass
        
if result==1:
    print("Final Output is Not- Met :-- Move this case for Manual Review ")
else:
    print("Final Output is Met")


























