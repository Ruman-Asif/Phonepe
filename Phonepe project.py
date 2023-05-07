
#------------------ extraction of Data from various folderds------------------------------------

#table1---------------------
import os
import pandas as pd
import json

list_of_states_atxn_path=[]
list_of_states_atxn=[]
rootdir = 'C:\\Users\\Ruman Asif\\Documents\\Personal\\guvi\\projects\\project2\\pulse\\data\\aggregated\\transaction\\country\\india\\state'
for file in os.listdir(rootdir):
    list_of_states_atxn.append(file)
    d = os.path.join(rootdir, file)
    if os.path.isdir(d):
        list_of_states_atxn_path.append(d)

years=[2018,2019,2020,2021,2022]

df1=pd.DataFrame()
for k in range(len(list_of_states_atxn_path)):
    for i in range(len(years)):
        for j in range(1,5):
            f=open(f"{list_of_states_atxn_path[k]}\\{years[i]}\\{j}.json") #dynamically collecting data
            data=json.load(f)
            df = pd.json_normalize(data["data"]["transactionData"], record_path=["paymentInstruments"], meta=['name'])
            df["State"]=list_of_states_atxn[k]
            df["Year"] = years[i]  # year added
            df["Quarter"] = j    # quarter assigned
            df1=pd.concat([df1, df],ignore_index=True)

df2=df1.iloc[:,[3,0,1,2,4,5,6]]

e={'andhra-pradesh': 'Andhra Pradesh', 'arunachal-pradesh': 'Arunachal Pradesh',
   'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh', 'chhattisgarh': 'Chhattisgarh',
   'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
   'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana',
   'himachal-pradesh': 'Himachal Pradesh', 'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand',
   'karnataka': 'Karnataka', 'kerala': 'Kerala', 'ladakh': 'Ladakh', 'madhya-pradesh': 'Madhya Pradesh',
   'maharashtra': 'Maharashtra', 'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram',
   'nagaland': 'Nagaland', 'odisha': 'Odisha', 'puducherry': 'Puducherry', 'punjab': 'Punjab',
   'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu', 'telangana': 'Telangana',
   'tripura': 'Tripura', 'uttar-pradesh': 'Uttarakhand', 'uttarakhand': 'Uttar Pradesh',
   'west-bengal': 'West Bengal', 'lakshadweep': 'Lakshadweep','andaman-&-nicobar-islands':'Andaman & Nicobar'}

index_statecol=df2.columns.get_loc('State')

for i in range(df2["State"].size):
   for beach in e:
      if(df2.iloc[i, index_statecol]==beach):
         df2.iloc[i, index_statecol]=e[beach]

df2 = df2.rename(columns={'name': 'Type of transaction'})  #Renaming the column name appropriately

# print(df2.head(),df2.tail()) #Uncomment to Check if dataframe created correctly
print("Aggregate data of transaction according to states created, pushing data to sql DB PhonePe1")

from sqlalchemy import create_engine

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="serendipity",
auth_plugin='mysql_native_password'
)

mycursor = mydb.cursor()
# mycursor.execute("CREATE DATABASE Phonepe1")  #DB to be created only once
mycursor.execute("USE Phonepe1")
mycursor.execute("drop table agg_txn_st")   #if running code again uncomment this and run
mycursor.execute("CREATE TABLE agg_txn_st ( name varchar(255),type varchar(255), count int, amount int, State varchar(255), Year int, Quarter int  )")

engine=create_engine('mysql+pymysql://root:serendipity@localhost/Phonepe1')
df2.to_sql('agg_txn_st',engine,if_exists='replace',index=False)
print("The data has been stored in database in table called --> ""agg_txn_st""")

# 2nd table starts here------------------------

list_of_states_atxn_path=[]
list_of_states_atxn=[]
rootdir = 'C:\\Users\\Ruman Asif\\Documents\\Personal\\guvi\\projects\\project2\\pulse\\data\\aggregated\\user\\country\\india\\state'
for file in os.listdir(rootdir):
    list_of_states_atxn.append(file)
    d = os.path.join(rootdir, file)


    if os.path.isdir(d):
        list_of_states_atxn_path.append(d)
# # print(list_of_states_atxn)
# print(list_of_states_atxn)


years=[2018,2019,2020,2021]
df1=pd.DataFrame()
for k in range(len(list_of_states_atxn_path)):
    for i in range(len(years)):
        for j in range(1,5):
            f=open(f"{list_of_states_atxn_path[k]}\\{years[i]}\\{j}.json") #dynamically collecting data
            data=json.load(f)
            df=pd.json_normalize(data["data"]["usersByDevice"])
            df["State"]=list_of_states_atxn[k]
            df["Year"] = years[i]  # year added
            df["Quarter"] = j    # quarter assigned
            df1=pd.concat([df1, df],ignore_index=True)

df2=df1.iloc[:,[3,0,1,2,4,5]]

e={'andhra-pradesh': 'Andhra Pradesh', 'arunachal-pradesh': 'Arunachal Pradesh',
   'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh', 'chhattisgarh': 'Chhattisgarh',
   'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
   'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana',
   'himachal-pradesh': 'Himachal Pradesh', 'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand',
   'karnataka': 'Karnataka', 'kerala': 'Kerala', 'ladakh': 'Ladakh', 'madhya-pradesh': 'Madhya Pradesh',
   'maharashtra': 'Maharashtra', 'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram',
   'nagaland': 'Nagaland', 'odisha': 'Odisha', 'puducherry': 'Puducherry', 'punjab': 'Punjab',
   'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu', 'telangana': 'Telangana',
   'tripura': 'Tripura', 'uttar-pradesh': 'Uttarakhand', 'uttarakhand': 'Uttar Pradesh',
   'west-bengal': 'West Bengal', 'lakshadweep': 'Lakshadweep','andaman-&-nicobar-islands':'Andaman & Nicobar'}

index_statecol=df2.columns.get_loc('State')

for i in range(df2["State"].size):
   for beach in e:
      if(df2.iloc[i, index_statecol]==beach):
         df2.iloc[i, index_statecol]=e[beach]


print("Aggregate data of ''USER'' according to states created, pushing data to sql DB PhonePe1")

from sqlalchemy import create_engine

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="serendipity",
auth_plugin='mysql_native_password'
)

mycursor = mydb.cursor()
# mycursor.execute("CREATE DATABASE Phonepe1")  #DB to be created only once
mycursor.execute("USE Phonepe1")
mycursor.execute("drop table agg_user_st")   #if running code again uncomment this and run
mycursor.execute("CREATE TABLE agg_user_st ( State varchar(255), Brand varchar(255),Count int, percentage int,  Year int, Quarter int  )")

engine=create_engine('mysql+pymysql://root:serendipity@localhost/Phonepe1')
df2.to_sql('agg_user_st',engine,if_exists='replace',index=False)
print("The data has been stored in database in table called --> ""agg_user_st""")
print("")

print("")


# group_by_state_data_agg_txn_st=df2.groupby('State')['amount'].sum().reset_index()

#table 3 top_user_ns data pulling from folders to df to sql here--------------

import pandas as pd
import json
years_top_user_ns=[2018,2019,2020,2021,2022]
commonpath="C:\\Users\\Ruman Asif\\Documents\\Personal\\guvi\\projects\\project2\\pulse\\data\\top\\user\\country\\india\\"
df1_top_user_ns=pd.DataFrame()
for i in range(len(years_top_user_ns)):
    for j in range(1,5):
        f=open(f"{commonpath}{years_top_user_ns[i]}\\{j}.json")
        data=json.load(f)
        # print(data)
        df_top_user_ns = pd.json_normalize(data["data"]["states"])
        # if(i!=2018 and j!=1):
        # print(df,df1)
        # print(j)
        df_top_user_ns["Year"]=years_top_user_ns[i]
        df_top_user_ns["Quarter"] = j
        df1_top_user_ns=pd.concat([df1_top_user_ns, df_top_user_ns],ignore_index=True)
        # print(j)
        # df1["Quarter"]=j
        # df1.insert(4, "qtr",j, True)
        # df1 = df1.assign(qtr=j)
# df2_top_user_ns=df1_top_user_ns.iloc[:,[3,0,1,2,4,5]]
# df1_top_user_ns.to_csv("top_user_ns1.csv")

df1_top_user_ns = df1_top_user_ns.rename(columns={'name': 'State'})  #Renaming the column name appropriately

e={'andhra-pradesh': 'Andhra Pradesh', 'arunachal-pradesh': 'Arunachal Pradesh',
   'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh', 'chhattisgarh': 'Chhattisgarh',
   'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
   'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana',
   'himachal-pradesh': 'Himachal Pradesh', 'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand',
   'karnataka': 'Karnataka', 'kerala': 'Kerala', 'ladakh': 'Ladakh', 'madhya-pradesh': 'Madhya Pradesh',
   'maharashtra': 'Maharashtra', 'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram',
   'nagaland': 'Nagaland', 'odisha': 'Odisha', 'puducherry': 'Puducherry', 'punjab': 'Punjab',
   'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu', 'telangana': 'Telangana',
   'tripura': 'Tripura', 'uttar-pradesh': 'Uttarakhand', 'uttarakhand': 'Uttar Pradesh',
   'west-bengal': 'West Bengal', 'lakshadweep': 'Lakshadweep','andaman-&-nicobar-islands':'Andaman & Nicobar'}

index_statecol=df1_top_user_ns.columns.get_loc('State')

for i in range(df1_top_user_ns["State"].size):
   for beach in e:
      if(df1_top_user_ns.iloc[i, index_statecol]==beach):
         df1_top_user_ns.iloc[i, index_statecol]=e[beach]

# df1_top_user_ns.to_csv("top_user_ns2.csv")
# print(df1_top_user_ns.head(),df1_top_user_ns.tail())
print("Top user data from different states pulled, now pushing data to mysql")
import pymysql
from sqlalchemy import create_engine

import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="serendipity",
auth_plugin='mysql_native_password'
)

mycursor = mydb.cursor()
# mycursor.execute("CREATE DATABASE Phonepe1")
mycursor.execute("USE Phonepe1")
mycursor.execute("drop table top_user_ns")
mycursor.execute("CREATE TABLE top_user_ns ( State varchar(255),registeredUsers int , Year int, Quarter int  )")

engine=create_engine('mysql+pymysql://root:serendipity@localhost/Phonepe1')
df1_top_user_ns.to_sql('top_user_ns',engine,if_exists='replace',index=False)

print("Table 3 created in sql db phonepe1 which is----> top_user_ns")
print("")

print("Now creating------> geo spatial graph of transactions in India in different states")

#importing data from sql tables--------------------

#table 1 data pulling

engine=create_engine('mysql+pymysql://root:serendipity@localhost/Phonepe1')
myQuery='''SELECT * FROM agg_txn_st'''
df2 = pd.read_sql_query(myQuery, engine)

#table 2 data pulling

engine = create_engine('mysql+pymysql://root:serendipity@localhost/Phonepe1')
myQuery = '''SELECT * FROM agg_user_st'''
df2_agg_user = pd.read_sql_query(myQuery, engine)

#table 3 data pulling from sql

engine = create_engine('mysql+pymysql://root:serendipity@localhost/Phonepe1')
myQuery = '''SELECT * FROM top_user_ns'''
df1_top_user_ns = pd.read_sql_query(myQuery, engine)


#Streamlit part starts from here------------------------------------

#Aggregate transaction statewise data is reported here(geo spatial)

import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Ruman's Phonepe app",page_icon='phonepe-logo-icon.png',layout='wide')
st.header("Analysis of PhonePe data!")
st.subheader("Hello there! Let's look how PhonePe company has performed in India")
st.write("[Source of my data >](https://github.com/PhonePe/pulse)")

with st.container():
    st.write("---")
    left_column, right_column=st.columns(2)
    with left_column:
        st.subheader('Below graph shows how each state fared according to type of transactions')

        options_amt_or_cnt = ['amount', 'count']
        # op_txn_type=list(df2["Type of transaction"].values)
        op_txn_type=['Recharge & bill payments','Peer-to-peer payments','Merchant payments','Financial Services','Others']
        op_agg_year = years
        op_quarter = [1, 2, 3, 4]

        selected_option_amt_or_cnt = st.selectbox('Transactions by Amount or Count', options_amt_or_cnt)
        selected_option_txn_type = st.multiselect('Choose the type of transactions, you can choose multiple',
                                                  op_txn_type, default=['Recharge & bill payments'])
        selected_option_agg_year = st.multiselect('Choose one or more years', op_agg_year, default=[2018])
        selected_option_agg_qtr = st.multiselect('Choose one or more quarters', op_quarter, default=[1])

        df3=df2.loc[(df2['Type of transaction'].isin(selected_option_txn_type)) & (df2['Year'].isin(selected_option_agg_year)) & (df2['Quarter'].isin(selected_option_agg_qtr))]

        group_by_state_data_agg_txn_st=df3.groupby('State')[selected_option_amt_or_cnt].sum().reset_index()

        fig1 = px.choropleth(
            group_by_state_data_agg_txn_st,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color=selected_option_amt_or_cnt,
            color_continuous_scale='purples'
        )

        fig1.update_geos(fitbounds="locations", visible=False)
        # st.write(group_by_state_data_agg_txn_st) # if needed to see dataframe
        st.plotly_chart(fig1)


    with right_column:


        # st.subheader('You can find out how the states fared with respect to the below options')
        st.subheader('Below graph shows you how each state fared according to mobile device, year and quarter')

        options_ct_per_agg_user = ['percentage', 'count']
        # op_txn_type=list(df2["Type of transaction"].values)
        op_agg_user_brand = df2_agg_user["brand"].unique()
        op_agg_year_user = [2018,2019,2020,2021]
        op_quarter_agg_user = [1, 2, 3, 4]

        selected_option_ct_or_per_agg_user = st.selectbox('Transactions by percentage or Count', options_ct_per_agg_user)
        selected_option_brand_agg_user = st.multiselect('Choose brand, you can choose multiple',
                                                  op_agg_user_brand, default=['Xiaomi'])
        selected_option_agg_year_user = st.multiselect('Choose one or more years', op_agg_year_user, default=[2018],key=1)
        selected_option_agg_qtr_user = st.multiselect('Choose one or more quarters', op_quarter_agg_user, default=[1],key=2)

        df3_agg_user = df2_agg_user.loc[(df2_agg_user['brand'].isin(selected_option_brand_agg_user)) & (
            df2_agg_user['Year'].isin(selected_option_agg_year_user)) & (df2_agg_user['Quarter'].isin(selected_option_agg_qtr_user))]

        group_by_state_data_agg_user = df3_agg_user.groupby('State')[selected_option_ct_or_per_agg_user].sum().reset_index()

        fig2 = px.choropleth(
            group_by_state_data_agg_user,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color=selected_option_ct_or_per_agg_user,
            color_continuous_scale='reds'
        )

        fig2.update_geos(fitbounds="locations", visible=False)
        # st.write(group_by_state_data_agg_txn_st) # if needed to see dataframe
        st.plotly_chart(fig2)

#Pie charts created here for more insights-----------------

with st.container():
    typeoftxn_share, phones_share_user_agg = st.columns(2)
    with typeoftxn_share:
        st.subheader('Which type of financial transactions were done the heighest shown below')
        pie_options_year_agg_txn=years
        pie_selected_option_agg_txn_year = st.multiselect('Choose one or more years', pie_options_year_agg_txn, default=[2018],key=3)
        df3_pie_agg_txn = df2.loc[df2['Year'].isin(pie_selected_option_agg_txn_year)]
        group_by_txntype_for_pie_agg_txn=df3_pie_agg_txn.groupby('Type of transaction')['count'].sum().reset_index()
                                         #df3_agg_user.groupby('State')[selected_option_ct_or_per_agg_user].sum().reset_index()
        # fig3 = px.pie(df2.isin(pie_selected_option_agg_txn_year), values='count', names='Type of transaction', title='Pie chart of transaction types')
        fig3 = px.pie(group_by_txntype_for_pie_agg_txn, values='count', names='Type of transaction', title='Pie chart of transaction types')

        st.plotly_chart(fig3)
    with phones_share_user_agg:
        st.subheader('Which brand of phones is used the highest by Phonepe app')
        pie_options_year_agg_user = [2018,2019,2020,2021]
        pie_selected_option_agg_user_year = st.multiselect('Choose one or more years', pie_options_year_agg_user,
                                                          default=[2018], key=4)
        df3_pie_agg_user = df2_agg_user.loc[df2_agg_user['Year'].isin(pie_selected_option_agg_txn_year)]
        group_by_txntype_for_pie_agg_user = df3_pie_agg_user.groupby('brand')['count'].sum().reset_index()

        fig4 = px.pie(group_by_txntype_for_pie_agg_user, values='count', names='brand',
                      title='Pie chart of phone brands')
        st.plotly_chart(fig4)

# Bar chart for top user stares------------------------

with st.container():
    col1,col2,col3 = st.columns(3)
    with col2:
        st.subheader("The top performing states in terms of number of registered users:")
        top_user_ns_op_year = [2018, 2019, 2020, 2021]
        top_user_ns_year_selected = st.multiselect('Choose one or more years', top_user_ns_op_year,
                                                       default=[2018], key=5)
        df2_top_user_ns = df1_top_user_ns.loc[df1_top_user_ns['Year'].isin(top_user_ns_year_selected)]
        fig5 = px.bar(df2_top_user_ns, x="State", y="registeredUsers", title="Top performing states")
        st.plotly_chart(fig5)

print("Thank you for trying my Phone pe App, please check streamlit for data visualization")
print("Created by Ruman Asif, DW45, Guvi")


#Program ends here and is maintainable further----------------------------------------------------------------------








