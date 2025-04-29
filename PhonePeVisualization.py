import streamlit as st
import pandas as pd
import numpy as np
import os
import json
import mysql.connector as db
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import warnings
warnings.filterwarnings('ignore')

#Create MySQL connection

user ='PhonePeTransaction'
password='ZXCmnb$3191'
host='localhost'
db_name='phonepetrans'
db_Connection=db.connect(host=host, user=user, password=password, database=db_name)
curr=db_Connection.cursor()

# ****1**** Decoding Transaction Dynamics on PhonePe
#1 Find total count and amount for each Transaction_type.
sql="""SELECT Transaction_type,
       SUM(Transaction_count) AS Total_Transactions,
       SUM(Transaction_amount) AS Total_Amount
FROM  phonepetrans.agg_transaction
GROUP BY Transaction_type
ORDER BY Total_Amount DESC;"""
curr.execute(sql)
TransData = curr.fetchall()
TransDataTable = pd.DataFrame(TransData, columns = curr.column_names)

#2 Find total count and amount for each state.
sql="""SELECT State,
       SUM(Transaction_count) AS Total_Transactions,
       SUM(Transaction_amount) AS Total_Amount
FROM  phonepetrans.agg_transaction
GROUP BY State
ORDER BY Total_Transactions DESC;"""
curr.execute(sql)
StateData = curr.fetchall()
StateDataTable = pd.DataFrame(StateData, columns = curr.column_names)

#3 Find total count and amount for each Quarter.
sql="""SELECT Quater,
       SUM(Transaction_count) AS Total_Transactions,
       SUM(Transaction_amount) AS Total_Amount
FROM agg_transaction
GROUP BY Quater
ORDER BY Total_Transactions DESC;"""
curr.execute(sql)
QuaterData = curr.fetchall()
QuaterDataTable = pd.DataFrame(QuaterData, columns = curr.column_names)
quarter_labels = {1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}
QuaterDataTable['Quater_Label'] = QuaterDataTable['Quater'].map(quarter_labels)

#4 Find total count and amount for each Year.
sql="""SELECT Year,
       SUM(Transaction_count) AS Total_Transactions,
       SUM(Transaction_amount) AS Total_Amount
FROM agg_transaction
GROUP BY Year
ORDER BY Total_Transactions DESC;"""
curr.execute(sql)
YearData = curr.fetchall()
YearDataTable = pd.DataFrame(YearData, columns = curr.column_names)

#5 Find total amount for each Transaction type and quater.
sql="""SELECT Transaction_type, Quater, SUM(Transaction_count) AS Total_count
FROM agg_transaction
GROUP BY Transaction_type, Quater
ORDER BY Transaction_type, Quater;"""
curr.execute(sql)
Trans_Dec_Data = curr.fetchall()
Trans_Dec_DataTable = pd.DataFrame(Trans_Dec_Data, columns = curr.column_names)
quarter_label = {1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}
Trans_Dec_DataTable['Quater_Label'] = Trans_Dec_DataTable['Quater'].map(quarter_label)

# ****2**** Device Dominance and User Engagement Analysis
#6 Top Brands by Total Usage across all states
sql="""SELECT Brand, SUM(BrandCount) AS TotalUsers
FROM agg_users
WHERE Brand IS NOT NULL AND Brand != 'NA'
GROUP BY Brand
ORDER BY TotalUsers DESC;"""
curr.execute(sql)
BrandData = curr.fetchall()
BrandDataTable = pd.DataFrame(BrandData, columns = curr.column_names)

#7 Total brand counts based on each region
sql="""SELECT State, Brand, SUM(BrandCount) AS BrandUsers
FROM agg_users
WHERE Brand IS NOT NULL AND Brand != 'NA'
GROUP BY State, Brand
ORDER BY State, BrandUsers DESC;"""
curr.execute(sql)
Reg_BrandData = curr.fetchall()
Reg_BrandDataTable = pd.DataFrame(Reg_BrandData, columns = curr.column_names)

#8 Sum of each brand counts based on year and quater
sql="""SELECT Year, Brand, SUM(BrandCount) AS TotalBrandUsers
FROM agg_users
WHERE Brand IS NOT NULL AND Brand != 'NA'
GROUP BY Year, Brand
ORDER BY Year, TotalBrandUsers DESC;"""
curr.execute(sql)
YrQtrData = curr.fetchall()
YrQtrDataDataTable = pd.DataFrame(YrQtrData, columns = curr.column_names)

#9 User Engagement by Region (App Opens/per User)
sql="""WITH DisUsers AS(
SELECT 
    State, Year,
    Quater, 
    MAX(DISTINCT RegisteredUsers) AS RegUsers,
    MAX(DISTINCT AppOpens) AS AppOpens
FROM 
    agg_users
GROUP BY 
    State, Year, Quater
)
SELECT State, 
       SUM(RegUsers) AS TotalUsers, 
       SUM(AppOpens) AS TotalAppOpens, 
       ROUND(SUM(AppOpens) / SUM(RegUsers), 2) AS EngagementRate
FROM DisUsers
GROUP BY State
ORDER BY EngagementRate DESC;"""
curr.execute(sql)
EngagementData = curr.fetchall()
EngagementDataTable = pd.DataFrame(EngagementData, columns = curr.column_names)

# ****3**** User Engagement and Growth Strategy
#10 Total Registered Users & App Opens per State
sql="""SELECT 
    State,
    SUM(RegisteredUsers) AS TotalRegisteredUsers,
    SUM(AppOpens) AS TotalAppOpens,
    ROUND(SUM(AppOpens)/SUM(RegisteredUsers), 2) AS EngagementRatio
FROM map_users
GROUP BY State
ORDER BY TotalAppOpens DESC;"""
curr.execute(sql)
Map_User_EngData = curr.fetchall()
Map_User_EngDataTable = pd.DataFrame(Map_User_EngData, columns = curr.column_names)

#11 Districts by RegisteredUsers and App Opens based on each states.
sql="""SELECT 
    State,
    StateDistrict,
    SUM(RegisteredUsers) AS TotalUsers,
    SUM(AppOpens) AS AppOpens,
    ROUND(SUM(AppOpens)/SUM(RegisteredUsers), 2) AS EngagementRatio
FROM map_users
GROUP BY State, StateDistrict
ORDER BY TotalUsers DESC;"""
curr.execute(sql)
Map_Dis_Data = curr.fetchall()
Map_Dis_DataTable = pd.DataFrame(Map_Dis_Data, columns = curr.column_names)

#12 Top district based on number of users and total Apps Opened.
sql="""SELECT 
    State,
    StateDistrict,
    SUM(RegisteredUsers) AS RegisteredUsers,
    SUM(AppOpens) AS TotalAppOpens
FROM map_users
GROUP BY State, StateDistrict
ORDER BY RegisteredUsers DESC;"""
curr.execute(sql)
Map_TopDis_Data = curr.fetchall()
Map_TopDis_DataTable = pd.DataFrame(Map_TopDis_Data, columns = curr.column_names)

#13 Districts with Highest Engagement Ratio
sql="""SELECT 
    State,
    StateDistrict,
    SUM(RegisteredUsers) AS TotalRegisUsers,
    SUM(AppOpens) AS AppOpens,
    ROUND(SUM(AppOpens)/SUM(RegisteredUsers), 2) AS EngagementRatio
FROM map_users
GROUP BY State, StateDistrict
-- HAVING TotalRegisUsers > 100000  -- filter to ignore low population districts
ORDER BY EngagementRatio DESC
LIMIT 10;"""
curr.execute(sql)
Map_DisRatio_Data = curr.fetchall()
Map_DisRatio_DataTable = pd.DataFrame(Map_DisRatio_Data, columns = curr.column_names)

# ****4**** Insurance Engagement Analysis
#14 Total transactions and amount by state
sql="""SELECT 
    State,
    SUM(Transaction_count) AS Total_Transactions,
    SUM(Transaction_amount) AS Total_Amount
FROM map_transaction
GROUP BY State
ORDER BY Total_Amount DESC;"""
curr.execute(sql)
Map_INSamount_Data = curr.fetchall()
Map_INSamount_DataTable = pd.DataFrame(Map_INSamount_Data, columns = curr.column_names)

#15 Total transactions and average transaction value by district
sql="""SELECT 
    StateDistrict,
    SUM(Transaction_count) AS Total_Transactions,
    SUM(Transaction_amount) AS Total_Amount,
    (SUM(Transaction_amount) / SUM(Transaction_Count)) AS Avg_Transaction_Value
FROM map_transaction
GROUP BY StateDistrict
ORDER BY Total_Transactions DESC;"""
curr.execute(sql)
Map_AvgTrans_Data = curr.fetchall()
Map_AvgTrans_DataTable = pd.DataFrame(Map_AvgTrans_Data, columns = curr.column_names)

#16 Quarterly growth trend for a state
sql="""SELECT 
    State,
    Quater,
    SUM(Transaction_count) AS Total_Transactions,
    SUM(Transaction_amount) AS Total_Amount
FROM map_transaction
GROUP BY State, Quater
ORDER BY State, Quater;"""
curr.execute(sql)
Map_QrtTrend_Data = curr.fetchall()
Map_QrtTrend_DataTable = pd.DataFrame(Map_QrtTrend_Data, columns = curr.column_names)
quart_labels = {1: 'Q1', 2: 'Q2', 3: 'Q3', 4: 'Q4'}
Map_QrtTrend_DataTable['Quater_Label'] = Map_QrtTrend_DataTable['Quater'].map(quart_labels)

State_DF = pd.DataFrame(Map_QrtTrend_DataTable['State'].unique(), columns=['State'])

def plot_transactions_by_quarter(state_name):
    filtered_df = Map_QrtTrend_DataTable[Map_QrtTrend_DataTable['State'] == state_name]
    fig = px.bar(filtered_df,
                 x="Quater_Label", y="Total_Transactions",
                 text="Total_Transactions",
                 title=f"Total Transactions by Quarter in {state_name.title()}",
                 labels={"Total_Transactions": "Total Transactions", "Quater_Label": "Quarter"})
    fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    fig.update_layout(width=1200, height=500, xaxis_tickangle=-45, yaxis_tickformat=".2s")
    st.plotly_chart(fig)

#17 Identify districts with high potential (low transactions but high average value)
#17 Insight: Districts with fewer transactions but high value per transaction can be areas of untapped potential.
sql="""SELECT 
    State,
    StateDistrict,
    SUM(Transaction_count) AS Total_Transactions,
    SUM(Transaction_amount) AS Total_Amount,
    (SUM(Transaction_amount) / SUM(Transaction_count)) AS Avg_Transaction
FROM map_transaction
GROUP BY State, StateDistrict
HAVING Total_Transactions < 100000
ORDER BY Avg_Transaction DESC;"""
curr.execute(sql)
Map_HighPotential_Data = curr.fetchall()
Map_HighPotential_DataTable = pd.DataFrame(Map_HighPotential_Data, columns = curr.column_names)

# ****5**** Transaction Analysis Across States and Districts
#18 Top States by Transaction Volume & Value
sql="""SELECT 
    State,
    SUM(Transaction_count) AS Total_Transactions,
    SUM(Transaction_amount) AS Total_Amount
FROM top_transaction
GROUP BY State
ORDER BY Total_Transactions DESC;"""
curr.execute(sql)
Top_StateTrans_Data = curr.fetchall()
Top_StateTrans_DataTable = pd.DataFrame(Top_StateTrans_Data, columns = curr.column_names)

#19 Top Districts by Transaction Volume & Value
sql="""SELECT 
    EntityValue AS District,
    State,
    SUM(Transaction_count) AS Total_Transactions,
    SUM(Transaction_amount) AS Total_Amount
FROM top_transaction
WHERE EntityLevel = 'District'
GROUP BY District, State
ORDER BY Total_Transactions DESC
LIMIT 15;"""
curr.execute(sql)
Top_DisTrans_Data = curr.fetchall()
Top_DisTrans_DataTable = pd.DataFrame(Top_DisTrans_Data, columns = curr.column_names)

#20 Top Pincode by Transaction Volume & Value
sql="""SELECT 
    EntityValue,
    State,
    SUM(Transaction_count) AS Total_Transactions,
    SUM(Transaction_amount) AS Total_Amount
FROM top_transaction
WHERE EntityLevel = 'Pincode'
GROUP BY EntityValue, State
ORDER BY Total_Transactions DESC
LIMIT 10;"""
curr.execute(sql)
Top_PincodeTrans_Data = curr.fetchall()
Top_PincodeTrans_DataTable = pd.DataFrame(Top_PincodeTrans_Data, columns = curr.column_names)

#21 Key Areas of High Users + High Value
# districts with high engagement and value, ideal for campaigns:
sql="""SELECT
    EntityValue AS District,
    State,
    SUM(Transaction_count) AS Total_Transactions,
    SUM(Transaction_amount) AS Total_Amount
FROM top_transaction
WHERE EntityLevel = 'District'
GROUP BY District, State
HAVING SUM(Transaction_count) > 1000000 AND SUM(Transaction_amount) > 1000000000
ORDER BY Total_Amount DESC
LIMIT 15;"""
curr.execute(sql)
HighUserValue_Data = curr.fetchall()
HighUserValue_DataTable = pd.DataFrame(HighUserValue_Data, columns = curr.column_names)

#22 Map View
sql="""SELECT Year, State, Quater,
       SUM(Transaction_count) AS Total_Transactions,
       SUM(Transaction_amount) AS Total_Amount
FROM agg_transaction
GROUP BY Year, State, Quater
ORDER BY Year;"""
curr.execute(sql)
MapData = curr.fetchall()
MapDataTable = pd.DataFrame(MapData, columns = curr.column_names)

# Correct State Name values
MapDataTable['State'] = MapDataTable['State'].str.replace('-', ' ', regex=False)
MapDataTable['State'] = MapDataTable['State'].str.title()
MapDataTable['State'] = MapDataTable['State'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
MapDataTable['State'] = MapDataTable['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
MapDataTable['Total_Transactions'] = pd.to_numeric(MapDataTable['Total_Transactions'], errors='coerce')

#Get Unique Set of values for States, Year and Quarter
Year_DF = pd.DataFrame(MapDataTable['Year'].unique(), columns=['Year'])
Quarter_DF = pd.DataFrame(MapDataTable['Quater'].unique(), columns=['Quater'])
#StateDF = pd.DataFrame(MapDataTable['State'].unique(), columns=['State'])


def plot_transactions_by_yearQuater(year,quarter):
    filtered_df = MapDataTable[(MapDataTable['Year'] ==year) & (MapDataTable['Quater']==quarter)]
    fig = px.choropleth(
    filtered_df,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='State',
    color='Total_Transactions',
    color_continuous_scale='Reds')
    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig)

# Example usage
#plot_transactions_by_yearQuater(2019,2)

















SideBarMenu = st.sidebar.radio('Navigation',['Home-Dashboard','Analytic Insights'])
if SideBarMenu=='Home-Dashboard':
    st.title('PhonePe Transaction Insights')
    st.subheader("Poblem Statement")
    st.write("With the increasing reliance on digital payment systems like PhonePe, understanding the dynamics of transactions, user engagement, and insurance-related data is crucial for improving services and targeting users effectively. This project aims to analyze and visualize aggregated values of payment categories, create maps for total values at state and district levels, and identify top-performing states, districts, and pin codes.")
    st.subheader("Scope of Analysis")
    st.write("We sincerely appreciate the opportunity to analyse the transactional dynamics, user engagement, and insurance-related data on PhonePe. Our analysis will include visualizations of payment category trends across states and districts, with a focus on identifying the top-performing states, districts, and PIN codes.")
if SideBarMenu=='Analytic Insights':
    st.title('Analytic Insights for PhonePe Transaction')
    SelectBoxMenu=st.selectbox("Select Business UseCase",["empty","Decoding Transaction Dynamics on PhonePe","Device Dominance and User Engagement Analysis","User Engagement and Growth Strategy","Insurance Engagement Analysis","Transaction Analysis Across States and Districts","Transaction Details via MAP"])
    if SelectBoxMenu=="empty":
        st.subheader("Please select any of the options from the drop-down")
    if SelectBoxMenu=="Decoding Transaction Dynamics on PhonePe":
        # ***1***
        fig = px.pie(TransDataTable, names='Transaction_type', values='Total_Transactions', title='Total Transactions Distribution by Transaction Type')
        st.plotly_chart(fig)
        # ***2***
        fig = px.line(StateDataTable, x='State', y='Total_Transactions', title='Total Transactions over each States', markers=True)
        fig.update_layout(width=1000, height=900, xaxis_title='State Name', yaxis_title='Number of Transactions', xaxis_tickangle=-45)
        st.plotly_chart(fig)
        # ***3***
        fig = px.pie(QuaterDataTable, names='Quater_Label', values='Total_Transactions', title='Total Transactions Distribution by Quarter')
        st.plotly_chart(fig)
        # ***4***
        fig = px.bar(YearDataTable, x=YearDataTable['Year'].astype(str), y=YearDataTable['Total_Transactions'], 
             title='Total Transactions Per Year', labels={'Total_Transactions': 'Total Transactions', 'x': 'Year'})
        fig.update_layout(xaxis_title='Year', yaxis_title='Number of Transactions', width=900, height=500)
        st.plotly_chart(fig)
        # ***5***
        fig = px.bar(Trans_Dec_DataTable, x='Quater_Label', y='Total_count', color='Transaction_type',
             barmode='stack', title='Total Transactions and Contribution by Type per Quarter',
             labels={
                 'Total_count': 'Total Transactions',
                 'Quater_Label': 'Quarter',
                 'Transaction_type': 'Transaction Type'})
        st.plotly_chart(fig)
    if SelectBoxMenu=="Device Dominance and User Engagement Analysis":
       # ***6***
       fig = px.line(BrandDataTable, x='Brand', y='TotalUsers', title='Total Users by Brand', markers=True)
       fig.update_layout(width=1000, height=500, xaxis_title='Brand', yaxis_title='Users', xaxis_tickangle=-45)
       st.plotly_chart(fig)
       # ***7***
       fig = px.bar(Reg_BrandDataTable, x='State', y='BrandUsers', color='Brand',
             barmode='stack', title='Brand Popularity by Region',
             labels={
                 'State': 'StateName',
                 'BrandUsers': 'Users',
                 'Brand': 'BrandName'})
       fig.update_layout(xaxis_title='StateName', yaxis_title='Users', width=900, height=500, xaxis_tickangle=-45)
       st.plotly_chart(fig)
       # ***8***
       fig = px.bar(YrQtrDataDataTable, x=YrQtrDataDataTable['Year'].astype(str), y='TotalBrandUsers', color='Brand',
             barmode='stack', title='Trend in Brand Usage Over Time',
             labels={
                 'Year': 'Years',
                 'TotalBrandUsers': 'TotalUsers',
                 'Brand': 'BrandName'})
       fig.update_layout(xaxis_title='Years', yaxis_title='Users', width=900, height=500, xaxis_tickangle=-45)
       st.plotly_chart(fig)
       # ***9***
       fig = px.line(EngagementDataTable, x='State', y='EngagementRate', title='User Engagement by Region', markers=True)
       fig.update_layout(width=1000, height=500, xaxis_title='State', yaxis_title='Ratio', xaxis_tickangle=-45)
       st.plotly_chart(fig)
    if SelectBoxMenu=="User Engagement and Growth Strategy":
        # ***10***
        fig = px.line(Map_User_EngDataTable, x='State', y='EngagementRatio', title='User Engagement by Region', markers=True)
        fig.update_layout(width=1000, height=500, xaxis_title='State', yaxis_title='Ratio', xaxis_tickangle=-45)
        st.plotly_chart(fig)
        # ***11***
        fig = px.bar(Map_Dis_DataTable, x='State', y='EngagementRatio', color='StateDistrict',
             barmode='stack', title='User engaements for each districts',
             labels={
                 'State': 'StateName',
                 'EngagementRatio': 'Ratio',
                 'StateDistrict': 'District'})
        fig.update_layout(xaxis_title='StateName', yaxis_title='Users', width=1200, height=500, xaxis_tickangle=-45)
        st.plotly_chart(fig)
        # ***12***
        fig = px.pie(Map_TopDis_DataTable.head(10), names='StateDistrict', values='TotalAppOpens', title='District-wise Share of Total App Opens')
        st.plotly_chart(fig)
        fig = px.pie(Map_TopDis_DataTable.head(10), names='StateDistrict', values='RegisteredUsers', title='District-wise Share of Total Registered Users')
        st.plotly_chart(fig)
        # ***13***
        fig = px.line(Map_DisRatio_DataTable, x='StateDistrict', y='EngagementRatio', title='Highest Engagement by District', markers=True)
        fig.update_layout(width=1000, height=500, xaxis_title='District', yaxis_title='Ratio', xaxis_tickangle=-45)
        st.plotly_chart(fig)
    if SelectBoxMenu=="Insurance Engagement Analysis":
        # ***14***
        fig = px.bar(Map_INSamount_DataTable, x="Total_Amount", y="State", orientation='h', title="Total Insurance Amount by State", labels={"Total_Amount": "Amount (₹)", "State": "StateNames"})
        fig.update_layout(xaxis_title='StateName', yaxis_title='Users', width=1200, height=1000, xaxis_tickangle=-45)
        st.plotly_chart(fig)
        # ***15***
        fig = px.bar(Map_AvgTrans_DataTable.head(20), x="Total_Amount", y="StateDistrict", orientation='h', title="Total Insurance Amount by District (Colored by Avg Transaction Value)",
            labels={
                 'Total_Amount': 'Amount',
                 'StateDistrict': 'Districts',
                 'Avg_Transaction_Value': 'Avg_trans'})
        fig.update_layout(xaxis_title='Amount', yaxis_title='State Districts', width=1200, height=1000, xaxis_tickangle=-45)
        st.plotly_chart(fig)
        # ***16***
        StateValue = st.selectbox('# Select any State from the given options',State_DF.values)
        plot_transactions_by_quarter(StateValue)
        # ***17***
        fig = px.bar(Map_HighPotential_DataTable, x='StateDistrict', y='Avg_Transaction', color='State',
             barmode='stack', title='Districts with low transactions but high potential',
             labels={
                 'StateDistrict': 'District',
                 'Avg_Transaction': 'Avg_Trans',
                 'State': 'StateName'})
        st.plotly_chart(fig)
    if SelectBoxMenu=="Transaction Analysis Across States and Districts":
        # ***18***
        fig = px.line(Top_StateTrans_DataTable, x='State', y='Total_Transactions', title='Top States by Transaction Volume', markers=True)
        fig.update_layout(width=1000, height=500, xaxis_title='Top_State', yaxis_title='Top_Trans', xaxis_tickangle=-45)
        st.plotly_chart(fig)
        fig = px.line(Top_StateTrans_DataTable, x='State', y='Total_Amount', title='Top States by Transaction Value', markers=True)
        fig.update_layout(width=1000, height=500, xaxis_title='Top_State', yaxis_title='Top_Value', xaxis_tickangle=-45)
        st.plotly_chart(fig)
        # ***19***
        fig = px.line(Top_DisTrans_DataTable, x='District', y='Total_Transactions', title='Top District by Transaction Volume', markers=True)
        fig.update_layout(width=1000, height=500, xaxis_title='Top_District', yaxis_title='Top_Trans', xaxis_tickangle=-45)
        st.plotly_chart(fig)
        # ***20***
        fig = px.scatter(Top_PincodeTrans_DataTable, x="EntityValue", y="Total_Transactions", color="State", hover_name="EntityValue", title="Top Pincode by Transaction Volume",
                         labels={
        "EntityValue": "Pincode",
        "Total_Transactions": "Number of Transactions"})
        st.plotly_chart(fig)
        # ***21***
        fig = px.bar(HighUserValue_DataTable, x='District', y='Total_Transactions', title='Areas of High Users + High Value', 
                     labels={'District': 'Districts', 'Total_Transactions': 'Trans'})
        fig.update_layout(xaxis_title='Districts', yaxis_title='Trans', width=900, height=500, xaxis_tickangle=-45)
        st.plotly_chart(fig)
    if SelectBoxMenu=="Transaction Details via MAP":
        st.subheader("Total Transaction across County by each Quarter")
        YearCol, QuarterCol = st.columns(2, gap='small')
        YearValue = YearCol.selectbox('# Select any year',Year_DF.values)
        QuarterValue = QuarterCol.selectbox('# Select any Quarter',Quarter_DF.values)
        plot_transactions_by_yearQuater(YearValue,QuarterValue)



curr.close()
db_Connection.close()


#Complete