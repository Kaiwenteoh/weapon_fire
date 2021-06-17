import pandas as pd
import json


# Read Json file
with open('log.json') as log_json:    
    data1 = json.load(log_json)  
data2 = pd.json_normalize(data1)


# Request - Content Body
rq_content_body = data2['Action.Request.Content.Body'].to_list()
rq_content_body_json = json.loads(rq_content_body[0])
rq_content_body_df = pd.json_normalize(rq_content_body_json)
rq_content_body_df = rq_content_body_df.add_prefix('Action.Request.Content.Body_')


# Join back to the existing df
data3 = data2.join(rq_content_body_df)
data3.drop('Action.Request.Content.Body', axis = 1, inplace = True)


# Response - Content Body without Data
rp_content_body = data3['Action.Response.Content.Body'].to_list()
rp_content_body_json = json.loads(rp_content_body[0])
rp_content_body_df = pd.json_normalize(rp_content_body_json)
rp_content_body_df_wth_data = rp_content_body_df.drop('data', axis = 1)
rp_content_body_df_wth_data = rp_content_body_df_wth_data.add_prefix('Action.Response.Content.Body_')


# Join back to the existing df
data4 = data3.join(rp_content_body_df_wth_data)
data4.drop('Action.Response.Content.Body', axis = 1, inplace = True)


# Response - Content Body Data without Result.Q_ACCESS
rp_content_body_df_data = rp_content_body_df['data'].to_list()
rp_content_body_df_data_json = json.loads(rp_content_body_df_data[0])
rp_content_body_df_data_json_df = pd.json_normalize(rp_content_body_df_data_json)
rp_content_body_df_data_json_df_wth_rqa = rp_content_body_df_data_json_df.drop('Result.Q_ACCESS', axis = 1)
rp_content_body_df_data_json_df_wth_rqa = rp_content_body_df_data_json_df_wth_rqa.add_prefix('Action.Response.Content.Body_data_')


# Join back to the existing df
data5 = data4.join(rp_content_body_df_data_json_df_wth_rqa)


# Response - Content Body Data Result.Q_ACCESS
rp_content_body_df_data_json_df_rqa = rp_content_body_df_data_json_df['Result.Q_ACCESS'].to_list()
data_df = pd.json_normalize(rp_content_body_df_data_json_df_rqa[0])
data_df = data_df.add_prefix('Action.Response.Content.Body_data_Result_Q_ACCESS')


# Combining df
data6 = pd.concat([data5]*len(data_df), ignore_index = True)
data7 = data6.join(data_df)
data7.sort_index(axis = 1, inplace = True)
