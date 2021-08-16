import sqlite3
import pandas as pd

# substitute username with your username
conn = sqlite3.connect('/Users/yilunjiang/Library/Messages/chat.db')

# connect to the database
cur = conn.cursor()

# get the names of the tables in the database
cur.execute(" select name from sqlite_master where type = 'table' ")
for name in cur.fetchall():
    print(name)

# get the 10 entries of the message table using pandas
messages = pd.read_sql_query("select * from message", conn)

# get the handles to apple-id mapping table
handles = pd.read_sql_query("select * from handle", conn)
# and join to the messages, on handle_id
messages.rename(columns={'ROWID': 'message_id'}, inplace=True)
handles.rename(columns={'id': 'phone_number', 'ROWID': 'handle_id'}, inplace=True)
merge_leve_1 = temp = pd.merge(messages[['text', 'handle_id', 'date', 'is_sent', 'message_id']],
                               handles[['handle_id', 'phone_number']], on='handle_id', how='left')

# get the handles to apple-id mapping table
handles = pd.read_sql_query("select * from handle", conn)
# and join to the messages, on handle_id
messages.rename(columns={'ROWID': 'message_id'}, inplace=True)
handles.rename(columns={'id': 'phone_number', 'ROWID': 'handle_id'}, inplace=True)
merge_leve_1 = temp = pd.merge(
    messages[['text', 'handle_id', 'date', 'is_sent', 'message_id', 'destination_caller_id', 'service']],
    handles[['handle_id', 'phone_number']], on='handle_id', how='left')

for value in range(len(merge_leve_1)):
    if merge_leve_1.is_sent[value] == 1:
        continue
    else:
        print('User ' + str(merge_leve_1.phone_number[value]) + ' use ' + str(
            merge_leve_1.service[value]) + ' text you: \n' + str(merge_leve_1.text[value]) + '\n----------------')
