import sqlite3
import pandas as pd
import smtplib
import time
from email.header import Header
from email.mime.text import MIMEText

message_count_present = 0
while True:
    # mail config
    sender = "jiangyilun2000@gmail.com"

    mail_host = "smtp.gmail.com"  # 设置服务器
    mail_user = "jiangyilun2000@gmail.com"  # 用户名
    mail_pass = "iqghngbrbjrphycl"  # 口令

    # substitute username with your username
    conn = sqlite3.connect('/Users/yilunjiang/Library/Messages/chat.db')

    # connect to the database
    cur = conn.cursor()

    # get the names of the tables in the database
    cur.execute(" select name from sqlite_master where type = 'table' ")
    # for name in cur.fetchall():
    #     print(name)

    # get the 10 entries of the message table using pandas
    # messages = pd.read_sql_query("select * from message", conn)
    messages = pd.read_sql_query(
        "select *, datetime(message.date + strftime('%s', '2001-01-01') ,'unixepoch','localtime') as date_uct from message",
        conn)

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

    # how to use that in the SQL query

    if message_count_present != len(merge_leve_1):
        message_count_present = len(merge_leve_1)

        if merge_leve_1.is_sent[message_count_present - 1] == 0:
            print('User ' + str(merge_leve_1.phone_number[message_count_present - 1]) + ' use ' + str(
                merge_leve_1.service[message_count_present - 1]) + ' text you: \n' + str(
                merge_leve_1.text[message_count_present - 1]) + '\n----------------')

            message_email = MIMEText(
                'User ' + str(merge_leve_1.phone_number[message_count_present - 1]) + ' use ' + str(
                merge_leve_1.service[message_count_present - 1]) + ' text you: \n' + str(
                merge_leve_1.text[message_count_present - 1]) + '\n----------------')
            receiver = 'jiangyilun2000@gmail.com'
            message_email['From'] = Header("JIANGBOT", 'utf-8')
            message_email['To'] = Header("JIANG Yilun", 'utf-8')
            subject_email = 'User ' + str(merge_leve_1.phone_number[message_count_present - 1]) + ' text you!'
            message_email['Subject'] = Header(subject_email, 'utf-8')
            try:
                smtpObj = smtplib.SMTP_SSL(mail_host, 465)
                smtpObj.ehlo()
                smtpObj.login(mail_user, mail_pass)
                smtpObj.sendmail(sender, receiver, message_email.as_string())
                print("邮件发送成功")

            except smtplib.SMTPException as e:
                print("无法发送邮件", e)

    # for value in range(len(merge_leve_1)):
    #     if merge_leve_1.is_sent[value] == 1:
    #         continue
    #     else:
    #         print('User ' + str(merge_leve_1.phone_number[value]) + ' use ' + str(
    #             merge_leve_1.service[value]) + ' text you: \n' + str(merge_leve_1.text[value]) + '\n----------------')
