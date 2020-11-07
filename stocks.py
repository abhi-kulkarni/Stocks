from oauth2client.service_account import ServiceAccountCredentials
import datetime
from googleapiclient import discovery
import pandas
import sys
from dotenv import load_dotenv
import os
from pathlib import Path
import json

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

positions_csv = sys.argv[1]
orders_csv = sys.argv[2]

# GETTING DATA FROM POSITIONS CSV

total_pl = 0
positions_data = pandas.read_csv(positions_csv)
df_positions = positions_data[['Instrument', 'P&L']]

for index, row in df_positions.iterrows():
    total_pl += row['P&L']

# GETTING DATA FROM ORDERS CSV

orders_data = pandas.read_csv(orders_csv)
df_orders = orders_data[['Instrument', 'Avg. price', 'Qty.', 'Status', 'Type', 'Time']]

orders_positions_date = ''
if len(df_orders['Time']) > 0:
    orders_positions_date = datetime.datetime.strptime(df_orders['Time'][0], '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')

required_list = []
g = {}

total_deductions = 0

total_actual_brokerage = 0
stt_ctt = 0
transaction_charges = 0
gst = 0
sebi = 0
stamp_charges = 0

total_buy_transaction_amt = 0
total_sell_transaction_amt = 0
total_transaction_amt = 0

for index, row in df_orders.iterrows():
    if row['Status'] == 'COMPLETE':

        quantity = row['Qty.'].split('/')[0]
        total_price = row['Avg. price'] * int(quantity)
        total_transaction_amt += total_price

        brokerage = 0.0003 * total_price
        actual_brokerage = 20 if brokerage > 20 else brokerage
        total_actual_brokerage += actual_brokerage

        if row['Type'] == 'BUY':
            total_buy_transaction_amt += total_price

        elif row['Type'] == 'SELL':
            total_sell_transaction_amt += total_price

        if row['Instrument'] in g:
            g[row['Instrument']].append(
                ['', str(row['Avg. price']), quantity, round(total_price, 2), round(brokerage, 2),
                 round(actual_brokerage, 2)])
        else:
            g[row['Instrument']] = [
                [row['Instrument'], str(row['Avg. price']), quantity, round(total_price, 2), round(brokerage, 2),
                 round(actual_brokerage, 2)]]

stt_ctt = 0.00025 * total_sell_transaction_amt
transaction_charges = 0.0000325 * total_transaction_amt
gst = 0.18 * (total_actual_brokerage + transaction_charges)
stamp_charges = 0.00003 * total_buy_transaction_amt

total_deductions = total_actual_brokerage + stt_ctt + transaction_charges + gst + sebi + stamp_charges

net_pl = total_pl - total_deductions

total_pl_row = ['P/L', '', '', round(total_pl, 2)]
total_brokerage = ['Total Brokerage', '', '', round(total_actual_brokerage, 2)]
total_stt = ['STT/CTT', '', '', round(stt_ctt, 2)]
total_transaction_charges = ['Total Transaction Charges', '', '', round(transaction_charges, 2)]
total_gst = ['GST', '', '', round(gst, 2)]
total_stamp_charges = ['Total Stamp Charges', '', '', round(stamp_charges, 2)]

total_transaction_row = ['Total Transaction', '', '', round(total_transaction_amt, 2)]
total_deduction_row = ['Total Deduction', '', '', round(total_deductions, 2)]
net_pl_row = ['Net P/L', '', '', round(net_pl, 2)]

total_value_list = [total_pl_row, total_brokerage, total_stt, total_transaction_charges, total_gst, total_stamp_charges,
                    total_transaction_row, total_deduction_row, net_pl_row]

g_list = list(g.values())
required_list = g_list[0]
for i in range(1, len(g_list)):
    required_list.extend(g_list[i])

# WRITING DATA TO GOOGLE SHEETS

scope = json.loads(os.getenv("SCOPE"))

credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)

service = discovery.build('sheets', 'v4', credentials=credentials)

spreadsheetId = os.getenv("SPREADSHEET_ID")
sheet_name = sys.argv[3]
sheetId = os.getenv("SHEET_ID_1") if sheet_name == 'Sheet1' else os.getenv("SHEET_ID_2")

range_ = sheet_name+"!A:A1"
curr_date = datetime.datetime.now().strftime('%d/%m/%Y')
empty_list = [['-----------------', '-----------------', '-----------------', '-----------------', '-----------------',
               '-----------------']]
date_row_data = [['Date', str(orders_positions_date)]]
header_row_data = [['Share', 'Price', 'Quantity', 'Total price', 'Calc. Brokerage', 'Actual Brokerage']]

empty_row = {
    "majorDimension": "ROWS",
    "values": empty_list
}

date_row = {
    "majorDimension": "ROWS",
    "values": date_row_data
}

header_row = {
    "majorDimension": "ROWS",
    "values": header_row_data
}

required_data = {
    "majorDimension": "ROWS",
    "values": required_list
}

total_values_data = {
    "majorDimension": "ROWS",
    "values": total_value_list
}

res = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range_, majorDimension="ROWS").execute()

sheet_data = res['values']

curr_new_line = len(sheet_data)-1

k = list(g.keys())

k.extend(['Date', 'P/L', 'Total Brokerage', 'Total Transaction', 'Total Deduction', 'Net P/L'])

service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=range_, body=empty_row,
                                       valueInputOption="USER_ENTERED").execute()

service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=range_, body=date_row,
                                       valueInputOption="USER_ENTERED").execute()

service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=range_, body=empty_row,
                                       valueInputOption="USER_ENTERED").execute()

service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=range_, body=header_row,
                                       valueInputOption="USER_ENTERED").execute()

service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=range_, body=required_data,
                                       valueInputOption="USER_ENTERED").execute()

service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=range_, body=empty_row,
                                       valueInputOption="USER_ENTERED").execute()

service.spreadsheets().values().append(spreadsheetId=spreadsheetId, range=range_, body=total_values_data,
                                       valueInputOption="USER_ENTERED").execute()

request = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=range_).execute()

# FORMAT SHEET
formatting_requests_list = []
for obj in k:
    d = {}
    not_d = {"bold": True}
    fgColor = {
        "red": 0,
        "green": 0,
        "blue": 1
    }
    d["foregroundColor"] = fgColor
    d["bold"] = True
    text_format = d if obj == "Date" else not_d
    data = {
        'addConditionalFormatRule': {
            'rule': {
                'ranges': [{
                    "sheetId": sheetId,
                    "startRowIndex": curr_new_line,
                    "endRowIndex": 200000,
                    "startColumnIndex": 0,
                    "endColumnIndex": 1,
                }],
                'booleanRule': {
                    'condition': {
                        "type": "TEXT_EQ",
                        "values": [
                            {
                                "userEnteredValue": obj
                            }
                        ]
                    },
                    'format': {
                        "textFormat": text_format
                    }
                }
            },
            'index': 0
        }
    }
    formatting_requests_list.append(data)

formatting_request_body = {
    "requests": formatting_requests_list
}

service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body=formatting_request_body).execute()

new_range_ = sheet_name+"!A:D"

response = service.spreadsheets().values().get(spreadsheetId=spreadsheetId, range=new_range_, majorDimension="ROWS").execute()

sheet_data = response['values']

# FORMAT CELLS

format_cell_list = []
for i in range(len(sheet_data)):
    data = {}
    if i > curr_new_line and len(sheet_data[i]) > 0 and (sheet_data[i][0] == 'Date'):
        data = {
            "repeatCell": {
                "range": {
                    "sheetId": sheetId,
                    "startRowIndex": i,
                    "endRowIndex": i + 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 2
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "bold": True,
                            "italic": True
                        }
                    }
                },
                "fields": "userEnteredFormat(textFormat)"
            }
        }
    elif i > curr_new_line and  len(sheet_data[i]) > 0 and (sheet_data[i][0] == 'P/L' or sheet_data[i][0] == 'Total Brokerage' or sheet_data[i][0] == 'Total Transaction'):
        data = {
            "repeatCell": {
                "range": {
                    "sheetId": sheetId,
                    "startRowIndex": i,
                    "endRowIndex": i+1,
                    "startColumnIndex": 3,
                    "endColumnIndex": 4
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "bold": True,
                        }
                    }
                },
                "fields": "userEnteredFormat(textFormat)"
            }
        }
    elif i > curr_new_line and len(sheet_data[i]) > 0 and sheet_data[i][0] == 'Total Deduction':
        data = {
            "repeatCell": {
                "range": {
                    "sheetId": sheetId,
                    "startRowIndex": i,
                    "endRowIndex": i + 1,
                    "startColumnIndex": 3,
                    "endColumnIndex": 4
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {
                            "foregroundColor": {
                                "red": 1.0,
                                "green": 0.0,
                                "blue": 0.0
                            },
                            "bold": True,
                        }
                    }
                },
                "fields": "userEnteredFormat(textFormat)"
            }
        }
    elif i > curr_new_line and len(sheet_data[i]) > 0 and sheet_data[i][0] == 'Net P/L':
        p_l = sheet_data[i][3].replace(',', '')
        bgColor = {
          "red": 1,
          "green": 1,
          "blue": 0
        } if float(p_l) > 0 else {
          "red": 1,
          "green": 0,
          "blue": 0
        }
        data = {
            "repeatCell": {
                "range": {
                    "sheetId": sheetId,
                    "startRowIndex": i,
                    "endRowIndex": i + 1,
                    "startColumnIndex": 3,
                    "endColumnIndex": 4
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": bgColor,
                        "textFormat": {
                            "bold": True,
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor, textFormat)"
            }
        }
    format_cell_list.append(data) if len(data) > 0 else ''

formatting_cell_request_body = {
    "requests": format_cell_list
}

service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetId, body=formatting_cell_request_body).execute()
