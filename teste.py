import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name("credenciais.json", scope)
client = gspread.authorize(creds)

SPREADSHEET_ID = "1eiHbe-NkZ4cM5tMtq2JN574rwa2thR6X7T40EZM_3TA"

sh = client.open_by_key(SPREADSHEET_ID)
print("✅ Abas encontradas:", [ws.title for ws in sh.worksheets()])
