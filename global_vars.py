from library import re

APP_VERSION = "1.1.5"
GITHUB_REPO = "AlleexMartinsT/pdfReader"
LAST_MVA = None
LAST_EH = None
LAST_HASH_MERGE = None
SALES_PERIOD = None 

# Armazena resultados separados por origem (MVA/EH)
results_by_source = {
    "MVA": [],
    "EH": []
}

regex_data = re.compile(r"^\s*\d{2}/\d{2}/\d{4}")
regex_negative = re.compile(r"[-−–]\s*\d")

list_results = []  # Lista para armazenar resultados de múltiplos PDFs
listFiles = []