from library import re

APP_VERSION = "1.1.4"
GITHUB_REPO = "AlleexMartinsT/pdfReader"
_ULTIMO_MVA = None
_ULTIMO_EH = None
ULTIMO_HASH_MESCLAGEM = None
PERIODO_VENDAS = None 

# Armazena resultados separados por origem (MVA/EH)
resultados_por_origem = {
    "MVA": [],
    "EH": []
}

regex_data = re.compile(r"^\s*\d{2}/\d{2}/\d{4}")
regex_negativo = re.compile(r"[-−–]\s*\d")

resultados_lista = []  # Lista para armazenar resultados de múltiplos PDFs
arquivosLista = []