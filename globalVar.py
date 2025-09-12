from library import re

APP_VERSION = "1.1.2"
GITHUB_REPO = "AlleexMartinsT/pdfReader"

regex_data = re.compile(r"^\s*\d{2}/\d{2}/\d{4}")
regex_negativo = re.compile(r"[-−–]\s*\d")

resultados_lista = []  # Lista para armazenar resultados de múltiplos PDFs
arquivosLista = []