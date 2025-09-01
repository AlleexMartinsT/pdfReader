from library import *

APP_VERSION = "1.0.6"
GITHUB_REPO = "AlleexMartinsT/pdfReader"

regex_data = re.compile(r"^\s*\d{2}/\d{2}/\d{4}")
regex_negativo = re.compile(r"[-−–]\s*\d")

resultados_list = []  # Lista para armazenar resultados de múltiplos PDFs
mapping = {}  # Mapeamento de vendedores (carregado depois)