from library import *
from globalVar import *
import difflib
import logging

# Configurar logging para depuração
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def resource_path(relative_path):
    """Retorna o caminho absoluto do recurso, compatível com PyInstaller."""
    if hasattr(sys, '_MEIPASS'):  # Executando empacotado
        base_path = sys._MEIPASS
        logger.debug(f"Executável: base_path = {base_path}")
    else:
        base_path = os.path.abspath(".")
        logger.debug(f"Desenvolvimento: base_path = {base_path}")
    return os.path.join(base_path, relative_path)

def load_mapping(path='mapping.json'):
    full_path = resource_path(path)
    logger.debug(f"Procurando mapping.json em: {full_path}")
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Arquivo de mapeamento não encontrado: {full_path}")
    with open(full_path, 'r', encoding='utf-8') as f:
        mp = json.load(f)
    return {k.strip().upper(): v.strip() for (k, v) in mp.items()}

mapping = load_mapping()
CANON_BY_VALUE_UPPER = {v.upper(): v for v in mapping.values()}

def save_mapping():
    """Salva o mapeamento atualizado no arquivo do usuário."""
    appdata_dir = os.path.join(os.getenv("APPDATA"), "RelatorioClientes")
    os.makedirs(appdata_dir, exist_ok=True)
    user_json = os.path.join(appdata_dir, "mapping.json")
    logger.debug(f"Salvando mapping em: {user_json}")
    with open(user_json, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=4, ensure_ascii=False)

def _normalize_key(s: str) -> str:
    if not s:
        return ""
    s = s.replace('\u00A0', ' ')                # NBSP -> espaço
    s = re.sub(r"^\s*\d+\s*", "", s)            # remove prefixo numérico "14 C O" -> "C O"
    s = re.sub(r"\s+", " ", s)                  # espaços múltiplos
    s = s.replace("–", "-").replace("—", "-")   # normaliza hifens
    return s.strip().upper()

def parse_number(num_str: str) -> float:
    """Converte string numérica em float, suportando formatos BR e US."""
    if not num_str:
        return 0.0
    num_str = num_str.strip()

    # Caso brasileiro: 79.833,85
    if "," in num_str and "." in num_str and num_str.rfind(",") > num_str.rfind("."):
        num_str = num_str.replace(".", "").replace(",", ".")
        return float(num_str)

    # Caso americano: 92,229.51
    if "," in num_str and "." in num_str and num_str.rfind(".") > num_str.rfind(","):
        num_str = num_str.replace(",", "")
        return float(num_str)

    # Caso só vírgula (310,75)
    if "," in num_str:
        return float(num_str.replace(",", "."))

    # Caso só ponto (310.75)
    if "." in num_str:
        return float(num_str)

    return float(num_str)

def format_number_br(num: float) -> str:
    """Formata número no padrão brasileiro com duas casas decimais."""
    return f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def canonicalize_name(raw: str) -> str:
    key = _normalize_key(raw)

    # 1) se existe como abreviação no mapping
    if key in mapping:
        return mapping[key]

    # 2) se já é o nome completo
    if key in CANON_BY_VALUE_UPPER:
        return CANON_BY_VALUE_UPPER[key]

    # 3) fuzzy matching
    match = difflib.get_close_matches(key, list(CANON_BY_VALUE_UPPER.keys()), n=1, cutoff=0.93)
    if match:
        return CANON_BY_VALUE_UPPER[match[0]]

    # fallback
    return raw.strip().title()

# --- Funções principais ---
def escolher_pdf(tree, progress_var, progress_bar, root):
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    if not caminho:
        return
    
    global resultados_list
    resultados_list = []
    
    for item in tree.get_children():
        tree.delete(item)

    try:
        logger.debug(f"Tentando abrir PDF: {caminho}")
        with open(caminho, 'rb') as f:  # Verifica se o arquivo é acessível
            logger.debug("Arquivo acessível com sucesso")
        with pdfplumber.open(caminho) as pdf:
            logger.debug(f"PDF aberto com sucesso. Total de páginas: {len(pdf.pages)}")
            if pdf.metadata.get("encrypted", False):
                messagebox.showerror("Erro", "Este PDF está protegido por senha.")
                return
    except FileNotFoundError as e:
        messagebox.showerror("Erro", f"Arquivo não encontrado: {e}")
        return
    except Exception as e:
        messagebox.showerror("Erro", f"Não foi possível abrir o PDF: {e}")
        logger.error(f"Exceção ao abrir PDF: {str(e)}")
        return

    res = processar_pdf(caminho, progress_var, progress_bar, root)
    resultados_list.append(res)
    atualizar_tree(tree)
    
    messagebox.showinfo("Concluído", "Processamento finalizado com sucesso!")

def adicionar_pdf(tree, progress_var, progress_bar, root):
    if not resultados_list:
        messagebox.showwarning("Aviso", "Selecione o primeiro PDF antes de adicionar outro.")
        return
    
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    if not caminho:
        return
    
    try:
        logger.debug(f"Tentando abrir PDF: {caminho}")
        with open(caminho, 'rb') as f:  # Verifica se o arquivo é acessível
            logger.debug("Arquivo acessível com sucesso")
        with pdfplumber.open(caminho) as pdf:
            logger.debug(f"PDF aberto com sucesso. Total de páginas: {len(pdf.pages)}")
            if pdf.metadata.get("encrypted", False):
                messagebox.showerror("Erro", "Este PDF está protegido por senha.")
                return
    except FileNotFoundError as e:
        messagebox.showerror("Erro", f"Arquivo não encontrado: {e}")
        return
    except Exception as e:
        messagebox.showerror("Erro", f"Não foi possível abrir o PDF: {e}")
        logger.error(f"Exceção ao abrir PDF: {str(e)}")
        return

    res = processar_pdf(caminho, progress_var, progress_bar, root)
    resultados_list.append(res)
    atualizar_tree(tree)
    
    messagebox.showinfo("Concluído", "Segundo PDF adicionado e relatórios mesclados!")

def atualizar_tree(tree):
    for item in tree.get_children():
        tree.delete(item)
    
    mesclado = mesclar_resultados(resultados_list)
    
    for vendedor, dados in sorted(mesclado.items()):
        total_vendas_str = ""
        if dados["total_vendas"] > 0:
            total_vendas_str = format_number_br(dados["total_vendas"])

        tree.insert("", "end", values=(
            vendedor,
            dados['atendidos'],
            dados['devolucoes'],
            dados['total_clientes'],
            total_vendas_str
        ))

def mesclar_resultados(resultados_list):
    mesclado = {}
    for res in resultados_list:
        for vend, dados in res.items():
            canon = canonicalize_name(vend)

            if canon not in mesclado:
                mesclado[canon] = {
                    "atendidos": 0,
                    "devolucoes": 0,
                    "total_clientes": 0,
                    "total_vendas": 0.0
                }

            mesclado[canon]["atendidos"]      += dados.get("atendidos", 0)
            mesclado[canon]["devolucoes"]     += dados.get("devolucoes", 0)
            mesclado[canon]["total_clientes"] += dados.get("total_clientes", 0)

            tv_str = str(dados.get("total_vendas", ""))
            mesclado[canon]["total_vendas"] += parse_number(tv_str)

    # Recalcula clientes finais
    for dados in mesclado.values():
        dados["total_clientes"] = dados["atendidos"] - dados["devolucoes"]

    return mesclado

def processar_pdf(caminho_pdf, progress_var, progress_bar, root):
    resultados = {}
    vendedor_atual = None

    def fechar_vendedor():
        nonlocal vendedor_atual
        if vendedor_atual and vendedor_atual in resultados:
            dados = resultados[vendedor_atual]
            dados["total_clientes"] = dados["atendidos"] - dados["devolucoes"]

    try:
        logger.debug(f"Processando PDF: {caminho_pdf}")
        with pdfplumber.open(caminho_pdf) as pdf:
            logger.debug(f"PDF aberto com sucesso. Total de páginas: {len(pdf.pages)}")
            total_paginas = len(pdf.pages)
            for i, pagina in enumerate(pdf.pages, start=1):
                logger.debug(f"Processando página {i}")
                texto = pagina.extract_text()
                if not texto:
                    logger.warning(f"Página {i} sem texto extraível")
                    continue
                linhas = texto.splitlines()
                for linha in linhas:
                    if "Vendedor: " in linha:
                        fechar_vendedor()
                        partes = linha.split("Vendedor: ", 1)[1].strip()
                        if partes:
                            palavras = partes.split()
                            if palavras[0].isdigit():
                                vendedor_bruto = " ".join(palavras[1:])
                            else:
                                vendedor_bruto = " ".join(palavras)

                            vendedor_atual = canonicalize_name(vendedor_bruto)

                            # se não estiver no mapping, perguntar ao usuário
                            if _normalize_key(vendedor_bruto) not in mapping:
                                if messagebox.askyesno("Novo Vendedor", f"O vendedor '{vendedor_bruto}' não está no mapeamento. Deseja adicioná-lo?"):
                                    mapping[_normalize_key(vendedor_bruto)] = vendedor_atual
                                    save_mapping()
                                    messagebox.showinfo("Sucesso", f"'{vendedor_atual}' adicionado ao mapeamento!")

                            resultados[vendedor_atual] = {
                                "atendidos": 0,
                                "devolucoes": 0,
                                "total_clientes": 0,
                                "total_vendas": ""
                            }
                        continue

                    if regex_data.match(linha):
                        if not vendedor_atual:
                            continue
                        resultados[vendedor_atual]["atendidos"] += 1
                        if regex_negativo.search(linha):
                            resultados[vendedor_atual]["devolucoes"] += 1

                    if "Totais" in linha and vendedor_atual:
                        match = re.search(r"Totais:\s*([\d\.\,]+)", linha)
                        if match:
                            resultados[vendedor_atual]["total_vendas"] = match.group(1)

                fechar_vendedor()
                progress = int((i / total_paginas) * 100)
                progress_var.set(progress)
                progress_bar.update_idletasks()
                root.update_idletasks()
    except pdfplumber.PDFSyntaxError as e:
        messagebox.showerror("Erro", f"Erro de sintaxe no PDF: {e}. Verifique se o arquivo não está corrompido.")
        logger.error(f"PDFSyntaxError: {str(e)}")
    except pdfplumber.PDFTextExtractionNotAllowedError as e:
        messagebox.showerror("Erro", f"Extração de texto não permitida: {e}. O PDF pode estar protegido.")
        logger.error(f"PDFTextExtractionNotAllowedError: {str(e)}")
    except Exception as e:
        messagebox.showerror("Erro", f"Ocorreu um erro interno ao processar o PDF: {e}")
        logger.error(f"Exceção geral: {str(e)}")
    
    return resultados

def ordenar_coluna(tree, col, reverse):
    dados = [(tree.set(k, col), k) for k in tree.get_children('')]
    
    def try_num(v):
        try:
            return float(v.replace(".", "").replace(",", "."))
        except:
            return v.lower()
    dados.sort(key=lambda t: try_num(t[0]), reverse=reverse)

    for index, (val, k) in enumerate(dados):
        tree.move(k, '', index)

    tree.heading(col, command=lambda: ordenar_coluna(tree, col, not reverse))

def check_for_updates():
    try:
        response = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest")
        response.raise_for_status()
        latest_version = response.json()["tag_name"].lstrip("v")
        if latest_version > APP_VERSION:
            if messagebox.askyesno("Atualização Disponível", f"Uma nova versão ({latest_version}) está disponível! Deseja baixar agora?"):
                asset_url = response.json()["assets"][0]["browser_download_url"]
                new_file = f"Relatório de Clientes {latest_version}.exe" 
                with open(new_file, "wb") as f:
                    f.write(requests.get(asset_url).content)
                messagebox.showinfo("Atualizado", f"Nova versão baixada como '{new_file}'. Feche o app, substitua o arquivo atual por esse novo e reinicie.")
        else:
            print("App atualizado.")
    except Exception as e:
        print(f"Erro ao checar updates: {e}")
        messagebox.showerror("Erro na Atualização", f"Ocorreu um erro ao checar atualizações: {e}")