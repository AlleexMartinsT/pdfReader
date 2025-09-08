from library import *
from globalVar import *
<<<<<<< HEAD

# Configuração de logging mais leve (somente avisos e erros)
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
progress_queue = queue.Queue()
cancel_event = threading.Event()
worker_thread = None

def cancelar_processamento():
    from tk import arquivos_label_var
    
    cancel_event.set()
    with progress_queue.mutex:
        progress_queue.queue.clear()
        arquivos_label_var.set("Nenhum arquivo carregado ainda")
    print("Cancelamento solicitado.")

def _poll_queue(root, tree, progress_var, progress_bar):
    """Roda na main thread: consome eventos vindos da thread e atualiza a UI."""
    try:
        while True:
            kind, payload = progress_queue.get_nowait()
            if kind == "progress":
                progress_var.set(payload)
                progress_bar.update_idletasks()
            elif kind == "done":
                if payload.get("__cancelled__"):
                    messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuário.")
                else:
                    resultados_list.append(payload)
                    atualizar_tree(tree)
                    messagebox.showinfo("Concluído", "Processamento finalizado com sucesso!")
                return  # para de ficar agendando _poll_queue
            elif kind == "error":
                messagebox.showerror("Erro", payload)
                return
    except queue.Empty:
        pass
    # agendar próxima rodada de checagem
    root.after(10, lambda: _poll_queue(root, tree, progress_var, progress_bar))
=======
import difflib
import logging

# Configurar logging para depuração
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b

def resource_path(relative_path):
    """Retorna o caminho absoluto do recurso, compatível com PyInstaller."""
    if hasattr(sys, '_MEIPASS'):  # Executando empacotado
        base_path = sys._MEIPASS
<<<<<<< HEAD
    else:
        base_path = os.path.abspath(".")
=======
        logger.debug(f"Executável: base_path = {base_path}")
    else:
        base_path = os.path.abspath(".")
        logger.debug(f"Desenvolvimento: base_path = {base_path}")
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
    return os.path.join(base_path, relative_path)

def load_mapping(path='mapping.json'):
    full_path = resource_path(path)
<<<<<<< HEAD
=======
    logger.debug(f"Procurando mapping.json em: {full_path}")
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
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
<<<<<<< HEAD
=======
    logger.debug(f"Salvando mapping em: {user_json}")
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
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
<<<<<<< HEAD

def extrair_planilha_online():
    import gspread
    import pandas as pd
    from oauth2client.service_account import ServiceAccountCredentials

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    cred_path = resource_path("credenciais.json")
    creds = ServiceAccountCredentials.from_json_keyfile_name(cred_path, scope)
    client = gspread.authorize(creds)

    SPREADSHEET_ID = "1eiHbe-NkZ4cM5tMtq2JN574rwa2thR6X7T40EZM_3TA"

    sheetMVA = client.open_by_key(SPREADSHEET_ID).worksheet("MVA")
    sheetEH = client.open_by_key(SPREADSHEET_ID).worksheet("EH")

    valoresMVA = sheetMVA.get_all_values()
    valoresEH = sheetEH.get_all_values()

    # pega cabeçalho da linha 2
    colsMVA = valoresMVA[1]
    colsEH = valoresEH[1]

    # 🔧 corrige duplicados
    colsMVA = [f"col{i}_{c}" if colsMVA.count(c) > 1 else c for i, c in enumerate(colsMVA)]
    colsEH = [f"col{i}_{c}" if colsEH.count(c) > 1 else c for i, c in enumerate(colsEH)]

    dfMVA = pd.DataFrame(valoresMVA[2:], columns=colsMVA)
    dfEH = pd.DataFrame(valoresEH[2:], columns=colsEH)

    return dfMVA, dfEH

def carregar_planilha(tree_planilha):
    try:
        # 🔎 Verificação
        '''if not arquivosLista or len(arquivosLista) < 2:
            messagebox.showwarning(
                "Aviso",
                "Você precisa carregar pelo menos 2 arquivos PDF antes de extrair os dados da planilha online."
            )
            return'''

        dfMVA, dfEH = extrair_planilha_online()

        # Junta as duas planilhas
        df_total = pd.concat([dfMVA, dfEH], ignore_index=True)

        # limpa tabela
        for item in tree_planilha.get_children():
            tree_planilha.delete(item)

        # Itera vendedores
        for _, row in df_total.iterrows():
            vendedor = str(row.iloc[0]).strip()
            if not vendedor:
                continue

            valores = row[1:]
            total = 0.0
            atendidos = 0  # contador de células somadas

            for v in valores:
                if pd.isna(v) or str(v).strip() == "":
                    continue
                try:
                    num = str(v).replace("R$", "").replace(".", "").replace(",", ".").strip()
                    total += float(num)
                    atendidos += 1  # conta cada célula válida
                except:
                    pass
                
            if total > 0:
                tree_planilha.insert(
                    "",
                    "end",
                    values=(vendedor, atendidos, f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
                )

        messagebox.showinfo("Sucesso", "✅ Planilha online carregada com sucesso!")

    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao carregar planilha: {e}")

def escolher_pdf_async(tree, progress_var, progress_bar, root, arquivos_label_var):
    
    global arquivosLista,resultados_list,worker_thread

    if not isinstance(arquivosLista, list):
        arquivosLista = []
    if arquivosLista and len(arquivosLista) > 1:
        arquivosLista.clear()
            
        
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    progress_var.set(0)
    
    if caminho:
        if caminho in arquivosLista:
            messagebox.showerror("Erro", "Arquivo já importado!")
            return
        else:
            arquivosLista.clear()
            arquivosLista.append(caminho)
    else:
        return
    
    arquivos_label_var.set(f"Arquivo carregado: {os.path.basename(caminho)}")
              
    # Zera estado, incluindo a lista de resultados
    cancel_event.clear()
=======
def escolher_pdf(tree, progress_var, progress_bar, root):
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    if not caminho:
        return
    
    global resultados_list
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
    resultados_list = []
    
    for item in tree.get_children():
        tree.delete(item)

<<<<<<< HEAD
    # Dispara worker
    def worker():
        try:
            resultados = processar_pdf_sem_ui(
                caminho,
                on_progress=lambda kind, payload: progress_queue.put((kind, payload)),
                cancel_event=cancel_event
            )

            # força progresso final
            progress_queue.put(("progress", 100))
            progress_queue.put(("done", resultados))
        except Exception as e:
            print(f"Erro na thread: {str(e)}")
            progress_queue.put(("error", str(e)))

    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()

    # Começa a escutar a fila na main thread
    _poll_queue(root, tree, progress_var, progress_bar)

def adicionar_pdf(tree, progress_var, progress_bar, root, arquivos_label_var):
    
    global arquivosLista
    # Checa se algum PDF ja foi importado antes
    if not resultados_list:
        messagebox.showwarning("Aviso", "Selecione o primeiro PDF antes de adicionar outro.")
        return
    
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    progress_var.set(0)
    
    if not caminho:
        return
    
    if caminho not in arquivosLista:
        arquivosLista.append(caminho)
    else:
        messagebox.showerror("Erro", "Arquivo já importado!")
        return
    
    atual = arquivos_label_var.get().replace("Arquivo carregado:", "").replace("Arquivos carregados:", "").strip()
    
    if atual and atual != "Nenhum arquivo carregado ainda":
        arquivos_label_var.set(f"Arquivos carregados: {atual}, {os.path.basename(caminho)}")
    else:
        arquivos_label_var.set(f"Arquivo carregado: {os.path.basename(caminho)}")
    
    try:
        with pdfplumber.open(caminho) as pdf:
=======
    try:
        logger.debug(f"Tentando abrir PDF: {caminho}")
        with open(caminho, 'rb') as f:  # Verifica se o arquivo é acessível
            logger.debug("Arquivo acessível com sucesso")
        with pdfplumber.open(caminho) as pdf:
            logger.debug(f"PDF aberto com sucesso. Total de páginas: {len(pdf.pages)}")
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
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

<<<<<<< HEAD
    # Reset progresso
    cancel_event.clear()

    # Thread worker
    def worker():
        try:
            res = processar_pdf_sem_ui(
                caminho,
                on_progress=lambda kind, payload: progress_queue.put((kind, payload)),
                cancel_event=cancel_event
            )

            # força progresso final
            # progress_queue.put(("progress", 100))
            progress_queue.put(("done_add", res))  # use um tipo diferente para não confundir com o primeiro PDF
        except Exception as e:
            logger.error(f"Erro ao processar PDF adicional: {str(e)}")
            progress_queue.put(("error", str(e)))

    global worker_thread
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()

    # começa a escutar fila
    def poll_queue_add():
        try:
            while True:
                kind, payload = progress_queue.get_nowait()
                if kind == "progress":
                    progress_var.set(payload)
                    progress_bar.update_idletasks()
                elif kind == "done_add":
                    if payload.get("__cancelled__"):
                        messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuário.")
                    else:
                        resultados_list.append(payload)
                        atualizar_tree(tree)
                        messagebox.showinfo("Concluído", "PDF adicional processado e mesclado!")
                    return
                elif kind == "error":
                    messagebox.showerror("Erro", payload)
                    return
        except queue.Empty:
            pass
        root.after(10, poll_queue_add)

    poll_queue_add()
    
=======
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

>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
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

<<<<<<< HEAD
def processar_pdf_sem_ui(caminho_pdf, on_progress=None, cancel_event=False):
    """
    Faz TODO o trabalho pesado AQUI, SEM chamar messagebox, progress_bar,
    root.update_idletasks, etc. Reporta progresso via on_progress(%).
    Retorna o dict 'resultados' igual ao que você já usa.
    """
    resultados = {}
    vendedor_atual = None

    # se não vier nada, cria versões "neutras"
    if on_progress is None:
        on_progress = lambda *args, **kwargs: None
    if cancel_event is None or isinstance(cancel_event, bool):
        cancel_event = threading.Event()
        
=======
def processar_pdf(caminho_pdf, progress_var, progress_bar, root):
    resultados = {}
    vendedor_atual = None

>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
    def fechar_vendedor():
        nonlocal vendedor_atual
        if vendedor_atual and vendedor_atual in resultados:
            dados = resultados[vendedor_atual]
            dados["total_clientes"] = dados["atendidos"] - dados["devolucoes"]

<<<<<<< HEAD
    with pdfplumber.open(caminho_pdf) as pdf:
        total = len(pdf.pages)
        for i, pagina in enumerate(pdf.pages, start=1):
            if cancel_event.is_set():
                return {"__cancelled__": True}

            try:
                texto = pagina.extract_text() or ""
                for linha in texto.splitlines():
=======
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
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
                    if "Vendedor: " in linha:
                        fechar_vendedor()
                        partes = linha.split("Vendedor: ", 1)[1].strip()
                        if partes:
                            palavras = partes.split()
<<<<<<< HEAD
                            if palavras and palavras[0].isdigit():
                                vendedor_bruto = " ".join(palavras[1:])
                            else:
                                vendedor_bruto = " ".join(palavras)
                            vendedor_atual = canonicalize_name(vendedor_bruto)
                            if vendedor_atual not in resultados:
                                resultados[vendedor_atual] = {
                                    "atendidos": 0,
                                    "devolucoes": 0,
                                    "total_clientes": 0,
                                    "total_vendas": ""
                                }
=======
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
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
                        continue

                    if regex_data.match(linha):
                        if not vendedor_atual:
                            continue
                        resultados[vendedor_atual]["atendidos"] += 1
                        if regex_negativo.search(linha):
                            resultados[vendedor_atual]["devolucoes"] += 1

                    if "Totais" in linha and vendedor_atual:
<<<<<<< HEAD
                        m = re.search(r"Totais:\s*([\d\.\,]+)", linha)
                        if m:
                            resultados[vendedor_atual]["total_vendas"] = m.group(1)

                # Atualiza o progresso a cada página
                progresso = int(i * 100 / max(1, total))
                on_progress("progress", progresso)
            except Exception as e:
                logger.error(f"Erro na página {i}: {str(e)}")
                on_progress("progress", 100)
                raise

        # Garante que o progresso chegue a 100% após o loop
        fechar_vendedor()
=======
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
    
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
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
<<<<<<< HEAD
        messagebox.showerror("Erro na Atualização", f"Ocorreu um erro ao checar atualizações: {e}")

def limpar_tabelas(tree, tree_planilha, arquivos_label_var, progress_var):
    # limpa tabela de PDFs
    for item in tree.get_children():
        tree.delete(item)

    # limpa tabela da planilha
    for item in tree_planilha.get_children():
        tree_planilha.delete(item)

    # reseta variáveis
    arquivos_label_var.set("Nenhum arquivo carregado ainda")
    progress_var.set(0)

    # também limpa lista de resultados
    from globalVar import resultados_list, arquivosLista
    resultados_list.clear()
    arquivosLista.clear()

    messagebox.showinfo("Limpo", "Todas as tabelas foram limpas com sucesso!")

def exportar_para_excel(tree):
    try:
        # Pegar os dados da Treeview
        cols = [tree.heading(col)["text"] for col in tree["columns"]]
        dados = []
        for item in tree.get_children():
            valores = tree.item(item)["values"]
            dados.append(valores)

        if not dados:
            messagebox.showwarning("Aviso", "Não há dados para exportar.")
            return

        # Converter para DataFrame
        df = pd.DataFrame(dados, columns=cols)
        
         # 🔹 Forçar colunas numéricas (se existirem)
        colunas_numericas = ["Atendidos", "Devoluções", "Total Final", "Total Vendas"]
        for col in colunas_numericas:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                    .str.extract(r"([\d\.]+)", expand=False)  # pega só números
                    .astype(float)
                )

        # Selecionar local para salvar
        caminho = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Arquivo Excel", "*.xlsx")],
            title="Salvar relatório"
        )
        if not caminho:
            return

        # Salvar em Excel
        df.to_excel(caminho, index=False, engine="openpyxl")
        messagebox.showinfo("Sucesso", f"✅ Relatório exportado para:\n{caminho}")

    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao exportar para Excel: {e}")
=======
        messagebox.showerror("Erro na Atualização", f"Ocorreu um erro ao checar atualizações: {e}")
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
