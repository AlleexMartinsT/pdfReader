from library import queue, threading, os, re, json, time, pdfplumber, logging, messagebox, difflib, filedialog, pd
from globalVar import arquivosLista, resultados_lista, regex_data, regex_negativo, APP_VERSION, GITHUB_REPO

# Configuração de logging mais leve (somente avisos e erros)
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)
progress_queue = queue.Queue()
cancel_event = threading.Event()

def cancelar_processamento(): 
    from tk import btn_cancelar
    
    cancel_event.set()
    with progress_queue.mutex:
        progress_queue.queue.clear()
        btn_cancelar.configure(state="disabled")

def _poll_queue(root, tree, progress_var, progress_bar, arquivos_label_var=None, caminho=None): 
    """Roda na main thread: consome eventos vindos da thread e atualiza a UI."""
    
    from tk import btn_cancelar
    
    try:
        while True:
            kind, payload = progress_queue.get_nowait()
            if kind == "progress":
                progress_var.set(payload)
                progress_bar.update_idletasks()
            elif kind == "done":
                btn_cancelar.configure(state="disabled")
                if payload.get("__cancelled__"):
                    progress_var.set(0)
                    messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuário.")
                else:
                    for item in tree.get_children():
                        tree.delete(item)
                    arquivosLista.clear()
                    arquivosLista.append(caminho)
                    arquivos_label_var.set(f"Arquivo carregado: {os.path.basename(caminho)}")
                    resultados_lista.append(payload)
                    atualizar_tree(tree)
                    messagebox.showinfo("Concluído", "Processamento finalizado com sucesso!")
                return 
            elif kind == "error":
                btn_cancelar.configure(state="disabled")
                messagebox.showerror("Erro", payload)
                return
    except queue.Empty:
        pass
    # agendar próxima rodada de checagem
    root.after(10, lambda: _poll_queue(root, tree, progress_var, progress_bar, arquivos_label_var, caminho))

def resource_path(relative_path): 
    import sys
    
    """Retorna o caminho absoluto do recurso, compatível com PyInstaller."""
    if hasattr(sys, '_MEIPASS'):  # Executando empacotado
        base_path = sys._MEIPASS
    else:
        base_path = getattr(sys, "_MEIPASS", os.path.dirname(__file__))
    return os.path.join(base_path, relative_path)

def load_mapping(path='mapping.json'): 
  
    full_path = resource_path(path)
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

def extrair_planilha_online():
    import gspread
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

def carregar_planilha_async(tree_planilha, progress_var, progress_bar, root):
    try:
        cancel_event.clear()
        progress_var.set(0)

        for item in tree_planilha.get_children():
            tree_planilha.delete(item)

        def worker():
            progressQueuePlanilha.put(("ui", {"action": "start_indeterminate"}))

            try:
                dfMVA, dfEH = extrair_planilha_online()
                df_total = pd.concat([dfMVA, dfEH], ignore_index=True)

                total_rows = len(df_total)
                resultados = []

                for i, (_, row) in enumerate(df_total.iterrows(), start=1):
                    # 🔹 Verifica se foi cancelado
                    if cancel_event.is_set():
                        progressQueuePlanilha.put(("done_planilha", {"__cancelled__": True}))
                        return

                    vendedor = str(row.iloc[0]).strip()
                    if not vendedor:
                        continue

                    valores = row[1:]
                    total = 0.0
                    atendidos = 0

                    for v in valores:
                        if pd.isna(v) or str(v).strip() == "":
                            continue
                        try:
                            num = str(v).replace("R$", "").replace(".", "").replace(",", ".").strip()
                            total += float(num)
                            atendidos += 1
                        except:
                            pass

                    if total > 0:
                        resultados.append((vendedor, atendidos, total))

                    # 🔹 Atualiza progresso gradualmente
                    progresso = int(i * 100 / max(1, total_rows))
                    progressQueuePlanilha.put(("progress", progresso))
                    time.sleep(0.01)

                progressQueuePlanilha.put(("done_planilha", resultados))

            except Exception as e:
                progressQueuePlanilha.put(("error", f"Erro ao carregar planilha: {e}"))

        progressQueuePlanilha = queue.Queue()
        worker_thread = threading.Thread(target=worker, daemon=True)
        worker_thread.start()

        def poll_queue_planilha():
            try:
                while True:
                    kind, payload = progressQueuePlanilha.get_nowait()
                    if kind == "progress":
                        if str(progress_bar["mode"]) == "indeterminate":
                            progress_bar.stop()
                            progress_bar.config(mode="determinate")
                        progress_var.set(payload)
                        progress_bar.update_idletasks()
                    elif kind == "ui":
                        action = payload.get("action")
                        if action == "start_indeterminate":
                            progress_bar.config(mode="indeterminate")
                            progress_bar.start(10)
                    elif kind == "done_planilha":
                        if isinstance(payload, dict) and payload.get("__cancelled__"):
                            progress_bar.stop()
                            progress_bar.config(mode="determinate")
                            progress_var.set(0)
                            messagebox.showinfo("Cancelado", "❌ Carregamento da planilha foi cancelado.")
                        else:
                            for vendedor, atendidos, total in payload:
                                tree_planilha.insert(
                                    "",
                                    "end",
                                    values=(
                                        vendedor,
                                        atendidos,
                                        f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                                    )
                                )
                            messagebox.showinfo("Sucesso", "✅ Planilha online carregada com sucesso!")
                        return
                    elif kind == "error":
                        messagebox.showerror("Erro", payload)
                        return
            except queue.Empty:
                pass
            root.after(10, poll_queue_planilha)

        poll_queue_planilha()

    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao iniciar carregamento da planilha: {e}")

def escolher_pdf_async(tree, progress_var, progress_bar, root, arquivos_label_var, btn_cancelar):
    
    global arquivosLista, resultados_lista
    worker_thread = None
    
    if not isinstance(arquivosLista, list):
        arquivosLista = []
    if arquivosLista and len(arquivosLista) > 1: # Para acaso tiver usado o botão "Adicionar PDF", então ele vai limpar tudo.
        arquivosLista.clear()
            
        
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    progress_var.set(0)
    
    if caminho:
        if caminho in arquivosLista:
            messagebox.showerror("Erro", "Arquivo já importado!")
            return       
    else:
        return
             
    # Zera estado, incluindo a lista de resultados
    cancel_event.clear()
    resultados_lista = []

    # Dispara worker
    def worker():
        try:
            resultados = processar_pdf_sem_ui(
                caminho,
                on_progress=lambda kind, payload: progress_queue.put((kind, payload)),
                cancel_event=cancel_event
            )
            progress_queue.put(("done", resultados))
        except Exception as e:
            print(f"Erro na thread: {str(e)}")
            progress_queue.put(("error", str(e)))
    
    btn_cancelar.configure(state="normal")
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()

    # Começa a escutar a fila na main thread
    _poll_queue(root, tree, progress_var, progress_bar, arquivos_label_var, caminho)

def adicionar_pdf(tree, progress_var, progress_bar, root, arquivos_label_var):
    from tk import btn_cancelar
     
    global arquivosLista
    local_queue = queue.Queue()
    
    # Checa se algum PDF ja foi importado antes
    
    if not resultados_lista or not arquivosLista:
        messagebox.showwarning("Aviso", "Selecione o primeiro PDF antes de adicionar outro.")
        return
        
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    progress_var.set(0)
    
    if not caminho:
        return
    
    if caminho in arquivosLista:
        messagebox.showerror("Erro", "Arquivo já importado!")
        return
    
    try:
        with pdfplumber.open(caminho) as pdf:
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
    
    atual = arquivos_label_var.get().replace("Arquivo carregado:", "").replace("Arquivos carregados:", "").strip()
    
    # Reset progresso
    cancel_event.clear()

    def atualizar_label_arquivos(arquivos_label_var):
        """Atualiza o label baseado em arquivosLista."""
        if not arquivosLista:
            arquivos_label_var.set("Nenhum arquivo carregado ainda")
        elif len(arquivosLista) == 1:
            arquivos_label_var.set(f"Arquivo carregado: {os.path.basename(arquivosLista[0])}")
        else:
            nomes = ", ".join(os.path.basename(p) for p in arquivosLista)
            arquivos_label_var.set(f"Arquivos carregados: {nomes}")

    # Thread worker
    def worker():
        try:
            res = processar_pdf_sem_ui(
                caminho,
                on_progress=lambda kind, payload: local_queue.put((kind, payload)),
                cancel_event=cancel_event
            )
            local_queue.put(("done_add", res))
        except Exception as e:
            local_queue.put(("error", str(e)))

    btn_cancelar.configure(state="normal")
    threading.Thread(target=worker, daemon=True).start()

    # começa a escutar fila
    def poll_queue_add():
        try:
            while True:
                kind, payload = local_queue.get_nowait()
                if kind == "progress":
                    progress_var.set(payload)
                    progress_bar.update_idletasks()
                elif kind == "done_add":
                    btn_cancelar.configure(state="disabled")
                    if payload.get("__cancelled__"):
                        progress_var.set(0)
                        messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuário.")
                    elif payload.get("__empty__"):
                        messagebox.showwarning("Aviso", "Nenhum dado foi encontrado neste PDF.")
                    else:
                        if atual and atual != "Nenhum arquivo carregado ainda":
                            arquivos_label_var.set(f"Arquivos carregados: {atual}, {os.path.basename(caminho)}")
                        else:
                            arquivos_label_var.set(f"Arquivo carregado: {os.path.basename(caminho)}")
                        arquivosLista.append(caminho)
                        resultados_lista.append(payload)
                        atualizar_tree(tree)
                        messagebox.showinfo("Concluído", "PDF adicional processado e mesclado!")
                    return
                elif kind == "error":
                    btn_cancelar.configure(state="disabled")
                    messagebox.showerror("Erro", payload)
                    return
        except queue.Empty:
            pass
        root.after(10, poll_queue_add)

    poll_queue_add()
    
def atualizar_tree(tree):
    for item in tree.get_children():
        tree.delete(item)
    
    mesclado = mesclar_resultados(resultados_lista)
    
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

def mesclar_resultados(resultados_lista):
    mesclado = {}
    cache_canon = {}  # 🔹 Cache para memoização de canonicalize_name

    for res in resultados_lista:
        for vend, dados in res.items():
            # Usa o cache para evitar chamadas repetidas a canonicalize_name
            if vend in cache_canon:
                canon = cache_canon[vend]
            else:
                canon = canonicalize_name(vend)
                cache_canon[vend] = canon

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

    # 🔹 Recalcula clientes finais uma vez ao final
    for dados in mesclado.values():
        dados["total_clientes"] = dados["atendidos"] - dados["devolucoes"]

    return mesclado

def processar_pdf_sem_ui(caminho_pdf, on_progress=None, cancel_event: threading.Event | None = None):
    """
    Faz TODO o trabalho pesado AQUI, SEM chamar messagebox, progress_bar,
    root.update_idletasks, etc. Reporta progresso via on_progress(%).
    Retorna o dict 'resultados'
    """
    resultados = {}
    vendedor_atual = None

    # se não vier nada, cria versões "neutras"
    if on_progress is None:
        on_progress = lambda *args, **kwargs: None
    if cancel_event is None:
        cancel_event = threading.Event()
        
    def fechar_vendedor():
        nonlocal vendedor_atual
        if vendedor_atual and vendedor_atual in resultados:
            dados = resultados[vendedor_atual]
            dados["total_clientes"] = dados["atendidos"] - dados["devolucoes"]

    with pdfplumber.open(caminho_pdf) as pdf:
        total = len(pdf.pages)
        for i, pagina in enumerate(pdf.pages, start=1):
            if cancel_event.is_set():
                return {"__cancelled__": True}

            try:
                texto = pagina.extract_text() or ""
                for linha in texto.splitlines():
                    if "Vendedor: " in linha:
                        fechar_vendedor()
                        partes = linha.split("Vendedor: ", 1)[1].strip()
                        if partes:
                            palavras = partes.split()
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
                        continue

                    if regex_data.match(linha):
                        if not vendedor_atual:
                            continue
                        resultados[vendedor_atual]["atendidos"] += 1
                        if regex_negativo.search(linha):
                            resultados[vendedor_atual]["devolucoes"] += 1

                    if "Totais" in linha and vendedor_atual:
                        m = re.search(r"Totais:\s*([\d\.\,]+)", linha)
                        if m:
                            resultados[vendedor_atual]["total_vendas"] = m.group(1)

                # Atualiza o progresso a cada página
                progresso = int(i * 100 / max(1, total))
                on_progress("progress", progresso)
            except Exception as e:
                logger.exception(f"Erro na página {i}: {e}")
                return {"__error__": str(e)}

        # Garante que o progresso chegue a 100% após o loop
        fechar_vendedor()
    if not resultados:
        return {"__empty__": True}
    return resultados

def ordenar_coluna(tree, col, reverse):
    dados = [(tree.set(k, col), k) for k in tree.get_children('')]
    
    def try_num(v):
        v = str(v)
        try:
            return float(v.replace(".", "").replace(",", "."))
        except:
            return v.lower()
        
    dados.sort(key=lambda t: try_num(t[0]), reverse=reverse)

    for index, (val, k) in enumerate(dados):
        tree.move(k, '', index)

    tree.heading(col, command=lambda: ordenar_coluna(tree, col, not reverse))

def check_for_updates(root):
    import requests
    
    def worker():
        try:
            # Checa versão
            response = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest", timeout=10)
            response.raise_for_status()
            data = response.json()
            latest_version = data["tag_name"].lstrip("v")

            if latest_version > APP_VERSION:
                # Mostra diálogo na thread principal usando after()
                def ask_user():
                    if messagebox.askyesno("Atualização Disponível",
                        f"Uma nova versão ({latest_version}) está disponível! Deseja baixar agora?"):
                        asset_url = data["assets"][0]["browser_download_url"]
                        new_file = f"Relatório de Clientes {latest_version}.exe"
                        try:
                            download = requests.get(asset_url, stream=True, timeout=30)
                            with open(new_file, "wb") as f:
                                for chunk in download.iter_content(8192):
                                    f.write(chunk)
                            messagebox.showinfo("Atualizado",
                                f"Nova versão baixada como '{new_file}'. "
                                "Feche o app, substitua o arquivo atual por esse novo e reinicie.")
                        except Exception as e:
                            messagebox.showerror("Erro no Download", f"Ocorreu um erro: {e}")
                root.after(0, ask_user)  # root é sua janela Tk principal
            else:
                print("App atualizado.")

        except Exception as e:
            root.after(0, lambda: messagebox.showerror("Erro na Atualização",
                                                       f"Ocorreu um erro ao checar atualizações: {e}"))

    threading.Thread(target=worker, daemon=True).start()

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
    from globalVar import resultados_lista, arquivosLista
    resultados_lista.clear()
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
                    pd.to_numeric(
                        df[col]
                        .astype(str)
                        .str.replace(".", "", regex=False)  # remove separador de milhar
                        .str.replace(",", ".", regex=False) # troca vírgula por ponto
                        .str.extract(r"([\d\.]+)", expand=False),  # pega só números
                        errors='coerce'  # valores inválidos viram NaN
                    )
                    .fillna(0.0)  # substitui NaN por 0.0 ou outro valor adequado
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
