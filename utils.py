from library import queue, threading, os, re, json, time, pdfplumber, messagebox, difflib, pd, hashlib
from global_vars import (
    listFiles, list_results, regex_data, regex_negative, 
    APP_VERSION, GITHUB_REPO, LAST_EH, LAST_MVA , LAST_HASH_MERGE,
    SALES_PERIOD
) 

# Configuração de logging mais leve (somente avisos e erros)
progress_queue = queue.Queue()
cancel_event = threading.Event()
LAST_STATE_SPREADSHEET = {}

def set_btn_cancel(state="disabled"):
    from tk import btn_cancel
    
    btn_cancel.configure(state=state)

def process_cancel(): 
    cancel_event.set()
    while not progress_queue.empty():
        try:
            progress_queue.get_nowait()
        except queue.Empty:
            break
    set_btn_cancel()
    # 🔹 Reseta barra
    from tk import progress_var, progress_bar
    progress_var.set(0)
    progress_bar.stop()
    progress_bar.config(mode="determinate")

def _poll_queue(root, tree, progress_var, progress_bar, label_files_var=None, path_var=None):
    """Consome eventos da fila em intervalos e atualiza a UI sem travar."""

    try:
        kind, payload = progress_queue.get_nowait()
    except queue.Empty:
        # Agenda a próxima checagem em 50ms (menos carga na CPU/UI)
        root.after(50, lambda: _poll_queue(root, tree, progress_var, progress_bar, label_files_var, path_var))
        return

    if kind == "progress":
        progress_var.set(payload)
        progress_bar.update_idletasks()

    elif kind == "done":
        set_btn_cancel()
        if payload.get("__cancelled__"):
            progress_var.set(0)
            messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuário.")
        else:
            # payload agora é {"results": resultados, "source": origem, "path_var": caminho}
            results = payload.get("resultados")
            source = payload.get("origem")
            path_var = payload.get("caminho")

            # garante que results_by_source exista no globalVar
            try:
                from global_vars import results_by_source
            except Exception:
                results_by_source = {"MVA": [], "EH": []}

            # armazena por origem
            if source not in results_by_source:
                results_by_source[source] = []
            results_by_source[source].append((path_var, results))

            # armazena lista global e atualiza a tree
            listFiles.append(path_var)
            list_results.append(results)

            # atualiza a interface (label e tree)
            label_files_var.set(f"Arquivo carregado: {os.path.basename(path_var)} ({source})")
            tree_update(tree)
            messagebox.showinfo("Concluído", f"Processamento finalizado ({source})!")

    elif kind == "error":
        set_btn_cancel()
        messagebox.showerror("Erro", payload)
        return

    # Sempre agenda a próxima checagem, exceto se houve erro (onde damos return acima)
    root.after(50, lambda: _poll_queue(root, tree, progress_var, progress_bar, label_files_var, path_var))

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
    """Converte string numérica em float, suportando formatos BR e US, removendo R$."""
    if not num_str:
        return 0.0
    num_str = str(num_str).strip().replace("R$", "").replace(" ", "")

    # Caso brasileiro: 79.833,85
    if "," in num_str and "." in num_str and num_str.rfind(",") > num_str.rfind("."):
        num_str = num_str.replace(".", "").replace(",", ".")
        return float(num_str)

    # Caso americano: 92,229.51
    if "," in num_str and "." in num_str and num_str.rfind(".") < num_str.rfind(","):
        num_str = num_str.replace(",", "")
        return float(num_str)

    # Caso só vírgula (310,75)
    if "," in num_str:
        return float(num_str.replace(",", "."))

    # Caso só ponto (310.75)
    if "." in num_str:
        return float(num_str)

    # Caso números puros
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

def criar_etiquetas():
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from tkinter import filedialog
    from global_vars import results_by_source

    # precisa ter MVA e EH carregados
    if not results_by_source["MVA"] or not results_by_source["EH"]:
        messagebox.showwarning("Aviso", "É necessário carregar os dois PDFs (MVA e EH).")
        return

    caminho = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("Arquivo PDF", "*.pdf")],
        title="Salvar etiquetas"
    )
    if not caminho:
        return

    c = canvas.Canvas(caminho, pagesize=A4)
    largura, altura = A4
    y = altura - 50
    c.setFont("Helvetica", 11)

    # junta os vendedores que aparecem em qualquer um dos dois
    vendedores = set()
    for _, res in results_by_source["MVA"]:
        vendedores.update(res.keys())
    for _, res in results_by_source["EH"]:
        vendedores.update(res.keys())

    for vendedor in sorted(vendedores):
        total_mva = 0.0
        total_eh = 0.0
        atendidos_mva = 0
        atendidos_eh = 0

        # soma MVA
        for _, res in results_by_source["MVA"]:
            if vendedor in res:
                total_mva += parse_number(res[vendedor].get("total_vendas", 0))
                atendidos_mva += res[vendedor].get("atendidos", 0)

        # soma EH
        for _, res in results_by_source["EH"]:
            if vendedor in res:
                total_eh += parse_number(res[vendedor].get("total_vendas", 0))
                atendidos_eh += res[vendedor].get("atendidos", 0)

        total_final = total_mva + total_eh
        atendidos_total = atendidos_mva + atendidos_eh

        # imprime no PDF em duas linhas
        linha1 = f"{vendedor} = {format_number_br(total_mva)} + {format_number_br(total_eh)} = {format_number_br(total_final)}"
        linha2 = f"Clientes atendidos = {atendidos_mva} + {atendidos_eh} = {atendidos_total}"

        c.drawString(50, y, linha1)
        y -= 15
        c.drawString(50, y, linha2)
        y -= 30

        if y < 50:  # quebra página
            c.showPage()
            c.setFont("Helvetica", 11)
            y = altura - 50

    c.save()
    messagebox.showinfo("Sucesso", f"✅ Etiquetas geradas em:\n{caminho}")

def extrair_planilha_online():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    
    global LAST_MVA, LAST_EH  # usar globais para comparar depois
    
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

    # corrige duplicados
    colsMVA = [f"col{i}_{c}" if colsMVA.count(c) > 1 else c for i, c in enumerate(colsMVA)]
    colsEH = [f"col{i}_{c}" if colsEH.count(c) > 1 else c for i, c in enumerate(colsEH)]

    dfMVA = pd.DataFrame(valoresMVA[2:], columns=colsMVA)
    dfEH = pd.DataFrame(valoresEH[2:], columns=colsEH)

    # 🔎 COMPARAÇÃO com os últimos dados exportados
    if LAST_MVA is not None and LAST_EH is not None:
        if dfMVA.equals(LAST_MVA) and dfEH.equals(LAST_EH):
            return None

    # Atualiza os globais com os novos dados
    LAST_MVA, LAST_EH = dfMVA.copy(), dfEH.copy()

    return dfMVA, dfEH

def carregar_planilha_async(tree_planilha, progress_var, progress_bar, root):
    from tk import btn_merge_spreadsheet
    
    try:
        cancel_event.clear()
        progress_var.set(0)

        def worker():
            progressQueuePlanilha.put(("ui", {"action": "start_indeterminate"}))

            try:
                resultado = extrair_planilha_online()
                if resultado is None:
                    progress_bar.stop()
                    progress_bar.config(mode="determinate")
                    progress_var.set(0)
                    return messagebox.showinfo("Aviso", "Nenhum dado novo foi adicionado")
                else:
                    for item in tree_planilha.get_children():
                        tree_planilha.delete(item)
                    if btn_merge_spreadsheet == "disabled":
                        btn_merge_spreadsheet.configure(state="normal")
                    
                    dfMVA, dfEH = resultado
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
                    time.sleep(0.02)

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

def source_pdf_async(tree, progress_var, progress_bar, root, label_files_var, btn_cancel, caminho, origem):
    """
    Inicia processamento do PDF já com o caminho e a origem (MVA/EH) escolhidos.
    Não pergunta nada ao usuário — a escolha já veio do tk.py.
    """
    global listFiles, list_results

    if not caminho:
        return

    # evita duplicados
    if caminho in listFiles:
        messagebox.showerror("Erro", "Arquivo já importado!")
        return

    cancel_event.clear()
    progress_var.set(0)

    # worker: roda processar_pdf_sem_ui (sem UI) e envia resultado para a fila com origem
    def worker():
        try:
            resultados = processar_pdf_sem_ui(
                caminho,
                on_progress=lambda kind, payload: progress_queue.put((kind, payload)),
                cancel_event=cancel_event
            )
            # empacota resultado com origem e caminho
            progress_queue.put(("done", {"resultados": resultados, "origem": origem, "caminho": caminho}))
        except Exception as e:

            progress_queue.put(("error", str(e)))

    # habilita botão cancelar (na main thread via chamada)
    btn_cancel.configure(state="normal")

    # inicia thread de processamento
    worker_thread = threading.Thread(target=worker, daemon=True)
    worker_thread.start()

    # inicializa o polling (main thread)
    _poll_queue(root, tree, progress_var, progress_bar, label_files_var)

def adicionar_pdf(tree, progress_var, progress_bar, root, label_files_var):
    from tkinter import filedialog
    from tk import btn_tag
    
    global listFiles, list_results
    local_queue = queue.Queue()
    
    # Se nenhum PDF foi carregado ainda, obriga usar "Escolher PDF" primeiro
    if not list_results or not listFiles:
        messagebox.showwarning("Aviso", "Selecione o primeiro PDF (com origem definida) antes de adicionar outro.")
        return

    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    if not caminho:
        return
    
    if caminho in listFiles:
        messagebox.showerror("Erro", "Arquivo já importado!")
        return
    
    try:
        with pdfplumber.open(caminho) as pdf:
            if pdf.metadata.get("encrypted", False):
                messagebox.showerror("Erro", "Este PDF está protegido por senha.")
                return
    except Exception as e:
        messagebox.showerror("Erro", f"Não foi possível abrir o PDF: {e}")
        return

    # --- Determina origem automaticamente ---
    try:
        from global_vars import results_by_source
    except Exception:
        results_by_source = {"MVA": [], "EH": []}

    if results_by_source["MVA"]:
        origem = "EH"
    elif results_by_source["EH"]:
        origem = "MVA"
    else:
        origem = "MVA"

    # Atualiza o label imediatamente com a origem atribuída
    label_files_var.set(
        f"Carregando: {os.path.basename(caminho)} ({origem})"
    )
    cancel_event.clear()
    progress_var.set(0)

    def worker():
        try:
            res = processar_pdf_sem_ui(
                caminho,
                on_progress=lambda kind, payload: local_queue.put((kind, payload)),
                cancel_event=cancel_event
            )
            # embala já com a origem
            local_queue.put(("done_add", {"resultados": res, "origem": origem, "caminho": caminho}))
        except Exception as e:
            local_queue.put(("error", str(e)))

    set_btn_cancel(state="normal")
    threading.Thread(target=worker, daemon=True).start()

    def poll_queue_add():
        try:
            while True:
                kind, payload = local_queue.get_nowait()
                if kind == "progress":
                    progress_var.set(payload)
                    progress_bar.update_idletasks()
                elif kind == "done_add":
                    set_btn_cancel()
                    resultados = payload["resultados"]
                    origem = payload["origem"]
                    caminho = payload["caminho"]

                    if resultados.get("__cancelled__"):
                        progress_var.set(0)
                        messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuário.")
                    elif resultados.get("__empty__"):
                        messagebox.showwarning("Aviso", "Nenhum dado foi encontrado neste PDF.")
                    else:
                        # Armazena os dados
                        listFiles.append(caminho)
                        list_results.append(resultados)
                        results_by_source[origem].append((caminho, resultados))  # salva também o caminho para referência

                        # 🔹 Monta o texto do label com todos os arquivos e origens
                        partes = []
                        for caminho_salvo in listFiles:
                            nome = os.path.basename(caminho_salvo)
                            origem_arquivo = "?"
                            for org, lista in results_by_source.items():
                                if any(c == caminho_salvo or (isinstance(c, tuple) and c[0] == caminho_salvo) for c in lista):
                                    origem_arquivo = org
                                    break
                            partes.append(f"{nome} ({origem_arquivo})")
                        if len(partes) == 1:
                            label_files_var.set(f"Arquivo carregado: {partes[0]}")
                        else:
                            label_files_var.set("Arquivos carregados: " + ", ".join(partes))


                        tree_update(tree)
                        btn_tag.configure(state="normal", fg_color="#44cc64")
                        messagebox.showinfo("Concluído", f"PDF adicional processado e atribuído a {origem}!")
                    return

                elif kind == "error":
                    set_btn_cancel()
                    messagebox.showerror("Erro", payload)
                    return
        except queue.Empty:
            pass
        root.after(10, poll_queue_add)

    poll_queue_add()
  
def tree_update(tree):
    for item in tree.get_children():
        tree.delete(item)
    
    mesclado = mesclar_resultados(list_results)
    
    for vendedor, dados in sorted(mesclado.items()):
        total_vendas_str = ""
        print(dados["total_vendas"])
        if dados["total_vendas"] > 0:
            total_vendas_str = format_number_br(dados["total_vendas"])
        else:
            total_vendas_str = format_number_br(abs(dados["total_vendas"]))   

        tree.insert("", "end", values=(
            vendedor,
            dados['atendidos'],
            dados['devolucoes'],
            dados['total_clientes'],
            total_vendas_str
        ))

def mesclar_resultados(list_results):
    mesclado = {}
    cache_canon = {}  # 🔹 Cache para memoização de canonicalize_name

    for res in list_results:
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
                                    "total_vendas": 0.0
                                }
                        continue

                    if regex_data.match(linha):
                        if not vendedor_atual:
                            continue
                        resultados[vendedor_atual]["atendidos"] += 1
                        if regex_negative.search(linha):
                            resultados[vendedor_atual]["devolucoes"] += 1

                    if "Totais" in linha and vendedor_atual:
                        m = re.search(r"Totais:\s*([\d\.\,]+)", linha)
                        if m:
                            resultados[vendedor_atual]["total_vendas"] = m.group(1)

                # Atualiza o progresso a cada página
                progresso = int(i * 100 / max(1, total))
                on_progress("progress", progresso)
            except Exception as e:
                return {"__error__": str(e)}

        # Garante que o progresso chegue a 100% após o loop
        fechar_vendedor()
    if not resultados:
        return {"__empty__": True}
    periodo = analisar_SALES_PERIOD(caminho_pdf)
    if periodo:
        print("Período de vendas:", periodo)
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

def limpar_tabelas(tree, tree_planilha, label_files_var, progress_var):
    
    global LAST_EH, LAST_MVA, LAST_STATE_SPREADSHEET, LAST_HASH_MERGE

    # limpa as tabelas
    for item in tree.get_children():
        tree.delete(item)
    for item in tree_planilha.get_children():
        tree_planilha.delete(item)

    # reseta variáveis da UI
    label_files_var.set("Nenhum arquivo carregado ainda")
    progress_var.set(0)

    # 🧹 Limpa histórico da mesclagem
    LAST_EH = None      
    LAST_MVA = None
    LAST_HASH_MERGE = None
    LAST_STATE_SPREADSHEET = {}    
    
    # também limpa lista de resultados
    from global_vars import list_results, listFiles
    from tk import btn_add_mais, btn_merge_spreadsheet, btn, btn_tag
    
    btn_merge_spreadsheet.configure(state="normal")
    btn_add_mais.configure(state="normal")
    btn.configure(state="normal")
    btn_tag.configure(state="disabled", fg_color="#EE9919", text_color_disabled="gray45")
    
    list_results.clear()
    listFiles.clear()

    messagebox.showinfo("Limpo", "Todas as tabelas foram limpas com sucesso!")
            
def _excel_export(tree):
    from tkinter import filedialog
    
    # Extrai os dados
    cols = [tree.heading(col)["text"] for col in tree["columns"]]
    dados = [tree.item(item)["values"] for item in tree.get_children()]

    if not dados:
        messagebox.showwarning("Aviso", "Não há dados para exportar.")
        return

    df = pd.DataFrame(dados, columns=cols)

    # Converter colunas numéricas
    colunas_numericas = ["Atendidos", "Devoluções", "Total Final", "Total Vendas"]
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False),
                errors="coerce"
            ).fillna(0.0)

    caminho = filedialog.asksaveasfilename(
        defaultextension=".xlsx",
        filetypes=[("Arquivo Excel", "*.xlsx")],
        title="Salvar relatório"
    )
    
    if not caminho:
        return
    
    df.to_excel(caminho, index=False, engine="openpyxl")
    
    messagebox.showinfo("Sucesso", f"✅ Relatório exportado para:\n{caminho}")

def _pdf_export(tree):
    
    from tkinter import filedialog
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    
    # Extrai os dados
    cols = [tree.heading(col)["text"] for col in tree["columns"]]
    dados = [tree.item(item)["values"] for item in tree.get_children()]

    if not dados:
        messagebox.showwarning("Aviso", "Não há dados para exportar.")
        return

    caminho = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("Arquivo PDF", "*.pdf")],
        title="Salvar relatório PDF"
    )
    if not caminho:
        return

    # Criar PDF simples
    c = canvas.Canvas(caminho, pagesize=A4)
    largura, altura = A4
    y = altura - 50
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "Relatório de Vendas")
    y -= 30

    # Cabeçalho
    c.setFont("Helvetica-Bold", 10)
    for i, col in enumerate(cols):
        c.drawString(50 + i * 120, y, col)
    y -= 20

    # Dados
    c.setFont("Helvetica", 9)
    for row in dados:
        for i, valor in enumerate(row):
            c.drawString(50 + i * 120, y, str(valor))
        y -= 20
        if y < 50:
            c.showPage()
            y = altura - 50

    c.save()
    messagebox.showinfo("Sucesso", f"✅ Relatório exportado para:\n{caminho}")

def mesclar_tabelas(tree, progress_var, progress_bar, root, label_files_var, tree_planilha, ):
    """
    Mescla os valores da planilha online (tree_planilha) na tabela de PDFs (tree).
    Atualiza a barra de progresso durante o processo.
    """
    from tk import btn_merge_spreadsheet, btn_add_mais, btn
    global LAST_HASH_MERGE, LAST_STATE_SPREADSHEET
    
    btn_merge_spreadsheet.configure(state="enabled")
    
     # 🔹 Verifica se alguma tabela está vazia
    if not tree.get_children():
        messagebox.showwarning("Aviso", "A tabela de PDFs está vazia. Importe pelo menos um PDF antes de mesclar.")
        return
    if not tree_planilha.get_children():
        messagebox.showwarning("Aviso", "A tabela da planilha está vazia. Carregue a planilha online antes de mesclar.")
        return
    
    dados_pdf_snapshot = [tree.item(i)["values"] for i in tree.get_children()]
    dados_planilha_snapshot = [tree_planilha.item(i)["values"] for i in tree_planilha.get_children()]

    # Serializa e gera hash único
    snapshot_str = json.dumps({
        "pdf": dados_pdf_snapshot,
        "planilha": dados_planilha_snapshot
    }, sort_keys=True)

    novo_hash = hashlib.md5(snapshot_str.encode()).hexdigest()

    if LAST_HASH_MERGE == novo_hash:
        messagebox.showinfo("Aviso", "⚠️ Esses dados já foram mesclados. Nenhuma alteração detectada.")
        return

    # Se for novo, salva para futuras comparações
    LAST_HASH_MERGE = novo_hash
    
    merge_queue = queue.Queue()

    # Thread worker para processamento pesado
    def worker():
        try:
            # 1) Extrai dados da Treeview PDFs
            dados_pdf = {}
            for item in tree.get_children():
                vals = tree.item(item)["values"]
                vendedor = str(vals[0]).strip()
                atendidos = int(vals[1])
                devolucoes = int(vals[2])
                total_clientes = int(vals[3])
                total_vendas = parse_number(str(vals[4]) if vals[4] else "0")
                dados_pdf[vendedor] = {
                    "atendidos": atendidos,
                    "devolucoes": devolucoes,
                    "total_clientes": total_clientes,
                    "total_vendas": total_vendas
                }

            # 2) Extrai dados da Treeview planilha
            dados_planilha = {}
            for idx, item in enumerate(tree_planilha.get_children(), start=1):
                vals = tree_planilha.item(item)["values"]
                vendedor = str(vals[0]).strip()
                atendidos = int(vals[1])
                total_vendas = parse_number(str(vals[2]) if vals[2] else "0")

                # 🔎 Verifica se este vendedor já existia e calcula apenas o incremento
                ultimo = LAST_STATE_SPREADSHEET.get(vendedor, {"atendidos":0, "total_vendas":0})
                delta_atendidos = max(0, atendidos - ultimo["atendidos"])
                delta_vendas = max(0, total_vendas - ultimo["total_vendas"])

                # Se nada mudou, pula para evitar soma duplicada
                if delta_atendidos == 0 and delta_vendas == 0:
                    continue
                
                dados_planilha[vendedor] = {
                    "atendidos": delta_atendidos,
                    "total_vendas": delta_vendas
                }

                # Atualiza o snapshot
                LAST_STATE_SPREADSHEET[vendedor] = {"atendidos": atendidos, "total_vendas": total_vendas}

                progresso = int(idx * 50 / max(1, len(tree_planilha.get_children())))
                merge_queue.put(("progress", progresso))
                time.sleep(0.01)


            # 3) Mescla os dados
            total_vendedores = len(set(dados_pdf.keys()) | set(dados_planilha.keys()))
            for idx, vendedor in enumerate(set(dados_pdf.keys()) | set(dados_planilha.keys()), start=1):
                pdf_data = dados_pdf.get(vendedor, {"atendidos":0,"devolucoes":0,"total_clientes":0,"total_vendas":0})
                plan_data = dados_planilha.get(vendedor, {"atendidos":0,"total_vendas":0})

                merged = {
                    "atendidos": pdf_data["atendidos"] + plan_data["atendidos"],
                    "devolucoes": pdf_data.get("devolucoes",0),
                    "total_clientes": (pdf_data["atendidos"] + plan_data["atendidos"]) - pdf_data.get("devolucoes",0),
                    "total_vendas": pdf_data["total_vendas"] + plan_data["total_vendas"]
                }

                dados_pdf[vendedor] = merged

                # Atualiza progresso
                progresso = 50 + int(idx * 50 / max(1, total_vendedores))
                merge_queue.put(("progress", progresso))
                time.sleep(0.01)

            merge_queue.put(("done", dados_pdf))
        except Exception as e:
            merge_queue.put(("error", str(e)))

    # Inicia thread
    threading.Thread(target=worker, daemon=True).start()

    # Poll da fila
    def poll_merge_queue():
        try:
            while True:
                kind, payload = merge_queue.get_nowait()
                if kind == "progress":
                    progress_var.set(payload)
                    progress_bar.update_idletasks()
                elif kind == "done":
                    btn_merge_spreadsheet.configure(state="disabled")
                    btn_add_mais.configure(state="disabled")
                    btn.configure(state="disabled")
                    if payload.get("__cancelled__"):
                        progress_var.set(0)
                        messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuário.")
                    # Atualiza Treeview dos PDFs
                    for item in tree.get_children():
                        tree.delete(item)
                    for vendedor, dados in sorted(payload.items()):
                        tree.insert("", "end", values=(
                            vendedor,
                            dados["atendidos"],
                            dados["devolucoes"],
                            dados["total_clientes"],
                            format_number_br(dados["total_vendas"])
                        ))
                    progress_var.set(100)
                    messagebox.showinfo("Concluído", "✅ Mesclagem de tabelas finalizada!")
                    return
                elif kind == "error":
                    messagebox.showerror("Erro", f"Erro na mesclagem: {payload}")
                    return
        except queue.Empty:
            pass
        root.after(10, poll_merge_queue)

    poll_merge_queue()

def analisar_SALES_PERIOD(caminho_pdf):
    """
    Analisa as datas de vendas em um PDF e retorna o período de vendas.
    """
    
    from datetime import datetime
    global SALES_PERIOD
    datas = []

    try:
        with pdfplumber.open(caminho_pdf) as pdf:
            for pagina in pdf.pages:
                texto = pagina.extract_text() or ""
                for linha in texto.splitlines():
                    # Procura datas com regex_data (já importado do globalVar)
                    if regex_data.match(linha):
                        data_str = regex_data.match(linha).group()
                        try:
                            data = datetime.strptime(data_str, "%d/%m/%Y")
                            datas.append(data)
                        except ValueError:
                            pass

        if not datas:
            SALES_PERIOD = None
            return None

        primeira = min(datas)
        ultima = max(datas)
        SALES_PERIOD = f"{primeira.strftime('%d/%m/%Y')} - {ultima.strftime('%d/%m/%Y')}"
        return SALES_PERIOD

    except Exception as e:
        print(f"Erro ao analisar período de vendas: {e}")
        SALES_PERIOD = None
        return None