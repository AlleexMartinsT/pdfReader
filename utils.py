import os
import re
import json
import threading
import queue
import difflib
from ui_dialogs import filedialog, messagebox
from global_vars import (
    listFiles, list_results, regex_data, regex_negative, 
    APP_VERSION, GITHUB_REPO, LAST_EH, LAST_MVA , LAST_HASH_MERGE,
    SALES_PERIOD
) 

# Configuração de logging mais leve (somente avisos e erros)
progress_queue = queue.Queue()
cancel_event = threading.Event()
LAST_STATE_SPREADSHEET = {}

_UI_REFS = {
    "btn_cancel": None,
    "progress_var": None,
    "progress_bar": None,
    "progress_var_online": None,
    "progress_bar_online": None,
    "btn_tag": None,
    "btn_add_mais": None,
    "btn_merge_spreadsheet": None,
    "btn_select_pdf": None,
}

REGEX_VENDOR_HEADER = re.compile(r"^\s*Vendedor(?:\(a\))?:\s*(.+?)\s*$", re.IGNORECASE)
REGEX_NEW_SALE_LINE = re.compile(r"^\s*(?:NFC|NF)-e\s+\d+\s+\d{2}/\d{2}/\d{4}\b", re.IGNORECASE)
REGEX_NEW_TOTALS_LINE = re.compile(r"^\s*Totais\s+R\$\s+([-\d\.,]+)\s+[-\d\.,]+\s*$", re.IGNORECASE)
REGEX_NEW_SALE_AMOUNT = re.compile(r"^\s*(?:NFC|NF)-e\s+\d+\s+\d{2}/\d{2}/\d{4}\b.*?\s+([-\d\.,]+)\s+[-\d\.,]+\s*$", re.IGNORECASE)
REGEX_ANY_DATE = re.compile(r"\b(\d{2}/\d{2}/\d{4})\b")
REGEX_D_MARKER = re.compile(r"\(\s*d\s*\)", re.IGNORECASE)


def _get_pd():
    import pandas as pd
    return pd


def _get_pdfplumber():
    import pdfplumber
    return pdfplumber


def set_ui_refs(**kwargs):
    _UI_REFS.update({k: v for k, v in kwargs.items() if k in _UI_REFS})

def set_btn_cancel(state="disabled"):
    btn_cancel = _UI_REFS.get("btn_cancel")
    if btn_cancel:
        btn_cancel.configure(state=state)


def _extract_vendor_name(line: str) -> str | None:
    match = REGEX_VENDOR_HEADER.match(line or "")
    if not match:
        return None
    return match.group(1).strip()


def _is_sale_entry_line(line: str) -> bool:
    if not line:
        return False
    return bool(regex_data.match(line) or REGEX_NEW_SALE_LINE.match(line))


def _extract_total_vendas(line: str) -> str | None:
    if not line:
        return None

    match = re.search(r"Totais:\s*([-\d\.,]+)", line)
    if match:
        return match.group(1)

    match = REGEX_NEW_TOTALS_LINE.match(line)
    if match:
        return match.group(1)

    return None


def _extract_sale_date(line: str) -> str | None:
    if not line:
        return None

    if REGEX_NEW_SALE_LINE.match(line):
        match = REGEX_ANY_DATE.search(line)
        if match:
            return match.group(1)

    match = regex_data.match(line)
    if not match:
        return None

    lowered = line.lower()
    if " ate " in lowered or " até " in lowered:
        return None

    return match.group().strip()


def _line_has_d_marker(line: str) -> bool:
    return bool(REGEX_D_MARKER.search(line or ""))


def _extract_sale_amount(line: str) -> float | None:
    match = REGEX_NEW_SALE_AMOUNT.match(line or "")
    if not match:
        return None
    try:
        return parse_number(match.group(1))
    except Exception:
        return None

def process_cancel(): 
    cancel_event.set()
    while not progress_queue.empty():
        try:
            progress_queue.get_nowait()
        except queue.Empty:
            break
    set_btn_cancel()
    # 🔹 Reseta barra
    progress_var = _UI_REFS.get("progress_var")
    progress_bar = _UI_REFS.get("progress_bar")
    progress_var_online = _UI_REFS.get("progress_var_online")
    progress_bar_online = _UI_REFS.get("progress_bar_online")
    if progress_var:
        progress_var.set(0)
    if progress_bar:
        progress_bar.stop()
        progress_bar.config(mode="determinate")
    if progress_var_online:
        progress_var_online.set(0)
    if progress_bar_online:
        progress_bar_online.stop()
        progress_bar_online.config(mode="determinate")


def _scroll_tree_to_top(tree) -> None:
    scroll = getattr(tree, "scroll_to_top", None)
    if callable(scroll):
        scroll()


def _has_visible_data(dados: dict) -> bool:
    atendidos = int(dados.get("atendidos", 0) or 0)
    devolucoes = int(dados.get("devolucoes", 0) or 0)
    total_clientes = int(dados.get("total_clientes", 0) or 0)

    try:
        total_vendas = parse_number(dados.get("total_vendas", 0))
    except Exception:
        total_vendas = 0.0

    return any((atendidos, devolucoes, total_clientes)) or abs(total_vendas) > 0


def _total_vendas_value(dados: dict) -> float:
    try:
        return parse_number(dados.get("total_vendas", 0))
    except Exception:
        return 0.0


def _sorted_rows_by_total_vendas(data: dict) -> list[tuple[str, dict]]:
    return sorted(
        data.items(),
        key=lambda item: (-_total_vendas_value(item[1]), item[0].casefold())
    )

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
        # payload agora ? {"results": resultados, "source": origem, "path_var": caminho}
        results = payload.get("resultados")
        source = payload.get("origem")
        path_var = payload.get("caminho")

        if not isinstance(results, dict):
            messagebox.showerror("Erro", "Resultado inv?lido do processamento.")
            return

        if results.get("__cancelled__"):
            progress_var.set(0)
            messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuário.")
            return
        if results.get("__empty__"):
            progress_var.set(0)
            messagebox.showwarning("Aviso", "Nenhum dado foi encontrado neste PDF.")
            return
        if results.get("__error__"):
            messagebox.showerror("Erro", results.get("__error__"))
            return

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
        _scroll_tree_to_top(tree)
        messagebox.showinfo("Conclu?do", f"Processamento finalizado ({source})!")
        for vendedor in results.keys():
            registrar_vendedor_db(vendedor)

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

mapping = None
CANON_BY_VALUE_UPPER = None


def _ensure_mapping_loaded():
    global mapping, CANON_BY_VALUE_UPPER
    if mapping is None:
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
    """Converte string numerica em float, suportando formatos BR e US, removendo R$."""
    if num_str is None:
        return 0.0
    if isinstance(num_str, (int, float)):
        return float(num_str)

    s = str(num_str).strip()
    if not s:
        return 0.0

    s = s.replace("R$", "").replace(" ", "").replace(" ", "")
    last_comma = s.rfind(",")
    last_dot = s.rfind(".")

    if last_comma != -1 and last_dot != -1:
        if last_comma > last_dot:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
        return float(s)

    if last_comma != -1:
        return float(s.replace(",", "."))

    return float(s)

def format_number_br(num: float) -> str:
    """Formata número no padrão brasileiro com duas casas decimais."""
    return f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def canonicalize_name(raw: str) -> str:
    _ensure_mapping_loaded()
    key = _normalize_key(raw)

    # 1) se existe como abreviação no mapping
    if key in mapping:
        return mapping[key]

    # 2) se já é o nome completo
    if key in CANON_BY_VALUE_UPPER:
        return CANON_BY_VALUE_UPPER[key]

    prefix_matches = [
        canon for canon_upper, canon in CANON_BY_VALUE_UPPER.items()
        if canon_upper.startswith(f"{key} ")
    ]
    if len(prefix_matches) == 1:
        return prefix_matches[0]

    # 3) fuzzy matching
    match = difflib.get_close_matches(key, list(CANON_BY_VALUE_UPPER.keys()), n=1, cutoff=0.93)
    if match:
        return CANON_BY_VALUE_UPPER[match[0]]

    # fallback
    return raw.strip().title()

# --- Funções principais ---

def criar_etiquetas_legacy():
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
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
        return False

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
        total_mva = total_eh = 0.0
        atendidos_mva = atendidos_eh = 0
        clientes_mva = clientes_eh = 0

        # soma MVA
        for _, res in results_by_source["MVA"]:
            if vendedor in res:
                total_mva += parse_number(res[vendedor].get("total_vendas", 0))
                atendidos_mva += res[vendedor].get("atendidos", 0)
                clientes_mva += res[vendedor].get("total_clientes", 0)

        # soma EH
        for _, res in results_by_source["EH"]:
            if vendedor in res:
                total_eh += parse_number(res[vendedor].get("total_vendas", 0))
                atendidos_eh += res[vendedor].get("atendidos", 0)
                clientes_eh += res[vendedor].get("total_clientes", 0)

        total_final = total_mva + total_eh
        clientes_total = clientes_mva + clientes_eh

        # imprime no PDF em duas linhas
        linha1 = f"{vendedor} = {format_number_br(total_mva)} + {format_number_br(total_eh)} = {format_number_br(total_final)}"
        linha2 = f"Clientes atendidos = {clientes_mva} + {clientes_eh} = {clientes_total}"

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

def _rows_from_tree_for_labels(tree):
    rows = []
    if tree is None:
        return rows

    for item in tree.get_children():
        values = tree.item(item).get("values", [])
        if not values:
            continue

        vendedor = str(values[0]).strip() if len(values) > 0 else ""
        if not vendedor:
            continue

        atendidos = int(parse_number(values[1])) if len(values) > 1 else 0
        devolucoes = int(parse_number(values[2])) if len(values) > 2 else 0
        total_clientes = int(parse_number(values[3])) if len(values) > 3 else 0
        total_vendas = parse_number(values[4]) if len(values) > 4 else 0.0

        rows.append({
            "vendedor": vendedor,
            "atendidos": atendidos,
            "devolucoes": devolucoes,
            "total_clientes": total_clientes,
            "total_vendas": total_vendas,
        })

    return rows


def criar_etiquetas(tree=None):
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from global_vars import results_by_source

    rows_from_table = _rows_from_tree_for_labels(tree)

    if not rows_from_table:
        if not results_by_source["MVA"] or not results_by_source["EH"]:
            messagebox.showwarning("Aviso", "E necessario carregar os dois PDFs (MVA e EH).")
            return

    caminho = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("Arquivo PDF", "*.pdf")],
        title="Salvar etiquetas"
    )
    if not caminho:
        return False

    c = canvas.Canvas(caminho, pagesize=A4)
    largura, altura = A4
    y = altura - 50
    c.setFont("Helvetica", 11)

    if rows_from_table:
        for row in sorted(rows_from_table, key=lambda x: x["vendedor"].lower()):
            linha1 = f"{row['vendedor']} = {format_number_br(row['total_vendas'])}"
            linha2 = (
                f"Atendidos: {row['atendidos']} | "
                f"Devolucoes: {row['devolucoes']} | "
                f"Total Final: {row['total_clientes']}"
            )
            c.drawString(50, y, linha1)
            y -= 15
            c.drawString(50, y, linha2)
            y -= 30
            if y < 50:
                c.showPage()
                c.setFont("Helvetica", 11)
                y = altura - 50
    else:
        vendedores = set()
        for _, res in results_by_source["MVA"]:
            vendedores.update(res.keys())
        for _, res in results_by_source["EH"]:
            vendedores.update(res.keys())

        for vendedor in sorted(vendedores):
            total_mva = total_eh = 0.0
            clientes_mva = clientes_eh = 0
            for _, res in results_by_source["MVA"]:
                if vendedor in res:
                    total_mva += parse_number(res[vendedor].get("total_vendas", 0))
                    clientes_mva += res[vendedor].get("total_clientes", 0)
            for _, res in results_by_source["EH"]:
                if vendedor in res:
                    total_eh += parse_number(res[vendedor].get("total_vendas", 0))
                    clientes_eh += res[vendedor].get("total_clientes", 0)

            total_final = total_mva + total_eh
            clientes_total = clientes_mva + clientes_eh
            linha1 = f"{vendedor} = {format_number_br(total_mva)} + {format_number_br(total_eh)} = {format_number_br(total_final)}"
            linha2 = f"Clientes atendidos = {clientes_mva} + {clientes_eh} = {clientes_total}"
            c.drawString(50, y, linha1)
            y -= 15
            c.drawString(50, y, linha2)
            y -= 30
            if y < 50:
                c.showPage()
                c.setFont("Helvetica", 11)
                y = altura - 50

    c.save()
    messagebox.showinfo("Sucesso", f"Etiquetas geradas em:\n{caminho}")


def extrair_planilha_online():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    pd = _get_pd()

    global LAST_MVA, LAST_EH  # usar globais para comparar depois

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    cred_path = resource_path(os.path.join("data", "credenciaisAPI.json"))

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

    # 🔎 COMPARAÇÃO com os últimos dados exportados (mantém compatibilidade)
    if LAST_MVA is not None and LAST_EH is not None:
        try:
            if dfMVA.equals(LAST_MVA) and dfEH.equals(LAST_EH):
                return None
        except Exception:
            # se ocorrer qualquer erro de comparação, continua (não bloqueia)
            pass

    # Atualiza os globais com os novos dados (mantém comportamento anterior)
    LAST_MVA, LAST_EH = dfMVA.copy(), dfEH.copy()

    # --- Agregação por vendedor (soma MVA + EH) ---
    agregados = {}
    canon_cache = {}

    # concatena ambas as abas para processar de forma uniforme
    df_total = pd.concat([dfMVA, dfEH], ignore_index=True)

    for row in df_total.itertuples(index=False):
        vendedor_raw = str(row[0]).strip()
        if not vendedor_raw or vendedor_raw.lower() in ["nan", "none", ""]:
            continue

        if vendedor_raw in canon_cache:
            vendedor = canon_cache[vendedor_raw]
        else:
            vendedor = canonicalize_name(vendedor_raw)
            canon_cache[vendedor_raw] = vendedor

        if vendedor not in agregados:
            agregados[vendedor] = {"atendidos": 0, "total_vendas": 0.0}

        atend_row = 0
        total_row = 0.0

        # percorre o resto das colunas da linha somando valores numéricos
        for v in row[1:]:
            if pd.isna(v) or str(v).strip() == "":
                continue
            try:
                num = parse_number(str(v))
                total_row += num
                atend_row += 1
            except Exception:
                # ignora conteúdos não numéricos
                continue

        agregados[vendedor]["atendidos"] += atend_row
        agregados[vendedor]["total_vendas"] += total_row

    # transforma em DataFrame ordenado
    df_agg = pd.DataFrame(
        [(v, d["atendidos"], d["total_vendas"]) for v, d in agregados.items()],
        columns=["vendedor", "atendidos", "total_vendas"]
    ).sort_values("vendedor").reset_index(drop=True)

    return dfMVA, dfEH, df_agg

def carregar_planilha_async(tree_planilha, progress_var, progress_bar, root):
    btn_merge_spreadsheet = _UI_REFS.get("btn_merge_spreadsheet")
    pd = _get_pd()

    try:
        cancel_event.clear()
        progress_var.set(0)
        set_btn_cancel(state="normal")

        def worker():
            progressQueuePlanilha.put(("ui", {"action": "start_indeterminate"}))
            try:
                resultado = extrair_planilha_online()
                if resultado is None:
                    progress_bar.stop()
                    progress_bar.config(mode="determinate")
                    progress_var.set(0)
                    set_btn_cancel()
                    return messagebox.showinfo("Aviso", "Nenhum dado novo foi adicionado")
                else:
                    for item in tree_planilha.get_children():
                        tree_planilha.delete(item)
                    if btn_merge_spreadsheet:
                        btn_merge_spreadsheet.configure(state="normal")

                    # agora extrai também o DataFrame agregado
                    dfMVA, dfEH, df_agg = resultado

                    total_rows = len(df_agg)
                    resultados = []

                    # percorre o df_agg (já somado por vendedor)
                    for i, row in enumerate(df_agg.itertuples(index=False, name=None), start=1):
                        # 🔹 Verifica se foi cancelado
                        if cancel_event.is_set():
                            progressQueuePlanilha.put(("done_planilha", {"__cancelled__": True}))
                            return

                        vendedor = str(row[0]).strip()
                        if not vendedor:
                            continue

                        atendidos = int(row[1]) if not pd.isna(row[1]) else 0
                        total = float(row[2]) if not pd.isna(row[2]) else 0.0

                        if atendidos > 0 or total > 0:
                            resultados.append((vendedor, atendidos, total))

                        # 🔹 Atualiza progresso gradualmente
                        progresso = int(i * 100 / max(1, total_rows))
                        progressQueuePlanilha.put(("progress", progresso))

                    progressQueuePlanilha.put(("done_planilha", resultados))

            except Exception as e:
                progressQueuePlanilha.put(("error", f"Erro ao carregar planilha: {e}"))

        progressQueuePlanilha = queue.Queue()
        worker_thread = threading.Thread(target=worker, daemon=True)
        worker_thread.start()

        def poll_queue_planilha():
            try:
                for _ in range(50):
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
                        set_btn_cancel()
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
                        set_btn_cancel()
                        messagebox.showerror("Erro", payload)
                        return
            except queue.Empty:
                pass
            root.after(10, poll_queue_planilha)

        poll_queue_planilha()

    except Exception as e:
        set_btn_cancel()
        messagebox.showerror("Erro", f"Erro ao iniciar carregamento da planilha: {e}")

def carregar_planilhas_duplas_async(tree_mva, tree_eh, progress_var, progress_bar, root):
    """Carrega as planilhas online (MVA e EH) em paralelo, cada uma no seu Treeview."""
    import threading, queue
    pd = _get_pd()
    global cancel_event
    btn_merge_spreadsheet = _UI_REFS.get("btn_merge_spreadsheet")

    try:
        cancel_event.clear()
        progress_var.set(0)
        set_btn_cancel(state="normal")
        progressQueuePlanilha = queue.Queue()

        def worker():
            progressQueuePlanilha.put(("ui", {"action": "start_indeterminate"}))
            try:
                resultado = extrair_planilha_online()
                if resultado is None:
                    progress_bar.stop()
                    progress_bar.config(mode="determinate")
                    progress_var.set(0)
                    set_btn_cancel()
                    return messagebox.showinfo("Aviso", "Nenhum dado novo foi adicionado")

                dfMVA, dfEH, _ = resultado  # ignoramos o df_agg por enquanto

                # limpa tabelas antes de preencher
                for tree in (tree_mva, tree_eh):
                    for item in tree.get_children():
                        tree.delete(item)

                if btn_merge_spreadsheet:
                    btn_merge_spreadsheet.configure(state="normal")

                # Preenche as duas tabelas
                def fill_tree(df, tree):
                    total_rows = len(df)
                    for i, row in enumerate(df.itertuples(index=False, name=None), start=1):
                        if cancel_event.is_set():
                            progressQueuePlanilha.put(("done_planilha", {"__cancelled__": True}))
                            return
                        vendedor = str(row[0]).strip()
                        if not vendedor:
                            continue
                        valores = row[1:]
                        atendidos = sum(1 for v in valores if str(v).strip() != "")
                        total = 0.0
                        for v in valores:
                            try:
                                total += parse_number(str(v))
                            except Exception:
                                pass
                        if atendidos > 0 or total > 0:
                            tree.insert("", "end", values=(vendedor, atendidos, f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")))
                        progresso = int(i * 50 / max(1, total_rows))  # 50% pra cada tabela
                        progressQueuePlanilha.put(("progress", progresso))

                fill_tree(dfMVA, tree_mva)
                fill_tree(dfEH, tree_eh)
                _scroll_tree_to_top(tree_mva)
                _scroll_tree_to_top(tree_eh)

                progressQueuePlanilha.put(("done_planilha", "ok"))

            except Exception as e:
                progressQueuePlanilha.put(("error", f"Erro ao carregar planilhas: {e}"))

        threading.Thread(target=worker, daemon=True).start()

        def poll_queue():
            try:
                for _ in range(50):
                    kind, payload = progressQueuePlanilha.get_nowait()
                    if kind == "progress":
                        if str(progress_bar["mode"]) == "indeterminate":
                            progress_bar.stop()
                            progress_bar.config(mode="determinate")
                        progress_var.set(payload)
                        progress_bar.update_idletasks()
                    elif kind == "ui":
                        if payload.get("action") == "start_indeterminate":
                            progress_bar.config(mode="indeterminate")
                            progress_bar.start(10)
                    elif kind == "done_planilha":
                        set_btn_cancel()
                        progress_bar.stop()
                        progress_bar.config(mode="determinate")
                        progress_var.set(100 if payload == "ok" else 0)
                        if isinstance(payload, dict) and payload.get("__cancelled__"):
                            messagebox.showinfo("Cancelado", "❌ Carregamento da planilha foi cancelado.")
                        else:
                            messagebox.showinfo("Sucesso", "✅ Planilhas online carregadas com sucesso!")
                        return
                    elif kind == "error":
                        set_btn_cancel()
                        messagebox.showerror("Erro", payload)
                        return
            except queue.Empty:
                pass
            root.after(10, poll_queue)

        poll_queue()

    except Exception as e:
        set_btn_cancel()
        messagebox.showerror("Erro", f"Erro ao iniciar carregamento das planilhas: {e}")

def source_pdf_async(tree, progress_var, progress_bar, root, label_files_var, btn_cancel, caminho, origem):
    """
    Inicia processamento do PDF já com o caminho e a origem (MVA/EH) escolhidos.
    Não pergunta nada ao usuário — a escolha já veio do tk.py.
    """
    global listFiles, list_results

    if not caminho:
        return False

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
    btn_tag = _UI_REFS.get("btn_tag")
    btn_add_mais = _UI_REFS.get("btn_add_mais")
    
    global listFiles, list_results
    local_queue = queue.Queue()
    
    # Se nenhum PDF foi carregado ainda, obriga usar "Escolher PDF" primeiro
    if not list_results or not listFiles:
        messagebox.showwarning("Aviso", "Selecione o primeiro PDF (com origem definida) antes de adicionar outro.")
        return

    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    if not caminho:
        return False
    
    if caminho in listFiles:
        messagebox.showerror("Erro", "Arquivo já importado!")
        return
    
    try:
        pdfplumber = _get_pdfplumber()
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
        f"Carregando: {os.path.basename(caminho)}({origem})"
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
            for _ in range(50):
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
                        if btn_tag:
                            btn_tag.configure(state="normal", fg_color="#44cc64")
                        if btn_add_mais:
                            btn_add_mais.configure(state="disabled", fg_color="#EE9919", text_color_disabled="#D92525")
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
    
    for vendedor, dados in _sorted_rows_by_total_vendas(mesclado):
        if not _has_visible_data(dados):
            continue
        total_vendas_str = ""
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
    _scroll_tree_to_top(tree)

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
    canon_cache = {}
    ajuste_total_d = {}
    pending_d_marker = False
    last_sale = None
    last_line_type = None

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

    def reset_sale_state():
        nonlocal pending_d_marker, last_sale, last_line_type
        pending_d_marker = False
        last_sale = None
        last_line_type = None

    def apply_d_marker_to_last_sale() -> bool:
        nonlocal last_sale
        if not last_sale or last_sale.get("vendedor") != vendedor_atual:
            return False

        dados = resultados.get(vendedor_atual)
        if not dados:
            return False

        if not last_sale["counted_as_return"]:
            dados["devolucoes"] += 1
            last_sale["counted_as_return"] = True

        amount = last_sale.get("amount")
        if not last_sale["d_adjusted"] and amount is not None and amount > 0:
            ajuste_total_d[vendedor_atual] = ajuste_total_d.get(vendedor_atual, 0.0) + amount
            last_sale["d_adjusted"] = True

        return True

    pdfplumber = _get_pdfplumber()
    with pdfplumber.open(caminho_pdf) as pdf:
        total = len(pdf.pages)
        for i, pagina in enumerate(pdf.pages, start=1):
            if cancel_event.is_set():
                return {"__cancelled__": True}
            try:
                texto = pagina.extract_text() or ""
                for linha in texto.splitlines():
                    vendedor_bruto = _extract_vendor_name(linha)
                    if vendedor_bruto is not None:
                        fechar_vendedor()
                        reset_sale_state()
                        if vendedor_bruto:
                            palavras = vendedor_bruto.split()
                            if palavras and palavras[0].isdigit():
                                vendedor_base = " ".join(palavras[1:])
                            else:
                                vendedor_base = " ".join(palavras)
                            if vendedor_base in canon_cache:
                                vendedor_atual = canon_cache[vendedor_base]
                            else:
                                vendedor_atual = canonicalize_name(vendedor_base)
                                canon_cache[vendedor_base] = vendedor_atual
                            if vendedor_atual not in resultados:
                                resultados[vendedor_atual] = {
                                    "atendidos": 0,
                                    "devolucoes": 0,
                                    "total_clientes": 0,
                                    "total_vendas": 0.0
                                }
                        last_line_type = "vendor"
                        continue

                    if _is_sale_entry_line(linha):
                        if not vendedor_atual:
                            continue

                        sale_amount = _extract_sale_amount(linha)
                        has_negative_return = bool(regex_negative.search(linha))
                        has_d_return = _line_has_d_marker(linha) or pending_d_marker

                        resultados[vendedor_atual]["atendidos"] += 1
                        last_sale = {
                            "vendedor": vendedor_atual,
                            "amount": sale_amount,
                            "counted_as_return": False,
                            "d_adjusted": False,
                        }

                        if has_negative_return:
                            resultados[vendedor_atual]["devolucoes"] += 1
                            last_sale["counted_as_return"] = True

                        if has_d_return:
                            apply_d_marker_to_last_sale()

                        pending_d_marker = False
                        last_line_type = "sale"
                        continue

                    if _line_has_d_marker(linha):
                        if last_line_type == "sale" and apply_d_marker_to_last_sale():
                            last_line_type = "marker"
                            continue
                        pending_d_marker = True
                        last_line_type = "marker"
                        continue

                    total_vendas = _extract_total_vendas(linha)
                    if total_vendas is not None and vendedor_atual:
                        total_bruto = parse_number(total_vendas)
                        resultados[vendedor_atual]["total_vendas"] = total_bruto - ajuste_total_d.get(vendedor_atual, 0.0)
                        reset_sale_state()
                        last_line_type = "total"
                        continue

                    if linha.strip():
                        last_line_type = "other"

                # Atualiza o progresso a cada página
                progresso = int(i * 100 / max(1, total))
                on_progress("progress", progresso)
            except Exception as e:
                return {"__error__": str(e)}

        # Garante que o progresso chegue a 100% após o loop
        fechar_vendedor()
    if not resultados:
        return {"__empty__": True}
    
    return resultados

def ordenar_coluna(tree, col, reverse):
    dados = [(tree.set(k, col), k) for k in tree.get_children()]
    
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
    import zipfile
    import shutil
    import subprocess
    import sys
    import re

    def version_key(raw: str):
        text = (raw or "").strip().lstrip("vV")
        nums = [int(x) for x in re.findall(r"\d+", text)]
        while len(nums) < 4:
            nums.append(0)
        return tuple(nums[:4])

    def resolve_latest_release() -> dict | None:
        headers = {"Accept": "application/vnd.github+json", "User-Agent": "RelatorioClientes-Updater"}
        timeout = 20

        # 1) endpoint direto do latest
        try:
            resp = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest", headers=headers, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            if data and data.get("tag_name"):
                return data
        except Exception:
            pass

        # 2) fallback: lista releases e pega maior versao valida (nao pre-release)
        try:
            resp = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/releases", headers=headers, timeout=timeout)
            resp.raise_for_status()
            releases = resp.json() or []
            candidates = [r for r in releases if not r.get("draft") and not r.get("prerelease") and r.get("tag_name")]
            if not candidates:
                return None
            candidates.sort(key=lambda r: version_key(r.get("tag_name", "")), reverse=True)
            return candidates[0]
        except Exception:
            return None

    def worker():
        try:
            data = resolve_latest_release()
            if not data or not data.get("tag_name"):
                return

            latest_version = data["tag_name"].lstrip("vV")

            if version_key(latest_version) > version_key(APP_VERSION):
                # Mostra dialogo na thread principal usando after()
                def ask_user():
                    if messagebox.askyesno("Atualizacao Disponivel",
                        f"Uma nova versao ({latest_version}) esta disponivel! Deseja baixar agora?"):
                        assets = data.get("assets", [])
                        zip_asset = None
                        for asset in assets:
                            name = asset.get("name", "").lower()
                            if name.endswith(".zip"):
                                zip_asset = asset
                                break
                        if not zip_asset:
                            messagebox.showerror("Erro", "Nenhum arquivo .zip encontrado na release.")
                            return

                        asset_url = zip_asset["browser_download_url"]
                        base_dir = os.path.join(os.getenv("LOCALAPPDATA", "."), "RelatorioClientes")
                        os.makedirs(base_dir, exist_ok=True)
                        zip_path = os.path.join(base_dir, f"RelatorioClientes-{latest_version}.zip")
                        extract_dir = os.path.join(base_dir, f"app-{latest_version}")
                        try:
                            download = requests.get(asset_url, stream=True, timeout=30)
                            with open(zip_path, "wb") as f:
                                for chunk in download.iter_content(8192):
                                    f.write(chunk)
                            if os.path.exists(extract_dir):
                                shutil.rmtree(extract_dir, ignore_errors=True)
                            with zipfile.ZipFile(zip_path, "r") as zf:
                                zf.extractall(extract_dir)

                            exe_path = os.path.join(extract_dir, "Relatorio de Clientes.exe")
                            if not os.path.exists(exe_path):
                                for root_dir, _dirs, files in os.walk(extract_dir):
                                    for fname in files:
                                        if fname.lower().endswith(".exe"):
                                            exe_path = os.path.join(root_dir, fname)
                                            break
                                    if os.path.exists(exe_path):
                                        break

                            if not os.path.exists(exe_path):
                                messagebox.showerror("Erro", "Nao foi possivel localizar o executavel na atualizacao.")
                                return

                            messagebox.showinfo("Atualizado",
                                "Nova versao baixada e extraida. O aplicativo sera reiniciado.")
                            try:
                                subprocess.Popen([exe_path])
                            except Exception as e:
                                messagebox.showerror("Erro", f"Falha ao iniciar nova versao: {e}")
                                return
                            sys.exit(0)
                        except Exception as e:
                            messagebox.showerror("Erro no Download", f"Ocorreu um erro: {e}")
                root.after(0, ask_user)  # root e sua janela principal
            else:
                print("App atualizado.")

        except Exception as e:
            root.after(0, lambda: messagebox.showerror("Erro na Atualizacao",
                                                       f"Ocorreu um erro ao checar atualizacoes: {e}"))

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
    btn_add_mais = _UI_REFS.get("btn_add_mais")
    btn_merge_spreadsheet = _UI_REFS.get("btn_merge_spreadsheet")
    btn = _UI_REFS.get("btn_select_pdf")
    btn_tag = _UI_REFS.get("btn_tag")

    from global_vars import results_by_source
    results_by_source["MVA"].clear()
    results_by_source["EH"].clear()

    if btn_merge_spreadsheet:
        btn_merge_spreadsheet.configure(state="normal")
    if btn_add_mais:
        btn_add_mais.configure(state="normal")
    if btn:
        btn.configure(state="normal")
    if btn_tag:
        btn_tag.configure(state="disabled", fg_color="#EE9919", text_color_disabled="gray45")
    
    list_results.clear()
    listFiles.clear()

    messagebox.showinfo("Limpo", "Todas as tabelas foram limpas com sucesso!")
            
def _excel_export(tree):
    pd = _get_pd()
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
        return False
    
    df.to_excel(caminho, index=False, engine="openpyxl")
    
    messagebox.showinfo("Sucesso", f"✅ Relatório exportado para:\n{caminho}")

def _pdf_export(tree) -> bool:

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
        return False

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

def limpar_tabelas_duplas(tree, tree_mva, tree_eh, label_files_var, progress_var):
    """Limpa todas as tabelas (PDF + MVA + EH) e reseta os indicadores."""
    global LAST_EH, LAST_MVA, LAST_STATE_SPREADSHEET, LAST_HASH_MERGE


    confirm = messagebox.askyesno("Confirmação", "Deseja realmente limpar todas as tabelas?")
    if not confirm:
        return

    for t in (tree, tree_mva, tree_eh):
        for item in t.get_children():
            t.delete(item)
            
        # 🧹 Limpa histórico da mesclagem
    LAST_EH = None      
    LAST_MVA = None
    LAST_HASH_MERGE = None
    LAST_STATE_SPREADSHEET = {}    
    
    # também limpa lista de resultados
    from global_vars import list_results, listFiles
    btn_add_mais = _UI_REFS.get("btn_add_mais")
    btn_merge_spreadsheet = _UI_REFS.get("btn_merge_spreadsheet")
    btn = _UI_REFS.get("btn_select_pdf")
    btn_tag = _UI_REFS.get("btn_tag")
    from global_vars import results_by_source
    results_by_source["MVA"].clear()
    results_by_source["EH"].clear()

    
    if btn_merge_spreadsheet:
        btn_merge_spreadsheet.configure(state="normal")
    if btn_add_mais:
        btn_add_mais.configure(state="normal")
    if btn:
        btn.configure(state="normal")
    if btn_tag:
        btn_tag.configure(state="disabled", fg_color="#EE9919", text_color_disabled="gray45")
    
    list_results.clear()
    listFiles.clear()

    label_files_var.set("Nenhum arquivo selecionado")
    progress_var.set(0)
    messagebox.showinfo("Limpeza concluída", "🧹 Todas as tabelas foram limpas com sucesso.")

def _hash_tree_snapshot(trees):
    import hashlib

    hasher = hashlib.md5()
    for tree in trees:
        for item in tree.get_children():
            values = tree.item(item)["values"]
            for val in values:
                hasher.update(str(val).encode("utf-8"))
                hasher.update(b"\x1f")
            hasher.update(b"\x1e")
    return hasher.hexdigest()



def mesclar_tabelas_duplas(tree, progress_var, progress_bar, root, label_files_var,
                           tree_mva, tree_eh):
    """
    Mescla os valores das planilhas online (MVA e EH) com a tabela de PDFs (tree).
    Soma os dados das duas planilhas e atualiza a barra de progresso.
    """
    btn_merge_spreadsheet = _UI_REFS.get("btn_merge_spreadsheet")
    btn_add_mais = _UI_REFS.get("btn_add_mais")
    btn = _UI_REFS.get("btn_select_pdf")
    global LAST_HASH_MERGE, LAST_STATE_SPREADSHEET

    import threading, queue
    if btn_merge_spreadsheet:
        btn_merge_spreadsheet.configure(state="enabled")

    # 🔹 Verifica se alguma tabela está vazia
    if not tree.get_children():
        messagebox.showwarning("Aviso", "A tabela de PDFs está vazia. Importe pelo menos um PDF antes de mesclar.")
        return
    if not tree_mva.get_children() and not tree_eh.get_children():
        messagebox.showwarning("Aviso", "As tabelas online estão vazias. Carregue as planilhas MVA e EH antes de mesclar.")
        return
    try:
        from global_vars import results_by_source
    except Exception:
        results_by_source = {"MVA": [], "EH": []}
    if not results_by_source.get("MVA") or not results_by_source.get("EH"):
        messagebox.showwarning("Aviso", "Importe os dois PDFs (MVA e EH) antes de mesclar.")
        return

    # Snapshot dos dados atuais (pra detectar duplicacoes)
    novo_hash = _hash_tree_snapshot((tree, tree_mva, tree_eh))
    if LAST_HASH_MERGE == novo_hash:
        messagebox.showinfo("Aviso", "⚠️ Esses dados já foram mesclados. Nenhuma alteração detectada.")
        return

    LAST_HASH_MERGE = novo_hash
    merge_queue = queue.Queue()

    # ------------------ THREAD WORKER ------------------
    def worker():
        try:
            # 1️⃣ Extrai dados da tabela de PDFs
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

            # 2️⃣ Extrai dados das planilhas MVA e EH
            def extrair_dados(tree_view):
                dados = {}
                for item in tree_view.get_children():
                    vals = tree_view.item(item)["values"]
                    vendedor = str(vals[0]).strip()
                    atendidos = int(vals[1])
                    total_vendas = parse_number(str(vals[2]) if vals[2] else "0")
                    if vendedor:
                        if vendedor not in dados:
                            dados[vendedor] = {"atendidos": 0, "total_vendas": 0.0}
                        dados[vendedor]["atendidos"] += atendidos
                        dados[vendedor]["total_vendas"] += total_vendas
                return dados

            dados_mva = extrair_dados(tree_mva)
            dados_eh = extrair_dados(tree_eh)

            # 3️⃣ Soma os dois (MVA + EH)
            dados_planilha_total = {}
            for vendedor in set(dados_mva.keys()) | set(dados_eh.keys()):
                m = dados_mva.get(vendedor, {"atendidos": 0, "total_vendas": 0.0})
                e = dados_eh.get(vendedor, {"atendidos": 0, "total_vendas": 0.0})
                dados_planilha_total[vendedor] = {
                    "atendidos": m["atendidos"] + e["atendidos"],
                    "total_vendas": m["total_vendas"] + e["total_vendas"]
                }

            # 4️⃣ Aplica controle de duplicação incremental (igual ao código original)
            novos_planilha = {}
            for idx, (vendedor, dados) in enumerate(dados_planilha_total.items(), start=1):
                ultimo = LAST_STATE_SPREADSHEET.get(vendedor, {"atendidos": 0, "total_vendas": 0.0})
                delta_atendidos = max(0, dados["atendidos"] - ultimo["atendidos"])
                delta_vendas = max(0, dados["total_vendas"] - ultimo["total_vendas"])
                if delta_atendidos == 0 and delta_vendas == 0:
                    continue

                novos_planilha[vendedor] = {
                    "atendidos": delta_atendidos,
                    "total_vendas": delta_vendas
                }
                LAST_STATE_SPREADSHEET[vendedor] = dados
                progresso = int(idx * 40 / max(1, len(dados_planilha_total)))
                merge_queue.put(("progress", progresso))

            # 5️⃣ Mescla tudo
            total_vendedores = len(set(dados_pdf.keys()) | set(novos_planilha.keys()))
            for idx, vendedor in enumerate(set(dados_pdf.keys()) | set(novos_planilha.keys()), start=1):
                pdf_data = dados_pdf.get(vendedor, {"atendidos": 0, "devolucoes": 0, "total_clientes": 0, "total_vendas": 0})
                plan_data = novos_planilha.get(vendedor, {"atendidos": 0, "total_vendas": 0})

                merged = {
                    "atendidos": pdf_data["atendidos"] + plan_data["atendidos"],
                    "devolucoes": pdf_data["devolucoes"],
                    "total_clientes": (pdf_data["atendidos"] + plan_data["atendidos"]) - pdf_data["devolucoes"],
                    "total_vendas": pdf_data["total_vendas"] + plan_data["total_vendas"]
                }
                dados_pdf[vendedor] = merged

                progresso = 40 + int(idx * 60 / max(1, total_vendedores))
                merge_queue.put(("progress", progresso))

            merge_queue.put(("done", dados_pdf))

        except Exception as e:
            merge_queue.put(("error", str(e)))

    threading.Thread(target=worker, daemon=True).start()

    # ------------------ POLL QUEUE ------------------
    def poll_merge_queue():
        try:
            for _ in range(50):
                kind, payload = merge_queue.get_nowait()
                if kind == "progress":
                    progress_var.set(payload)
                    progress_bar.update_idletasks()
                elif kind == "done":
                    if btn_merge_spreadsheet:
                        btn_merge_spreadsheet.configure(state="disabled")
                    if btn_add_mais:
                        btn_add_mais.configure(state="disabled")
                    if btn:
                        btn.configure(state="disabled")

                    for item in tree.get_children():
                        tree.delete(item)

                    for vendedor, dados in _sorted_rows_by_total_vendas(payload):
                        if not _has_visible_data(dados):
                            continue
                        tree.insert("", "end", values=(
                            vendedor,
                            dados["atendidos"],
                            dados["devolucoes"],
                            dados["total_clientes"],
                            format_number_br(dados["total_vendas"])
                        ))
                    _scroll_tree_to_top(tree)
                    progress_var.set(100)
                    messagebox.showinfo("Concluído", "✅ Mesclagem das tabelas (PDF + MVA + EH) finalizada!")
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
        pdfplumber = _get_pdfplumber()
        with pdfplumber.open(caminho_pdf) as pdf:
            for pagina in pdf.pages:
                texto = pagina.extract_text() or ""
                for linha in texto.splitlines():
                    data_str = _extract_sale_date(linha)
                    if not data_str:
                        continue
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

# ----------------- Conexão com Supabase -----------------

import json

_supabase = None

def get_supabase():
    global _supabase
    if _supabase is None:
        try:
            from supabase import create_client
            cred_path = resource_path(os.path.join("data", "credenciaisDB.json"))
            with open(cred_path, "r", encoding="utf-8") as f:
                creds = json.load(f)

            url = creds["SUPABASE_URL"]
            key = creds["SUPABASE_KEY"]
            _supabase = create_client(url, key)
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao conectar ao Supabase: {e}")
            raise
    return _supabase

def listar_vendedores_db():
    """Retorna a lista de vendedores cadastrados no Supabase."""
    try:
        supabase = get_supabase()
        data = supabase.table("vendedores").select("nome").order("nome").execute()
        return [v["nome"] for v in data.data] if data and hasattr(data, "data") else []
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao listar vendedores: {e}")
        return []

def registrar_vendedor_db(nome: str):
    """Registra um vendedor no banco (evita duplicados)."""
    try:
        supabase = get_supabase()
        existe = supabase.table("vendedores").select("id").eq("nome", nome).execute()
        if not existe.data:
            supabase.table("vendedores").insert({"nome": nome}).execute()
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao registrar vendedor: {e}")

def excluir_ultimo_feedback(vendedor: str):
    """Exclui o último feedback de um vendedor"""
    try:
        supabase = get_supabase()
        # pega o último registro
        data = supabase.table("feedbacks").select("*").eq("vendedor", vendedor).order("created_at", desc=True).limit(1).execute()
        if data.data:
            fid = data.data[0]["id"]
            supabase.table("feedbacks").delete().eq("id", fid).execute()
            return True
        return False
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao excluir feedback: {e}")
        return False

def atualizar_ultimo_feedback(vendedor: str, novo_texto: str):
    """Atualiza o último feedback de um vendedor"""
    try:
        supabase = get_supabase()
        data = supabase.table("feedbacks").select("*").eq("vendedor", vendedor).order("created_at", desc=True).limit(1).execute()
        if data.data:
            fid = data.data[0]["id"]
            supabase.table("feedbacks").update({"feedback": novo_texto}).eq("id", fid).execute()
            return True
        return False
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao editar feedback: {e}")
        return False

def salvar_feedback_db(vendedor: str, texto: str):
    from datetime import datetime
    """Salva o feedback de um vendedor no Supabase."""
    try:
        supabase = get_supabase()
        agora = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        data, count = supabase.table("feedbacks").insert({
            "vendedor": vendedor,
            "feedback": texto,
            "created_at": agora   # sobrescreve no formato desejado
        }).execute()
        return True
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao salvar feedback no banco: {e}")
        return False

def carregar_feedbacks_db(vendedor: str):
    """Carrega todos os feedbacks de um vendedor do Supabase."""
    try:
        supabase = get_supabase()
        data = supabase.table("feedbacks").select("*").eq("vendedor", vendedor).order("created_at").execute()
        return data.data if data and hasattr(data, "data") else []
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao carregar feedbacks: {e}")
        return []
