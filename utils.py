import os
import re
import json
import time
import html
import csv
import socket
import shutil
import tempfile
import zipfile
import asyncio
import threading
import subprocess
import queue
import difflib
import unicodedata
from datetime import datetime
from pathlib import Path

import requests

from ui_dialogs import filedialog, messagebox
from global_vars import (
    listFiles, list_results, regex_data, regex_negative, 
    APP_VERSION, GITHUB_REPO, LAST_EH, LAST_MVA , LAST_HASH_MERGE,
    SALES_PERIOD, MINHAS_NOTAS_LOGIN, MINHAS_NOTAS_PASSWORD,
    ZWEB_USERNAME, ZWEB_PASSWORD, ZWEB_BASE_URL,
) 

# ConfiguraÃ§Ã£o de logging mais leve (somente avisos e erros)
progress_queue = queue.Queue()
cancel_event = threading.Event()
LAST_STATE_SPREADSHEET = {}
_MINHAS_NOTAS_CACHE = {}

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










def _empty_caixa_report(caminho_pdf: str, pdf_info: dict | None = None) -> dict:
    return {
        "arquivo": os.path.basename(caminho_pdf),
        "caixa_modelo": "",
        "arquivo_tipo": "pdf_sem_texto" if (pdf_info or {}).get("pdf_sem_texto") else "",
        "periodo": None,
        "pedidos_total": 0,
        "pedidos_balcao": 0,
        "pedidos_caixa": 0,
        "pedidos_excluidos": 0,
        "pedidos_excluidos_cliente": 0,
        "pedidos_excluidos_documento": 0,
        "pedidos_excluidos_cancelados": 0,
        "pedidos_editando": 0,
        "pedidos_outros_status": 0,
        "total_documento": 0.0,
        "total_excluido": 0.0,
        "total_excluido_cancelados": 0.0,
        "total_caixa": 0.0,
        "itens_caixa": [],
        "itens_excluidos": [],
        **(pdf_info or {}),
    }


def _empty_resumo_nfce_report(caminho_pdf: str, pdf_info: dict | None = None) -> dict:
    return {
        "arquivo": os.path.basename(caminho_pdf),
        "resumo_modelo": "",
        "periodo": None,
        "quantidade_nfce": 0,
        "total_nfce": 0.0,
        "nfces": [],
        "nfces_faltantes_sequencia": [],
        **(pdf_info or {}),
    }


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
    if " ate " in lowered or " atÃ© " in lowered:
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
    # ðŸ”¹ Reseta barra
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
        # Agenda a prÃ³xima checagem em 50ms (menos carga na CPU/UI)
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
            messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuÃ¡rio.")
            return
        if results.get("__empty__"):
            progress_var.set(0)
            messagebox.showwarning(
                "Aviso",
                results.get("__warning__") or "Nenhum dado foi encontrado neste PDF.",
            )
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

    # Sempre agenda a prÃ³xima checagem, exceto se houve erro (onde damos return acima)
    root.after(50, lambda: _poll_queue(root, tree, progress_var, progress_bar, label_files_var, path_var))

def resource_path(relative_path): 
    import sys
    
    """Retorna o caminho absoluto do recurso, compatÃ­vel com PyInstaller."""
    if hasattr(sys, '_MEIPASS'):  # Executando empacotado
        base_path = sys._MEIPASS
    else:
        base_path = getattr(sys, "_MEIPASS", os.path.dirname(__file__))
    return os.path.join(base_path, relative_path)

def load_mapping(path='mapping.json'): 
  
    full_path = resource_path(path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Arquivo de mapeamento nÃ£o encontrado: {full_path}")
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
    """Salva o mapeamento atualizado no arquivo do usuÃ¡rio."""
    appdata_dir = os.path.join(os.getenv("APPDATA"), "RelatorioClientes")
    os.makedirs(appdata_dir, exist_ok=True)
    user_json = os.path.join(appdata_dir, "mapping.json")
    with open(user_json, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=4, ensure_ascii=False)

def _normalize_key(s: str) -> str:
    if not s:
        return ""
    s = s.replace('\u00A0', ' ')                # NBSP -> espaÃ§o
    s = re.sub(r"^\s*\d+\s*", "", s)            # remove prefixo numÃ©rico "14 C O" -> "C O"
    s = re.sub(r"\s+", " ", s)                  # espaÃ§os mÃºltiplos
    s = s.replace("â€“", "-").replace("â€”", "-")   # normaliza hifens
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

    s = s.replace("R$", "").replace(" ", "").replace("Â ", "")
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
    """Formata nÃºmero no padrÃ£o brasileiro com duas casas decimais."""
    return f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _normalize_caixa_client(name: str) -> str:
    import unicodedata

    normalized = unicodedata.normalize("NFKD", name or "")
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"\s+", " ", normalized).strip().upper()
    return normalized


def _classify_caixa_document(description: str) -> str:
    normalized = _normalize_caixa_client(description)
    if (
        "NOTA FISCAL DE CONSUMIDOR" in normalized
        and "ELETR" in normalized
    ):
        return "Nota Fiscal de Consumidor Eletronica"
    if (
        "NOTA FISCAL" in normalized
        and "ELETR" in normalized
        and "CONSUMIDOR" not in normalized
    ):
        return "Nota Fiscal Eletronica"
    return description.strip()


def _is_eh_counter_client(cliente: str) -> bool:
    compacto = re.sub(r"[^A-Z]", "", _normalize_caixa_client(cliente))
    return compacto.startswith("CLIENTEBALC")


def _decode_report_response_text(response) -> str:
    content = response.content or b""
    encodings = []
    for encoding in (
        getattr(response, "apparent_encoding", None),
        getattr(response, "encoding", None),
        "utf-8",
        "latin1",
    ):
        encoding = str(encoding or "").strip()
        if encoding and encoding not in encodings:
            encodings.append(encoding)

    for encoding in encodings:
        try:
            texto = content.decode(encoding)
        except Exception:
            continue
        if texto:
            return texto
    return content.decode("utf-8", errors="ignore")


def _clean_zweb_html_value(value: str) -> str:
    texto = html.unescape(str(value or ""))
    texto = re.sub(r"<[^>]+>", " ", texto)
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def _extract_zweb_period(html_text: str) -> str | None:
    match = re.search(
        r"(\d{2}/\d{2}/\d{4})\s+at\S*\s+(\d{2}/\d{2}/\d{4})",
        html_text,
        re.IGNORECASE,
    )
    if not match:
        datas = re.findall(r"\d{2}/\d{2}/\d{4}", html_text[:4000])
        if len(datas) >= 2:
            return f"{datas[0]} - {datas[1]}"
        return None
    return f"{match.group(1)} - {match.group(2)}"


def _extract_zweb_fiscal_emission_iso(value: str) -> str:
    texto = str(value or "").strip()
    if not texto:
        return ""

    match = re.match(r"^(\d{4}-\d{2}-\d{2})", texto)
    if match:
        return match.group(1)

    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(texto, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _is_zweb_fiscal_cancelled(item: dict) -> bool:
    if item.get("canceledXml"):
        return True
    try:
        return int(item.get("status")) == 3
    except (TypeError, ValueError):
        return False


def _build_zweb_fiscal_status_map(itens: list[dict]) -> dict:
    status_map = {}
    for item in itens:
        numero = _normalize_fiscal_number(item.get("numero", ""))
        if not numero or numero in status_map:
            continue
        try:
            valor = round(float(item.get("valorTotal", 0.0) or 0.0), 2)
        except (TypeError, ValueError):
            valor = 0.0
        status_map[numero] = {
            "numero": numero,
            "numero_exibicao": _display_fiscal_number(numero),
            "cancelada": _is_zweb_fiscal_cancelled(item),
            "status_codigo": item.get("status"),
            "status_transmissao": item.get("statusTransmissao"),
            "modelo": str(item.get("modelo", "") or "").strip(),
            "emissao": _extract_zweb_fiscal_emission_iso(item.get("emission", "")),
            "valor": valor,
        }
    return status_map

def _analisar_html_pedidos_importados_eh(html_text: str, arquivo: str = "Pedidos importados - Zweb") -> dict:
    from bs4 import BeautifulSoup
    with open("debug_zweb.html", "w", encoding="utf-8") as f:
        f.write(html_text)
    periodo = _extract_zweb_period(html_text)
    itens_brutos = []

    soup = BeautifulSoup(html_text, "html.parser")
    blocks = soup.find_all("div", class_="mt-3")

    for block in blocks:
        # Check if this block looks like an order (has the border style usually)
        style = block.get("style", "")
        if "border" not in style or "#A4A5A7" not in style:
            continue

        pedido_val = ""
        cliente_val = ""
        documento_val = ""
        tipo_val = ""
        valor_val = ""

        spans = block.find_all("span")
        for i, sp in enumerate(spans):
            sp_text = sp.get_text(strip=True)
            sp_lower = sp_text.lower()
            if "número do pedido:" in sp_lower and i + 1 < len(spans):
                pedido_val = spans[i+1].get_text(strip=True)
            elif "cliente:" in sp_lower and i + 1 < len(spans):
                cliente_val = spans[i+1].get_text(strip=True)
            elif sp_lower.startswith("n") and re.search(r'\d{6,}', sp_text):
                m = re.search(r'(\d{6,})', sp_text)
                if m:
                    documento_val = m.group(1)
        
        table = block.find("table")
        if table:
            tb = table.find("tbody")
            if tb:
                tr_data = tb.find("tr")
                if tr_data:
                    tr_tds = tr_data.find_all("td")
                    if len(tr_tds) >= 3:
                        n_span = tr_tds[0].find("span")
                        if n_span:
                            m = re.search(r'(\d{6,})', n_span.text)
                            if m:
                                documento_val = m.group(1)
                        tipo_val = tr_tds[1].get_text(strip=True)
                        valor_val = tr_tds[2].get_text(strip=True)

        if not all((cliente_val, documento_val, tipo_val, valor_val)):
            continue

        cliente = _clean_zweb_html_value(cliente_val)
        documento_fiscal = _normalize_fiscal_number(documento_val)
        descricao_tipo = _clean_zweb_html_value(tipo_val)
        valor = round(parse_number(valor_val), 2)

        itens_brutos.append(
            {
                "pedido_importado": pedido_val,
                "pedido": documento_fiscal,
                "cliente": cliente,
                "documento": _classify_caixa_document(descricao_tipo),
                "valor": valor,
            }
        )

    total_documento = round(sum(item["valor"] for item in itens_brutos), 2)
    total_excluido = 0.0
    pedidos_caixa = 0
    pedidos_balcao = 0
    pedidos_excluidos = 0
    pedidos_excluidos_cliente = 0
    pedidos_excluidos_documento = 0
    itens_caixa = []
    itens_excluidos = []

    for pedido in itens_brutos:
        cliente = pedido["cliente"]
        valor = pedido["valor"]
        is_balcao = _is_eh_counter_client(cliente)
        is_nfe = pedido["documento"] == "Nota Fiscal Eletronica"
        motivos = []

        if is_balcao:
            pedidos_balcao += 1
        else:
            pedidos_excluidos_cliente += 1
            motivos.append("Cliente diferente")

        if is_nfe:
            pedidos_excluidos_documento += 1
            motivos.append("NF-e")

        if motivos:
            total_excluido += valor
            pedidos_excluidos += 1
            itens_excluidos.append(
                {
                    "pedido": pedido["pedido"],
                    "cliente": cliente,
                    "documento": pedido["documento"],
                    "motivo": " + ".join(motivos),
                    "valor": valor,
                }
            )
            continue

        pedidos_caixa += 1
        itens_caixa.append(
            {
                "pedido": pedido["pedido"],
                "cliente": cliente,
                "documento": pedido["documento"],
                "valor": valor,
            }
        )

    total_excluido = round(total_excluido, 2)
    total_caixa = round(total_documento - total_excluido, 2)
    return {
        "arquivo": arquivo,
        "caixa_modelo": "EH",
        "arquivo_tipo": "pedidos_importados_eh",
        "periodo": periodo,
        "pedidos_total": len(itens_brutos),
        "pedidos_balcao": pedidos_balcao,
        "pedidos_caixa": pedidos_caixa,
        "pedidos_excluidos": pedidos_excluidos,
        "pedidos_excluidos_cliente": pedidos_excluidos_cliente,
        "pedidos_excluidos_documento": pedidos_excluidos_documento,
        "pedidos_excluidos_cancelados": 0,
        "total_documento": total_documento,
        "total_excluido": total_excluido,
        "total_excluido_cancelados": 0.0,
        "total_caixa": total_caixa,
        "itens_caixa": sorted(
            itens_caixa,
            key=lambda item: (item["pedido"], item["cliente"].casefold()),
        ),
        "itens_excluidos": sorted(
            itens_excluidos,
            key=lambda item: (-item["valor"], item["cliente"].casefold(), item["pedido"]),
        ),
    }


def _aplicar_filtro_canceladas_pedidos_eh(relatorio: dict, fiscal_status_map: dict) -> dict:
    if (relatorio.get("caixa_modelo") or "").upper() != "EH" or not fiscal_status_map:
        return relatorio

    itens_caixa_filtrados = []
    itens_excluidos = [{**item} for item in relatorio.get("itens_excluidos", [])]
    cancelados_count = 0
    cancelados_valor = 0.0

    for item in relatorio.get("itens_caixa", []):
        numero = _normalize_fiscal_number(item.get("pedido", ""))
        fiscal_info = fiscal_status_map.get(numero) or {}
        if fiscal_info.get("cancelada"):
            valor = round(float(item.get("valor", 0.0)), 2)
            cancelados_count += 1
            cancelados_valor = round(cancelados_valor + valor, 2)
            itens_excluidos.append(
                {
                    "pedido": item.get("pedido", ""),
                    "cliente": item.get("cliente", ""),
                    "documento": "NFC-e cancelada",
                    "motivo": "Cupom cancelado",
                    "valor": valor,
                }
            )
            continue
        itens_caixa_filtrados.append({**item})

    if not cancelados_count:
        return relatorio

    total_excluido = round(float(relatorio.get("total_excluido", 0.0)) + cancelados_valor, 2)
    total_caixa = round(float(relatorio.get("total_documento", 0.0)) - total_excluido, 2)

    return {
        **relatorio,
        "pedidos_caixa": len(itens_caixa_filtrados),
        "pedidos_excluidos": len(itens_excluidos),
        "pedidos_excluidos_cancelados": int(relatorio.get("pedidos_excluidos_cancelados", 0)) + cancelados_count,
        "total_excluido": total_excluido,
        "total_excluido_cancelados": round(
            float(relatorio.get("total_excluido_cancelados", 0.0)) + cancelados_valor,
            2,
        ),
        "total_caixa": total_caixa,
        "itens_caixa": sorted(
            itens_caixa_filtrados,
            key=lambda current: (current.get("pedido", ""), str(current.get("cliente", "")).casefold()),
        ),
        "itens_excluidos": sorted(
            itens_excluidos,
            key=lambda current: (
                -round(float(current.get("valor", 0.0)), 2),
                str(current.get("cliente", "")).casefold(),
                current.get("pedido", ""),
            ),
        ),
    }


def _display_zweb_short_date(data_texto: str) -> str:
    texto = str(data_texto or "").strip()
    if not texto:
        return ""
    for formato in ("%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(texto, formato).strftime("%d/%m/%Y")
        except ValueError:
            continue
    return texto


def _build_zweb_payment_report_meta(label: str) -> dict | None:
    titulo = _clean_zweb_html_value(label)
    if "|" in titulo:
        titulo = titulo.split("|", 1)[1].strip()
    titulo = _clean_zweb_html_value(titulo)
    titulo_normalizado = _normalize_caixa_client(titulo)

    if not titulo_normalizado:
        return None
    if "PAGAMENTO INSTANTANEO" in titulo_normalizado and "PIX" in titulo_normalizado:
        return {
            "key": "pix_fechamento",
            "tab_title": "PIX Fechamento",
            "menu_text": "Abrir PIX fechamento",
            "summary_label": "PIX fechamento",
            "total_label": "Total PIX fechamento",
            "section_label": "Transações PIX no Fechamento",
            "empty_message": "Nenhuma transação PIX encontrada no Fechamento para este dia.",
            "forma_pagamento": titulo,
        }
    if "DINHEIRO" in titulo_normalizado:
        return {
            "key": "dinheiro",
            "tab_title": "Dinheiro",
            "menu_text": "Abrir dinheiro",
            "summary_label": "Dinheiro",
            "total_label": "Total dinheiro",
            "section_label": "Transações em dinheiro",
            "empty_message": "Nenhuma transação em dinheiro encontrada para este dia.",
            "forma_pagamento": titulo,
        }
    if "CARTAO DE CREDITO" in titulo_normalizado:
        return {
            "key": "cartao_credito",
            "tab_title": "Cartão de Crédito",
            "menu_text": "Abrir cartão de crédito",
            "summary_label": "Cartão de crédito",
            "total_label": "Total cartão de crédito",
            "section_label": "Transações em cartão de crédito",
            "empty_message": "Nenhuma transação em cartão de crédito encontrada para este dia.",
            "forma_pagamento": titulo,
        }
    if "CARTAO DE DEBITO" in titulo_normalizado:
        return {
            "key": "cartao_debito",
            "tab_title": "Cartão de Débito",
            "menu_text": "Abrir cartão de débito",
            "summary_label": "Cartão de débito",
            "total_label": "Total cartão de débito",
            "section_label": "Transações em cartão de débito",
            "empty_message": "Nenhuma transação em cartão de débito encontrada para este dia.",
            "forma_pagamento": titulo,
        }

    slug = re.sub(r"[^a-z0-9]+", "_", titulo_normalizado.casefold()).strip("_")
    if not slug:
        return None
    titulo_minusculo = titulo.casefold()
    return {
        "key": slug,
        "tab_title": titulo,
        "menu_text": f"Abrir {titulo_minusculo}",
        "summary_label": titulo,
        "total_label": f"Total {titulo_minusculo}",
        "section_label": f"Transações - {titulo}",
        "empty_message": f"Nenhuma transação em {titulo_minusculo} encontrada para este dia.",
        "forma_pagamento": titulo,
    }


def _normalize_ascii_text(value: str) -> str:
    return unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode().casefold()


def _display_eh_order_number(numero: str) -> str:
    return _display_fiscal_number(_normalize_fiscal_number(numero))


def _candidate_local_report_dirs() -> list[Path]:
    dirs = []
    seen = set()
    for raw in [os.getcwd(), _runtime_user_dir(), os.path.join(os.path.expanduser("~"), "Downloads")]:
        path = Path(str(raw or "")).expanduser()
        key = str(path).casefold()
        if not path.exists() or key in seen:
            continue
        seen.add(key)
        dirs.append(path)
    return dirs


def _read_pdf_text(path: str | Path) -> str:
    pdfplumber = _get_pdfplumber()
    with pdfplumber.open(str(path)) as pdf:
        return "\n".join((page.extract_text() or "") for page in pdf.pages)


def _read_text_file(path: str | Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            return Path(path).read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return Path(path).read_text(encoding="utf-8", errors="ignore")


def _find_eh_local_payment_reports(data_br: str) -> dict[str, str | None]:
    pix_csv_matches: list[Path] = []
    pix_pdf_matches: list[Path] = []
    card_matches: list[Path] = []
    patterns = ("*.pdf", "*.csv")

    preferred_dirs: list[Path] = []
    seen = set()
    for raw in [os.getcwd(), _runtime_user_dir()]:
        path = Path(str(raw or "")).expanduser()
        key = str(path).casefold()
        if not path.exists() or key in seen:
            continue
        seen.add(key)
        preferred_dirs.append(path)

    for directory in preferred_dirs:
        for pattern in patterns:
            for path in directory.glob(pattern):
                name_norm = _normalize_ascii_text(path.name)
                if "pix" not in name_norm and "cart" not in name_norm:
                    continue
                try:
                    if path.suffix.lower() == ".csv":
                        text = _read_text_file(path)
                    else:
                        text = _read_pdf_text(path)
                except Exception:
                    continue
                text_norm = _normalize_ascii_text(text)
                if data_br not in text:
                    continue
                if path.suffix.lower() == ".csv" and "data da venda" in text_norm and "valor bruto" in text_norm:
                    pix_csv_matches.append(path)
                elif "extrato pix" in text_norm:
                    pix_pdf_matches.append(path)
                elif "relatorio de historico de vendas" in text_norm and "periodo de venda" in text_norm:
                    card_matches.append(path)

    def _pick_latest(paths: list[Path]) -> str | None:
        if not paths:
            return None
        latest = max(paths, key=lambda item: item.stat().st_mtime)
        return str(latest)

    return {
        "pix": _pick_latest(pix_csv_matches) or _pick_latest(pix_pdf_matches),
        "cartoes": _pick_latest(card_matches),
    }


def _build_pix_report_from_caixa_pdf(caminho_pdf: str, data_br: str) -> dict:
    text = _read_pdf_text(caminho_pdf)
    itens = []
    itens_todos = []
    for raw in text.splitlines():
        line = " ".join(str(raw or "").split())
        match = re.match(
            r"^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2}:\d{2})\s+(RECEBIDO|ENVIADO)\s+([A-ZÇÃÕÁÉÍÓÚ]+)(?:\s+(.*?))?\s+R\$\s*([-\d\.,]+)$",
            line,
            re.IGNORECASE,
        )
        if not match:
            continue
        data_venda, hora_venda, tipo, situacao, nome, valor_str = match.groups()
        if data_venda != data_br:
            continue
        item = {
            "data_venda": f"{data_venda} às {hora_venda}",
            "ordem": datetime.strptime(f"{data_venda} {hora_venda}", "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"),
            "tipo_pix": str(tipo).upper(),
            "situacao": str(situacao).upper(),
            "nome": str(nome or "").strip(),
            "valor_bruto": round(parse_number(valor_str), 2),
        }
        itens_todos.append(item)
        if item["tipo_pix"] == "RECEBIDO" and item["situacao"] == "EFETIVADO":
            itens.append(
                {
                    "data_venda": item["data_venda"],
                    "ordem": item["ordem"],
                    "tipo_pix": item["tipo_pix"],
                    "situacao": item["situacao"],
                    "nome": item["nome"],
                    "valor_bruto": item["valor_bruto"],
                }
            )

    itens.sort(key=lambda item: item.get("ordem", ""))
    total = round(sum(float(item.get("valor_bruto", 0.0)) for item in itens), 2)
    periodo = f"{data_br} - {data_br}"
    return {
        "arquivo": os.path.basename(caminho_pdf),
        "caminho": caminho_pdf,
        "periodo": periodo,
        "quantidade_autorizados": len(itens),
        "total_autorizado": total,
        "itens_autorizados": itens,
        "quantidade_relatorio": len(itens),
        "total_relatorio": total,
        "consistente": True,
        "origem": "caixa_pix_pdf",
        "mensagem": None if itens else "Nenhuma transação PIX recebida encontrada no relatório local para este dia.",
        "itens_todos": itens_todos,
        "tab_title": "PIX CAIXA",
        "menu_text": "Abrir PIX CAIXA",
        "summary_label": "PIX CAIXA",
        "total_label": "Total PIX CAIXA",
        "section_label": "Transações PIX recebidas na CAIXA",
        "empty_message": "Nenhuma transação PIX recebida encontrada para este dia.",
        "table_headers": ("Data da venda", "Valor bruto"),
        "table_mode": "data_valor",
        "categoria": "pix_caixa",
    }


def _build_pix_report_from_caixa_csv(caminho_csv: str, data_br: str) -> dict:
    text = _read_text_file(caminho_csv)
    itens = []
    itens_todos = []
    reader = csv.DictReader(text.splitlines(), delimiter=";")
    for row in reader:
        normalized_row = {_normalize_ascii_text(key): str(value or "").strip() for key, value in row.items()}
        data_raw = normalized_row.get("data da venda", "")
        valor_raw = normalized_row.get("valor bruto", "")
        situacao_raw = str(normalized_row.get("status", "")).upper()
        codigo_raw = normalized_row.get("cod. de autorizacao", "")
        if not data_raw or not valor_raw:
            continue
        match = re.match(
            r"^(\d{2}/\d{2}/\d{4})\s+(?:as|às)\s+(\d{2}:\d{2})(?::(\d{2}))?$",
            _normalize_ascii_text(data_raw),
        )
        if not match:
            continue
        data_venda = match.group(1)
        if data_venda != data_br:
            continue
        hora = f"{match.group(2)}:{match.group(3) or '00'}"
        item = {
            "data_venda": f"{data_venda} às {hora[:5]}",
            "ordem": datetime.strptime(f"{data_venda} {hora}", "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"),
            "tipo_pix": "RECEBIDO",
            "situacao": situacao_raw,
            "nome": str(codigo_raw or "").strip(),
            "valor_bruto": round(parse_number(valor_raw), 2),
        }
        itens_todos.append(item)
        if _normalize_ascii_text(item["situacao"]) in {
            _normalize_ascii_text("APROVADA"),
            _normalize_ascii_text("AUTORIZADA"),
            _normalize_ascii_text("EFETIVADO"),
        }:
            itens.append(
                {
                    "data_venda": item["data_venda"],
                    "ordem": item["ordem"],
                    "tipo_pix": item["tipo_pix"],
                    "situacao": item["situacao"],
                    "nome": item["nome"],
                    "valor_bruto": item["valor_bruto"],
                }
            )

    itens.sort(key=lambda item: item.get("ordem", ""))
    total = round(sum(float(item.get("valor_bruto", 0.0)) for item in itens), 2)
    periodo = f"{data_br} - {data_br}"
    return {
        "arquivo": os.path.basename(caminho_csv),
        "caminho": caminho_csv,
        "periodo": periodo,
        "quantidade_autorizados": len(itens),
        "total_autorizado": total,
        "itens_autorizados": itens,
        "quantidade_relatorio": len(itens),
        "total_relatorio": total,
        "consistente": True,
        "origem": "caixa_pix_csv",
        "mensagem": None if itens else "Nenhuma transação PIX recebida encontrada no relatório local para este dia.",
        "itens_todos": itens_todos,
        "tab_title": "PIX CAIXA",
        "menu_text": "Abrir PIX CAIXA",
        "summary_label": "PIX CAIXA",
        "total_label": "Total PIX CAIXA",
        "section_label": "Transações PIX recebidas na CAIXA",
        "empty_message": "Nenhuma transação PIX recebida encontrada para este dia.",
        "table_headers": ("Data da venda", "Valor bruto"),
        "table_mode": "data_valor",
        "categoria": "pix_caixa",
    }


def _build_card_reports_from_caixa_pdf(caminho_pdf: str, data_br: str) -> dict[str, dict]:
    text = _read_pdf_text(caminho_pdf)
    buckets = {
        "cartao_credito_caixa": [],
        "cartao_debito_caixa": [],
    }

    for raw in text.splitlines():
        line = " ".join(str(raw or "").split())
        if "Aprovada" not in line and "Cancelada" not in line:
            continue
        tokens = line.split(" ")
        if len(tokens) < 17 or not re.match(r"^\d{2}/\d{2}/\d{4}$", tokens[0]):
            continue

        data_venda = tokens[0]
        if data_venda != data_br or tokens[-1] != "Aprovada":
            continue

        meio = " ".join(tokens[5:-9])
        meio_normalizado = _normalize_ascii_text(meio)
        if "debito" in meio_normalizado:
            key = "cartao_debito_caixa"
        elif "credito" in meio_normalizado or "parcelado" in meio_normalizado:
            key = "cartao_credito_caixa"
        else:
            continue

        buckets[key].append(
            {
                "numero": tokens[4],
                "numero_exibicao": tokens[4].lstrip("0") or tokens[4],
                "data_venda": f"{data_venda} às {tokens[2]}",
                "valor_bruto": round(parse_number(tokens[-6]), 2),
            }
        )

    reports: dict[str, dict] = {}
    periodo = f"{data_br} - {data_br}"
    meta_by_key = {
        "cartao_credito_caixa": {
            "tab_title": "Cartão de Crédito CAIXA",
            "menu_text": "Abrir cartão de crédito CAIXA",
            "summary_label": "Cartão de crédito CAIXA",
            "total_label": "Total cartão de crédito CAIXA",
            "section_label": "Transações em cartão de crédito na CAIXA",
            "empty_message": "Nenhuma transação em cartão de crédito da CAIXA encontrada para este dia.",
        },
        "cartao_debito_caixa": {
            "tab_title": "Cartão de Débito CAIXA",
            "menu_text": "Abrir cartão de débito CAIXA",
            "summary_label": "Cartão de débito CAIXA",
            "total_label": "Total cartão de débito CAIXA",
            "section_label": "Transações em cartão de débito na CAIXA",
            "empty_message": "Nenhuma transação em cartão de débito da CAIXA encontrada para este dia.",
        },
    }

    for key, itens in buckets.items():
        itens.sort(key=lambda item: (item.get("data_venda", ""), item.get("numero", "")))
        total = round(sum(float(item.get("valor_bruto", 0.0)) for item in itens), 2)
        meta = meta_by_key[key]
        reports[key] = {
            "arquivo": os.path.basename(caminho_pdf),
            "caminho": caminho_pdf,
            "periodo": periodo,
            "quantidade_autorizados": len(itens),
            "total_autorizado": total,
            "itens_autorizados": itens,
            "quantidade_relatorio": len(itens),
            "total_relatorio": total,
            "consistente": True,
            "origem": "caixa_cartoes_pdf",
            "mensagem": None if itens else meta["empty_message"],
            "categoria": key,
            "tab_title": meta["tab_title"],
            "menu_text": meta["menu_text"],
            "summary_label": meta["summary_label"],
            "total_label": meta["total_label"],
            "section_label": meta["section_label"],
            "empty_message": meta["empty_message"],
            "table_headers": ("Comprovante", "Data", "Valor"),
            "table_mode": "numero_data_valor",
        }

    return reports


def _build_generic_aux_report(
    *,
    categoria: str,
    tab_title: str,
    menu_text: str,
    summary_label: str,
    total_label: str,
    section_label: str,
    headers: tuple[str, ...],
    rows: list[tuple[str, ...]],
    periodo: str | None,
    quantidade: int,
    total: float,
    empty_message: str,
) -> dict:
    return {
        "arquivo": "",
        "caminho": "",
        "periodo": periodo,
        "quantidade_autorizados": quantidade,
        "total_autorizado": round(float(total or 0.0), 2),
        "itens_autorizados": [],
        "quantidade_relatorio": quantidade,
        "total_relatorio": round(float(total or 0.0), 2),
        "consistente": True,
        "origem": "eh_auxiliar",
        "mensagem": None if rows else empty_message,
        "categoria": categoria,
        "tab_title": tab_title,
        "menu_text": menu_text,
        "summary_label": summary_label,
        "total_label": total_label,
        "section_label": section_label,
        "empty_message": empty_message,
        "table_headers": headers,
        "table_rows": rows,
        "table_widths": [100, 220, 200, 120][: len(headers)],
        "table_mode": "custom",
    }


def _multiset_match_by_value(
    esquerda: list[dict],
    direita: list[dict],
    *,
    campo_esquerda: str = "valor",
    campo_direita: str = "valor",
) -> tuple[list[tuple[dict, dict]], list[dict], list[dict]]:
    buckets: dict[int, list[dict]] = {}
    for item in direita:
        key = int(round(float(item.get(campo_direita, 0.0)) * 100))
        buckets.setdefault(key, []).append(item)

    matched: list[tuple[dict, dict]] = []
    left_only: list[dict] = []
    for item in esquerda:
        key = int(round(float(item.get(campo_esquerda, 0.0)) * 100))
        if buckets.get(key):
            matched.append((item, buckets[key].pop(0)))
        else:
            left_only.append(item)

    right_only: list[dict] = []
    for values in buckets.values():
        right_only.extend(values)
    return matched, left_only, right_only


def _consume_matches_against_nf(
    itens_externos: list[dict],
    itens_nf: list[dict],
    *,
    campo_externo: str,
) -> tuple[list[tuple[dict, dict]], list[dict], list[dict]]:
    buckets: dict[int, list[dict]] = {}
    for item in itens_nf:
        key = int(round(float(item.get("valor", 0.0)) * 100))
        buckets.setdefault(key, []).append(item)

    matched: list[tuple[dict, dict]] = []
    remaining: list[dict] = []
    for item in itens_externos:
        key = int(round(float(item.get(campo_externo, 0.0)) * 100))
        if buckets.get(key):
            matched.append((item, buckets[key].pop(0)))
        else:
            remaining.append(item)
    remaining_nf: list[dict] = []
    for values in buckets.values():
        remaining_nf.extend(values)
    return matched, remaining, remaining_nf


def _build_eh_nf_filtered_report(relatorio_caixa: dict) -> dict | None:
    itens_nf = [
        item
        for item in (relatorio_caixa.get("itens_excluidos") or [])
        if "NOTA FISCAL ELETRONICA" in _normalize_caixa_client(item.get("documento", ""))
    ]
    if not itens_nf:
        return None

    rows = [
        (
            _display_eh_order_number(item.get("pedido", "")),
            str(item.get("cliente", "")),
            str(item.get("motivo", "")),
            f"R$ {format_number_br(item.get('valor', 0.0))}",
        )
        for item in itens_nf
    ]
    report = _build_generic_aux_report(
        categoria="nf_pedidos_eh",
        tab_title="NF-e Filtradas",
        menu_text="Abrir NF-e filtradas",
        summary_label="NF-e filtradas",
        total_label="Total NF-e filtradas",
        section_label="Pedidos NF-e filtrados",
        headers=("Pedido", "Cliente", "Motivo", "Valor"),
        rows=rows,
        periodo=relatorio_caixa.get("periodo"),
        quantidade=len(itens_nf),
        total=sum(float(item.get("valor", 0.0)) for item in itens_nf),
        empty_message="Nenhuma NF-e filtrada encontrada.",
    )
    report["table_widths"] = [90, 250, 220, 110]
    return report


def _build_eh_alerts_report(
    periodo: str | None,
    rows: list[tuple[str, ...]],
    *,
    pix_fechamento_rows: list[tuple[str, str]] | None = None,
    pix_maquina_rows: list[tuple[str, str]] | None = None,
    cartao_fechamento_rows: list[tuple[str, str]] | None = None,
    cartao_maquina_rows: list[tuple[str, str]] | None = None,
) -> dict | None:
    pix_fechamento_rows = list(pix_fechamento_rows or [])
    pix_maquina_rows = list(pix_maquina_rows or [])
    cartao_fechamento_rows = list(cartao_fechamento_rows or [])
    cartao_maquina_rows = list(cartao_maquina_rows or [])

    observacao_rows = [
        row
        for row in rows
        if row and row[0] not in {"CF sem Transação Bancária", "Transação Bancária sem CF/NF"}
    ]
    total_rows = (
        len(pix_fechamento_rows)
        + len(pix_maquina_rows)
        + len(cartao_fechamento_rows)
        + len(cartao_maquina_rows)
        + len(observacao_rows)
    )
    if total_rows <= 0:
        return None

    table_rows: list[tuple[str, ...]] = []
    for section_title, section_rows in (
        ("PIX - CF sem Transação Bancária", pix_fechamento_rows),
        ("PIX - Transação Bancária sem CF/NF", pix_maquina_rows),
        ("Cartões - CF sem Transação Bancária", cartao_fechamento_rows),
        ("Cartões - Transação Bancária sem CF/NF", cartao_maquina_rows),
        ("Observações", observacao_rows),
    ):
        if not section_rows:
            continue
        table_rows.append((section_title, "", ""))
        table_rows.extend(section_rows)

    total = 0.0
    for collection in (pix_fechamento_rows, pix_maquina_rows, cartao_fechamento_rows, cartao_maquina_rows):
        for _desc, valor in collection:
            total += parse_number(valor)

    report = _build_generic_aux_report(
        categoria="alertas_eh",
        tab_title="Conciliação Bancária",
        menu_text="Abrir conciliação bancária",
        summary_label="Pendências",
        total_label="Total pendências",
        section_label="Pendências e sobras da conciliação bancária",
        headers=("Tipo", "Detalhe", "Valor"),
        rows=table_rows,
        periodo=periodo,
        quantidade=total_rows,
        total=total,
        empty_message="Nenhum alerta encontrado.",
    )
    report["table_widths"] = [170, 360, 110]
    report["pix_fechamento_rows"] = pix_fechamento_rows
    report["pix_maquina_rows"] = pix_maquina_rows
    report["cartao_fechamento_rows"] = cartao_fechamento_rows
    report["cartao_maquina_rows"] = cartao_maquina_rows
    report["observacao_rows"] = observacao_rows
    return report


def _build_eh_card_mismatch_report(
    periodo: str | None,
    itens_fechamento: list[dict],
    itens_maquina: list[dict],
) -> dict | None:
    if not itens_fechamento and not itens_maquina:
        return None

    def _tipo_curto(titulo: str) -> str:
        titulo_norm = _normalize_caixa_client(titulo)
        if "CREDITO" in titulo_norm:
            return "Crédito"
        if "DEBITO" in titulo_norm:
            return "Débito"
        return str(titulo or "").strip()

    itens_fechamento = sorted(
        itens_fechamento,
        key=lambda item: (_tipo_curto(item.get("titulo", "")), str(item.get("numero_exibicao") or "")),
    )
    itens_maquina = sorted(
        itens_maquina,
        key=lambda item: (_tipo_curto(item.get("titulo", "")), str(item.get("data_venda") or "")),
    )

    rows: list[tuple[str, ...]] = []
    fechamento_rows: list[tuple[str, str]] = []
    maquina_rows: list[tuple[str, str]] = []
    total = 0.0
    total += sum(float(item.get("valor", 0.0)) for item in itens_fechamento)
    total += sum(float(item.get("valor", 0.0)) for item in itens_maquina)

    for item in itens_fechamento:
        fechamento_rows.append(
            (
                f"{_tipo_curto(item.get('titulo', ''))}: CF {item.get('numero_exibicao') or '-'}",
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
        )

    for item in itens_maquina:
        maquina_rows.append(
            (
                f"{_tipo_curto(item.get('titulo', ''))}: {item.get('data_venda') or item.get('numero_exibicao') or '-'}",
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
        )

    max_len = max(len(itens_fechamento), len(itens_maquina))
    for idx in range(max_len):
        fechamento_item = itens_fechamento[idx] if idx < len(itens_fechamento) else None
        maquina_item = itens_maquina[idx] if idx < len(itens_maquina) else None

        fechamento_desc = ""
        fechamento_valor = ""
        if fechamento_item:
            fechamento_desc = (
                f"{_tipo_curto(fechamento_item.get('titulo', ''))}: "
                f"CF {fechamento_item.get('numero_exibicao') or '-'}"
            )
            fechamento_valor = f"R$ {format_number_br(fechamento_item.get('valor', 0.0))}"

        maquina_desc = ""
        maquina_valor = ""
        if maquina_item:
            maquina_desc = (
                f"{_tipo_curto(maquina_item.get('titulo', ''))}: "
                f"{maquina_item.get('data_venda') or maquina_item.get('numero_exibicao') or '-'}"
            )
            maquina_valor = f"R$ {format_number_br(maquina_item.get('valor', 0.0))}"

        rows.append((fechamento_desc, fechamento_valor, maquina_desc, maquina_valor))

    report = _build_generic_aux_report(
        categoria="cartoes_conciliacao_eh",
        tab_title="Conciliação Cartões",
        menu_text="Abrir conciliação cartões",
        summary_label="Pendências cartões",
        total_label="Total pendências cartões",
        section_label="Valores de cartões sem correspondência",
        headers=("Fechamento EH", "Valor EH", "Máquina", "Valor Banco"),
        rows=rows,
        periodo=periodo,
        quantidade=len(rows),
        total=total,
        empty_message="Nenhuma pendência de cartão encontrada.",
    )
    report["table_widths"] = [250, 110, 250, 110]
    report["fechamento_rows"] = fechamento_rows
    report["maquina_rows"] = maquina_rows
    report["fechamento_headers"] = ("Fechamento EH", "Valor EH")
    report["maquina_headers"] = ("Máquina", "Valor Banco")
    report["fechamento_section_label"] = "CF sem Transação Bancária"
    report["maquina_section_label"] = "Transação Bancária sem CF/NF"
    report["fechamento_empty_message"] = "Nenhum CF sem transação bancária encontrado."
    report["maquina_empty_message"] = "Nenhuma transação bancária sem CF/NF encontrada."
    report["fechamento_widths"] = [250, 110]
    report["maquina_widths"] = [250, 110]
    return report


def _analisar_html_fechamento_caixa_eh(html_text: str, arquivo: str = "Fechamento de caixa - Zweb") -> dict:
    periodo = _extract_zweb_period(html_text)
    nfces_map = {}
    totalizadores = {}
    relatorios_pagamento_brutos = {}
    total_abertura = 0.0
    total_sangria = 0.0
    total_geral = 0.0

    section_pattern = re.compile(
        r'<div class="mt-4">\s*<div class="d-flex justify-content-between">\s*'
        r'<div class="fw-bolder fs-6">\s*(?P<titulo>.*?)\s*</div>\s*'
        r'<div>\s*<span class="fw-bolder">Abertura:\s*</span>\s*(?P<abertura>.*?)\s*'
        r'<span class="fw-bolder">Fechamento:\s*</span>\s*(?P<fechamento>.*?)\s*</div>\s*</div>\s*</div>\s*'
        r'<table class="striped-table mt-2">(?P<tabela>.*?)</table>\s*'
        r'<div class="totalizer-footer">.*?<div class="footer-content">\s*(?P<total>[-\d\.,]+)\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    row_block_pattern = re.compile(
        r"<tr\b[^>]*>(?P<row>.*?)</tr>",
        re.IGNORECASE | re.DOTALL,
    )
    row_number_pattern = re.compile(r"<td>\s*(\d{6,})\s*</td>", re.IGNORECASE | re.DOTALL)
    row_date_pattern = re.compile(r"<td>\s*(\d{2}/\d{2}/\d{2})\s*</td>", re.IGNORECASE | re.DOTALL)
    row_money_pattern = re.compile(r"R\$\s*([-\d\.,]+)", re.IGNORECASE)

    for match in section_pattern.finditer(html_text):
        titulo = _clean_zweb_html_value(match.group("titulo"))
        meta_pagamento = _build_zweb_payment_report_meta(titulo)
        bucket_pagamento = None
        if meta_pagamento:
            bucket_pagamento = relatorios_pagamento_brutos.setdefault(
                meta_pagamento["key"],
                {
                    **meta_pagamento,
                    "itens": [],
                    "total_secao": 0.0,
                },
            )
            bucket_pagamento["total_secao"] = round(
                float(bucket_pagamento.get("total_secao", 0.0)) + parse_number(match.group("total")),
                2,
            )
        for row_match in row_block_pattern.finditer(match.group("tabela")):
            row_html = row_match.group("row")
            numero_match = row_number_pattern.search(row_html)
            data_match = row_date_pattern.search(row_html)
            valores_linha = [round(parse_number(valor_str), 2) for valor_str in row_money_pattern.findall(row_html)]

            if not numero_match or not data_match or not valores_linha:
                continue

            numero = numero_match.group(1)
            data_venda = data_match.group(1)
            numero_normalizado = _normalize_fiscal_number(numero)
            valor = round(sum(valores_linha), 2)
            data_exibicao = _display_zweb_short_date(data_venda)
            existente = nfces_map.get(numero_normalizado)
            if existente:
                existente["valor"] = round(existente["valor"] + valor, 2)
                if titulo not in existente["descricao"]:
                    existente["descricao"] = f"{existente['descricao']} + {titulo}"
            else:
                nfces_map[numero_normalizado] = {
                    "numero": numero_normalizado,
                    "numero_exibicao": _display_fiscal_number(numero_normalizado),
                    "descricao": titulo,
                    "data_venda": data_exibicao,
                    "valor": valor,
                }
            if bucket_pagamento is not None:
                for valor_parcela in valores_linha:
                    bucket_pagamento["itens"].append(
                        {
                            "numero": numero_normalizado,
                            "numero_exibicao": _display_fiscal_number(numero_normalizado),
                            "data_venda": data_exibicao,
                            "valor_bruto": valor_parcela,
                        }
                    )

    totalizer_pattern = re.compile(
        r"<tr[^>]*>\s*<td[^>]*>\s*(.*?)\s*</td>\s*<td[^>]*>\s*R\$\s*([-\d\.,]+)\s*</td>\s*</tr>",
        re.IGNORECASE | re.DOTALL,
    )
    totalizer_block_match = re.search(
        r'<table class="striped-table totalizers-table.*?</table>',
        html_text,
        re.IGNORECASE | re.DOTALL,
    )
    if totalizer_block_match:
        for descricao, valor_str in totalizer_pattern.findall(totalizer_block_match.group(0)):
            label = _clean_zweb_html_value(descricao)
            valor = round(parse_number(valor_str), 2)
            totalizadores[label] = valor
            label_normalizado = _normalize_caixa_client(label)
            if "ABERTURA" in label_normalizado:
                total_abertura = valor
            elif "SANGRIA" in label_normalizado:
                total_sangria = valor
            elif "TOTAL GERAL" in label_normalizado:
                total_geral = valor

    itens_nfce = sorted(nfces_map.values(), key=lambda item: item["numero"])
    total_nfce = round(sum(item["valor"] for item in itens_nfce), 2)
    if not total_geral:
        total_geral = round(total_nfce + total_abertura - total_sangria, 2)

    relatorios_pagamento = {}
    for key, bucket in relatorios_pagamento_brutos.items():
        itens = sorted(
            bucket.get("itens", []),
            key=lambda item: (item.get("data_venda") or "", item.get("numero") or ""),
        )
        total_itens = round(sum(float(item.get("valor_bruto", 0.0)) for item in itens), 2)
        total_reportado = round(float(totalizadores.get(bucket["forma_pagamento"], bucket.get("total_secao", 0.0))), 2)
        relatorios_pagamento[key] = {
            "arquivo": arquivo,
            "caminho": "",
            "periodo": periodo,
            "quantidade_autorizados": len(itens),
            "total_autorizado": total_reportado,
            "itens_autorizados": itens,
            "quantidade_relatorio": len(itens),
            "total_relatorio": total_reportado,
            "consistente": abs(total_itens - total_reportado) < 0.01,
            "origem": "zweb_fechamento_caixa",
            "mensagem": None if itens else bucket.get("empty_message"),
            "categoria": key,
            "tab_title": bucket.get("tab_title"),
            "menu_text": bucket.get("menu_text"),
            "summary_label": bucket.get("summary_label"),
            "total_label": bucket.get("total_label"),
            "section_label": bucket.get("section_label"),
            "empty_message": bucket.get("empty_message"),
            "table_headers": ("NFC-e", "Data", "Valor"),
            "table_mode": "numero_data_valor",
        }

    return {
        "arquivo": arquivo,
        "arquivo_tipo": "fechamento_caixa_zweb",
        "arquivo_resumo_titulo": "Arquivo Fechamento",
        "total_resumo_titulo": "Total Fechamento de caixa",
        "subtitle": (
            "Compara o total de caixa dos pedidos com o Fechamento de caixa do Zweb "
            "e aponta as NFC-e faltantes."
        ),
        "resumo_modelo": "EH",
        "periodo": periodo,
        "quantidade_nfce": len(itens_nfce),
        "total_nfce": total_nfce,
        "total_geral": total_geral,
        "total_abertura": total_abertura,
        "total_sangria": total_sangria,
        "totalizadores": totalizadores,
        "relatorios_pagamento": relatorios_pagamento,
        "nfces": itens_nfce,
        "nfces_faltantes_sequencia": _find_missing_fiscal_numbers([item["numero"] for item in itens_nfce]),
        "fiscal_status_map": {},
    }




def _mva_report_type_from_text(texto: str) -> str:
    texto_normalizado = _normalize_caixa_client(texto)
    if "DAV - ORCAMENTO" in texto_normalizado:
        return "orcamentos_mva"
    if "DAV - PEDIDOS DE VENDA" in texto_normalizado:
        return "exportacao_dados_mva"
    return "mva_desconhecido"


def _mva_report_label(arquivo_tipo: str) -> str:
    if arquivo_tipo == "orcamentos_mva":
        return "Orcamentos"
    if arquivo_tipo == "exportacao_dados_mva":
        return "Exportacao de dados"
    return "MVA"


def _normalize_mva_description(resto_linha: str) -> str:
    descricao = re.sub(r"^\s*\d+\s+", "", (resto_linha or "").strip())
    return descricao or "-"


def _extract_mva_word_rows(page) -> list[list[dict]]:
    rows_by_top = {}
    for word in page.extract_words(use_text_flow=False):
        rows_by_top.setdefault(round(word["top"], 1), []).append(word)

    grouped_rows = []
    for top in sorted(rows_by_top):
        row = sorted(rows_by_top[top], key=lambda item: item["x0"])
        if any(re.fullmatch(r"\d{6,}", item["text"]) and item["x0"] < 60 for item in row):
            grouped_rows.append(row)
    return grouped_rows


def _extract_mva_column_bounds(page) -> dict:
    rows_by_top = {}
    for word in page.extract_words(use_text_flow=False):
        rows_by_top.setdefault(round(word["top"], 1), []).append(word)

    header_row = None
    for top in sorted(rows_by_top):
        row = sorted(rows_by_top[top], key=lambda item: item["x0"])
        normalized = [_normalize_caixa_client(item["text"]) for item in row]
        if "STATUS" in normalized:
            header_row = row
            break

    bounds = {
        "code_x1": 140.0,
        "vendor_x0": None,
        "status_x0": None,
    }
    if not header_row:
        return bounds

    for item in header_row:
        normalized = _normalize_caixa_client(item["text"])
        if normalized in {"CODIGO", "CÃ“DIGO"}:
            bounds["code_x1"] = item["x1"]
        elif normalized == "VENDEDOR":
            bounds["vendor_x0"] = item["x0"]
        elif normalized == "STATUS":
            bounds["status_x0"] = item["x0"]
    return bounds


def _extract_mva_description_from_row(row_words: list[dict], bounds: dict, fallback: str) -> str:
    if not row_words:
        return fallback

    left_bound = float(bounds.get("code_x1") or 140.0) + 2.0
    right_bound = None
    vendor_x0 = bounds.get("vendor_x0")
    status_x0 = bounds.get("status_x0")
    if vendor_x0 is not None:
        right_bound = float(vendor_x0) - 32.0
    elif status_x0 is not None:
        right_bound = float(status_x0) - 20.0

    descricao = " ".join(
        item["text"]
        for item in row_words
        if item["x0"] >= left_bound and (right_bound is None or item["x1"] <= right_bound)
    ).strip()
    return descricao or fallback


def _parse_period_bounds(periodo: str) -> tuple[object | None, object | None]:
    from datetime import datetime

    inicio, fim = _extract_period_range(periodo or "")
    if not inicio or not fim:
        return None, None
    return (
        datetime.strptime(inicio, "%d/%m/%Y"),
        datetime.strptime(fim, "%d/%m/%Y"),
    )




def combinar_relatorios_caixa_mva(relatorios: list[dict]) -> dict:
    from datetime import datetime

    relatorios_validos = [
        rel
        for rel in relatorios
        if rel and (rel.get("caixa_modelo") or "").upper() == "MVA"
    ]
    if not relatorios_validos:
        return {
            "arquivo": "",
            "caixa_modelo": "MVA",
            "arquivo_tipo": "mva_davs_combinado",
            "periodo": None,
            "pedidos_total": 0,
            "pedidos_balcao": 0,
            "pedidos_caixa": 0,
            "pedidos_excluidos": 0,
            "pedidos_excluidos_cliente": 0,
            "pedidos_excluidos_documento": 0,
            "pedidos_editando": 0,
            "pedidos_outros_status": 0,
            "total_documento": 0.0,
            "total_excluido": 0.0,
            "total_caixa": 0.0,
            "itens_caixa": [],
            "itens_excluidos": [],
        }

    datas_inicio = []
    datas_fim = []
    for relatorio in relatorios_validos:
        inicio, fim = _parse_period_bounds(relatorio.get("periodo", ""))
        if inicio:
            datas_inicio.append(inicio)
        if fim:
            datas_fim.append(fim)

    periodo = None
    if datas_inicio and datas_fim:
        periodo = (
            f"{min(datas_inicio).strftime('%d/%m/%Y')} - "
            f"{max(datas_fim).strftime('%d/%m/%Y')}"
        )

    itens_caixa = []
    itens_excluidos = []
    for relatorio in relatorios_validos:
        for item in relatorio.get("itens_caixa", []):
            itens_caixa.append({**item})
        for item in relatorio.get("itens_excluidos", []):
            itens_excluidos.append({**item})

    return {
        "arquivo": " + ".join(relatorio.get("arquivo") or "-" for relatorio in relatorios_validos),
        "caixa_modelo": "MVA",
        "arquivo_tipo": "mva_davs_combinado",
        "periodo": periodo,
        "pedidos_total": sum(relatorio.get("pedidos_total", 0) for relatorio in relatorios_validos),
        "pedidos_balcao": 0,
        "pedidos_caixa": sum(relatorio.get("pedidos_caixa", 0) for relatorio in relatorios_validos),
        "pedidos_excluidos": sum(relatorio.get("pedidos_excluidos", 0) for relatorio in relatorios_validos),
        "pedidos_excluidos_cliente": 0,
        "pedidos_excluidos_documento": 0,
        "pedidos_editando": sum(relatorio.get("pedidos_editando", 0) for relatorio in relatorios_validos),
        "pedidos_outros_status": sum(relatorio.get("pedidos_outros_status", 0) for relatorio in relatorios_validos),
        "total_documento": round(
            sum(float(relatorio.get("total_documento", 0.0)) for relatorio in relatorios_validos),
            2,
        ),
        "total_excluido": round(
            sum(float(relatorio.get("total_excluido", 0.0)) for relatorio in relatorios_validos),
            2,
        ),
        "total_caixa": round(
            sum(float(relatorio.get("total_caixa", 0.0)) for relatorio in relatorios_validos),
            2,
        ),
        "itens_caixa": sorted(
            itens_caixa,
            key=lambda item: item.get("pedido", ""),
        ),
        "itens_excluidos": sorted(
            itens_excluidos,
            key=lambda item: (
                item.get("documento", ""),
                item.get("origem_mva", ""),
                item.get("pedido", ""),
            ),
        ),
    }


def validar_arquivo_caixa_mva(relatorio: dict, arquivo_tipo_esperado: str) -> tuple[bool, str]:
    if relatorio and relatorio.get("pdf_sem_texto"):
        return False, _pdf_sem_texto_message(relatorio.get("arquivo"))

    if not relatorio or (relatorio.get("caixa_modelo") or "").upper() != "MVA":
            return False, "O arquivo selecionado não parece ser um relatório de Caixa MVA."

    if arquivo_tipo_esperado == "exportacao_dados_mva":
        if relatorio.get("arquivo_tipo") != "exportacao_dados_mva":
            return False, "O arquivo selecionado no passo 1 não parece ser a Exportação de dados da MVA."
    elif arquivo_tipo_esperado == "orcamentos_mva":
        if relatorio.get("arquivo_tipo") != "orcamentos_mva":
            return False, "O arquivo selecionado no passo 2 não parece ser o relatório de Orçamentos da MVA."

    if relatorio.get("pedidos_total", 0) <= 0:
        return False, "O arquivo selecionado não trouxe nenhum DAV válido."
    return True, ""




def _normalize_fiscal_number(numero: str) -> str:
    digits = re.sub(r"\D", "", str(numero or ""))
    if not digits:
        return ""
    return digits.zfill(9)


def _display_fiscal_number(numero: str) -> str:
    normalized = _normalize_fiscal_number(numero)
    if not normalized:
        return ""
    return str(int(normalized))


def _extract_period_range(periodo: str) -> tuple[str | None, str | None]:
    match = re.search(r"(\d{2}/\d{2}/\d{4})\s*-\s*(\d{2}/\d{2}/\d{4})", periodo or "")
    if not match:
        return None, None
    return match.group(1), match.group(2)


def _period_to_iso_date(periodo: str) -> str | None:
    inicio, fim = _extract_period_range(periodo or "")
    if not inicio or not fim or inicio != fim:
        return None
    try:
        return datetime.strptime(inicio, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _iso_to_br_date(data_iso: str) -> str | None:
    try:
        return datetime.strptime(data_iso, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return None


def _period_from_br_dates(datas: list[str]) -> str | None:
    datas_validas = []
    for data_str in datas:
        try:
            datas_validas.append(datetime.strptime(str(data_str).strip(), "%d/%m/%Y"))
        except ValueError:
            continue

    if not datas_validas:
        return None

    inicio = min(datas_validas).strftime("%d/%m/%Y")
    fim = max(datas_validas).strftime("%d/%m/%Y")
    return f"{inicio} - {fim}"


def _runtime_user_dir() -> str:
    import sys

    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def _zweb_browser_profile_dir() -> str:
    base_dir = (
        os.environ.get("LOCALAPPDATA")
        or os.environ.get("APPDATA")
        or _runtime_user_dir()
    )
    profile_dir = os.path.join(base_dir, "RelatorioClientes", "zweb_browser_profile")
    os.makedirs(profile_dir, exist_ok=True)
    return profile_dir


def _load_zweb_credentials() -> dict | None:
    username = str(ZWEB_USERNAME or "").strip()
    password = str(ZWEB_PASSWORD or "").strip()
    base_url = str(ZWEB_BASE_URL or "").strip().rstrip("/")
    if username and password and base_url.startswith("http"):
        return {
            "username": username,
            "password": password,
            "base_url": base_url,
            "sign_in_url": f"{base_url}/#/sign-in",
            "dashboard_url": f"{base_url}/#/dashboard",
            "finance_movimentations_url": f"{base_url}/#/finance/movimentations",
            "finance_reports_url": f"{base_url}/#/finance/reports",
            "document_reports_url": f"{base_url}/#/document/reports",
        }

    base_dir = os.path.dirname(os.path.abspath(__file__))
    for filename in ("credenciais.txt", "credencias.txt"):
        caminho = os.path.join(base_dir, filename)
        if not os.path.isfile(caminho):
            continue
        try:
            with open(caminho, "r", encoding="utf-8", errors="ignore") as arquivo:
                linhas = [linha.strip() for linha in arquivo.readlines() if linha.strip()]
        except OSError:
            continue

        marker = next(
            (linha for linha in linhas if _normalize_caixa_client(linha) == "CONTA ZWEB:"),
            "",
        )
        if not marker:
            continue

        try:
            idx = linhas.index(marker)
        except ValueError:
            continue

        username = str(linhas[idx + 1] if len(linhas) > idx + 1 else "").strip()
        password = str(linhas[idx + 2] if len(linhas) > idx + 2 else "").strip()
        base_url = str(linhas[idx + 3] if len(linhas) > idx + 3 else "").strip().rstrip("/")
        if username and password and base_url.startswith("http"):
            return {
                "username": username,
                "password": password,
                "base_url": base_url,
                "sign_in_url": f"{base_url}/#/sign-in",
                "dashboard_url": f"{base_url}/#/dashboard",
                "finance_movimentations_url": f"{base_url}/#/finance/movimentations",
                "finance_reports_url": f"{base_url}/#/finance/reports",
                "document_reports_url": f"{base_url}/#/document/reports",
            }
    return None


def _find_chromium_browser_path() -> str | None:
    env_candidates = [
        os.environ.get("CHROMIUM_PATH"),
        os.environ.get("CHROME_PATH"),
        os.environ.get("EDGE_PATH"),
    ]
    common_paths = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files\Chromium\Application\chrome.exe",
        r"C:\Program Files (x86)\Chromium\Application\chrome.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    ]

    for caminho in [*env_candidates, *common_paths]:
        caminho = str(caminho or "").strip()
        if caminho and os.path.isfile(caminho):
            return caminho
    return None


def _pick_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_devtools_ready(port: int, timeout: float = 30.0) -> dict:
    import urllib.request
    import urllib.error
    deadline = time.time() + timeout
    last_exc = None

    while time.time() < deadline:
        try:
            resposta = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=1)
            resposta.raise_for_status()
            return resposta.json() or {}
        except Exception as exc:
            last_exc = exc
            time.sleep(0.4)

    raise RuntimeError(f"Não foi possível iniciar o Chromium para gerar o relatório PIX: {last_exc}")


def _emit_pix_status(on_status, message: str) -> None:
    if callable(on_status):
        try:
            on_status(str(message or "").strip())
        except Exception:
            pass


def _prepare_chromium_profile(profile_dir: str, download_dir: str) -> None:
    default_dir = os.path.join(profile_dir, "Default")
    os.makedirs(default_dir, exist_ok=True)

    prefs_path = os.path.join(default_dir, "Preferences")
    prefs = {
        "credentials_enable_service": False,
        "autofill": {
            "enabled": False,
        },
        "download": {
            "default_directory": download_dir,
            "prompt_for_download": False,
        },
        "profile": {
            "password_manager_enabled": False,
            "default_content_setting_values": {
                "geolocation": 2,
                "notifications": 2,
            },
        },
    }

    try:
        with open(prefs_path, "w", encoding="utf-8") as arquivo:
            json.dump(prefs, arquivo)
    except OSError:
        pass


def _launch_browser_process(chrome_args: list[str]) -> subprocess.Popen:
    popen_kwargs = {
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
    }
    if os.name == "nt":
        popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return subprocess.Popen(chrome_args, **popen_kwargs)


def gerar_relatorios_caixa_eh_zweb(
    data_br: str,
    on_status=None,
    cancel_event: threading.Event | None = None,
) -> tuple[dict, dict, dict]:
    cancel_event = cancel_event or threading.Event()

    def _check_cancelled() -> None:
        if cancel_event.is_set():
            raise RuntimeError("__cancelled__")

    credenciais = _load_zweb_credentials()
    if not credenciais:
        raise ValueError("As credenciais do Zweb não foram encontradas no credenciais.txt.")

    try:
        data_iso = datetime.strptime(str(data_br or "").strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("A data do Caixa EH e invalida.") from exc

    local_payment_reports = _find_eh_local_payment_reports(data_br)
    local_pix_pdf = local_payment_reports.get("pix")
    local_card_pdf = local_payment_reports.get("cartoes")
    avisos_usuario: list[str] = []

    navegador = _find_chromium_browser_path()
    if not navegador:
        raise RuntimeError("Nenhum navegador Chromium compativel foi encontrado para acessar o Zweb.")

    profile_root = _zweb_browser_profile_dir()
    profile_dir = ""
    port = 0
    proc = None

    async def _run() -> tuple[str, str, dict, list[dict], str | None]:
        import websockets

        _check_cancelled()
        meta = _wait_for_devtools_ready(port)
        ws_url = str(meta.get("webSocketDebuggerUrl") or "").strip()
        if not ws_url:
            raise RuntimeError("Não foi possível conectar ao Chromium para acessar o Zweb.")

        async with websockets.connect(ws_url, max_size=50_000_000) as conn:
            next_id = 0
            pending = {}

            async def recv_loop():
                while True:
                    _check_cancelled()
                    mensagem = json.loads(await conn.recv())
                    if "id" in mensagem and mensagem["id"] in pending:
                        pending.pop(mensagem["id"]).set_result(mensagem)

            recv_task = asyncio.create_task(recv_loop())

            async def cdp(method: str, params: dict | None = None, session_id: str | None = None, timeout: float = 60.0):
                nonlocal next_id
                _check_cancelled()
                next_id += 1
                future = asyncio.get_running_loop().create_future()
                pending[next_id] = future
                mensagem = {"id": next_id, "method": method}
                if params:
                    mensagem["params"] = params
                if session_id:
                    mensagem["sessionId"] = session_id
                await conn.send(json.dumps(mensagem))
                resposta = await asyncio.wait_for(future, timeout)
                if "error" in resposta:
                    raise RuntimeError(resposta["error"])
                return resposta.get("result", {})

            async def eval_js(session_id: str, expression: str):
                _check_cancelled()
                resposta = await cdp(
                    "Runtime.evaluate",
                    {
                        "expression": expression,
                        "returnByValue": True,
                        "awaitPromise": True,
                    },
                    session_id=session_id,
                )
                return resposta.get("result", {}).get("value")

            async def wait_for_condition(session_id: str, expression: str, timeout: float = 60.0, step: float = 0.4):
                deadline = time.time() + timeout
                last_value = None
                while time.time() < deadline:
                    _check_cancelled()
                    try:
                        last_value = await eval_js(session_id, expression)
                        if last_value:
                            return last_value
                    except Exception as exc:
                        last_value = str(exc)
                    await asyncio.sleep(step)
                raise TimeoutError(f"{expression} :: ultimo retorno={last_value!r}")

            async def open_route(session_id: str, url: str, ready_text: str) -> None:
                _check_cancelled()
                await cdp("Page.navigate", {"url": url}, session_id=session_id)
                await wait_for_condition(session_id, "document.readyState === 'complete'", timeout=60.0)
                await wait_for_condition(
                    session_id,
                    f"""
                    (() => {{
                        const text = document.body ? document.body.innerText : '';
                        return location.href.includes({url.split('#', 1)[-1]!r}) || text.toUpperCase().includes({ready_text.upper()!r});
                    }})()
                    """,
                    timeout=60.0,
                )

            async def ensure_logged_in(session_id: str) -> None:
                _check_cancelled()
                _emit_pix_status(on_status, "Abrindo login do Zweb...")
                await cdp("Page.navigate", {"url": credenciais["sign_in_url"]}, session_id=session_id)
                await wait_for_condition(session_id, "document.readyState === 'complete'", timeout=60.0)
                estado_login = await wait_for_condition(
                    session_id,
                    "location.href.includes('/dashboard') || location.hash.includes('/dashboard') || document.querySelectorAll('input').length >= 2",
                    timeout=60.0,
                )
                if estado_login and await eval_js(
                    session_id,
                    "location.href.includes('/dashboard') || location.hash.includes('/dashboard')",
                ):
                    return
                _emit_pix_status(on_status, "Autenticando no Zweb...")
                await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const setNativeValue = (el, value) => {{
                            if (!el) return false;
                            const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
                            setter.call(el, value);
                            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            el.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                            return true;
                        }};
                        const inputs = [...document.querySelectorAll('input')];
                        const email = inputs.find((el) => (el.name || '').toLowerCase() === 'email') || inputs[0];
                        const password = inputs.find((el) => (el.type || '').toLowerCase() === 'password') || inputs[1];
                        if (!setNativeValue(email, {credenciais["username"]!r})) return false;
                        if (!setNativeValue(password, {credenciais["password"]!r})) return false;
                        const button = document.querySelector('button[type="submit"], button.btn-primary, button');
                        button?.click();
                        return true;
                    }})()
                    """,
                )
                _emit_pix_status(on_status, "Entrando no painel do Zweb...")
                await wait_for_condition(
                    session_id,
                    "location.href.includes('/dashboard') || location.hash.includes('/dashboard')",
                    timeout=90.0,
                )

            async def click_report_button(session_id: str, titulo: str) -> None:
                _check_cancelled()
                await wait_for_condition(
                    session_id,
                    f"""
                    (() => {{
                        const normalize = (value) =>
                            (value || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .replace(/\\s+/g, ' ')
                                .trim()
                                .toUpperCase();
                        const targetTitle = {titulo.upper()!r};
                        const rows = [...document.querySelectorAll('div.row.mb-5.p-4.bg-light.align-items-center.rounded')];
                        return rows.some((el) => normalize(el.innerText || el.textContent).includes(targetTitle));
                    }})()
                    """,
                    timeout=30.0,
                )
                clicked = await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const normalize = (value) =>
                            (value || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .replace(/\\s+/g, ' ')
                                .trim()
                                .toUpperCase();
                        const targetTitle = {titulo.upper()!r};
                        const rows = [...document.querySelectorAll('div.row.mb-5.p-4.bg-light.align-items-center.rounded')];
                        const row = rows.find((el) => normalize(el.innerText || el.textContent).includes(targetTitle));
                        const button = row?.querySelector('button.btn.btn-primary.btn-sm');
                        if (!button) return false;
                        button.scrollIntoView({{ block: 'center', inline: 'center' }});
                        button.click();
                        return true;
                    }})()
                    """,
                )
                if not clicked:
                    raise RuntimeError(f"Não foi possível localizar o relatório {titulo} no Zweb.")

            async def wait_modal(session_id: str, titulo: str) -> None:
                _check_cancelled()
                await wait_for_condition(
                    session_id,
                    f"""
                    (() => {{
                        const modal = document.querySelector('.modal.show#modal-wrapper');
                        if (!modal) return false;
                        const text = (modal.innerText || '').toUpperCase();
                        return text.includes({titulo.upper()!r});
                    }})()
                    """,
                    timeout=30.0,
                )

            async def set_modal_period(session_id: str, from_selector: str, to_selector: str) -> None:
                _check_cancelled()
                preenchido = await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const setValue = (selector, value) => {{
                            const input = document.querySelector(selector);
                            if (!input) return false;
                            const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value').set;
                            setter.call(input, value);
                            input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                            input.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            input.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                            return true;
                        }};
                        const okFrom = setValue({from_selector!r}, {data_br!r});
                        const okTo = setValue({to_selector!r}, {data_br!r});
                        return okFrom && okTo;
                    }})()
                    """,
                )
                if not preenchido:
                    raise RuntimeError("Não foi possível preencher o período do relatório no Zweb.")

            async def ensure_html_format(session_id: str) -> None:
                _check_cancelled()
                await eval_js(
                    session_id,
                    """
                    (() => {
                        const radio = [...document.querySelectorAll('input[type="radio"]')]
                            .find((el) => String(el.value || '').toUpperCase() === 'HTML');
                        if (!radio) return true;
                        if (!radio.checked) {
                            radio.click();
                            radio.dispatchEvent(new Event('change', { bubbles: true }));
                        }
                        return true;
                    })()
                    """,
                )

            async def prepare_fechamento_filters(session_id: str) -> None:
                _check_cancelled()
                prepared = await eval_js(
                    session_id,
                    r"""
                    (() => {
                        const normalize = (value) =>
                            (value || '')
                                .normalize('NFD')
                                .replace(/[\u0300-\u036f]/g, '')
                                .replace(/\s+/g, ' ')
                                .trim()
                                .toUpperCase();

                        document.querySelectorAll('.multiselect__tag-icon').forEach((el) => el.click());

                        const multiselect = document.querySelector('.z-select-multiple .multiselect') || document.querySelector('.z-select-right-icon');
                        multiselect?.click();

                        const option = [...document.querySelectorAll('.multiselect__option')]
                            .find((el) => normalize(el.innerText || el.textContent).startsWith('001'));
                        if (!option) return false;
                        option.click();

                        const labelNode = [...document.querySelectorAll('label, span, div')]
                            .find((el) => normalize(el.innerText || el.textContent).includes('AGRUPAR POR FORMA DE PAGAMENTO'));
                        const containers = [
                            labelNode,
                            labelNode?.closest('label'),
                            labelNode?.closest('div'),
                            labelNode?.parentElement,
                            labelNode?.parentElement?.parentElement,
                        ].filter(Boolean);
                        let checkbox = null;
                        for (const container of containers) {
                            checkbox = container.querySelector?.('input[type="checkbox"]') || null;
                            if (checkbox) break;
                        }
                        if (checkbox && !checkbox.checked) checkbox.click();
                        return true;
                    })()
                    """,
                )
                if not prepared:
                    raise RuntimeError("Não foi possível selecionar o Caixa 001 no Zweb.")

            async def capture_report_url(session_id: str) -> str:
                _check_cancelled()
                await eval_js(
                    session_id,
                    """
                    (() => {
                        window.__codexReportUrl = '';
                        window.__codexOriginalOpen = window.__codexOriginalOpen || window.open;
                        window.open = function(url) {
                            window.__codexReportUrl = url || '';
                            return null;
                        };
                        return true;
                    })()
                    """,
                )
                clicked = await eval_js(
                    session_id,
                    """
                    (() => {
                        const button = document.querySelector('.modal.show#modal-wrapper .modal-footer button.btn.btn-primary');
                        if (!button) return false;
                        button.click();
                        return true;
                    })()
                    """,
                )
                if not clicked:
                    raise RuntimeError("Não foi possível confirmar a geração do relatório no Zweb.")

                return await wait_for_condition(
                    session_id,
                    """
                    (() => {
                        const direct = window.__codexReportUrl || '';
                        if (direct) return direct;
                        const resources = performance.getEntriesByType('resource') || [];
                        const entry = [...resources]
                            .reverse()
                            .find((item) => /\\/uploads\\/reports\\/report\\/.*\\.html/i.test(String(item && item.name || '')));
                        return entry ? entry.name : '';
                    })()
                    """,
                    timeout=60.0,
                )

            async def fetch_report_html(session_id: str, report_key: str) -> str:
                _check_cancelled()
                if report_key == "pedidos_importados":
                    _emit_pix_status(on_status, "Gerando Pedidos importados...")
                    await open_route(session_id, credenciais["document_reports_url"], "PEDIDOS IMPORTADOS")
                    await click_report_button(session_id, "Pedidos importados")
                    await wait_modal(session_id, "Pedidos importados")
                    await set_modal_period(session_id, "#from_date input.dp__input", "#to_date input.dp__input")
                    await ensure_html_format(session_id)
                else:
                    _emit_pix_status(on_status, "Gerando Fechamento de caixa...")
                    await open_route(session_id, credenciais["finance_reports_url"], "FECHAMENTO DE CAIXA")
                    await click_report_button(session_id, "Fechamento de caixa")
                    await wait_modal(session_id, "Fechamento de caixa")
                    await set_modal_period(session_id, "#fromDate input.dp__input", "#toDate input.dp__input")
                    await ensure_html_format(session_id)
                    await prepare_fechamento_filters(session_id)

                report_url = str(await capture_report_url(session_id) or "").strip()
                if not report_url:
                    raise RuntimeError("O Zweb não retornou a URL do relatório solicitado.")

                _check_cancelled()
                resposta = requests.get(report_url, timeout=90.0)
                resposta.raise_for_status()
                return _decode_report_response_text(resposta)

            async def fetch_fiscal_nfce_status_map(session_id: str) -> dict:
                _check_cancelled()
                _emit_pix_status(on_status, "Consultando Fiscal > NFC-e...")
                await open_route(session_id, f"{credenciais['base_url']}/#/fiscal/nfce", "NFC-E")

                itens_encontrados = []
                pagina = 1
                max_results = 100

                while pagina <= 30:
                    _check_cancelled()
                    payload = {
                        "modelos": ["65", "59"],
                        "page": pagina,
                        "maxResults": max_results,
                    }
                    resposta = await eval_js(
                        session_id,
                        f"""
                        (async () => {{
                            const token = localStorage.getItem('token') || '';
                            const response = await fetch('https://api.zweb.com.br/rpc/v2/fiscal.get-nfe-paginate', {{
                                method: 'POST',
                                credentials: 'include',
                                headers: {{
                                    'Accept': 'application/json',
                                    'Content-Type': 'application/json',
                                    ...(token ? {{ 'Authorization': `Bearer ${{token}}` }} : {{}})
                                }},
                                body: JSON.stringify({json.dumps(payload, ensure_ascii=False)})
                            }});
                            const text = await response.text();
                            return {{
                                ok: response.ok,
                                status: response.status,
                                text
                            }};
                        }})()
                        """,
                    )
                    status_code = int((resposta or {}).get("status") or 0)
                    if status_code >= 400:
                        raise RuntimeError(
                            f"O Zweb retornou erro ao consultar Fiscal > NFC-e ({status_code})."
                        )

                    try:
                        payload_resposta = json.loads((resposta or {}).get("text") or "{}")
                    except json.JSONDecodeError as exc:
                        raise RuntimeError(
                            "O Zweb retornou um JSON inválido ao consultar Fiscal > NFC-e."
                        ) from exc

                    pagina_itens = payload_resposta.get("data") or []
                    if not isinstance(pagina_itens, list) or not pagina_itens:
                        break

                    datas_pagina = []
                    for item in pagina_itens:
                        emissao_iso = _extract_zweb_fiscal_emission_iso(item.get("emission", ""))
                        if not emissao_iso:
                            continue
                        datas_pagina.append(emissao_iso)
                        if emissao_iso == data_iso:
                            itens_encontrados.append(item)

                    if not datas_pagina:
                        break

                    data_mais_antiga = min(datas_pagina)
                    if data_mais_antiga < data_iso:
                        break

                    pagina += 1

                return _build_zweb_fiscal_status_map(itens_encontrados)

            async def fetch_finance_pix_transactions(session_id: str) -> list[dict]:
                _check_cancelled()
                _emit_pix_status(on_status, "Consultando Financeiro > Movimentações...")
                await open_route(session_id, credenciais["finance_movimentations_url"], "MOVIMENTACOES")

                pagina = 1
                max_results = 200
                itens_encontrados = []

                while pagina <= 30:
                    _check_cancelled()
                    payload = {
                        "done": True,
                        "sort": {"order": "DESC", "key": "paymentDate"},
                        "page": pagina,
                        "maxResults": max_results,
                    }
                    resposta = await eval_js(
                        session_id,
                        f"""
                        (async () => {{
                            const token = localStorage.getItem('token') || '';
                            const response = await fetch('https://api.zweb.com.br/rpc/v2/finance.get-transactions-paginate', {{
                                method: 'POST',
                                credentials: 'include',
                                headers: {{
                                    'Accept': 'application/json',
                                    'Content-Type': 'application/json',
                                    ...(token ? {{ 'Authorization': `Bearer ${{token}}` }} : {{}})
                                }},
                                body: JSON.stringify({json.dumps(payload, ensure_ascii=False)})
                            }});
                            const text = await response.text();
                            return {{
                                ok: response.ok,
                                status: response.status,
                                text
                            }};
                        }})()
                        """,
                    )
                    status_code = int((resposta or {}).get("status") or 0)
                    if status_code >= 400:
                        raise RuntimeError(
                            f"O Zweb retornou erro ao consultar Financeiro > Movimentações ({status_code})."
                        )

                    try:
                        payload_resposta = json.loads((resposta or {}).get("text") or "{}")
                    except json.JSONDecodeError as exc:
                        raise RuntimeError(
                            "O Zweb retornou um JSON inválido ao consultar Financeiro > Movimentações."
                        ) from exc

                    pagina_itens = payload_resposta.get("data") or []
                    if not isinstance(pagina_itens, list) or not pagina_itens:
                        break

                    datas_pagina = []
                    for item in pagina_itens:
                        pagamento_iso = str(item.get("paymentDate") or "").strip()[:10]
                        if not pagamento_iso:
                            continue
                        datas_pagina.append(pagamento_iso)
                        if pagamento_iso == data_iso:
                            itens_encontrados.append(item)

                    if not datas_pagina:
                        break

                    data_mais_antiga = min(datas_pagina)
                    if data_mais_antiga < data_iso:
                        break

                    pagina += 1

                return itens_encontrados

            try:
                _check_cancelled()
                target_id = (await cdp("Target.createTarget", {"url": "about:blank"})).get("targetId")
                if not target_id:
                    raise RuntimeError("Não foi possível abrir a aba oculta do Zweb.")

                session_id = (await cdp("Target.attachToTarget", {"targetId": target_id, "flatten": True})).get("sessionId")
                if not session_id:
                    raise RuntimeError("Não foi possível anexar a aba oculta do Zweb.")

                await cdp("Page.enable", session_id=session_id)
                await cdp("Runtime.enable", session_id=session_id)

                _check_cancelled()
                await ensure_logged_in(session_id)
                html_pedidos = await fetch_report_html(session_id, "pedidos_importados")
                html_fechamento = await fetch_report_html(session_id, "fechamento_caixa")
                try:
                    fiscal_status_map = await fetch_fiscal_nfce_status_map(session_id)
                except Exception:
                    fiscal_status_map = {}
                try:
                    pix_transactions = await fetch_finance_pix_transactions(session_id)
                    pix_error = None
                except Exception as exc:
                    pix_transactions = []
                    pix_error = str(exc)
                return html_pedidos, html_fechamento, fiscal_status_map, pix_transactions, pix_error
            finally:
                recv_task.cancel()
                try:
                    await recv_task
                except BaseException:
                    pass

    last_error = None
    html_pedidos = ""
    html_fechamento = ""
    fiscal_status_map = {}
    pix_transactions = []
    pix_error = None
    for tentativa in range(2):
        _check_cancelled()
        profile_dir = tempfile.mkdtemp(prefix="run_", dir=profile_root)
        port = _pick_free_local_port()
        _prepare_chromium_profile(profile_dir, _runtime_user_dir())
        chrome_args = [
            navegador,
            f"--remote-debugging-port={port}",
            f"--user-data-dir={profile_dir}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-background-networking",
            "--disable-component-update",
            "--disable-popup-blocking",
            "--disable-notifications",
            "--deny-permission-prompts",
            "--disable-save-password-bubble",
            "--disable-features=PasswordManagerOnboarding,AutofillServerCommunication",
            "--window-size=1400,900",
            "--headless=new",
            "--disable-gpu",
            "about:blank",
        ]

        _emit_pix_status(on_status, "Acessando Zweb...")
        proc = _launch_browser_process(chrome_args)
        try:
            html_pedidos, html_fechamento, fiscal_status_map, pix_transactions, pix_error = asyncio.run(
                asyncio.wait_for(_run(), timeout=180.0)
            )
            last_error = None
            break
        except TimeoutError:
            last_error = RuntimeError(
                "O Zweb demorou demais para responder durante a autenticação ou geração dos relatórios."
            )
            if tentativa >= 1:
                raise last_error
            time.sleep(1.0)
        except Exception as exc:
            last_error = exc
            if tentativa >= 1:
                raise
            time.sleep(1.0)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
            shutil.rmtree(profile_dir, ignore_errors=True)

    if last_error is not None:
        raise last_error

    _emit_pix_status(on_status, "Processando Pedidos importados...")
    relatorio = _analisar_html_pedidos_importados_eh(html_pedidos)
    _emit_pix_status(on_status, "Processando Fechamento de caixa...")
    relatorio_fechamento = _analisar_html_fechamento_caixa_eh(html_fechamento)
    relatorio_fechamento["fiscal_status_map"] = fiscal_status_map or {}
    relatorio = _aplicar_filtro_canceladas_pedidos_eh(relatorio, fiscal_status_map or {})

    if local_card_pdf:
        _emit_pix_status(on_status, "Lendo relatorio local de cartoes...")
        try:
            relatorios_cartao = _build_card_reports_from_caixa_pdf(local_card_pdf, data_br)
            relatorios_validos = {
                key: report for key, report in relatorios_cartao.items() if report.get("itens_autorizados")
            }
            if relatorios_validos:
                relatorio_fechamento["relatorios_pagamento"].update(relatorios_validos)
            else:
                avisos_usuario.append(
                    f'O arquivo "{os.path.basename(local_card_pdf)}" não trouxe transações de cartão para {data_br} e foi ignorado.'
                )
        except Exception as exc:
            avisos_usuario.append(
                f'Não foi possível ler o arquivo "{os.path.basename(local_card_pdf)}" para {data_br}: {exc}'
            )

    if local_pix_pdf:
        _emit_pix_status(on_status, "Lendo relatorio local de PIX...")
        try:
            if str(local_pix_pdf).lower().endswith(".csv"):
                relatorio_pix = _build_pix_report_from_caixa_csv(local_pix_pdf, data_br)
            else:
                relatorio_pix = _build_pix_report_from_caixa_pdf(local_pix_pdf, data_br)
            if relatorio_pix.get("quantidade_autorizados", 0) <= 0:
                avisos_usuario.append(
                    f'O arquivo "{os.path.basename(local_pix_pdf)}" não trouxe transações PIX para {data_br} e foi ignorado.'
                )
                relatorio_pix = _build_pix_report_from_zweb_movimentations(data_iso, pix_transactions, erro=pix_error)
        except Exception as exc:
            avisos_usuario.append(
                f'Não foi possível ler o arquivo "{os.path.basename(local_pix_pdf)}" para {data_br}: {exc}'
            )
            relatorio_pix = _build_pix_report_from_zweb_movimentations(data_iso, pix_transactions, erro=str(exc))
    else:
        relatorio_pix = _build_pix_report_from_zweb_movimentations(data_iso, pix_transactions, erro=pix_error)

    if avisos_usuario:
        relatorio_fechamento["avisos_usuario"] = list(dict.fromkeys(avisos_usuario))
        if relatorio_pix is not None:
            relatorio_pix["avisos_usuario"] = list(dict.fromkeys(avisos_usuario))

    if relatorio_pix:
        relatorio_fechamento.setdefault("relatorios_pagamento", {})
        relatorio_fechamento["relatorios_pagamento"][str(relatorio_pix.get("categoria") or "pagamentos_digitais_nfce")] = relatorio_pix

    return relatorio, relatorio_fechamento, relatorio_pix


def _parse_caixa_pix_datetime(data_hora: str) -> tuple[str, datetime]:
    texto = str(data_hora or "").strip()
    if not texto:
        return "", datetime.min

    normalizado = texto.replace("Z", "+00:00")
    try:
        data = datetime.fromisoformat(normalizado)
        return data.strftime("%d/%m/%Y as %H:%M"), data
    except ValueError:
        pass

    match = re.match(r"(\d{4}-\d{2}-\d{2})[T\s](\d{2}:\d{2})(?::\d{2}(?:\.\d+)?)?", texto)
    if match:
        texto_br = f"{match.group(1)} {match.group(2)}"
        try:
            data = datetime.strptime(texto_br, "%Y-%m-%d %H:%M")
            return data.strftime("%d/%m/%Y as %H:%M"), data
        except ValueError:
            pass

    return texto, datetime.min


def _build_pix_report_from_zweb_movimentations(data_iso: str, lancamentos: list[dict], erro: str | None = None) -> dict:
    itens_autorizados = []

    for lancamento in lancamentos or []:
        data_pagamento = str(lancamento.get("paymentDate") or "").strip()
        if not data_pagamento.startswith(data_iso):
            continue

        descricao = str(lancamento.get("description") or "").strip()
        if not re.search(r"REF\.\s*NFCE\b", descricao, re.IGNORECASE):
            continue
        if re.search(r"REF\.\s*NFE\b", descricao, re.IGNORECASE):
            continue
        if re.search(r"\bDINHEIRO\b", descricao, re.IGNORECASE):
            continue

        valor_base = lancamento.get("paidValue")
        if valor_base in (None, ""):
            valor_base = lancamento.get("value")
        valor = round(float(valor_base or 0.0), 2)
        if valor <= 0:
            continue

        data_venda, ordem = _parse_caixa_pix_datetime(data_pagamento)
        if not data_venda:
            data_venda = _iso_to_br_date(data_iso) or data_iso

        itens_autorizados.append(
            {
                "data_venda": data_venda,
                "valor_bruto": valor,
                "_sort": ordem,
            }
        )

    itens_autorizados.sort(key=lambda item: (item.get("_sort") or datetime.min, item.get("data_venda") or ""))
    for item in itens_autorizados:
        item.pop("_sort", None)

    total_autorizado = round(sum(float(item.get("valor_bruto", 0.0)) for item in itens_autorizados), 2)
    data_br = _iso_to_br_date(data_iso) or data_iso
    periodo = f"{data_br} - {data_br}"

    mensagem = None
    if erro:
        mensagem = f"Não foi possível consultar Financeiro > Movimentações no Zweb:\n{erro}"
    elif not itens_autorizados:
        mensagem = "Nenhum pagamento digital de NFC-e foi encontrado em Financeiro > Movimentações para este dia."

    return {
        "arquivo": "Zweb Financeiro > Movimentações",
        "caminho": "",
        "periodo": periodo,
        "quantidade_autorizados": len(itens_autorizados),
        "total_autorizado": total_autorizado,
        "itens_autorizados": itens_autorizados,
        "quantidade_relatorio": len(itens_autorizados),
        "total_relatorio": total_autorizado,
        "consistente": True,
        "origem": "zweb_movimentacoes",
        "mensagem": mensagem,
        "tab_title": "Pagamentos Digitais",
        "menu_text": "Abrir pagamentos digitais",
        "summary_label": "Pagamentos digitais",
        "total_label": "Total pagamentos digitais",
        "section_label": "Transações digitais de NFC-e",
        "empty_message": "Nenhum pagamento digital de NFC-e encontrado para este dia.",
        "table_headers": ("Data da venda", "Valor bruto"),
        "table_mode": "data_valor",
        "categoria": "pagamentos_digitais_nfce",
    }


def _load_minhas_notas_credentials() -> tuple[str, str] | None:
    login = str(MINHAS_NOTAS_LOGIN or "").strip()
    password = str(MINHAS_NOTAS_PASSWORD or "").strip()
    if login and password:
        return login, password

    base_dir = os.path.dirname(os.path.abspath(__file__))
    for filename in ("credenciais.txt", "credencias.txt"):
        caminho = os.path.join(base_dir, filename)
        if not os.path.isfile(caminho):
            continue
        try:
            with open(caminho, "r", encoding="utf-8") as arquivo:
                linhas = [linha.strip() for linha in arquivo.readlines() if linha.strip()]
        except OSError:
            return None
        if len(linhas) >= 2:
            return linhas[0], linhas[1]
    return None


def _authenticate_minhas_notas(login: str, password: str) -> str:
    resposta = requests.post(
        "https://api.clippfacil.com.br/rpc/v2/application.authenticate",
        json={
            "login": login,
            "password": password,
            "isSharedAccess": True,
        },
        headers={
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
        },
        timeout=20,
    )
    resposta.raise_for_status()
    payload = resposta.json() or {}
    token = str(payload.get("access_token") or "").strip()
    if not token:
        raise ValueError("Não foi possível autenticar no Minhas Notas.")
    return token


def _fetch_minhas_notas_nfes(
    access_token: str,
    data_iso: str,
    cache_namespace: str = "",
) -> list[dict]:
    cache_key = ((cache_namespace or access_token[-8:]).lower(), data_iso)
    if cache_key in _MINHAS_NOTAS_CACHE:
        return [dict(item) for item in _MINHAS_NOTAS_CACHE[cache_key]]

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt_BR",
        "Authorization-Compufacil": access_token,
        "Content-Type": "application/json;charset=UTF-8",
    }
    documentos = []
    pagina = 1
    max_results = 100

    while True:
        resposta = requests.post(
            "https://api.clippfacil.com.br/rpc/v1/clipp.get-xml-by-emissor",
            json={
                "page": pagina,
                "maxResults": max_results,
                "model": "55",
                "fromEmission": data_iso,
                "toEmission": data_iso,
            },
            headers=headers,
            timeout=20,
        )
        resposta.raise_for_status()
        payload = resposta.json() or {}
        pagina_docs = payload.get("data") or []
        documentos.extend(pagina_docs)

        total = int(payload.get("total") or 0)
        if not pagina_docs or len(documentos) >= total or len(pagina_docs) < max_results:
            break
        pagina += 1

    normalizados = []
    for item in documentos:
        try:
            valor = round(float(item.get("totalValue", 0.0)), 2)
        except (TypeError, ValueError):
            continue
        normalizados.append(
            {
                "numero": str(item.get("number") or "").strip(),
                "valor": valor,
                "status": int(item.get("status") or 0),
                "tipo": str(item.get("type") or "").strip(),
                "cliente": str(item.get("customerName") or "").strip(),
                "cpf_cnpj": str(item.get("customerIdentification") or "").strip(),
                "emissao": str(item.get("emission") or "").strip(),
            }
        )

    _MINHAS_NOTAS_CACHE[cache_key] = [dict(item) for item in normalizados]
    return [dict(item) for item in normalizados]


def _match_davs_with_minhas_notas_nfes(
    davs_sem_cupom: list[dict],
    periodo: str,
) -> tuple[list[dict], list[dict], str | None]:
    data_iso = _period_to_iso_date(periodo)
    if not data_iso or not davs_sem_cupom:
        return list(davs_sem_cupom), [], None

    credenciais = _load_minhas_notas_credentials()
    if not credenciais:
        return list(davs_sem_cupom), [], None

    try:
        login, password = credenciais
        access_token = _authenticate_minhas_notas(login, password)
        nfes = [
            item
            for item in _fetch_minhas_notas_nfes(access_token, data_iso, cache_namespace=login)
            if item.get("status") == 1 and item.get("tipo") == "1"
        ]
    except Exception as exc:
        return list(davs_sem_cupom), [], str(exc)

    nfes_por_valor = {}
    for item in sorted(
        nfes,
        key=lambda dado: (
            round(float(dado.get("valor", 0.0)), 2),
            dado.get("numero", ""),
        ),
    ):
        valor = round(float(item.get("valor", 0.0)), 2)
        nfes_por_valor.setdefault(valor, []).append(item)

    restantes = []
    encontrados = []
    for item in sorted(davs_sem_cupom, key=lambda dado: dado.get("pedido", "")):
        valor = round(float(item.get("valor", 0.0)), 2)
        candidatos = nfes_por_valor.get(valor) or []
        if not candidatos:
            restantes.append(item)
            continue
        nfe = candidatos.pop(0)
        encontrados.append(
            {
                "pedido": str(item.get("pedido") or "").strip(),
                "valor": valor,
                "numero_nfe": str(nfe.get("numero") or "").strip(),
                "cliente_nfe": str(nfe.get("cliente") or "").strip(),
                "cpf_cnpj_nfe": str(nfe.get("cpf_cnpj") or "").strip(),
                "emissao_nfe": str(nfe.get("emissao") or "").strip(),
            }
        )

    return restantes, encontrados, None


def _find_missing_fiscal_numbers(numbers: list[str]) -> list[str]:
    normalized_numbers = [
        _normalize_fiscal_number(numero)
        for numero in numbers
        if _normalize_fiscal_number(numero)
    ]
    if not normalized_numbers:
        return []

    ints = sorted({int(numero) for numero in normalized_numbers})
    missing = []
    for current, nxt in zip(ints, ints[1:]):
        if nxt - current > 1:
            for numero in range(current + 1, nxt):
                missing.append(_normalize_fiscal_number(numero))
    return missing


def validar_periodo_relatorios_caixa(
    relatorio_caixa: dict,
    relatorio_nfce: dict,
    titulo_secundario: str = "Resumo NFC-e",
) -> tuple[bool, str]:
    inicio_caixa, fim_caixa = _extract_period_range(relatorio_caixa.get("periodo", ""))
    inicio_resumo, fim_resumo = _extract_period_range(relatorio_nfce.get("periodo", ""))

    if not all((inicio_caixa, fim_caixa, inicio_resumo, fim_resumo)):
        return False, "Não foi possível identificar o período dos dois relatórios."
    if inicio_caixa != fim_caixa:
        return False, "O relatorio de pedidos importados precisa ser de um unico dia."
    if inicio_resumo != fim_resumo:
        return False, f"O {titulo_secundario} precisa ser de um unico dia."
    if inicio_caixa != inicio_resumo:
        return False, (
            f"Os relatorios sao de dias diferentes.\n"
            f"Pedidos importados: {inicio_caixa}\n"
            f"{titulo_secundario}: {inicio_resumo}"
        )
    return True, inicio_caixa


def validar_relatorio_pedidos_importados(
    relatorio: dict,
    modelo_esperado: str | None = None,
) -> tuple[bool, str]:
    if relatorio and relatorio.get("pdf_sem_texto"):
        return False, _pdf_sem_texto_message(relatorio.get("arquivo"))

    modelo = (relatorio.get("caixa_modelo") or "").upper()
    modelo_esperado = (modelo_esperado or "").upper()

    if modelo_esperado and modelo and modelo != modelo_esperado:
        if modelo_esperado == "MVA":
            return False, "O arquivo selecionado no passo 1 não parece ser uma Exportação de dados da MVA."
        return False, "O arquivo selecionado no passo 1 não parece ser um relatório de pedidos importados da EH."

    if not relatorio or relatorio.get("pedidos_total", 0) <= 0:
        if modelo_esperado == "MVA":
            return False, "O arquivo selecionado no passo 1 não parece ser uma Exportação de dados da MVA."
        return False, "O arquivo selecionado no passo 1 não parece ser um relatório de pedidos importados."
    if not relatorio.get("periodo"):
        if modelo_esperado == "MVA":
            return True, ""
        return False, "Não foi possível identificar o período no relatório de pedidos importados."
    if relatorio.get("total_documento", 0.0) <= 0:
        if modelo_esperado == "MVA":
            return False, "A Exportação de dados da MVA não trouxe um total válido."
        return False, "O relatório de pedidos importados não trouxe um total válido."
    return True, ""


def validar_relatorio_resumo_nfce(
    relatorio: dict,
    modelo_esperado: str | None = None,
) -> tuple[bool, str]:
    if relatorio and relatorio.get("pdf_sem_texto"):
        return False, _pdf_sem_texto_message(relatorio.get("arquivo"))

    modelo = (relatorio.get("resumo_modelo") or "EH").upper()
    modelo_esperado = (modelo_esperado or "").upper()
    if modelo_esperado and modelo != modelo_esperado:
        if modelo_esperado == "MVA":
            return False, "O arquivo selecionado no último passo não parece ser o relatório de Cupons da MVA."
        return False, "O arquivo selecionado no passo 2 não parece ser um Resumo NFC-e."
    if not relatorio or relatorio.get("quantidade_nfce", 0) <= 0:
        if modelo_esperado == "MVA":
            return False, "O arquivo selecionado no último passo não parece ser o relatório de Cupons da MVA."
        return False, "O arquivo selecionado no passo 2 não parece ser um Resumo NFC-e."
    if not relatorio.get("periodo"):
        if modelo_esperado == "MVA":
            return False, "Não foi possível identificar o período no relatório de Cupons."
        return False, "Não foi possível identificar o período no Resumo NFC-e."
    if relatorio.get("total_nfce", 0.0) <= 0:
        if modelo_esperado == "MVA":
            return False, "O relatório de Cupons não trouxe um total válido."
        return False, "O Resumo NFC-e não trouxe um total válido."
    return True, ""








def _comparar_caixa_resumo_nfce_eh(relatorio_caixa: dict, relatorio_nfce: dict) -> dict:
    itens_caixa = relatorio_caixa.get("itens_caixa", [])
    itens_nfce = relatorio_nfce.get("nfces", [])
    fiscal_status_map = relatorio_nfce.get("fiscal_status_map") or {}
    relatorios_pagamento = dict(relatorio_nfce.get("relatorios_pagamento") or {})
    nf_report = _build_eh_nf_filtered_report(relatorio_caixa)
    if nf_report:
        relatorios_pagamento[nf_report["categoria"]] = nf_report

    nf_filtradas = [
        item
        for item in (relatorio_caixa.get("itens_excluidos") or [])
        if "NOTA FISCAL ELETRONICA" in _normalize_caixa_client(item.get("documento", ""))
    ]
    nf_pool = [dict(item) for item in nf_filtradas]

    fechamento_map = {
        _normalize_fiscal_number(item.get("numero", "")): {
            "numero": _normalize_fiscal_number(item.get("numero", "")),
            "numero_exibicao": item.get("numero_exibicao") or _display_fiscal_number(item.get("numero", "")),
            "valor": round(float(item.get("valor", 0.0)), 2),
            "descricao": item.get("descricao", ""),
        }
        for item in itens_nfce
        if _normalize_fiscal_number(item.get("numero", ""))
    }

    registros_map: dict[tuple[str, str, str], dict] = {}
    alert_rows: list[tuple[str, str, str]] = []
    alert_seen: set[tuple[str, str, str]] = set()
    pix_fechamento_only: list[dict] = []
    pix_machine_only: list[dict] = []
    card_fechamento_only: list[dict] = []
    card_machine_only: list[dict] = []

    def _money_text(value: float | None) -> str:
        return "-" if value in (None, "") else f"R$ {format_number_br(value)}"

    def _add_alert(tipo: str, detalhe: str, valor: str) -> None:
        key = (tipo, detalhe, valor)
        if key in alert_seen:
            return
        alert_seen.add(key)
        alert_rows.append(key)

    def _add_registro(numero: str, numero_exibicao: str, valor: float | None, origem: str, observacao: str) -> None:
        numero_normalizado = _normalize_fiscal_number(numero)
        key = (numero_normalizado, origem, observacao)
        valor_normalizado = None if valor in (None, "") else round(float(valor), 2)
        entry = registros_map.get(key)
        if entry is None:
            registros_map[key] = {
                "numero": numero_normalizado,
                "numero_exibicao": numero_exibicao or _display_fiscal_number(numero_normalizado),
                "origem": origem,
                "observacao": observacao,
                "valor": valor_normalizado,
            }
            return
        if valor_normalizado is not None:
            entry["valor"] = round(float(entry.get("valor") or 0.0) + valor_normalizado, 2)

    pedidos_cancelados = []
    numeros_pedidos_conferidos = set()
    numeros_pedidos_pendentes = set()

    for item in itens_caixa:
        numero = _normalize_fiscal_number(item.get("pedido", ""))
        if not numero:
            continue
        valor = round(float(item.get("valor", 0.0)), 2)
        fechamento_item = fechamento_map.get(numero)
        if fechamento_item and abs(float(fechamento_item.get("valor", 0.0)) - valor) < 0.01:
            numeros_pedidos_conferidos.add(numero)
            continue

        if fechamento_item:
            motivo = f"Valor divergente no Fechamento de caixa ({_money_text(fechamento_item.get('valor'))})"
        else:
            motivo = "CF não encontrado no Fechamento de caixa"

        pendencia = {
            "numero": numero,
            "numero_exibicao": _display_fiscal_number(numero),
            "valor": valor,
            "motivo": motivo,
        }
        if (fiscal_status_map.get(numero) or {}).get("cancelada"):
            pedidos_cancelados.append(pendencia)
            _add_alert("Cupom cancelado", f"CF {pendencia['numero_exibicao']}: {motivo}", _money_text(valor))
        else:
            numeros_pedidos_pendentes.add(numero)
            _add_registro(numero, pendencia["numero_exibicao"], valor, "Pedido", motivo)
            _add_alert("Pedido pendente", f"CF {pendencia['numero_exibicao']}: {motivo}", _money_text(valor))

    dinheiro_report = relatorios_pagamento.get("dinheiro") or {}
    for item in dinheiro_report.get("itens_autorizados") or []:
        numero = _normalize_fiscal_number(item.get("numero", ""))
        if not numero or numero in numeros_pedidos_conferidos or numero in numeros_pedidos_pendentes:
            continue
        if (fiscal_status_map.get(numero) or {}).get("cancelada"):
            continue
        valor = round(float(item.get("valor_bruto", 0.0)), 2)
        _add_registro(
            numero,
            item.get("numero_exibicao") or _display_fiscal_number(numero),
            valor,
            "Fechamento",
            "Dinheiro sem pedido correspondente",
        )
        _add_alert(
            "CF sem pedido",
            f"Dinheiro: CF {item.get('numero_exibicao') or _display_fiscal_number(numero)}",
            _money_text(valor),
        )

    comparacoes = [
        (
            "PIX CAIXA",
            relatorios_pagamento.get("pix_caixa") or relatorios_pagamento.get("pagamentos_digitais_nfce"),
            relatorios_pagamento.get("pix_fechamento"),
            "valor_bruto",
        ),
        (
            "Cartão de Crédito CAIXA",
            relatorios_pagamento.get("cartao_credito_caixa"),
            relatorios_pagamento.get("cartao_credito"),
            "valor_bruto",
        ),
        (
            "Cartão de Débito CAIXA",
            relatorios_pagamento.get("cartao_debito_caixa"),
            relatorios_pagamento.get("cartao_debito"),
            "valor_bruto",
        ),
    ]
    comparacoes_cartao = {"Cartão de Crédito CAIXA", "Cartão de Débito CAIXA"}


    for titulo_pagamento, report_externo, report_fechamento, campo_valor in comparacoes:
        if not report_fechamento:
            continue
        itens_fechamento = list(report_fechamento.get("itens_autorizados") or [])

        if not report_externo:
            _add_alert("Relatório ausente", f"{titulo_pagamento}: relatório local não encontrado na pasta raiz.", "-")
            continue
            alert_rows.append(("Relatório ausente", f"{titulo_pagamento}: relatório local não encontrado na pasta raiz.", "-"))
            for item in itens_fechamento:
                numero = _normalize_fiscal_number(item.get("numero", ""))
                if numero and numero not in numeros_pedidos_pendentes:
                    _add_registro(
                        numero,
                        item.get("numero_exibicao") or _display_fiscal_number(numero),
                        item.get("valor_bruto"),
                        "Fechamento",
                        f"{titulo_pagamento} sem relatório local correspondente",
                    )
                    fechamento_only_total += round(float(item.get("valor_bruto", 0.0)), 2)
            continue

        itens_externos = list(report_externo.get("itens_autorizados") or [])
        _matched, externos_sem_fechamento, fechamento_sem_externo = _multiset_match_by_value(
            itens_externos,
            itens_fechamento,
            campo_esquerda=campo_valor,
            campo_direita="valor_bruto",
        )
        _matched_nf, externos_restantes, nf_pool = _consume_matches_against_nf(
            externos_sem_fechamento,
            nf_pool,
            campo_externo=campo_valor,
        )

        for item in externos_restantes:
            valor = round(float(item.get(campo_valor, 0.0)), 2)
            detail = item.get("data_venda") or item.get("numero_exibicao") or item.get("numero") or "-"
            if titulo_pagamento == "PIX CAIXA":
                pix_machine_only.append(
                    {
                        "titulo": titulo_pagamento,
                        "data_venda": detail,
                        "valor": valor,
                    }
                )
            if titulo_pagamento in comparacoes_cartao:
                card_machine_only.append(
                    {
                        "titulo": titulo_pagamento,
                        "data_venda": detail,
                        "valor": valor,
                    }
                )
            alert_rows.append(
                (
                    "Transação Bancária sem CF/NF",
                    f"{titulo_pagamento}: {detail}",
                    _money_text(valor),
                )
            )

        for item in fechamento_sem_externo:
            numero = _normalize_fiscal_number(item.get("numero", ""))
            if not numero or numero in numeros_pedidos_pendentes:
                continue
            if (fiscal_status_map.get(numero) or {}).get("cancelada"):
                continue
            valor = round(float(item.get("valor_bruto", 0.0)), 2)
            _add_registro(
                numero,
                item.get("numero_exibicao") or _display_fiscal_number(numero),
                valor,
                "Fechamento",
                f"{titulo_pagamento} sem pagamento correspondente na máquina",
            )
            if titulo_pagamento == "PIX CAIXA":
                pix_fechamento_only.append(
                    {
                        "titulo": titulo_pagamento,
                        "numero_exibicao": item.get("numero_exibicao") or _display_fiscal_number(numero),
                        "valor": valor,
                    }
                )
            if titulo_pagamento in comparacoes_cartao:
                card_fechamento_only.append(
                    {
                        "titulo": titulo_pagamento,
                        "numero_exibicao": item.get("numero_exibicao") or _display_fiscal_number(numero),
                        "valor": valor,
                    }
                )
            alert_rows.append(
                (
                    "CF sem Transação Bancária",
                    f"{titulo_pagamento}: CF {item.get('numero_exibicao') or _display_fiscal_number(numero)}",
                    _money_text(valor),
                )
            )

    pix_caixa_report = relatorios_pagamento.get("pix_caixa")
    pix_fechamento_report = relatorios_pagamento.get("pix_fechamento")
    if pix_caixa_report and pix_fechamento_report:
        pix_cancelados = [
            item
            for item in (pix_caixa_report.get("itens_todos") or [])
            if item.get("tipo_pix") == "RECEBIDO" and "CANCEL" in _normalize_ascii_text(item.get("situacao", ""))
        ]
        pix_efetivados = list(pix_caixa_report.get("itens_autorizados") or [])
        valores_fechamento_pix = [round(float(item.get("valor_bruto", 0.0)), 2) for item in (pix_fechamento_report.get("itens_autorizados") or [])]
        for item in pix_cancelados:
            valor = round(float(item.get("valor_bruto", 0.0)), 2)
            if valor not in valores_fechamento_pix:
                continue
            ordem_cancelada = str(item.get("ordem") or "")
            tem_subsequente = any(
                round(float(outro.get("valor_bruto", 0.0)), 2) == valor
                and str(outro.get("ordem") or "") > ordem_cancelada
                for outro in pix_efetivados
            )
            if not tem_subsequente:
                alert_rows.append(
                    (
                        "PIX cancelado",
                        f"Cancelar cupom: PIX cancelado em {item.get('data_venda')} permaneceu no Fechamento",
                        _money_text(valor),
                    )
                )

    registros = list(registros_map.values())
    valor_canceladas_ignoradas = round(sum(item.get("valor", 0.0) for item in pedidos_cancelados), 2)
    valor_faltantes = round(
        sum(float(item.get("valor", 0.0)) for item in registros if item.get("valor") not in (None, "")),
        2,
    )
    total_caixa = round(float(relatorio_caixa.get("total_caixa", 0.0)), 2)
    total_resumo = round(float(relatorio_nfce.get("total_nfce", 0.0)), 2)

    alertas_report = _build_eh_alerts_report(
        relatorio_caixa.get("periodo"),
        alert_rows,
        pix_fechamento_rows=[
            (f"CF {item.get('numero_exibicao') or '-'}", f"R$ {format_number_br(item.get('valor', 0.0))}")
            for item in pix_fechamento_only
        ],
        pix_maquina_rows=[
            (str(item.get("data_venda") or "-"), f"R$ {format_number_br(item.get('valor', 0.0))}")
            for item in pix_machine_only
        ],
        cartao_fechamento_rows=[
            (
                f"{('Crédito' if 'CREDITO' in _normalize_caixa_client(item.get('titulo', '')) else 'Débito')}: CF {item.get('numero_exibicao') or '-'}",
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
            for item in card_fechamento_only
        ],
        cartao_maquina_rows=[
            (
                f"{('Crédito' if 'CREDITO' in _normalize_caixa_client(item.get('titulo', '')) else 'Débito')}: {item.get('data_venda') or '-'}",
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
            for item in card_machine_only
        ],
    )
    if alertas_report:
        relatorios_pagamento[alertas_report["categoria"]] = alertas_report
        for key in (
            "pix_caixa",
            "pix_fechamento",
            "cartao_credito",
            "cartao_debito",
            "cartao_credito_caixa",
            "cartao_debito_caixa",
        ):
            if relatorios_pagamento.get(key):
                relatorios_pagamento[key]["hidden_in_menu"] = True

    status = "Confere"
    if registros or any(row for row in alert_rows if row[0] not in {"Cupom cancelado"}):
        status = "Faltante"

    periodo_unico, _ = _extract_period_range(relatorio_caixa.get("periodo", ""))

    return {
        "caixa_modelo": "EH",
        "arquivo_caixa": relatorio_caixa.get("arquivo"),
        "arquivo_caixa_titulo": relatorio_caixa.get("arquivo_caixa_titulo") or "Arquivo Pedidos",
        "arquivo_resumo": relatorio_nfce.get("arquivo"),
        "arquivo_resumo_titulo": relatorio_nfce.get("arquivo_resumo_titulo"),
        "subtitle": relatorio_nfce.get("subtitle"),
        "periodo": periodo_unico or relatorio_caixa.get("periodo"),
        "total_caixa": total_caixa,
        "total_caixa_titulo": relatorio_caixa.get("total_caixa_titulo") or "Total Pedidos Caixa",
        "total_resumo_nfce": total_resumo,
        "total_resumo_titulo": relatorio_nfce.get("total_resumo_titulo"),
        "nfces_faltantes_count": len(registros),
        "valor_faltantes": valor_faltantes,
        "status": status,
        "canceladas_ignoradas_count": len(pedidos_cancelados),
        "canceladas_ignoradas_valor": valor_canceladas_ignoradas,
        "avisos_usuario": list(relatorio_nfce.get("avisos_usuario") or []),
        "relatorios_pagamento": relatorios_pagamento,
        "alertas_count": len(alert_rows),
        "registros_conferencia": sorted(
            registros,
            key=lambda item: (
                int(item["numero"]) if str(item.get("numero") or "").isdigit() else 0,
                item["origem"],
                item.get("observacao", ""),
            ),
        ),
    }


def _is_mva_cupom_client(cliente: str) -> bool:
    return _normalize_caixa_client(cliente) in {"CLIENTE BALCAO", "CLIENTES DIVERSOS"}


def _infer_mva_davs_sem_cupom(itens_caixa: list[dict], itens_nfce: list[dict]) -> list[dict]:
    elegiveis = [
        item
        for item in itens_caixa
        if _is_mva_cupom_client(item.get("cliente", ""))
    ]
    if not elegiveis or not itens_nfce:
        return sorted(elegiveis, key=lambda item: item.get("pedido", ""))

    davs_sorted = sorted(
        elegiveis,
        key=lambda item: (round(float(item.get("valor", 0.0)), 2), item.get("pedido", "")),
    )
    nfce_sorted = sorted(
        itens_nfce,
        key=lambda item: (round(float(item.get("valor", 0.0)), 2), item.get("numero", "")),
    )

    n = len(davs_sorted)
    m = len(nfce_sorted)

    def _melhor_estado(candidatos):
        prioridade = {"match": 0, "skip_nfce": 1, "skip_dav": 2}
        return min(
            candidatos,
            key=lambda candidato: (
                candidato[0][0],
                candidato[0][1],
                prioridade[candidato[1]],
            ),
        )

    dp = [[(0, 0.0)] * (m + 1) for _ in range(n + 1)]
    caminho = [[""] * (m + 1) for _ in range(n + 1)]

    for i in range(1, n + 1):
        dp[i][0] = (i, 0.0)
        caminho[i][0] = "skip_dav"
    for j in range(1, m + 1):
        dp[0][j] = (0, 0.0)
        caminho[0][j] = "skip_nfce"

    for i in range(1, n + 1):
        for j in range(1, m + 1):
            valor_dav = round(float(davs_sorted[i - 1].get("valor", 0.0)), 2)
            valor_nfce = round(float(nfce_sorted[j - 1].get("valor", 0.0)), 2)
            candidatos = [
                ((dp[i - 1][j][0] + 1, dp[i - 1][j][1]), "skip_dav"),
                (dp[i][j - 1], "skip_nfce"),
                ((dp[i - 1][j - 1][0], dp[i - 1][j - 1][1] + abs(valor_dav - valor_nfce)), "match"),
            ]
            melhor_estado, decisao = _melhor_estado(candidatos)
            dp[i][j] = melhor_estado
            caminho[i][j] = decisao

    faltantes = []
    i = n
    j = m
    while i > 0 or j > 0:
        decisao = caminho[i][j] if i >= 0 and j >= 0 else ""
        if decisao == "match":
            i -= 1
            j -= 1
        elif decisao == "skip_nfce":
            j -= 1
        else:
            if i > 0:
                faltantes.append(davs_sorted[i - 1])
            i -= 1

    return sorted(faltantes, key=lambda item: item.get("pedido", ""))


def _comparar_caixa_resumo_nfce_mva(relatorio_caixa: dict, relatorio_nfce: dict) -> dict:
    itens_caixa = relatorio_caixa.get("itens_caixa", [])
    itens_nfce = relatorio_nfce.get("nfces", [])
    itens_cupom_base = [
        item
        for item in itens_caixa
        if _is_mva_cupom_client(item.get("cliente", ""))
    ]

    davs_sem_cupom = _infer_mva_davs_sem_cupom(itens_caixa, itens_nfce)
    davs_sem_cupom, nfes_identificadas, erro_minhas_notas = _match_davs_with_minhas_notas_nfes(
        davs_sem_cupom,
        relatorio_caixa.get("periodo", ""),
    )
    cfs_faltantes = sorted(
        {
            _normalize_fiscal_number(numero)
            for numero in relatorio_nfce.get("nfces_faltantes_sequencia", [])
            if _normalize_fiscal_number(numero)
        },
        key=int,
    )

    registros = []
    for item in davs_sem_cupom:
        pedido = re.sub(r"\D", "", str(item.get("pedido", "")))
        registros.append(
            {
                "numero": pedido,
                "numero_exibicao": f"DAV {_display_fiscal_number(pedido) if pedido else item.get('pedido', '-')}",
                "origem": "DAV",
                "observacao": "DAV sem cupom",
                "valor": round(float(item.get("valor", 0.0)), 2),
            }
        )
    for numero in cfs_faltantes:
        registros.append(
            {
                "numero": numero,
                "numero_exibicao": f"CF {_display_fiscal_number(numero)}",
                "origem": "CF",
                "observacao": "Cupom faltante na sequencia",
                "valor": None,
            }
        )

    total_caixa = round(sum(float(item.get("valor", 0.0)) for item in itens_cupom_base), 2)
    total_resumo = round(float(relatorio_nfce.get("total_nfce", 0.0)), 2)
    if nfes_identificadas:
        valor_faltantes = round(
            sum(float(item.get("valor", 0.0)) for item in davs_sem_cupom),
            2,
        )
    else:
        valor_faltantes = round(total_caixa - total_resumo, 2)
    status = "Confere" if abs(valor_faltantes) < 0.01 else "Faltante"
    periodo_unico, _ = _extract_period_range(relatorio_caixa.get("periodo", ""))
    subtitle = (
        "Compara os DAVs aptos para cupom com o relatório de Cupons e aponta DAVs/CF para conferência."
    )
    if nfes_identificadas:
        subtitle += (
            f" NF-e identificadas automaticamente no Minhas Notas: {len(nfes_identificadas)}."
        )
    elif erro_minhas_notas:
        subtitle += " Consulta ao Minhas Notas indisponivel nesta analise."

    return {
        "fechamento_modelo": "MVA",
        "caixa_modelo": "MVA",
        "subtitle": subtitle,
        "arquivo_caixa": relatorio_caixa.get("arquivo"),
        "arquivo_resumo": relatorio_nfce.get("arquivo"),
        "arquivo_resumo_titulo": "Arquivo Cupons",
        "periodo": periodo_unico or relatorio_caixa.get("periodo"),
        "total_caixa": total_caixa,
        "total_caixa_titulo": "Total DAVs para cupom",
        "total_resumo_nfce": total_resumo,
        "total_resumo_titulo": "Total Cupons",
        "nfces_faltantes_count": len(registros),
        "faltantes_titulo": "DAVs/CF faltantes",
        "valor_faltantes": valor_faltantes,
        "nfes_identificadas_count": len(nfes_identificadas),
        "nfes_identificadas_valor": round(
            sum(float(item.get("valor", 0.0)) for item in nfes_identificadas),
            2,
        ),
        "status": status,
        "secao_titulo": "DAVs/CF para conferência",
        "empty_message": "Nenhum DAV/CF faltante encontrado.",
        "registros_conferencia": sorted(
            registros,
            key=lambda item: (
                0 if item.get("origem") == "DAV" else 1,
                int(item["numero"]) if item.get("numero") else 0,
            ),
        ),
        "nfes_identificadas": nfes_identificadas,
        "erro_minhas_notas": erro_minhas_notas,
    }


def comparar_caixa_resumo_nfce(relatorio_caixa: dict, relatorio_nfce: dict) -> dict:
    if (relatorio_caixa.get("caixa_modelo") or "").upper() == "MVA" or (
        relatorio_nfce.get("resumo_modelo") or ""
    ).upper() == "MVA":
        return _comparar_caixa_resumo_nfce_mva(relatorio_caixa, relatorio_nfce)
    return _comparar_caixa_resumo_nfce_eh(relatorio_caixa, relatorio_nfce)

def canonicalize_name(raw: str) -> str:
    _ensure_mapping_loaded()
    key = _normalize_key(raw)

    # 1) se existe como abreviaÃ§Ã£o no mapping
    if key in mapping:
        return mapping[key]

    # 2) se jÃ¡ Ã© o nome completo
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

# --- FunÃ§Ãµes principais ---

def criar_etiquetas_legacy():
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from global_vars import results_by_source

    # precisa ter MVA e EH carregados
    if not results_by_source["MVA"] or not results_by_source["EH"]:
        messagebox.showwarning("Aviso", "Ã‰ necessÃ¡rio carregar os dois PDFs (MVA e EH).")
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

        if y < 50:  # quebra pÃ¡gina
            c.showPage()
            c.setFont("Helvetica", 11)
            y = altura - 50

    c.save()
    messagebox.showinfo("Sucesso", f"âœ… Etiquetas geradas em:\n{caminho}")

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

    # pega cabeÃ§alho da linha 2
    colsMVA = valoresMVA[1]
    colsEH = valoresEH[1]

    # corrige duplicados
    colsMVA = [f"col{i}_{c}" if colsMVA.count(c) > 1 else c for i, c in enumerate(colsMVA)]
    colsEH = [f"col{i}_{c}" if colsEH.count(c) > 1 else c for i, c in enumerate(colsEH)]

    dfMVA = pd.DataFrame(valoresMVA[2:], columns=colsMVA)
    dfEH = pd.DataFrame(valoresEH[2:], columns=colsEH)

    # ðŸ”Ž COMPARAÃ‡ÃƒO com os Ãºltimos dados exportados (mantÃ©m compatibilidade)
    if LAST_MVA is not None and LAST_EH is not None:
        try:
            if dfMVA.equals(LAST_MVA) and dfEH.equals(LAST_EH):
                return None
        except Exception:
            # se ocorrer qualquer erro de comparaÃ§Ã£o, continua (nÃ£o bloqueia)
            pass

    # Atualiza os globais com os novos dados (mantÃ©m comportamento anterior)
    LAST_MVA, LAST_EH = dfMVA.copy(), dfEH.copy()

    # --- AgregaÃ§Ã£o por vendedor (soma MVA + EH) ---
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

        # percorre o resto das colunas da linha somando valores numÃ©ricos
        for v in row[1:]:
            if pd.isna(v) or str(v).strip() == "":
                continue
            try:
                num = parse_number(str(v))
                total_row += num
                atend_row += 1
            except Exception:
                # ignora conteÃºdos nÃ£o numÃ©ricos
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

                    # agora extrai tambÃ©m o DataFrame agregado
                    dfMVA, dfEH, df_agg = resultado

                    total_rows = len(df_agg)
                    resultados = []

                    # percorre o df_agg (jÃ¡ somado por vendedor)
                    for i, row in enumerate(df_agg.itertuples(index=False, name=None), start=1):
                        # ðŸ”¹ Verifica se foi cancelado
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

                        # ðŸ”¹ Atualiza progresso gradualmente
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
                            messagebox.showinfo("Cancelado", "âŒ Carregamento da planilha foi cancelado.")
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
                            messagebox.showinfo("Sucesso", "âœ… Planilha online carregada com sucesso!")
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
                            messagebox.showinfo("Cancelado", "âŒ Carregamento da planilha foi cancelado.")
                        else:
                            messagebox.showinfo("Sucesso", "âœ… Planilhas online carregadas com sucesso!")
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
    cache_canon = {}  # ðŸ”¹ Cache para memoizaÃ§Ã£o de canonicalize_name

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

    # ðŸ”¹ Recalcula clientes finais uma vez ao final
    for dados in mesclado.values():
        dados["total_clientes"] = dados["atendidos"] - dados["devolucoes"]

    return mesclado


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
                                messagebox.showerror("Erro", "Não foi possível localizar o executável na atualização.")
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

    # reseta variÃ¡veis da UI
    label_files_var.set("Nenhum arquivo carregado ainda")
    progress_var.set(0)

    # ðŸ§¹ Limpa histÃ³rico da mesclagem
    LAST_EH = None      
    LAST_MVA = None
    LAST_HASH_MERGE = None
    LAST_STATE_SPREADSHEET = {}    
    
    # tambÃ©m limpa lista de resultados
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
        messagebox.showwarning("Aviso", "NÃ£o hÃ¡ dados para exportar.")
        return

    df = pd.DataFrame(dados, columns=cols)

    # Converter colunas numÃ©ricas
    colunas_numericas = ["Atendidos", "DevoluÃ§Ãµes", "Total Final", "Total Vendas"]
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
        title="Salvar relatÃ³rio"
    )
    
    if not caminho:
        return False
    
    df.to_excel(caminho, index=False, engine="openpyxl")
    
    messagebox.showinfo("Sucesso", f"âœ… RelatÃ³rio exportado para:\n{caminho}")

def _pdf_export(tree) -> bool:

    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    
    # Extrai os dados
    cols = [tree.heading(col)["text"] for col in tree["columns"]]
    dados = [tree.item(item)["values"] for item in tree.get_children()]

    if not dados:
        messagebox.showwarning("Aviso", "NÃ£o hÃ¡ dados para exportar.")
        return

    caminho = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("Arquivo PDF", "*.pdf")],
        title="Salvar relatÃ³rio PDF"
    )
    if not caminho:
        return False

    # Criar PDF simples
    c = canvas.Canvas(caminho, pagesize=A4)
    largura, altura = A4
    y = altura - 50
    c.setFont("Helvetica-Bold", 12)
    c.drawString(50, y, "RelatÃ³rio de Vendas")
    y -= 30

    # CabeÃ§alho
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
    messagebox.showinfo("Sucesso", f"âœ… RelatÃ³rio exportado para:\n{caminho}")

def limpar_tabelas_duplas(tree, tree_mva, tree_eh, label_files_var, progress_var):
    """Limpa todas as tabelas (PDF + MVA + EH) e reseta os indicadores."""
    global LAST_EH, LAST_MVA, LAST_STATE_SPREADSHEET, LAST_HASH_MERGE


    confirm = messagebox.askyesno("ConfirmaÃ§Ã£o", "Deseja realmente limpar todas as tabelas?")
    if not confirm:
        return

    for t in (tree, tree_mva, tree_eh):
        for item in t.get_children():
            t.delete(item)
            
        # ðŸ§¹ Limpa histÃ³rico da mesclagem
    LAST_EH = None      
    LAST_MVA = None
    LAST_HASH_MERGE = None
    LAST_STATE_SPREADSHEET = {}    
    
    # tambÃ©m limpa lista de resultados
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
    messagebox.showinfo("Limpeza concluÃ­da", "ðŸ§¹ Todas as tabelas foram limpas com sucesso.")

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

    # ðŸ”¹ Verifica se alguma tabela estÃ¡ vazia
    if not tree.get_children():
        messagebox.showwarning("Aviso", "A tabela de PDFs estÃ¡ vazia. Importe pelo menos um PDF antes de mesclar.")
        return
    if not tree_mva.get_children() and not tree_eh.get_children():
        messagebox.showwarning("Aviso", "As tabelas online estÃ£o vazias. Carregue as planilhas MVA e EH antes de mesclar.")
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
        messagebox.showinfo("Aviso", "âš ï¸ Esses dados jÃ¡ foram mesclados. Nenhuma alteraÃ§Ã£o detectada.")
        return

    LAST_HASH_MERGE = novo_hash
    merge_queue = queue.Queue()

    # ------------------ THREAD WORKER ------------------
    def worker():
        try:
            # 1ï¸âƒ£ Extrai dados da tabela de PDFs
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

            # 2ï¸âƒ£ Extrai dados das planilhas MVA e EH
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

            # 3ï¸âƒ£ Soma os dois (MVA + EH)
            dados_planilha_total = {}
            for vendedor in set(dados_mva.keys()) | set(dados_eh.keys()):
                m = dados_mva.get(vendedor, {"atendidos": 0, "total_vendas": 0.0})
                e = dados_eh.get(vendedor, {"atendidos": 0, "total_vendas": 0.0})
                dados_planilha_total[vendedor] = {
                    "atendidos": m["atendidos"] + e["atendidos"],
                    "total_vendas": m["total_vendas"] + e["total_vendas"]
                }

            # 4ï¸âƒ£ Aplica controle de duplicaÃ§Ã£o incremental (igual ao cÃ³digo original)
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

            # 5ï¸âƒ£ Mescla tudo
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
                    messagebox.showinfo("ConcluÃ­do", "âœ… Mesclagem das tabelas (PDF + MVA + EH) finalizada!")
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
    Analisa as datas de vendas em um PDF e retorna o perÃ­odo de vendas.
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
        print(f"Erro ao analisar perÃ­odo de vendas: {e}")
        SALES_PERIOD = None
        return None

# ----------------- ConexÃ£o com Supabase -----------------

from api_client import (
    get_supabase,
    listar_vendedores_db,
    registrar_vendedor_db,
    excluir_ultimo_feedback,
    atualizar_ultimo_feedback,
    salvar_feedback_db,
    carregar_feedbacks_db
)


# ImportaÃ§Ãµes delegadas ao pdf_parser
from pdf_parser import (_get_pd, _get_pdfplumber, _inspect_pdf_text_layer, _pdf_sem_texto_message, _analisar_pdf_caixa_eh, _analisar_pdf_caixa_mva, analisar_pdf_caixa, _analisar_pdf_resumo_nfce_eh, _analisar_pdf_resumo_nfce_mva, analisar_pdf_resumo_nfce, source_pdf_async, adicionar_pdf, processar_pdf_sem_ui)
