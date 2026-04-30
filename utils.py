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
import base64
import threading
import subprocess
import queue
import difflib
import unicodedata
from urllib.parse import urljoin
from datetime import datetime
from pathlib import Path

import requests

try:
    from ftfy import fix_text as _ftfy_fix_text
except Exception:
    _ftfy_fix_text = None

from ui_dialogs import filedialog, messagebox
from global_vars import (
    listFiles, list_results, regex_data, regex_negative, 
    APP_VERSION, GITHUB_REPO, LAST_EH, LAST_MVA , LAST_HASH_MERGE,
    SALES_PERIOD, MINHAS_NOTAS_LOGIN, MINHAS_NOTAS_PASSWORD,
    ZWEB_USERNAME, ZWEB_PASSWORD, ZWEB_BASE_URL,
    GMAIL_OAUTH_CLIENT_ID, GMAIL_OAUTH_CLIENT_SECRET,
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

_MOJIBAKE_MARKERS = ("\u00c3", "\u00c2", "\u00e2", "\ud83d")
_TEXT_FALLBACK_REPLACEMENTS = {
    "Relat?rio": "Relatório",
    "relat?rio": "relatório",
    "N?o": "Não",
    "n?o": "não",
    "m?quina": "máquina",
    "M?quina": "Máquina",
    "Per?odo": "Período",
    "Pend?ncias": "Pendências",
    "Transa??o": "Transação",
    "Transa??es": "Transações",
    "transa??o": "transação",
    "transa??es": "transações",
    "Cart?o": "Cartão",
    "cart?o": "cartão",
    "Cr?dito": "Crédito",
    "cr?dito": "crédito",
    "D?bito": "Débito",
    "d?bito": "débito",
    "Eletr?nica": "Eletrônica",
    "eletr?nica": "eletrônica",
    "Impress?o": "Impressão",
    "impress?o": "impressão",
    "Confirma??o": "Confirmação",
    "conclu?da": "concluída",
    "selec??o": "seleção",
}


def corrigir_texto(texto: str) -> str:
    """Corrige mojibake comum em textos PT-BR usando ftfy e fallback manual."""
    if not texto or not isinstance(texto, str):
        return texto

    corrigido = texto
    if _ftfy_fix_text is not None:
        try:
            corrigido = _ftfy_fix_text(corrigido)
        except Exception:
            pass

    if any(marker in corrigido for marker in _MOJIBAKE_MARKERS):
        for _ in range(2):
            try:
                candidato = corrigido.encode("latin1").decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                break
            if candidato == corrigido:
                break
            corrigido = candidato
            if _ftfy_fix_text is not None:
                try:
                    corrigido = _ftfy_fix_text(corrigido)
                except Exception:
                    pass

    for origem, destino in _TEXT_FALLBACK_REPLACEMENTS.items():
        corrigido = corrigido.replace(origem, destino)

    return corrigido


def corrigir_estrutura_texto(valor):
    if isinstance(valor, str):
        return corrigir_texto(valor)
    if isinstance(valor, list):
        return [corrigir_estrutura_texto(item) for item in valor]
    if isinstance(valor, tuple):
        return tuple(corrigir_estrutura_texto(item) for item in valor)
    if isinstance(valor, dict):
        return {
            corrigir_estrutura_texto(chave) if isinstance(chave, str) else chave: corrigir_estrutura_texto(item)
            for chave, item in valor.items()
        }
    return valor


# Compatibilidade com chamadas legadas.
globals()["corrigirCodifica\u00e7\u00e3o"] = corrigir_texto
globals()[corrigir_texto("corrigirCodifica????o")] = corrigir_texto


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

def _project_base_dir() -> str:
    import sys

    candidates = [
        Path(r"D:\pdfReader"),
    ]
    if getattr(sys, "frozen", False):
        candidates.append(Path(os.path.dirname(os.path.abspath(sys.executable))))
    candidates.append(Path(os.path.dirname(os.path.abspath(__file__))))

    for candidate in candidates:
        if candidate.is_dir():
            return str(candidate)
    return str(candidates[-1])


def resource_path(relative_path): 
    import sys

    """Retorna o caminho absoluto do recurso, compatÃ­vel com PyInstaller."""
    preferred_base = _project_base_dir()
    preferred_path = os.path.join(preferred_base, relative_path)
    if os.path.exists(preferred_path):
        return preferred_path
    if hasattr(sys, '_MEIPASS'):  # Executando empacotado
        bundled_path = os.path.join(sys._MEIPASS, relative_path)
        if os.path.exists(bundled_path):
            return bundled_path
    return preferred_path

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
    """Formata número no padrão brasileiro com duas casas decimais."""
    return f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def normalizarClienteCaixa(nome: str, mantemAcentos: bool = False) -> str:
    """
    Normaliza o nome do cliente para comparações, opcionalmente mantendo acentuações.
    """
    import unicodedata

    if not nome:
        return ""

    normalizado = unicodedata.normalize("NFKD", nome)
    if not mantemAcentos:
        normalizado = "".join(ch for ch in normalizado if not unicodedata.combining(ch))

    return normalizado.strip().upper()

# Alias para compatibilidade legada
def _normalize_caixa_client(name: str) -> str:
    return normalizarClienteCaixa(name, mantemAcentos=False)



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
    for raw in [_active_report_dir()]:
        path = Path(str(raw or "")).expanduser()
        key = str(path).casefold()
        if not path.exists() or key in seen:
            continue
        seen.add(key)
        dirs.append(path)
    return dirs


def _active_report_dir() -> str:
    import sys

    if getattr(sys, "frozen", False):
        base_dir = os.path.dirname(os.path.abspath(sys.executable))
    else:
        base_dir = os.getcwd()
    if base_dir and os.path.isdir(base_dir):
        return base_dir
    return _runtime_user_dir()


def _save_zweb_html_report(data_br: str, report_key: str, html_text: str) -> str:
    report_dir = Path(_active_report_dir())
    report_dir.mkdir(parents=True, exist_ok=True)
    safe_date = str(data_br or "").replace("/", "-").strip() or "sem-data"
    filename_map = {
        "pedidos_importados": f"Pedidos_importados_{safe_date}_eh_zweb_auto.html",
        "fechamento_caixa": f"Fechamento_de_caixa_{safe_date}_eh_zweb_auto.html",
    }
    filename = filename_map.get(report_key) or f"Relatorio_Zweb_{safe_date}_{report_key}_auto.html"
    target = report_dir / filename
    target.write_text(str(html_text or ""), encoding="utf-8")
    return str(target)


_LOCAL_REPORT_SUFFIXES = (".csv", ".xlsx", ".pdf")


def _effective_local_report_suffix(path_like: str | Path) -> str:
    path = Path(path_like)
    suffix = path.suffix.lower()
    if suffix == ".crdownload":
        nested_suffix = Path(path.stem).suffix.lower()
        if nested_suffix in _LOCAL_REPORT_SUFFIXES:
            return nested_suffix
    return suffix


def _finalize_local_report_path(path_like: str | Path) -> str:
    path = Path(path_like)
    if path.suffix.lower() != ".crdownload":
        return str(path)
    if _effective_local_report_suffix(path) not in _LOCAL_REPORT_SUFFIXES:
        return str(path)
    target = path.with_suffix("")
    try:
        path.replace(target)
        return str(target)
    except Exception:
        if target.exists():
            return str(target)
        return str(path)


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


def _read_xlsx_rows_fallback(path: str | Path) -> list[tuple[str, list[list[str]]]]:
    import posixpath
    import xml.etree.ElementTree as ET
    from zipfile import ZipFile

    ns_main = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rel_ns = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    ns_pkg = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}

    def _xlsx_col_index(ref: str) -> int:
        letters = re.match(r"([A-Z]+)", str(ref or "").upper())
        if not letters:
            return 0
        value = 0
        for ch in letters.group(1):
            value = (value * 26) + (ord(ch) - 64)
        return value

    def _shared_strings(archive: ZipFile) -> list[str]:
        if "xl/sharedStrings.xml" not in archive.namelist():
            return []
        root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
        items: list[str] = []
        for si in root.findall("x:si", ns_main):
            texts = [node.text or "" for node in si.findall(".//x:t", ns_main)]
            items.append("".join(texts))
        return items

    def _sheet_paths(archive: ZipFile) -> list[tuple[str, str]]:
        workbook_name = "xl/workbook.xml"
        rels_name = "xl/_rels/workbook.xml.rels"
        if workbook_name not in archive.namelist():
            return [("sheet1", "xl/worksheets/sheet1.xml")] if "xl/worksheets/sheet1.xml" in archive.namelist() else []
        workbook_root = ET.fromstring(archive.read(workbook_name))
        rel_map: dict[str, str] = {}
        if rels_name in archive.namelist():
            rel_root = ET.fromstring(archive.read(rels_name))
            for rel in rel_root.findall("r:Relationship", ns_pkg):
                rel_id = str(rel.attrib.get("Id") or "")
                target = str(rel.attrib.get("Target") or "")
                if rel_id and target:
                    rel_map[rel_id] = posixpath.normpath(posixpath.join("xl", target))
        sheets: list[tuple[str, str]] = []
        for sheet in workbook_root.findall(".//x:sheets/x:sheet", ns_main):
            name = str(sheet.attrib.get("name") or "sheet")
            rel_id = str(sheet.attrib.get(rel_ns) or "")
            target = rel_map.get(rel_id)
            if target and target in archive.namelist():
                sheets.append((name, target))
        if not sheets and "xl/worksheets/sheet1.xml" in archive.namelist():
            sheets.append(("sheet1", "xl/worksheets/sheet1.xml"))
        return sheets

    def _sheet_rows(archive: ZipFile, sheet_path: str, shared: list[str]) -> list[list[str]]:
        root = ET.fromstring(archive.read(sheet_path))
        rows: list[list[str]] = []
        for row in root.findall(".//x:sheetData/x:row", ns_main):
            values: dict[int, str] = {}
            max_col = 0
            for cell in row.findall("x:c", ns_main):
                ref = str(cell.attrib.get("r") or "")
                col = _xlsx_col_index(ref)
                max_col = max(max_col, col)
                cell_type = str(cell.attrib.get("t") or "")
                if cell_type == "inlineStr":
                    value = "".join(node.text or "" for node in cell.findall(".//x:t", ns_main))
                else:
                    node = cell.find("x:v", ns_main)
                    raw = str(node.text or "") if node is not None else ""
                    if cell_type == "s" and raw.isdigit():
                        idx = int(raw)
                        value = shared[idx] if 0 <= idx < len(shared) else raw
                    else:
                        value = raw
                if col:
                    values[col] = value
            if max_col:
                rows.append([values.get(idx, "") for idx in range(1, max_col + 1)])
        return rows

    with ZipFile(path) as archive:
        shared = _shared_strings(archive)
        return [(sheet_name, _sheet_rows(archive, sheet_path, shared)) for sheet_name, sheet_path in _sheet_paths(archive)]


def _read_excel_text(path: str | Path) -> str:
    import pandas as pd

    chunks: list[str] = []
    try:
        xls = pd.ExcelFile(path)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
            if df.empty:
                continue
            chunks.append(sheet_name)
            chunks.extend(" ".join(str(value or "").strip() for value in row if str(value or "").strip()) for row in df.fillna("").values.tolist())
    except Exception:
        for sheet_name, rows in _read_xlsx_rows_fallback(path):
            if not rows:
                continue
            chunks.append(sheet_name)
            chunks.extend(
                " ".join(str(value or "").strip() for value in row if str(value or "").strip())
                for row in rows
                if any(str(value or "").strip() for value in row)
            )
    return "\n".join(chunks)


def _extract_local_report_date_br(text: str) -> str | None:
    if not text:
        return None
    match = re.search(r"(\d{2}/\d{2}/\d{4})\s+a\s+(\d{2}/\d{2}/\d{4})", text, re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", text)
    if match:
        return match.group(1)
    return None


def _find_eh_local_payment_reports(data_br: str, *, company: str = "EH") -> dict[str, object]:
    pix_csv_matches: list[Path] = []
    pix_xlsx_matches: list[Path] = []
    pix_pdf_matches: list[Path] = []
    card_matches: list[Path] = []
    avisos: list[str] = []
    patterns = ("*.pdf", "*.csv", "*.xlsx", "*.crdownload")
    company_norm = _normalize_ascii_text(company) or "eh"

    preferred_dirs = _candidate_local_report_dirs()

    for directory in preferred_dirs:
        for pattern in patterns:
            for path in directory.glob(pattern):
                try:
                    effective_suffix = _effective_local_report_suffix(path)
                    if effective_suffix == ".csv":
                        text = _read_text_file(path)
                    elif effective_suffix == ".xlsx":
                        text = _read_excel_text(path)
                    elif effective_suffix == ".pdf":
                        text = _read_pdf_text(path)
                    else:
                        continue
                except Exception:
                    continue

                text_norm = _normalize_ascii_text(text)
                detected_date = _extract_local_report_date_br(text)
                report_kind = None
                normalized_path = Path(_finalize_local_report_path(path))
                if effective_suffix == ".csv" and "data da venda" in text_norm and "valor bruto" in text_norm:
                    report_kind = "pix_csv"
                elif effective_suffix == ".xlsx" and "relatorio de vendas pix" in text_norm and "valor total de vendas finalizadas" in text_norm:
                    report_kind = "pix_xlsx"
                elif "extrato pix" in text_norm:
                    report_kind = "pix_pdf"
                elif effective_suffix == ".xlsx" and "data da venda" in text_norm and "valor bruto" in text_norm and "status" in text_norm:
                    report_kind = "cartoes"
                elif "relatorio de historico de vendas" in text_norm and "periodo de venda" in text_norm:
                    report_kind = "cartoes"
                else:
                    continue

                if detected_date and detected_date != data_br:
                    tipo = "PIX" if report_kind.startswith("pix") else "cart?es"
                    avisos.append(
                        f'O arquivo "{path.name}" foi identificado como relat?rio de {tipo}, mas o conte?do ? de {detected_date} e n?o de {data_br}. '
                        f'Ele foi ignorado.'
                    )
                    continue

                if report_kind == "pix_csv":
                    pix_csv_matches.append(normalized_path)
                elif report_kind == "pix_xlsx":
                    pix_xlsx_matches.append(normalized_path)
                elif report_kind == "pix_pdf":
                    pix_pdf_matches.append(normalized_path)
                else:
                    card_matches.append(normalized_path)

    def _pick_latest(paths: list[Path]) -> str | None:
        if not paths:
            return None
        explicit_other_company_paths = [
            item
            for item in paths
            if any(f"_{other}_auto" in item.stem.casefold() for other in ("eh", "mva") if other != company_norm)
        ]
        paths = [item for item in paths if item not in explicit_other_company_paths]
        if not paths:
            return None
        def _score(item: Path) -> tuple[int, float]:
            name = item.stem.casefold()
            if f"_{company_norm}_auto" in name:
                company_score = 3
            elif "_auto" in name:
                company_score = 1
            else:
                company_score = 0
            return (company_score, item.stat().st_mtime)

        latest = max(paths, key=_score)
        return str(latest)

    return {
        "pix": _pick_latest(pix_csv_matches + pix_xlsx_matches) or _pick_latest(pix_pdf_matches),
        "cartoes": _pick_latest(card_matches),
        "avisos": list(dict.fromkeys(avisos)),
    }


def _integrate_local_payment_reports(
    relatorio_fechamento: dict,
    data_br: str,
    *,
    avisos_usuario: list[str] | None = None,
    relatorio_pix_padrao: dict | None = None,
    company: str = "EH",
) -> tuple[dict, dict | None, list[str]]:
    avisos = list(avisos_usuario or [])
    local_payment_reports = _find_eh_local_payment_reports(data_br, company=company)
    local_pix_pdf = local_payment_reports.get("pix")
    local_card_pdf = local_payment_reports.get("cartoes")
    avisos.extend(local_payment_reports.get("avisos") or [])
    scope_windows = _report_scope_windows(relatorio_fechamento)
    scope_label = " dentro do escopo horário do fechamento" if scope_windows else ""

    relatorios_pagamento = dict(relatorio_fechamento.get("relatorios_pagamento") or {})
    relatorio_pix = _filter_payment_report_to_scope(relatorio_pix_padrao, scope_windows)

    if local_card_pdf:
        try:
            relatorios_cartao = _build_card_reports_from_caixa(local_card_pdf, data_br)
            relatorios_validos = {}
            for key, report in relatorios_cartao.items():
                report_filtrado = _filter_payment_report_to_scope(report, scope_windows)
                if report_filtrado.get("itens_autorizados"):
                    relatorios_validos[key] = report_filtrado
            if relatorios_validos:
                relatorios_pagamento.update(relatorios_validos)
            else:
                avisos.append(
                    f'O arquivo "{os.path.basename(local_card_pdf)}" não trouxe transações de cartão para {data_br}{scope_label} e foi ignorado.'
                )
        except Exception as exc:
            avisos.append(
                f'Não foi possível ler o arquivo "{os.path.basename(local_card_pdf)}" para {data_br}: {exc}'
            )

    if local_pix_pdf:
        try:
            pix_suffix = _effective_local_report_suffix(local_pix_pdf)
            if pix_suffix == ".csv":
                relatorio_pix = _build_pix_report_from_caixa_csv(local_pix_pdf, data_br)
            elif pix_suffix == ".xlsx":
                relatorio_pix = _build_pix_report_from_caixa_xlsx(local_pix_pdf, data_br)
            else:
                relatorio_pix = _build_pix_report_from_caixa_pdf(local_pix_pdf, data_br)
            relatorio_pix = _filter_payment_report_to_scope(relatorio_pix, scope_windows)
            if relatorio_pix.get("quantidade_autorizados", 0) <= 0:
                avisos.append(
                    f'O arquivo "{os.path.basename(local_pix_pdf)}" não trouxe transações PIX para {data_br}{scope_label} e foi ignorado.'
                )
                relatorio_pix = _filter_payment_report_to_scope(relatorio_pix_padrao, scope_windows)
        except Exception as exc:
            avisos.append(
                f'Não foi possível ler o arquivo "{os.path.basename(local_pix_pdf)}" para {data_br}: {exc}'
            )
            relatorio_pix = _filter_payment_report_to_scope(relatorio_pix_padrao, scope_windows)

    relatorio_fechamento["relatorios_pagamento"] = relatorios_pagamento
    if relatorio_pix:
        relatorios_pagamento[str(relatorio_pix.get("categoria") or "pagamentos_digitais_nfce")] = relatorio_pix

    avisos = list(dict.fromkeys(avisos))
    if avisos:
        relatorio_fechamento["avisos_usuario"] = avisos
        if relatorio_pix is not None:
            relatorio_pix["avisos_usuario"] = avisos

    _cleanup_eh_auto_payment_reports(local_pix_pdf, local_card_pdf)
    return relatorio_fechamento, relatorio_pix, avisos


def _load_azulzinha_credentials(company: str = "EH") -> dict | None:
    company_norm = _normalize_ascii_text(company)
    target_cnpj = "34.636.193/0001-93" if company_norm == "eh" else "18.471.209/0001-07"
    env_user = os.environ.get(f"AZULZINHA_{company_norm.upper()}_CNPJ")
    env_password = os.environ.get(f"AZULZINHA_{company_norm.upper()}_PASSWORD")
    if env_user and env_password:
        return {
            "cnpj": str(env_user).strip(),
            "password": str(env_password).strip(),
            "base_url": "https://portal.azulzinhadacaixa.com.br",
            "login_url": "https://portal.azulzinhadacaixa.com.br/",
            "sales_url": "https://portal.azulzinhadacaixa.com.br/MinhasVendas?Router=0",
        }

    base_dir = Path(_runtime_user_dir())
    candidate_files = [base_dir / "credenciais.txt", base_dir / "credencias.txt", base_dir / "passo-a-passo.txt"]
    cnpj_pattern = re.compile(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")

    for caminho in candidate_files:
        if not caminho.is_file():
            continue
        try:
            linhas = [corrigir_texto(linha.strip()) for linha in caminho.read_text(encoding="utf-8", errors="ignore").splitlines()]
        except OSError:
            continue
        for idx, linha in enumerate(linhas):
            if not cnpj_pattern.match(linha):
                continue
            senha = str(linhas[idx + 1] if len(linhas) > idx + 1 else "").strip()
            if linha == target_cnpj and senha:
                return {
                    "cnpj": linha,
                    "password": senha,
                    "base_url": "https://portal.azulzinhadacaixa.com.br",
                    "login_url": "https://portal.azulzinhadacaixa.com.br/",
                    "sales_url": "https://portal.azulzinhadacaixa.com.br/MinhasVendas?Router=0",
                }
    return None


def _azulzinha_browser_profile_dir(company: str = "EH") -> str:
    root = os.path.join(_runtime_user_dir(), "azulzinha_browser")
    profile = os.path.join(root, _normalize_ascii_text(company) or "eh")
    os.makedirs(profile, exist_ok=True)
    return profile


def _azulzinha_device_aliases(company: str = "EH") -> list[str]:
    company_norm = _normalize_ascii_text(company)
    if company_norm == "mva":
        return [
            "MVA COMERCIO",
            "MVA COMÉRCIO",
            "MVA FINANCEIRO",
            "MVA 01",
            "MVA",
        ]
    return [
        "EH FINANCEIRO",
        "EH 01",
        "ELETRONICA HORIZONTE",
        "HORIZONTE",
        "EH VAL",
        "MARIA EDUARDA",
    ]


def _load_gmail_token_credentials() -> tuple[str, str] | None:
    login = str(MINHAS_NOTAS_LOGIN or "").strip()
    password = str(MINHAS_NOTAS_PASSWORD or "").strip()
    if login and password:
        return login, password

    base_dir = Path(_runtime_user_dir())
    for filename in ("credenciais.txt", "credencias.txt"):
        caminho = base_dir / filename
        if not caminho.is_file():
            continue
        try:
            linhas = [linha.strip() for linha in caminho.read_text(encoding="utf-8", errors="ignore").splitlines() if linha.strip()]
        except OSError:
            continue
        for idx, linha in enumerate(linhas):
            if "@" in linha and idx + 1 < len(linhas):
                return linha, linhas[idx + 1]
    return None


def _gmail_oauth_token_path() -> Path:
    return Path(_runtime_user_dir()) / "gmail_oauth_token.json"


def _load_gmail_oauth_client_credentials() -> tuple[str, str] | None:
    client_id = str(GMAIL_OAUTH_CLIENT_ID or "").strip()
    client_secret = str(GMAIL_OAUTH_CLIENT_SECRET or "").strip()
    if client_id and client_secret:
        return client_id, client_secret

    env_client_id = str(os.environ.get("GMAIL_OAUTH_CLIENT_ID") or "").strip()
    env_client_secret = str(os.environ.get("GMAIL_OAUTH_CLIENT_SECRET") or "").strip()
    if env_client_id and env_client_secret:
        return env_client_id, env_client_secret

    config_path = Path(_runtime_user_dir()) / "gmail_oauth_client.json"
    if not config_path.is_file():
        return None

    try:
        payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None

    if isinstance(payload, dict):
        for candidate in (payload, payload.get("installed"), payload.get("web")):
            if not isinstance(candidate, dict):
                continue
            file_client_id = str(candidate.get("client_id") or "").strip()
            file_client_secret = str(candidate.get("client_secret") or "").strip()
            if file_client_id and file_client_secret:
                return file_client_id, file_client_secret
    return None


def _get_gmail_api_credentials(on_status=None):
    client_credentials = _load_gmail_oauth_client_credentials()
    if not client_credentials:
        return None
    client_id, client_secret = client_credentials

    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
    except Exception as exc:
        _emit_pix_status(on_status, f"Bibliotecas OAuth do Google indisponiveis: {exc}")
        return None

    scopes = ["https://www.googleapis.com/auth/gmail.readonly"]
    token_path = _gmail_oauth_token_path()
    creds = None
    if token_path.is_file():
        try:
            creds = Credentials.from_authorized_user_file(str(token_path), scopes=scopes)
        except Exception:
            creds = None

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            token_path.write_text(creds.to_json(), encoding="utf-8")
        except Exception:
            creds = None

    if creds and creds.valid:
        return creds

    _emit_pix_status(on_status, "Autorizando leitura do Gmail no navegador...")
    flow = InstalledAppFlow.from_client_config(
        {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost"],
            }
        },
        scopes=scopes,
    )
    creds = flow.run_local_server(
        host="127.0.0.1",
        port=0,
        authorization_prompt_message="Abra o link abaixo no navegador para autorizar o Gmail, se ele nao abrir sozinho:\n{url}",
        success_message="Autorizacao do Gmail concluida. Pode voltar ao aplicativo.",
        open_browser=True,
    )
    if creds:
        token_path.write_text(creds.to_json(), encoding="utf-8")
    return creds


def _extract_gmail_api_body(payload: dict) -> str:
    chunks = []

    def _walk(part: dict) -> None:
        mime = str(part.get("mimeType") or "")
        body = part.get("body") or {}
        data = body.get("data")
        if mime in {"text/plain", "text/html"} and data:
            try:
                chunks.append(base64.urlsafe_b64decode(data + "===").decode("utf-8", errors="ignore"))
            except Exception:
                pass
        for child in part.get("parts") or []:
            _walk(child)

    _walk(payload or {})
    return "\n".join(chunks)


def _extract_fiserv_email_token(text: str) -> str | None:
    texto = corrigir_texto(str(text or ""))
    texto_sem_style = re.sub(r"(?is)<style\b.*?>.*?</style>", " ", texto)
    texto_sem_script = re.sub(r"(?is)<script\b.*?>.*?</script>", " ", texto_sem_style)
    texto_visivel = re.sub(r"(?is)<[^>]+>", " ", texto_sem_script)
    texto_visivel = html.unescape(texto_visivel)
    texto_visivel = re.sub(r"\s+", " ", texto_visivel).strip()

    candidatos_prioritarios = []
    candidatos_contexto = []

    for match in re.finditer(r"(?<!\d)(\d{6,8})(?!\d)", texto_visivel):
        inicio = max(0, match.start() - 120)
        fim = min(len(texto_visivel), match.end() + 120)
        contexto = _normalize_ascii_text(texto_visivel[inicio:fim])
        codigo = match.group(1)
        if any(
            frase in contexto
            for frase in (
                "seu codigo de verificacao",
                "codigo de verificacao",
                "codigo enviado",
                "codigo que enviamos",
            )
        ):
            candidatos_prioritarios.append(codigo)
            continue
        if any(palavra in contexto for palavra in ("token", "codigo", "verificacao", "validacao", "acesso", "seguranca")):
            candidatos_contexto.append(codigo)

    if candidatos_prioritarios:
        return candidatos_prioritarios[0]
    if candidatos_contexto:
        return candidatos_contexto[0]

    match = re.search(r"(?<!\d)(\d{6})(?!\d)", texto_visivel)
    return match.group(1) if match else None


def _is_likely_login_token_email(sender: str, subject: str, body: str) -> bool:
    texto = _normalize_ascii_text("\n".join([str(sender or ""), str(subject or ""), str(body or "")]))
    return any(
        marker in texto
        for marker in (
            "fiserv",
            "codigo de verificacao",
            "codigo de verificação",
            "verificacao de login",
            "verificação de login",
            "validar o login",
            "codigo enviado",
            "seguranca desta conta",
        )
    )


def _write_gmail_body_debug(
    entries: list[dict[str, object]],
    *,
    title: str | None = None,
    summary_lines: list[str] | None = None,
    note: str | None = None,
    extra_lines: list[str] | None = None,
) -> None:
    try:
        linhas = []
        if title:
            linhas.extend([title, "=" * 100, ""])
        if extra_lines:
            linhas.extend([str(item) for item in extra_lines if str(item or "").strip()])
            if linhas and linhas[-1] != "":
                linhas.append("")
        if summary_lines:
            linhas.append("Últimos e-mails identificados:")
            for line in summary_lines[:8]:
                linhas.append(f"- {line}")
            linhas.append("")
        if note:
            linhas.append(note)
            linhas.append("")
        for idx, entry in enumerate(entries[:8], start=1):
            msg_ts = float(entry.get("timestamp") or 0.0)
            horario = "--:--:--"
            if msg_ts:
                try:
                    horario = datetime.fromtimestamp(msg_ts).strftime("%d/%m/%Y %H:%M:%S")
                except Exception:
                    pass
            remetente = corrigir_texto(str(entry.get("sender") or "Remetente desconhecido"))
            assunto = corrigir_texto(str(entry.get("subject") or "Sem assunto"))
            msg_id = str(entry.get("message_id") or "").strip()
            token = str(entry.get("token") or "").strip() or "(nenhum)"
            decisao = corrigir_texto(str(entry.get("decision") or "sem decisao"))
            snippet = corrigir_texto(str(entry.get("snippet") or "")).strip()
            corpo = corrigir_texto(str(entry.get("body") or "")).strip()
            linhas.extend(
                [
                    "=" * 100,
                    f"Email #{idx}",
                    f"Horário: {horario}",
                    f"Message ID: {msg_id or '(desconhecido)'}",
                    f"Remetente: {remetente}",
                    f"Assunto: {assunto}",
                    f"Token extraído: {token}",
                    f"Decisão do app: {decisao}",
                    "",
                    "Snippet retornado pela API:",
                    snippet or "(vazio)",
                    "",
                    "Corpo visível analisado pelo parser:",
                    corpo or "(vazio)",
                    "",
                ]
            )
        destino = Path(_runtime_user_dir()) / "body_email.txt"
        destino.write_text("\n".join(linhas), encoding="utf-8")
    except Exception:
        pass


def _fetch_fiserv_token_from_gmail(
    sent_after: float,
    timeout: float = 90.0,
    on_status=None,
    ignored_tokens: set[str] | None = None,
    allow_recent_fallback: bool = True,
) -> str | None:
    credenciais = _load_gmail_token_credentials()
    if not credenciais:
        return None

    ignored_tokens_normalized = {
        str(token or "").strip()
        for token in (ignored_tokens or set())
        if str(token or "").strip()
    }

    _write_gmail_body_debug(
        [],
        title="Debug do Gmail - nova tentativa",
        note="Aguardando leitura dos últimos e-mails pelo aplicativo.",
    )

    deadline = time.time() + timeout
    last_error = None
    fallback_recent_token: tuple[float, str] | None = None
    checked_full_ids: set[str] = set()
    gmail_query = "from:no-reply@fiserv.com newer_than:7d"
    gmail_debug_limit = 8

    def _format_candidate_summary(msg_ts: float, sender: str, subject: str) -> str:
        horario = "--:--:--"
        if msg_ts:
            try:
                horario = datetime.fromtimestamp(msg_ts).strftime("%H:%M:%S")
            except Exception:
                pass
        remetente = (sender or "Remetente desconhecido").strip()
        if len(remetente) > 45:
            remetente = remetente[:42] + "..."
        assunto = (subject or "Sem assunto").strip()
        if len(assunto) > 55:
            assunto = assunto[:52] + "..."
        return f"{horario} | {remetente} | {assunto}"

    def _compute_poll_interval() -> float:
        remaining = max(0.0, deadline - time.time())
        elapsed = max(0.0, timeout - remaining)
        if elapsed < 30.0:
            return 3.0
        if elapsed < 90.0:
            return 5.0
        return 10.0

    try:
        creds = _get_gmail_api_credentials(on_status=on_status)
        if not creds:
            raise RuntimeError("Credenciais OAuth do Gmail indisponíveis.")
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {creds.token}"})
        while time.time() < deadline:
            _emit_pix_status(on_status, "Consultando e-mails recentes no Gmail via API...")
            response = session.get(
                "https://gmail.googleapis.com/gmail/v1/users/me/messages",
                params={
                    "q": gmail_query,
                    "maxResults": gmail_debug_limit,
                    "includeSpamTrash": "false",
                    "fields": "messages/id,resultSizeEstimate",
                },
                timeout=15.0,
            )
            if response.status_code == 401 and getattr(creds, "refresh_token", None):
                try:
                    from google.auth.transport.requests import Request
                    creds.refresh(Request())
                    _gmail_oauth_token_path().write_text(creds.to_json(), encoding="utf-8")
                    session.headers.update({"Authorization": f"Bearer {creds.token}"})
                    _emit_pix_status(on_status, "Token OAuth do Gmail renovado. Repetindo consulta...")
                    continue
                except Exception:
                    pass
            response.raise_for_status()
            messages = (response.json() or {}).get("messages") or []
            newest_match: tuple[float, str] | None = None
            ignored_marker_seen = False
            ignored_rejected_logged = False
            older_than_rejected_logged = False
            candidates: list[tuple[float, str, str, str]] = []
            api_debug_entries: list[dict[str, object]] = []
            debug_context_lines = [
                f"Consulta Gmail usada: {gmail_query}",
                f"Fallback recente permitido nesta tentativa: {'sim' if allow_recent_fallback else 'não'}",
                (
                    "Tokens já descartados nesta rodada: "
                    + (", ".join(sorted(ignored_tokens_normalized)) if ignored_tokens_normalized else "(nenhum)")
                ),
                "",
            ]
            for item in messages:
                msg_id = str(item.get("id") or "").strip()
                if not msg_id:
                    continue
                msg_resp = session.get(
                    f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}",
                    params={
                        "format": "metadata",
                        "metadataHeaders": ["Subject", "From"],
                        "fields": "id,internalDate,payload/headers",
                    },
                    timeout=15.0,
                )
                msg_resp.raise_for_status()
                payload = msg_resp.json() or {}
                internal_date_ms = float(payload.get("internalDate") or 0.0)
                msg_ts = internal_date_ms / 1000.0 if internal_date_ms else 0.0
                headers = payload.get("payload", {}).get("headers") or []
                subject = next((str(h.get("value") or "") for h in headers if str(h.get("name") or "").lower() == "subject"), "")
                sender = next((str(h.get("value") or "") for h in headers if str(h.get("name") or "").lower() == "from"), "")
                candidates.append((msg_ts, msg_id, subject, sender))
            candidates.sort(key=lambda item: item[0], reverse=True)
            candidate_summaries = [
                _format_candidate_summary(msg_ts, sender, subject)
                for msg_ts, _msg_id, subject, sender in candidates[:gmail_debug_limit]
            ]
            for msg_ts, msg_id, subject, sender in candidates[:gmail_debug_limit]:
                if msg_id in checked_full_ids:
                    continue
                msg_resp = session.get(
                    f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}",
                    params={"format": "full", "fields": "id,internalDate,payload,snippet"},
                    timeout=15.0,
                )
                msg_resp.raise_for_status()
                payload = msg_resp.json() or {}
                checked_full_ids.add(msg_id)
                body = _extract_gmail_api_body(payload.get("payload") or {})
                snippet = str(payload.get("snippet") or "").strip()
                searchable_text = body
                if snippet:
                    searchable_text = f"{searchable_text}\n{snippet}".strip() if searchable_text else snippet
                token = _extract_fiserv_email_token(f"{subject}\n{searchable_text}")
                api_debug_entries.append(
                    {
                        "timestamp": msg_ts,
                        "message_id": msg_id,
                        "sender": sender,
                        "subject": subject,
                        "snippet": snippet,
                        "body": searchable_text,
                        "token": token or "",
                        "decision": "sem_token",
                    }
                )
                if token and _is_likely_login_token_email(sender, subject, searchable_text):
                    if token in ignored_tokens_normalized:
                        ignored_marker_seen = True
                        api_debug_entries[-1]["decision"] = "token_ja_rejeitado"
                        if not ignored_rejected_logged:
                            _emit_pix_status(
                                on_status,
                                "Codigo de login ja rejeitado anteriormente; aguardando um novo e-mail.",
                            )
                            ignored_rejected_logged = True
                        continue
                    if not fallback_recent_token or msg_ts > fallback_recent_token[0]:
                        fallback_recent_token = (msg_ts, token)
                    if ignored_marker_seen and ignored_tokens_normalized and not allow_recent_fallback:
                        api_debug_entries[-1]["decision"] = "token_mais_antigo_que_rejeitado"
                        if not older_than_rejected_logged:
                            _emit_pix_status(
                                on_status,
                                "O codigo disponivel e mais antigo que o rejeitado; aguardando um e-mail mais novo.",
                            )
                            older_than_rejected_logged = True
                        continue
                    if not newest_match or msg_ts > newest_match[0]:
                        newest_match = (msg_ts, token)
                    api_debug_entries[-1]["decision"] = "token_pronto_para_validacao"
                    continue
                if token:
                    api_debug_entries[-1]["decision"] = "token_extraido_sem_confirmacao_de_contexto"
                else:
                    api_debug_entries[-1]["decision"] = "nenhum_token_extraido"
            _write_gmail_body_debug(
                api_debug_entries,
                title="Debug do Gmail via API",
                summary_lines=candidate_summaries,
                extra_lines=debug_context_lines,
                note=(
                    None
                    if api_debug_entries
                    else "Nenhum corpo de e-mail foi lido nesta tentativa da API."
                ),
            )
            if newest_match:
                _emit_pix_status(on_status, f"Codigo de login pronto para validacao na Caixa: {newest_match[1]}.")
                return newest_match[1]
            wait_seconds = _compute_poll_interval()
            _emit_pix_status(
                on_status,
                f"Nenhum token novo ainda. Nova tentativa em {int(wait_seconds)}s.",
            )
            time.sleep(wait_seconds)
    except Exception as exc:
        last_error = exc
        _emit_pix_status(on_status, f"Nao foi possivel ler o token no Gmail via OAuth: {exc}")

    import email
    import imaplib
    from email.header import decode_header
    from email.utils import parsedate_to_datetime

    login, password = credenciais

    def _decode_mime(value: str) -> str:
        partes = []
        for chunk, charset in decode_header(value or ""):
            if isinstance(chunk, bytes):
                partes.append(chunk.decode(charset or "utf-8", errors="ignore"))
            else:
                partes.append(str(chunk or ""))
        return "".join(partes)

    def _message_text(message) -> str:
        chunks = []
        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                disposition = str(part.get("Content-Disposition") or "").lower()
                if "attachment" in disposition or content_type not in {"text/plain", "text/html"}:
                    continue
                payload = part.get_payload(decode=True)
                if payload is None:
                    continue
                chunks.append(payload.decode(part.get_content_charset() or "utf-8", errors="ignore"))
        else:
            payload = message.get_payload(decode=True)
            if payload is not None:
                chunks.append(payload.decode(message.get_content_charset() or "utf-8", errors="ignore"))
        return "\n".join(chunks)

    while time.time() < deadline:
        try:
            _emit_pix_status(on_status, "Lendo e-mails recentes no Gmail via IMAP...")
            with imaplib.IMAP4_SSL("imap.gmail.com", 993) as mail:
                mail.login(login, password)
                mail.select("INBOX")
                status, data = mail.uid(
                    "search",
                    None,
                    "X-GM-RAW",
                    '"in:inbox"',
                )
                if status != "OK":
                    status, data = mail.uid("search", None, "ALL")
                ids = (data[0] or b"").split()[-20:]
                newest_match: tuple[float, str] | None = None
                ignored_marker_seen = False
                ignored_rejected_logged = False
                older_than_rejected_logged = False
                imap_candidates: list[tuple[float, str, str]] = []
                imap_debug_entries: list[dict[str, object]] = []
                for uid in reversed(ids):
                    status, msg_data = mail.uid("fetch", uid, "(RFC822)")
                    if status != "OK" or not msg_data:
                        continue
                    raw = next((item[1] for item in msg_data if isinstance(item, tuple) and len(item) > 1), None)
                    if not raw:
                        continue
                    msg = email.message_from_bytes(raw)
                    try:
                        msg_dt = parsedate_to_datetime(msg.get("Date") or "")
                        msg_ts = msg_dt.timestamp()
                    except Exception:
                        msg_ts = 0.0
                    remetente = _normalize_ascii_text(msg.get("From") or "")
                    assunto = _decode_mime(msg.get("Subject") or "")
                    corpo = _message_text(msg)
                    imap_candidates.append((msg_ts, msg.get("From") or "", assunto))
                    imap_debug_entries.append(
                        {
                            "timestamp": msg_ts,
                            "sender": msg.get("From") or "",
                            "subject": assunto,
                            "body": corpo,
                        }
                    )
                    token = _extract_fiserv_email_token(f"{assunto}\n{corpo}")
                    if token and _is_likely_login_token_email(remetente, assunto, corpo):
                        if token in ignored_tokens_normalized:
                            ignored_marker_seen = True
                            if not ignored_rejected_logged:
                                _emit_pix_status(
                                    on_status,
                                    "O token retornado via IMAP ja foi descartado; aguardando um novo codigo.",
                                )
                                ignored_rejected_logged = True
                            continue
                        if not fallback_recent_token or msg_ts > fallback_recent_token[0]:
                            fallback_recent_token = (msg_ts, token)
                        if ignored_marker_seen and ignored_tokens_normalized and not allow_recent_fallback:
                            if not older_than_rejected_logged:
                                _emit_pix_status(
                                    on_status,
                                    "O codigo encontrado via IMAP e mais antigo que o rejeitado; aguardando um e-mail mais novo.",
                                )
                                older_than_rejected_logged = True
                            continue
                        if not newest_match or msg_ts > newest_match[0]:
                            newest_match = (msg_ts, token)
                        continue
                        if not fallback_recent_token or msg_ts > fallback_recent_token[0]:
                            fallback_recent_token = (msg_ts, token)
                        if not newest_match or msg_ts > newest_match[0]:
                            newest_match = (msg_ts, token)
                        _emit_pix_status(
                            on_status,
                            f"Token compatível com login encontrado via IMAP ({assunto_resumido}).",
                        )
                _write_gmail_body_debug(
                    imap_debug_entries,
                    title="Debug do Gmail via IMAP",
                    summary_lines=[
                        _format_candidate_summary(msg_ts, sender, subject)
                        for msg_ts, sender, subject in imap_candidates[:5]
                    ],
                    note=(
                        None
                        if imap_debug_entries
                        else "Nenhum corpo de e-mail foi lido nesta tentativa do IMAP."
                    ),
                )
                if newest_match:
                    _emit_pix_status(on_status, f"Codigo de login via IMAP pronto para validacao na Caixa: {newest_match[1]}.")
                    return newest_match[1]
        except Exception as exc:
            last_error = exc
        wait_seconds = _compute_poll_interval()
        _emit_pix_status(
            on_status,
            f"IMAP ainda sem token novo. Nova tentativa em {int(wait_seconds)}s.",
        )
        time.sleep(wait_seconds)

    if fallback_recent_token and not allow_recent_fallback:
        _emit_pix_status(
            on_status,
            "Nenhum token novo chegou a tempo; codigos antigos nao serao reutilizados nesta tentativa.",
        )
        fallback_recent_token = None

    if fallback_recent_token:
        _emit_pix_status(on_status, "Nenhum e-mail novo da Fiserv chegou a tempo; reutilizando o código mais recente do Gmail.")
        return fallback_recent_token[1]

    if last_error:
        _emit_pix_status(on_status, f"Falha final ao ler token no Gmail: {last_error}")
    return None


def _wait_for_downloaded_report(
    download_dir: str,
    data_br: str,
    kind: str,
    started_at: float,
    timeout: float = 90.0,
) -> str | None:
    deadline = time.time() + timeout
    suffixes = ("*.csv", "*.xlsx", "*.crdownload") if kind == "pix" else ("*.pdf", "*.xlsx", "*.crdownload")
    while time.time() < deadline:
        for pattern in suffixes:
            candidates = sorted(
                Path(download_dir).glob(pattern),
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )
            for path in candidates:
                try:
                    effective_suffix = _effective_local_report_suffix(path)
                    if path.stat().st_mtime < started_at - 1:
                        continue
                    if effective_suffix not in _LOCAL_REPORT_SUFFIXES or path.name.endswith(".tmp"):
                        continue
                    if effective_suffix == ".csv":
                        text = _read_text_file(path)
                    elif effective_suffix == ".xlsx":
                        text = _read_excel_text(path)
                    else:
                        text = _read_pdf_text(path)
                    text_norm = _normalize_ascii_text(text)
                    detected_date = _extract_local_report_date_br(text)
                    if detected_date and detected_date != data_br:
                        continue
                    if kind == "pix" and (
                        (effective_suffix == ".csv" and "data da venda" in text_norm and "valor bruto" in text_norm)
                        or (effective_suffix == ".xlsx" and "relatorio de vendas pix" in text_norm and "valor total de vendas finalizadas" in text_norm)
                    ):
                        normalized_path = _finalize_local_report_path(path)
                        if effective_suffix == ".xlsx":
                            converted = _convert_pix_xlsx_to_csv(normalized_path)
                            return converted or normalized_path
                        return normalized_path
                    if kind == "cartoes" and (
                        "relatorio de historico de vendas" in text_norm and "periodo de venda" in text_norm
                        or (effective_suffix == ".xlsx" and "data da venda" in text_norm and "valor bruto" in text_norm and "status" in text_norm)
                    ):
                        return _finalize_local_report_path(path)
                except Exception:
                    continue
        time.sleep(0.6)
    return None


_AZULZINHA_DEBUG_ARTIFACT_PATTERNS = (
    "azulzinha_export_payload_*.json",
    "azulzinha_network_candidates_*.json",
    "azulzinha_export_signals_*.json",
    "azulzinha_export_ready_debug_*.html",
    "azulzinha_export_ready_state_*.json",
    "azulzinha_export_popup_*.html",
    "azulzinha_export_state_*.json",
    "azulzinha_export_debug_*.html",
)


def _cleanup_azulzinha_debug_artifacts() -> None:
    base_dir = Path(_active_report_dir())
    visited: set[Path] = set()
    for pattern in _AZULZINHA_DEBUG_ARTIFACT_PATTERNS:
        try:
            matches = list(base_dir.glob(pattern))
        except Exception:
            continue
        for path in matches:
            if path in visited or not path.is_file():
                continue
            visited.add(path)
            try:
                path.unlink()
            except Exception:
                pass


_EH_AUTO_PAYMENT_REPORT_PREFIXES = (
    "relatorio_de_vendas_pix_",
    "historico_simplificado_de_vendas_",
)
_EH_AUTO_PAYMENT_REPORT_SUFFIXES = (".csv", ".xlsx", ".pdf", ".crdownload")
_AUTO_ZWEB_REPORT_PREFIXES = (
    "pedidos_importados_",
    "fechamento_de_caixa_",
    "relatorio_zweb_",
)
_AUTO_ZWEB_REPORT_SUFFIXES = (".html",)


def _is_eh_auto_payment_report_path(path_like: str | os.PathLike | None) -> bool:
    if not path_like:
        return False
    try:
        path = Path(path_like)
    except TypeError:
        return False
    name = path.name.casefold()
    if not any(name.startswith(prefix) for prefix in _EH_AUTO_PAYMENT_REPORT_PREFIXES):
        return False
    if path.suffix.lower() not in _EH_AUTO_PAYMENT_REPORT_SUFFIXES:
        return False
    return "_auto" in path.stem.casefold()


def _is_generated_auto_report_path(path_like: str | os.PathLike | None) -> bool:
    if _is_eh_auto_payment_report_path(path_like):
        return True
    if not path_like:
        return False
    try:
        path = Path(path_like)
    except TypeError:
        return False
    name = path.name.casefold()
    if not any(name.startswith(prefix) for prefix in _AUTO_ZWEB_REPORT_PREFIXES):
        return False
    if path.suffix.lower() not in _AUTO_ZWEB_REPORT_SUFFIXES:
        return False
    return "_auto" in path.stem.casefold()


def _cleanup_eh_auto_payment_reports(*paths_like: str | os.PathLike | None) -> None:
    targets: dict[str, Path] = {}
    scan_dirs: dict[str, Path] = {str(Path(_active_report_dir())).casefold(): Path(_active_report_dir())}
    for raw_path in paths_like:
        if not _is_eh_auto_payment_report_path(raw_path):
            continue
        path = Path(str(raw_path))
        scan_dirs[str(path.parent).casefold()] = path.parent
        base_name = path.name[:-11] if path.name.casefold().endswith(".crdownload") else path.name
        for candidate in (path.parent / f"{base_name}.crdownload",):
            if not candidate.is_file():
                continue
            if not _is_eh_auto_payment_report_path(candidate):
                continue
            targets[str(candidate).casefold()] = candidate

    for directory in scan_dirs.values():
        try:
            partials = list(directory.glob("*.crdownload"))
        except Exception:
            continue
        for candidate in partials:
            name = candidate.name.casefold()
            if (
                "relatorio_de_vendas_pix" in name
                or ("historico" in name and "vendas" in name)
            ):
                targets[str(candidate).casefold()] = candidate

    for candidate in targets.values():
        try:
            candidate.unlink()
        except Exception:
            pass


def cleanup_generated_auto_reports(report_dir: str | os.PathLike | None = None) -> None:
    try:
        base_dir = Path(report_dir or _active_report_dir())
    except TypeError:
        return
    if not base_dir.exists() or not base_dir.is_dir():
        return

    try:
        candidates = list(base_dir.iterdir())
    except Exception:
        return

    for candidate in candidates:
        if not candidate.is_file():
            continue
        if not _is_generated_auto_report_path(candidate):
            continue
        try:
            candidate.unlink()
        except Exception:
            pass


def list_generated_auto_reports(report_dir: str | os.PathLike | None = None) -> list[Path]:
    try:
        base_dir = Path(report_dir or _active_report_dir())
    except TypeError:
        return []
    if not base_dir.exists() or not base_dir.is_dir():
        return []

    try:
        candidates = list(base_dir.iterdir())
    except Exception:
        return []

    report_paths: list[Path] = []
    for candidate in candidates:
        if not candidate.is_file():
            continue
        if not _is_generated_auto_report_path(candidate):
            continue
        report_paths.append(candidate)
    return sorted(report_paths, key=lambda item: item.name.casefold())


def baixar_relatorios_caixa_eh_azulzinha(
    data_br: str,
    on_status=None,
    cancel_event: threading.Event | None = None,
    token_callback=None,
    need_pix: bool = True,
    need_cartoes: bool = True,
    company: str = "EH",
) -> dict[str, object]:
    cancel_event = cancel_event or threading.Event()
    company_norm = _normalize_ascii_text(company) or "eh"
    company_label = "MVA" if company_norm == "mva" else "EH"

    def _check_cancelled() -> None:
        if cancel_event.is_set():
            raise RuntimeError("__cancelled__")

    if not need_pix and not need_cartoes:
        return {"pix": None, "cartoes": None, "avisos": []}

    credenciais = _load_azulzinha_credentials(company_label)
    if not credenciais:
        return {
            "pix": None,
            "cartoes": None,
            "avisos": [f"As credenciais da Azulzinha/Caixa da {company_label} nao foram encontradas no credenciais.txt ou passo-a-passo.txt."],
        }

    navegador = _find_chromium_browser_path()
    if not navegador:
        return {
            "pix": None,
            "cartoes": None,
            "avisos": ["Nenhum navegador Chromium compativel foi encontrado para baixar os relatorios da Azulzinha/Caixa."],
        }

    download_dir = _active_report_dir()
    artifacts_dir = Path(download_dir)
    profile_root = _azulzinha_browser_profile_dir(company_label)
    profile_dir = tempfile.mkdtemp(prefix=f"run_{company_norm}_", dir=profile_root)
    browser_download_dir = tempfile.mkdtemp(prefix=f"downloads_{company_norm}_", dir=profile_root)
    _prepare_chromium_profile(profile_dir, browser_download_dir)
    _cleanup_azulzinha_debug_artifacts()
    port = _pick_free_local_port()
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
        "--disable-gpu",
        "about:blank",
    ]

    def _download_start_time() -> float:
        return time.time() - 1.0

    async def _run() -> dict[str, object]:
        import websockets

        _check_cancelled()
        meta = _wait_for_devtools_ready(port)
        ws_url = str(meta.get("webSocketDebuggerUrl") or "").strip()
        if not ws_url:
            raise RuntimeError("Nao foi possivel conectar ao Chromium para acessar a Azulzinha/Caixa.")

        async with websockets.connect(ws_url, max_size=50_000_000) as conn:
            next_id = 0
            pending = {}
            event_log: list[dict] = []
            event_cond = asyncio.Condition()

            async def recv_loop():
                while True:
                    _check_cancelled()
                    mensagem = json.loads(await conn.recv())
                    if "id" in mensagem and mensagem["id"] in pending:
                        pending.pop(mensagem["id"]).set_result(mensagem)
                    elif "method" in mensagem:
                        async with event_cond:
                            event_log.append(mensagem)
                            if len(event_log) > 400:
                                del event_log[:200]
                            event_cond.notify_all()

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

            async def eval_js(session_id: str, expression: str, timeout: float = 60.0):
                resposta = await cdp(
                    "Runtime.evaluate",
                    {"expression": expression, "returnByValue": True, "awaitPromise": True},
                    session_id=session_id,
                    timeout=timeout,
                )
                return resposta.get("result", {}).get("value")

            async def wait_for_condition(
                session_id: str,
                expression: str,
                timeout: float = 60.0,
                step: float = 0.5,
                description: str | None = None,
            ):
                deadline = time.time() + timeout
                last_value = None
                while time.time() < deadline:
                    _check_cancelled()
                    try:
                        last_value = await eval_js(session_id, expression, timeout=15.0)
                        if last_value:
                            return last_value
                    except Exception as exc:
                        last_value = str(exc)
                    await asyncio.sleep(step)
                detail = f" :: ultimo retorno={last_value!r}" if last_value not in (None, "") else ""
                if description:
                    raise TimeoutError(f"{description}{detail}")
                raise TimeoutError(f"{expression}{detail}")

            async def wait_for_event(
                predicate,
                timeout: float = 30.0,
                start_index: int = 0,
            ) -> dict | None:
                deadline = time.time() + timeout
                cursor = start_index
                while time.time() < deadline:
                    async with event_cond:
                        for idx in range(cursor, len(event_log)):
                            mensagem = event_log[idx]
                            if predicate(mensagem):
                                return mensagem
                        cursor = len(event_log)
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            break
                        try:
                            await asyncio.wait_for(event_cond.wait(), timeout=min(1.0, remaining))
                        except asyncio.TimeoutError:
                            pass
                return None

            def _save_captured_report_bytes(
                payload: bytes,
                kind: str,
                suffix: str,
                source_name: str,
            ) -> str | None:
                safe_suffix = suffix if suffix.startswith(".") else f".{suffix}"
                filename = (
                    f"Relatorio_de_Vendas_Pix_{data_br.replace('/', '-')}_{company_norm}_auto{safe_suffix}"
                    if kind == "pix"
                    else f"Historico_Simplificado_de_vendas_{data_br.replace('/', '-')}_{company_norm}_auto{safe_suffix}"
                )
                target = artifacts_dir / filename
                try:
                    target.write_bytes(payload)
                except Exception:
                    return None
                try:
                    if safe_suffix.lower() == ".csv":
                        text = _read_text_file(target)
                    elif safe_suffix.lower() == ".xlsx":
                        text = _read_excel_text(target)
                    else:
                        text = _read_pdf_text(target)
                    text_norm = _normalize_ascii_text(text)
                    detected_date = _extract_local_report_date_br(text)
                    if detected_date and detected_date != data_br:
                        return None
                    if kind == "pix":
                        if safe_suffix.lower() == ".csv" and "data da venda" in text_norm and "valor bruto" in text_norm:
                            return str(target)
                        if safe_suffix.lower() == ".xlsx" and "relatorio de vendas pix" in text_norm and "valor total de vendas finalizadas" in text_norm:
                            converted = _convert_pix_xlsx_to_csv(str(target))
                            return converted or str(target)
                        if safe_suffix.lower() == ".pdf" and "valor bruto" in text_norm and "pix" in text_norm:
                            return str(target)
                    else:
                        if safe_suffix.lower() == ".xlsx":
                            text = _read_excel_text(target)
                            text_norm = _normalize_ascii_text(text)
                        if (
                            "relatorio de historico de vendas" in text_norm and "periodo de venda" in text_norm
                            or (safe_suffix.lower() == ".xlsx" and "data da venda" in text_norm and "valor bruto" in text_norm and "status" in text_norm)
                        ):
                            return str(target)
                        if safe_suffix.lower() == ".xlsx":
                            try:
                                card_reports = _build_card_reports_from_caixa_xlsx(str(target), data_br)
                            except Exception:
                                card_reports = {}
                            if any(report.get("itens_autorizados") for report in card_reports.values()):
                                return str(target)
                except Exception:
                    pass
                try:
                    if target.exists():
                        target.unlink()
                except Exception:
                    pass
                return None

            def _persist_downloaded_report(path_like: str | None, kind: str) -> str | None:
                if not path_like:
                    return None
                try:
                    source_path = Path(str(path_like))
                except Exception:
                    return path_like
                try:
                    payload = source_path.read_bytes()
                except Exception:
                    return str(source_path)
                normalized_source = _finalize_local_report_path(source_path)
                effective_suffix = _effective_local_report_suffix(normalized_source) or ".pdf"
                saved = _save_captured_report_bytes(payload, kind, effective_suffix, str(source_path))
                if saved:
                    try:
                        normalized_path = Path(normalized_source)
                        if normalized_path.exists() and normalized_path.resolve() != Path(saved).resolve():
                            normalized_path.unlink()
                        if source_path.exists() and source_path.resolve() != Path(saved).resolve():
                            source_path.unlink()
                    except Exception:
                        pass
                    return saved
                return normalized_source

            def _find_export_url_in_payload(payload):
                if isinstance(payload, dict):
                    for value in payload.values():
                        found = _find_export_url_in_payload(value)
                        if found:
                            return found
                elif isinstance(payload, list):
                    for value in payload:
                        found = _find_export_url_in_payload(value)
                        if found:
                            return found
                elif isinstance(payload, str):
                    text = payload.strip()
                    if text.startswith("http://") or text.startswith("https://"):
                        return text
                    if "/screenservices/" in text or "/download" in text.lower():
                        return text
                return None

            def _extract_binary_payload_from_json(payload):
                if isinstance(payload, dict):
                    binary_data = payload.get("BinaryData")
                    if isinstance(binary_data, str) and binary_data.strip():
                        raw = base64.b64decode(binary_data)
                        header = raw[:8]
                        if header.startswith(b"PK"):
                            suffix = ".xlsx"
                        elif header.startswith(b"%PDF"):
                            suffix = ".pdf"
                        else:
                            try:
                                preview = raw[:512].decode("utf-8", errors="ignore").lower()
                            except Exception:
                                preview = ""
                            suffix = ".csv" if (";" in preview or "," in preview or "valor bruto" in preview) else ".bin"
                        return raw, suffix
                    for value in payload.values():
                        found = _extract_binary_payload_from_json(value)
                        if found:
                            return found
                elif isinstance(payload, list):
                    for value in payload:
                        found = _extract_binary_payload_from_json(value)
                        if found:
                            return found
                return None

            async def wait_for_captured_report(
                session_id: str,
                kind: str,
                event_start_index: int,
                timeout: float = 40.0,
            ) -> str | None:
                seen_candidates: list[dict[str, object]] = []
                seen_export_urls: set[str] = set()

                def is_candidate(message: dict) -> bool:
                    method = str(message.get("method") or "")
                    params = message.get("params") or {}
                    if method == "Browser.downloadWillBegin":
                        suggested = str(params.get("suggestedFilename") or "").lower()
                        url = str(params.get("url") or "").lower()
                        if kind == "pix":
                            return (
                                suggested.endswith((".csv", ".xlsx", ".pdf"))
                                or any(token in suggested for token in ("csv", "xlsx", "pix"))
                                or "pix" in url
                            )
                        return suggested.endswith(".pdf") or "pdf" in suggested or "historico" in url
                    if method == "Network.requestWillBeSent":
                        if str(message.get("sessionId") or "") != str(session_id):
                            return False
                        request = params.get("request") or {}
                        url = str(request.get("url") or "").lower()
                        if kind == "pix":
                            return (
                                "pix" in url
                                or "export" in url
                                or "download" in url
                                or "screenservices" in url
                                or "gerar" in url
                                or "arquivo" in url
                            )
                        return "historico" in url and ("export" in url or "download" in url or "pdf" in url)
                    if method != "Network.responseReceived":
                        return False
                    if str(message.get("sessionId") or "") != str(session_id):
                        return False
                    response = params.get("response") or {}
                    url = str(response.get("url") or "").lower()
                    mime_type = str(response.get("mimeType") or "").lower()
                    headers = {str(k).lower(): str(v) for k, v in (response.get("headers") or {}).items()}
                    content_disposition = headers.get("content-disposition", "").lower()
                    if kind == "pix":
                        return (
                            "text/csv" in mime_type
                            or "application/json" in mime_type
                            or "spreadsheet" in mime_type
                            or "excel" in mime_type
                            or ".xlsx" in content_disposition
                            or "csv" in content_disposition
                            or "pdf" in content_disposition
                            or ("pix" in url)
                            or ("export" in url or "download" in url or "screenservices" in url or "gerar" in url or "arquivo" in url)
                        )
                    return (
                        "application/pdf" in mime_type
                        or ".pdf" in url
                        or "pdf" in content_disposition
                        or ("historico" in url and ("export" in url or "download" in url))
                    )

                deadline = time.time() + timeout
                processed_request_ids: set[str] = set()
                browser_download_seen = False

                async def try_browser_export_urls() -> str | None:
                    try:
                        captured_urls = await eval_js(
                            session_id,
                            """
                            (() => {
                                const data = window.__codexExportSignals || {};
                                const urls = [];
                                for (const value of [
                                    ...(data.openedUrls || []),
                                    ...(data.anchorUrls || []),
                                    ...(data.iframeUrls || []),
                                    data.lastUrl || '',
                                ]) {
                                    const text = String(value || '').trim();
                                    if (text) urls.push(text);
                                }
                                return [...new Set(urls)];
                            })()
                            """,
                            timeout=5.0,
                        ) or []
                    except Exception:
                        captured_urls = []
                    for export_url in captured_urls:
                        export_url = str(export_url or "").strip()
                        if not export_url or export_url in seen_export_urls:
                            continue
                        seen_export_urls.add(export_url)
                        try:
                            fetched = await eval_js(
                                session_id,
                                f"""
                                (async () => {{
                                    const url = {export_url!r};
                                    const response = await fetch(url, {{ credentials: 'include' }});
                                    const buffer = await response.arrayBuffer();
                                    const bytes = new Uint8Array(buffer);
                                    let binary = '';
                                    const chunk = 0x8000;
                                    for (let i = 0; i < bytes.length; i += chunk) {{
                                        binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
                                    }}
                                    return {{
                                        ok: response.ok,
                                        status: response.status,
                                        mimeType: response.headers.get('content-type') || '',
                                        contentDisposition: response.headers.get('content-disposition') || '',
                                        body: btoa(binary),
                                    }};
                                }})()
                                """,
                                timeout=60.0,
                            ) or {}
                            if fetched.get("ok") and fetched.get("body"):
                                fetched_mime = str(fetched.get("mimeType") or "").lower()
                                fetched_disp = str(fetched.get("contentDisposition") or "").lower()
                                fetched_suffix = ".csv" if ("csv" in fetched_mime or "csv" in fetched_disp) else ".pdf"
                                fetched_payload = base64.b64decode(str(fetched.get("body") or ""))
                                saved = _save_captured_report_bytes(fetched_payload, kind, fetched_suffix, export_url)
                                if saved:
                                    return saved
                        except Exception:
                            pass
                    return None

                while time.time() < deadline:
                    direct_saved = await try_browser_export_urls()
                    if direct_saved:
                        return direct_saved
                    event = await wait_for_event(is_candidate, timeout=min(5.0, max(0.5, deadline - time.time())), start_index=event_start_index)
                    if not event:
                        await asyncio.sleep(0.5)
                        event_start_index = len(event_log)
                        continue
                    method = str(event.get("method") or "")
                    params = event.get("params") or {}
                    if method == "Browser.downloadWillBegin":
                        browser_download_seen = True
                        await asyncio.sleep(2.0)
                        found = _wait_for_downloaded_report(browser_download_dir, data_br, kind, time.time() - 5.0, timeout=10.0)
                        if found:
                            return _persist_downloaded_report(found, kind)
                        event_start_index = len(event_log)
                        continue

                    if method == "Network.requestWillBeSent":
                        request = params.get("request") or {}
                        seen_candidates.append(
                            {
                                "method": method,
                                "requestId": str(params.get("requestId") or ""),
                                "url": str(request.get("url") or ""),
                                "postData": str(request.get("postData") or "")[:4000],
                            }
                        )
                        event_start_index = len(event_log)
                        continue

                    request_id = str(params.get("requestId") or "")
                    if not request_id or request_id in processed_request_ids:
                        event_start_index = len(event_log)
                        continue
                    processed_request_ids.add(request_id)
                    response = params.get("response") or {}
                    seen_candidates.append(
                        {
                            "method": method,
                            "requestId": request_id,
                            "url": str(response.get("url") or ""),
                            "status": response.get("status"),
                            "mimeType": str(response.get("mimeType") or ""),
                            "headers": {str(k): str(v) for k, v in (response.get("headers") or {}).items()},
                        }
                    )
                    headers = {str(k).lower(): str(v) for k, v in (response.get("headers") or {}).items()}
                    content_disposition = headers.get("content-disposition", "")
                    mime_type = str(response.get("mimeType") or "").lower()
                    disposition_norm = content_disposition.lower()
                    if "csv" in mime_type or "csv" in disposition_norm:
                        suffix = ".csv"
                    elif (
                        "spreadsheet" in mime_type
                        or "excel" in mime_type
                        or ".xlsx" in disposition_norm
                        or "sheet" in disposition_norm
                    ):
                        suffix = ".xlsx"
                    else:
                        suffix = ".pdf"
                    try:
                        body = await cdp("Network.getResponseBody", {"requestId": request_id}, session_id=session_id, timeout=20.0)
                        raw_body = body.get("body") or ""
                        payload = base64.b64decode(raw_body) if body.get("base64Encoded") else str(raw_body).encode("utf-8", errors="ignore")
                        if "application/json" in mime_type:
                            try:
                                json_text = payload.decode("utf-8", errors="ignore")
                                json_payload = json.loads(json_text)
                                try:
                                    debug_path = artifacts_dir / f"azulzinha_export_payload_{kind}.json"
                                    debug_path.write_text(json.dumps(json_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                                except Exception:
                                    pass
                                embedded_binary = _extract_binary_payload_from_json(json_payload)
                                if embedded_binary:
                                    _emit_pix_status(
                                        on_status,
                                        f"Resposta JSON com arquivo embutido detectada para {kind}.",
                                    )
                                    embedded_payload, embedded_suffix = embedded_binary
                                    saved = _save_captured_report_bytes(
                                        embedded_payload,
                                        kind,
                                        embedded_suffix,
                                        str(response.get("url") or ""),
                                    )
                                    if saved:
                                        return saved
                                export_url = _find_export_url_in_payload(json_payload)
                                if export_url:
                                    if export_url.startswith("/"):
                                        base_url = str(response.get("url") or "")
                                        export_url = urljoin(base_url, export_url)
                                    fetched = await eval_js(
                                        session_id,
                                        f"""
                                        (async () => {{
                                            const url = {export_url!r};
                                            const response = await fetch(url, {{ credentials: 'include' }});
                                            const buffer = await response.arrayBuffer();
                                            const bytes = new Uint8Array(buffer);
                                            let binary = '';
                                            const chunk = 0x8000;
                                            for (let i = 0; i < bytes.length; i += chunk) {{
                                                binary += String.fromCharCode(...bytes.subarray(i, i + chunk));
                                            }}
                                            return {{
                                                ok: response.ok,
                                                status: response.status,
                                                mimeType: response.headers.get('content-type') || '',
                                                contentDisposition: response.headers.get('content-disposition') || '',
                                                body: btoa(binary),
                                            }};
                                        }})()
                                        """,
                                        timeout=60.0,
                                    ) or {}
                                    if fetched.get("ok") and fetched.get("body"):
                                        fetched_mime = str(fetched.get("mimeType") or "").lower()
                                        fetched_disp = str(fetched.get("contentDisposition") or "").lower()
                                        if "csv" in fetched_mime or "csv" in fetched_disp:
                                            fetched_suffix = ".csv"
                                        elif (
                                            "spreadsheet" in fetched_mime
                                            or "excel" in fetched_mime
                                            or ".xlsx" in fetched_disp
                                            or "sheet" in fetched_disp
                                        ):
                                            fetched_suffix = ".xlsx"
                                        else:
                                            fetched_suffix = ".pdf"
                                        fetched_payload = base64.b64decode(str(fetched.get("body") or ""))
                                        saved = _save_captured_report_bytes(fetched_payload, kind, fetched_suffix, export_url)
                                        if saved:
                                            return saved
                            except Exception:
                                pass
                        saved = _save_captured_report_bytes(payload, kind, suffix, str(response.get("url") or ""))
                        if saved:
                            return saved
                    except Exception:
                        pass
                    event_start_index = len(event_log)

                if browser_download_seen:
                    return _persist_downloaded_report(
                            _wait_for_downloaded_report(browser_download_dir, data_br, kind, time.time() - 10.0, timeout=10.0),
                        kind,
                    )
                direct_saved = await try_browser_export_urls()
                if direct_saved:
                    return direct_saved
                try:
                    debug_path = artifacts_dir / f"azulzinha_network_candidates_{kind}.json"
                    debug_path.write_text(json.dumps(seen_candidates, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception:
                    pass
                try:
                    export_signals = await eval_js(
                        session_id,
                        """
                        (() => {
                            const data = window.__codexExportSignals || {};
                            return {
                                openedUrls: data.openedUrls || [],
                                anchorUrls: data.anchorUrls || [],
                                iframeUrls: data.iframeUrls || [],
                                lastUrl: data.lastUrl || '',
                            };
                        })()
                        """,
                        timeout=5.0,
                    )
                    debug_signals_path = artifacts_dir / f"azulzinha_export_signals_{kind}.json"
                    debug_signals_path.write_text(json.dumps(export_signals, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception:
                    pass
                return None

            async def navigate(session_id: str, url: str) -> None:
                await cdp("Page.navigate", {"url": url}, session_id=session_id)
                await wait_for_condition(session_id, "document.readyState === 'complete' || document.readyState === 'interactive'", timeout=80.0)

            async def click_by_text(session_id: str, text_options: list[str], timeout: float = 30.0) -> bool:
                options = [_normalize_ascii_text(item).upper() for item in text_options]
                expression = f"""
                (() => {{
                    const targets = {json.dumps(options)};
                    const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                    const visible = (el) => {{
                        const rect = el.getBoundingClientRect();
                        const style = getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                    }};
                    const nodes = [...document.querySelectorAll('button, a, [role="button"], li, span, div, label')].filter(visible);
                    for (const target of targets) {{
                        const matches = nodes
                            .map((el) => {{
                                const text = norm(el.innerText || el.textContent || '');
                                if (!text.includes(target)) return null;
                                const clickable = el.closest('button, a, [role="button"], label') || el.closest('li, div, span') || el;
                                const clickableText = norm(clickable.innerText || clickable.textContent || '');
                                const role = norm(clickable.getAttribute('role') || '');
                                const tag = norm(clickable.tagName || '');
                                const interactiveScore = tag === 'BUTTON' || tag === 'A' || tag === 'LABEL' || role === 'BUTTON' ? 500 : 0;
                                const exactScore = clickableText === target || text === target ? 200 : 0;
                                return {{
                                    clickable,
                                    score: interactiveScore + exactScore - clickableText.length,
                                }};
                            }})
                            .filter(Boolean)
                            .sort((a, b) => b.score - a.score);
                        const match = matches.length ? matches[0].clickable : null;
                        if (match) {{
                            match.scrollIntoView({{ block: 'center', inline: 'center' }});
                            match.click();
                            return true;
                        }}
                    }}
                    return false;
                }})()
                """
                deadline = time.time() + timeout
                while time.time() < deadline:
                    _check_cancelled()
                    if await eval_js(session_id, expression):
                        return True
                    await asyncio.sleep(0.5)
                return False

            async def click_card_by_text(session_id: str, text_options: list[str], timeout: float = 30.0) -> bool:
                options = [_normalize_ascii_text(item).upper() for item in text_options]
                expression = f"""
                (() => {{
                    const targets = {json.dumps(options)};
                    const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                    const visible = (el) => {{
                        const rect = el.getBoundingClientRect();
                        const style = getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                    }};
                    const nodes = [...document.querySelectorAll('[id$="-Content"], .card-content, .ph.card.card-content')].filter(visible);
                    for (const target of targets) {{
                        const matches = nodes
                            .map((el) => {{
                                const text = norm(el.innerText || el.textContent);
                                if (!text.includes(target)) return null;
                                const className = (el.className || '').toString().toUpperCase();
                                const id = (el.id || '').toUpperCase();
                                const isCard = className.includes('CARD-CONTENT') || className.includes('PH CARD');
                                const exact = text === target || text.endsWith(target);
                                return {{
                                    el,
                                    score: (exact ? 1000 : 0) + (isCard ? 200 : 0) - text.length,
                                }};
                            }})
                            .filter(Boolean)
                            .sort((a, b) => b.score - a.score);
                        const node = matches.length ? matches[0].el : null;
                        if (node) {{
                            node.scrollIntoView({{ block: 'center', inline: 'center' }});
                            node.click();
                            return true;
                        }}
                    }}
                    return false;
                }})()
                """
                deadline = time.time() + timeout
                while time.time() < deadline:
                    _check_cancelled()
                    if await eval_js(session_id, expression):
                        return True
                    await asyncio.sleep(0.5)
                return False

            async def activate_sales_tab(session_id: str, tab_id: str, timeout: float = 40.0) -> None:
                expected_content = f"{tab_id}Content"
                expression = f"""
                (() => {{
                    const root = document.getElementById({tab_id!r});
                    if (!root) return 'missing-tab';
                    const button = root.querySelector('button[role="tab"]');
                    if (!button) return 'missing-button';
                    const content = document.getElementById({expected_content!r})?.querySelector('[role="tabpanel"]');
                    if (button.getAttribute('aria-selected') === 'true' && content && content.getAttribute('aria-hidden') === 'false') {{
                        return 'ready';
                    }}
                    button.scrollIntoView({{ block: 'center', inline: 'center' }});
                    button.focus();
                    button.dispatchEvent(new MouseEvent('mousedown', {{ bubbles: true, cancelable: true, view: window }}));
                    button.click();
                    button.dispatchEvent(new MouseEvent('mouseup', {{ bubbles: true, cancelable: true, view: window }}));
                    return 'clicked';
                }})()
                """
                deadline = time.time() + timeout
                last_state = ""
                while time.time() < deadline:
                    _check_cancelled()
                    state = await eval_js(session_id, expression)
                    last_state = str(state or "")
                    if state in {"missing-tab", "missing-button"}:
                        await asyncio.sleep(0.5)
                        continue
                    try:
                        await wait_for_condition(
                            session_id,
                            f"""
                            (() => {{
                                const button = document.querySelector("#{tab_id} button[role='tab']");
                                const content = document.querySelector("#{expected_content} [role='tabpanel']");
                                return !!button && !!content &&
                                    button.getAttribute('aria-selected') === 'true' &&
                                    content.getAttribute('aria-hidden') === 'false';
                            }})()
                            """,
                            timeout=2.5,
                            step=0.25,
                        )
                        return
                    except Exception:
                        await asyncio.sleep(0.5)
                try:
                    html_debug = await eval_js(
                        session_id,
                        "document.documentElement ? document.documentElement.outerHTML : ''",
                        timeout=15.0,
                    )
                    if html_debug:
                        debug_path = artifacts_dir / f"azulzinha_tabs_debug_{tab_id}.html"
                        debug_path.write_text(str(html_debug), encoding="utf-8")
                except Exception:
                    pass
                if last_state == "missing-tab":
                    raise RuntimeError(f"Nao foi possivel localizar a aba {tab_id} no portal Azulzinha/Caixa.")
                if last_state == "missing-button":
                    raise RuntimeError(f"Nao foi possivel localizar o botao da aba {tab_id} no portal Azulzinha/Caixa.")
                raise RuntimeError(f"Nao foi possivel abrir a aba {tab_id} no portal Azulzinha/Caixa.")

            async def wait_for_portal_settle(
                session_id: str,
                tab_id: str | None = None,
                timeout: float = 20.0,
                context_label: str = "a tela atual",
            ) -> None:
                content_selector = (
                    f"#{tab_id}Content [role='tabpanel']"
                    if tab_id
                    else "header[role='tablist'], #HistoricoVendas button[role='tab'], #Pix button[role='tab']"
                )
                settle_expression = f"""
                    (() => {{
                        const text = (document.body?.innerText || '')
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .toUpperCase();
                        const href = String(location.href || '').toUpperCase();
                        const visible = (el) => {{
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        }};
                        const loginInput = document.querySelector('#b2-b1-b4-InputMask');
                        const passwordInput = document.querySelector('#b2-b1-Input_Password');
                        const loginTextVisible = (
                            text.includes('ACESSE SUA CONTA') ||
                            (text.includes('CNPJ, CPF OU USUARIO') && text.includes('SENHA'))
                        );
                        const deviceVisible = text.includes('SELECIONE O DISPOSITIVO');
                        const tokenDeliveryVisible = (
                            text.includes('ESCOLHA POR ONDE DESEJA RECEBER') ||
                            text.includes('RECEBER POR E-MAIL') ||
                            text.includes('@GMAIL.COM')
                        );
                        const tokenVisible = Boolean(
                            [...document.querySelectorAll('input')]
                                .filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled && visible(el))
                                .length
                        ) && (
                            text.includes('TOKEN') ||
                            text.includes('CODIGO') ||
                            text.includes('E-MAIL') ||
                            text.includes('EMAIL')
                        );
                        const invalidVisible = (
                            text.includes('CODIGO INVALIDO') ||
                            text.includes('TOKEN INVALIDO') ||
                            text.includes('TOKEN EXPIRADO') ||
                            text.includes('TENTATIVAS')
                        );
                        if (href.includes('/_ERROR.HTML') || text.includes('ERROR PROCESSING YOUR REQUEST')) return 'portal_error';
                        if (
                            href.includes('ISTIMEOUT=TRUE') ||
                            (visible(loginInput) && visible(passwordInput)) ||
                            (
                                loginTextVisible &&
                                !deviceVisible &&
                                !tokenDeliveryVisible &&
                                !tokenVisible &&
                                !invalidVisible
                            )
                        ) return 'login';
                        const content = document.querySelector({content_selector!r});
                        if (!content || (content.getAttribute && content.getAttribute('aria-hidden') === 'true')) return '';
                        const busyNodes = [
                            ...document.querySelectorAll('[aria-busy="true"], .ph-item, .ph-picture, .ph-picture-small, .spinner-border, .spinner-grow, .loading, .skeleton, .ant-skeleton')
                        ].filter(visible);
                        if (busyNodes.length) return '';
                        const body = (content.innerText || content.textContent || document.body?.innerText || '').trim();
                        return body.length > 20 ? 'ready' : '';
                    }})()
                    """
                try:
                    state = await wait_for_condition(
                        session_id,
                        settle_expression,
                        timeout=timeout,
                        step=0.4,
                    )
                except Exception:
                    await capture_portal_html_debug_v2(session_id, "azulzinha_portal_settle_debug.html")
                    raise
                if state == "portal_error":
                    raise RuntimeError(f"A Caixa abriu a pagina de erro durante {context_label}.")
                if state == "login":
                    raise RuntimeError(f"A Caixa voltou para o login durante {context_label}.")

            async def wait_for_tab_content(session_id: str, tab_id: str, timeout: float = 45.0) -> None:
                content_id = f"{tab_id}Content"
                state = await wait_for_condition(
                    session_id,
                    f"""
                    (() => {{
                        const text = (document.body?.innerText || '')
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .toUpperCase();
                        const href = String(location.href || '').toUpperCase();
                        if (href.includes('/_ERROR.HTML') || text.includes('ERROR PROCESSING YOUR REQUEST')) return 'portal_error';
                        const content = document.querySelector("#{content_id} [role='tabpanel']");
                        if (!content || content.getAttribute('aria-hidden') !== 'false') return '';
                        const body = content.innerText || content.textContent || '';
                        const html = content.innerHTML || '';
                        const visible = (el) => {{
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        }};
                        const busyNodes = [...content.querySelectorAll('[aria-busy="true"], .ph-item, .ph-picture, .ph-picture-small, .spinner-border, .spinner-grow, .loading, .skeleton, .ant-skeleton')]
                            .filter(visible);
                        if (busyNodes.length) return '';
                        if (body.trim().length > 40 || html.includes('data-testid') || html.includes('Exportar') || html.includes('Filtro')) return 'ready';
                        return '';
                    }})()
                    """,
                    timeout=timeout,
                    step=0.5,
                )
                if state == "portal_error":
                    try:
                        html_debug = await eval_js(
                            session_id,
                            "document.documentElement ? document.documentElement.outerHTML : ''",
                            timeout=15.0,
                        )
                        if html_debug:
                            debug_path = artifacts_dir / f"azulzinha_tab_error_{tab_id}.html"
                            debug_path.write_text(str(html_debug), encoding="utf-8")
                    except Exception:
                        pass
                    raise RuntimeError(f"A Caixa abriu a pagina de erro ao carregar a aba {tab_id}.")
                await asyncio.sleep(0.6)
                await wait_for_portal_settle(
                    session_id,
                    tab_id=tab_id,
                    timeout=20.0,
                    context_label=f"carregar a aba {tab_id}",
                )

            async def focus_selector(session_id: str, selector: str) -> bool:
                return bool(
                    await eval_js(
                        session_id,
                        f"""
                        (() => {{
                            const el = document.querySelector({selector!r});
                            if (!el) return false;
                            el.focus();
                            if ('value' in el) el.value = '';
                            return true;
                        }})()
                        """,
                    )
                )

            async def insert_text(session_id: str, selector: str, text: str) -> None:
                if not await focus_selector(session_id, selector):
                    raise RuntimeError(f"Nao foi possivel localizar o campo {selector} na Azulzinha/Caixa.")
                for char in str(text or ""):
                    await cdp("Input.insertText", {"text": char}, session_id=session_id)
                    await asyncio.sleep(0.04)
                await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const el = document.querySelector({selector!r});
                        if (!el) return false;
                        el.dispatchEvent(new KeyboardEvent('keyup', {{ bubbles: true, key: '0' }}));
                        el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                        el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                        return true;
                    }})()
                    """,
                )

            async def press_tab(session_id: str) -> None:
                await cdp(
                    "Input.dispatchKeyEvent",
                    {"type": "keyDown", "windowsVirtualKeyCode": 9, "nativeVirtualKeyCode": 9, "key": "Tab", "code": "Tab"},
                    session_id=session_id,
                )
                await cdp(
                    "Input.dispatchKeyEvent",
                    {"type": "keyUp", "windowsVirtualKeyCode": 9, "nativeVirtualKeyCode": 9, "key": "Tab", "code": "Tab"},
                    session_id=session_id,
                )

            async def click_selector_native(session_id: str, selector: str, root_selector: str | None = None) -> bool:
                rect = await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const root = {root_selector!r} ? document.querySelector({root_selector!r}) : document;
                        if (!root) return null;
                        const el = [...root.querySelectorAll({selector!r})].find((candidate) => {{
                            const r = candidate.getBoundingClientRect();
                            const style = getComputedStyle(candidate);
                            return r.width > 0 && r.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        }});
                        if (!el) return null;
                        const r = el.getBoundingClientRect();
                        const style = getComputedStyle(el);
                        if (r.width <= 0 || r.height <= 0 || style.visibility === 'hidden' || style.display === 'none') return null;
                        return {{
                            x: r.left + (r.width / 2),
                            y: r.top + (r.height / 2),
                        }};
                    }})()
                    """,
                )
                if not rect:
                    return False
                await cdp(
                    "Input.dispatchMouseEvent",
                    {"type": "mousePressed", "x": rect["x"], "y": rect["y"], "button": "left", "clickCount": 1},
                    session_id=session_id,
                )
                await cdp(
                    "Input.dispatchMouseEvent",
                    {"type": "mouseReleased", "x": rect["x"], "y": rect["y"], "button": "left", "clickCount": 1},
                    session_id=session_id,
                )
                return True

            async def set_date_inputs(session_id: str) -> None:
                ok = await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const value = {data_br!r};
                        const isoValue = value.split('/').reverse().join('-');
                        const digits = value.replace(/\\D/g, '');
                        const setNativeValue = (el, val) => {{
                            const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
                            const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
                            if (setter) setter.call(el, val); else el.value = val;
                            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            el.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                        }};
                        const inputs = [...document.querySelectorAll('input')]
                            .filter((el) => {{
                                const type = (el.type || '').toLowerCase();
                                const name = ((el.name || '') + ' ' + (el.id || '') + ' ' + (el.placeholder || '') + ' ' + (el.getAttribute('aria-label') || '')).toLowerCase();
                                return type !== 'hidden' && !el.disabled && (
                                    type === 'date' || name.includes('data') || name.includes('periodo') || /\\d{{2}}\\/\\d{{2}}\\/\\d{{4}}/.test(el.value || '')
                                );
                            }});
                        if (!inputs.length) return false;
                        const flatpickrInput = document.querySelector('input[id$="InputPicker"]');
                        if (flatpickrInput && flatpickrInput._flatpickr) {{
                            try {{
                                flatpickrInput._flatpickr.setDate([isoValue, isoValue], true, 'Y-m-d');
                            }} catch (_err) {{}}
                        }}
                        for (const input of inputs.slice(0, 2)) {{
                            input.focus();
                            setNativeValue(input, (input.type || '').toLowerCase() === 'date' ? isoValue : digits);
                            setNativeValue(input, (input.type || '').toLowerCase() === 'date' ? isoValue : value);
                        }}
                        return true;
                    }})()
                    """,
                )
                if not ok:
                    ok = await eval_js(
                        session_id,
                        f"""
                        (() => {{
                            const value = {data_br!r};
                            const visible = (el) => {{
                                const rect = el.getBoundingClientRect();
                                const style = getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                            }};
                            const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                            const clickable = [...document.querySelectorAll('input, button, a, [role="button"], div, span')]
                                .filter(visible)
                                .find((el) => {{
                                    const text = norm(el.innerText || el.textContent || el.placeholder || el.getAttribute('aria-label') || '');
                                    const id = norm(el.id || '');
                                    return text.includes('DATA') || text.includes('PERIODO') || id.includes('DATA') || id.includes('PERIODO');
                                }});
                            if (!clickable) return false;
                            clickable.click();
                            const candidates = [...document.querySelectorAll('input')]
                                .filter((el) => visible(el) && (el.type || '').toLowerCase() !== 'hidden' && !el.disabled);
                            const target = candidates.find((el) => {{
                                const meta = norm((el.name || '') + ' ' + (el.id || '') + ' ' + (el.placeholder || '') + ' ' + (el.getAttribute('aria-label') || ''));
                                return meta.includes('DATA') || meta.includes('PERIODO');
                            }}) || candidates[0];
                            if (!target) return false;
                            const proto = target instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
                            const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
                            const finalValue = (target.type || '').toLowerCase() === 'date' ? value.split('/').reverse().join('-') : value;
                            target.focus();
                            if (setter) setter.call(target, finalValue); else target.value = finalValue;
                            target.dispatchEvent(new Event('input', {{ bubbles: true }}));
                            target.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            target.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                            return true;
                        }})()
                        """,
                    )
                if not ok:
                    try:
                        html_debug = await eval_js(
                            session_id,
                            "document.documentElement ? document.documentElement.outerHTML : ''",
                            timeout=15.0,
                        )
                        if html_debug:
                            debug_path = artifacts_dir / "azulzinha_pix_date_debug.html"
                            debug_path.write_text(str(html_debug), encoding="utf-8")
                    except Exception:
                        pass
                    raise RuntimeError("Nao foi possivel preencher a data no portal Azulzinha/Caixa.")
                await eval_js(
                    session_id,
                    """
                    (() => {
                        const visible = (el) => {
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        };
                        const button = document.querySelector('[data-testid="generic-calendar-button-aplicar"]');
                        if (!button || !visible(button)) return false;
                        button.scrollIntoView({ block: 'center', inline: 'center' });
                        button.click();
                        return true;
                    })()
                    """,
                )
                try:
                    await wait_for_condition(
                        session_id,
                        """
                        (() => {
                            const wrapper = document.querySelector('[data-testid="calendar-main-div"]')?.closest('[aria-hidden]');
                            return !wrapper || wrapper.getAttribute('aria-hidden') === 'true';
                        })()
                        """,
                        timeout=15.0,
                        step=0.25,
                    )
                except Exception:
                    pass
                await asyncio.sleep(0.8)

            async def ensure_all_establishments_selected(
                session_id: str,
                tab_id: str,
                timeout: float = 25.0,
            ) -> bool:
                expression = f"""
                (() => {{
                    const visible = (el) => {{
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        const style = getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                    }};
                    const content = document.querySelector("#{tab_id}Content [role='tabpanel']");
                    if (!content || content.getAttribute('aria-hidden') === 'true') return 'missing-content';
                    const filterTrigger = [
                        ...content.querySelectorAll(
                            '[data-testid="vendas-hoje-link-filtrar"], [data-testid="historico-vendas-container-filtros"], [data-testid*="container-filtros"], [data-testid*="link-filtrar"]'
                        )
                    ].find(visible);
                    const sidebar = [
                        ...document.querySelectorAll('aside.osui-sidebar, [role="complementary"]')
                    ].find((el) => visible(el) && (
                        el.querySelector('[data-testid="generic-filter-btn-resultados"]') ||
                        el.querySelector('[data-testid="generic-filter-accordion-title-estabelecimentos"]') ||
                        el.querySelector('[data-testid="generic-filter-check-all-estabelecimentos"]')
                    ));
                    if (!filterTrigger && !sidebar) return 'no-filter';
                    if (!sidebar) {{
                        if (!filterTrigger) return 'no-filter';
                        filterTrigger.scrollIntoView({{ block: 'center', inline: 'center' }});
                        filterTrigger.click();
                        return 'opening';
                    }}
                    const title = sidebar.querySelector('[data-testid="generic-filter-accordion-title-estabelecimentos"]');
                    if (!title) return 'no-estabelecimentos';
                    const titleWrapper = title.closest('[role="button"]') || title.closest('.osui-accordion-item__title') || title;
                    const accordionItem = title.closest('.osui-accordion-item');
                    const contentWrapper = accordionItem
                        ? accordionItem.querySelector('.osui-accordion-item__content, [id$="ContentWrapper"]')
                        : null;
                    const collapsed = Boolean(
                        contentWrapper && (
                            contentWrapper.getAttribute('aria-hidden') === 'true' ||
                            String(contentWrapper.className || '').includes('--is-collapsed')
                        )
                    );
                    if (collapsed) {{
                        titleWrapper.scrollIntoView({{ block: 'center', inline: 'center' }});
                        titleWrapper.click();
                        return 'expanding';
                    }}
                    const establishmentChecks = [
                        ...sidebar.querySelectorAll('[data-testid^="generic-filter-check-estabelecimento-"]')
                    ].filter((el) => !el.disabled);
                    if (!establishmentChecks.length) return 'no-estabelecimentos';
                    if (establishmentChecks.length > 1) {{
                        const checkedCount = establishmentChecks.filter((el) => el.checked).length;
                        if (checkedCount < establishmentChecks.length) {{
                            const checkAll = sidebar.querySelector('[data-testid="generic-filter-check-all-estabelecimentos"]');
                            if (checkAll && !checkAll.disabled && visible(checkAll)) {{
                                checkAll.click();
                                return 'selecting-all';
                            }}
                            const unchecked = establishmentChecks.filter((el) => !el.checked);
                            for (const checkbox of unchecked) {{
                                checkbox.click();
                            }}
                            return unchecked.length ? 'selecting-manual' : 'selected';
                        }}
                    }}
                    const applyButton = sidebar.querySelector('[data-testid="generic-filter-btn-resultados"]');
                    if (!applyButton || !visible(applyButton) || applyButton.disabled) return 'ready';
                    applyButton.click();
                    return establishmentChecks.length > 1 ? 'applied-multiple' : 'applied-single';
                }})()
                """
                deadline = time.time() + timeout
                found_filter = False
                applied_multiple = False
                while time.time() < deadline:
                    _check_cancelled()
                    state = str(await eval_js(session_id, expression, timeout=15.0) or "").strip()
                    if state in {
                        "opening",
                        "expanding",
                        "selecting-all",
                        "selecting-manual",
                        "selected",
                    }:
                        found_filter = True
                        await asyncio.sleep(0.6)
                        continue
                    if state in {"applied-multiple", "applied-single"}:
                        found_filter = True
                        applied_multiple = state == "applied-multiple"
                        try:
                            await wait_for_condition(
                                session_id,
                                """
                                (() => {
                                    const visible = (el) => {
                                        if (!el) return false;
                                        const rect = el.getBoundingClientRect();
                                        const style = getComputedStyle(el);
                                        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                    };
                                    const sidebar = [
                                        ...document.querySelectorAll('aside.osui-sidebar, [role="complementary"]')
                                    ].find((el) => visible(el) && (
                                        el.querySelector('[data-testid="generic-filter-btn-resultados"]') ||
                                        el.querySelector('[data-testid="generic-filter-accordion-title-estabelecimentos"]')
                                    ));
                                    return !sidebar;
                                })()
                                """,
                                timeout=15.0,
                                step=0.25,
                            )
                        except Exception:
                            pass
                        await wait_for_portal_settle(
                            session_id,
                            tab_id=tab_id,
                            timeout=20.0,
                            context_label=f"aplicar os estabelecimentos da aba {tab_id}",
                        )
                        return applied_multiple
                    if state in {"ready", "no-estabelecimentos"}:
                        return applied_multiple
                    if state in {"no-filter", "missing-content"}:
                        return False
                    await asyncio.sleep(0.5)
                if found_filter:
                    raise RuntimeError(
                        f"A Caixa nao concluiu a aplicacao do filtro de estabelecimentos na aba {tab_id}."
                    )
                return False

            async def list_establishment_filter_options(
                session_id: str,
                tab_id: str,
                timeout: float = 25.0,
            ) -> list[str]:
                expression = f"""
                (() => {{
                    const visible = (el) => {{
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        const style = getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                    }};
                    const content = document.querySelector("#{tab_id}Content [role='tabpanel']");
                    if (!content || content.getAttribute('aria-hidden') === 'true') return '';
                    const filterTrigger = [
                        ...content.querySelectorAll(
                            '[data-testid="vendas-hoje-link-filtrar"], [data-testid="historico-vendas-container-filtros"], [data-testid*="container-filtros"], [data-testid*="link-filtrar"]'
                        )
                    ].find(visible);
                    const sidebar = [
                        ...document.querySelectorAll('aside.osui-sidebar, [role="complementary"]')
                    ].find((el) => visible(el) && (
                        el.querySelector('[data-testid="generic-filter-btn-resultados"]') ||
                        el.querySelector('[data-testid="generic-filter-accordion-title-estabelecimentos"]')
                    ));
                    if (!filterTrigger && !sidebar) return JSON.stringify([]);
                    if (!sidebar) {{
                        if (!filterTrigger) return JSON.stringify([]);
                        filterTrigger.scrollIntoView({{ block: 'center', inline: 'center' }});
                        filterTrigger.click();
                        return '';
                    }}
                    const title = sidebar.querySelector('[data-testid="generic-filter-accordion-title-estabelecimentos"]');
                    if (!title) return JSON.stringify([]);
                    const titleWrapper = title.closest('[role="button"]') || title.closest('.osui-accordion-item__title') || title;
                    const accordionItem = title.closest('.osui-accordion-item');
                    const contentWrapper = accordionItem
                        ? accordionItem.querySelector('.osui-accordion-item__content, [id$="ContentWrapper"]')
                        : null;
                    const collapsed = Boolean(
                        contentWrapper && (
                            contentWrapper.getAttribute('aria-hidden') === 'true' ||
                            String(contentWrapper.className || '').includes('--is-collapsed')
                        )
                    );
                    if (collapsed) {{
                        titleWrapper.scrollIntoView({{ block: 'center', inline: 'center' }});
                        titleWrapper.click();
                        return '';
                    }}
                    const options = [
                        ...sidebar.querySelectorAll('[data-testid^="generic-filter-check-estabelecimento-"]')
                    ]
                        .filter((el) => !el.disabled)
                        .map((el) => String(el.getAttribute('data-testid') || '').replace('generic-filter-check-estabelecimento-', '').trim())
                        .filter(Boolean);
                    return JSON.stringify(options);
                }})()
                """
                deadline = time.time() + timeout
                while time.time() < deadline:
                    _check_cancelled()
                    raw = await eval_js(session_id, expression, timeout=15.0)
                    text = str(raw or "").strip()
                    if not text:
                        await asyncio.sleep(0.5)
                        continue
                    try:
                        options = json.loads(text)
                    except Exception:
                        options = []
                    if isinstance(options, list):
                        return [str(item).strip() for item in options if str(item).strip()]
                    return []
                return []

            async def apply_establishment_filter_selection(
                session_id: str,
                tab_id: str,
                selected_ids: list[str],
                timeout: float = 25.0,
            ) -> int:
                desired_ids = [str(item).strip() for item in selected_ids if str(item).strip()]
                if not desired_ids:
                    return 0
                desired_ids_js = json.dumps(desired_ids, ensure_ascii=False)
                expression = f"""
                (() => {{
                    const desired = new Set({desired_ids_js});
                    const visible = (el) => {{
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        const style = getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                    }};
                    const content = document.querySelector("#{tab_id}Content [role='tabpanel']");
                    if (!content || content.getAttribute('aria-hidden') === 'true') return 'missing-content';
                    const filterTrigger = [
                        ...content.querySelectorAll(
                            '[data-testid="vendas-hoje-link-filtrar"], [data-testid="historico-vendas-container-filtros"], [data-testid*="container-filtros"], [data-testid*="link-filtrar"]'
                        )
                    ].find(visible);
                    const sidebar = [
                        ...document.querySelectorAll('aside.osui-sidebar, [role="complementary"]')
                    ].find((el) => visible(el) && (
                        el.querySelector('[data-testid="generic-filter-btn-resultados"]') ||
                        el.querySelector('[data-testid="generic-filter-accordion-title-estabelecimentos"]')
                    ));
                    if (!filterTrigger && !sidebar) return 'no-filter';
                    if (!sidebar) {{
                        if (!filterTrigger) return 'no-filter';
                        filterTrigger.scrollIntoView({{ block: 'center', inline: 'center' }});
                        filterTrigger.click();
                        return 'opening';
                    }}
                    const title = sidebar.querySelector('[data-testid="generic-filter-accordion-title-estabelecimentos"]');
                    if (!title) return 'no-estabelecimentos';
                    const titleWrapper = title.closest('[role="button"]') || title.closest('.osui-accordion-item__title') || title;
                    const accordionItem = title.closest('.osui-accordion-item');
                    const contentWrapper = accordionItem
                        ? accordionItem.querySelector('.osui-accordion-item__content, [id$="ContentWrapper"]')
                        : null;
                    const collapsed = Boolean(
                        contentWrapper && (
                            contentWrapper.getAttribute('aria-hidden') === 'true' ||
                            String(contentWrapper.className || '').includes('--is-collapsed')
                        )
                    );
                    if (collapsed) {{
                        titleWrapper.scrollIntoView({{ block: 'center', inline: 'center' }});
                        titleWrapper.click();
                        return 'expanding';
                    }}
                    const establishmentChecks = [
                        ...sidebar.querySelectorAll('[data-testid^="generic-filter-check-estabelecimento-"]')
                    ]
                        .filter((el) => !el.disabled)
                        .map((el) => {{
                            const value = String(el.getAttribute('data-testid') || '').replace('generic-filter-check-estabelecimento-', '').trim();
                            return {{ el, value }};
                        }})
                        .filter((entry) => entry.value);
                    if (!establishmentChecks.length) return 'no-estabelecimentos';
                    const needsToggle = establishmentChecks.filter((entry) => desired.has(entry.value) !== entry.el.checked);
                    if (needsToggle.length) {{
                        needsToggle[0].el.click();
                        return 'toggling';
                    }}
                    const applyButton = sidebar.querySelector('[data-testid="generic-filter-btn-resultados"]');
                    if (!applyButton || !visible(applyButton) || applyButton.disabled) return 'ready';
                    applyButton.click();
                    return 'applied';
                }})()
                """
                deadline = time.time() + timeout
                while time.time() < deadline:
                    _check_cancelled()
                    state = str(await eval_js(session_id, expression, timeout=15.0) or "").strip()
                    if state in {"opening", "expanding", "toggling"}:
                        await asyncio.sleep(0.5)
                        continue
                    if state == "applied":
                        try:
                            await wait_for_condition(
                                session_id,
                                """
                                (() => {
                                    const visible = (el) => {
                                        if (!el) return false;
                                        const rect = el.getBoundingClientRect();
                                        const style = getComputedStyle(el);
                                        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                    };
                                    const sidebar = [
                                        ...document.querySelectorAll('aside.osui-sidebar, [role="complementary"]')
                                    ].find((el) => visible(el) && (
                                        el.querySelector('[data-testid="generic-filter-btn-resultados"]') ||
                                        el.querySelector('[data-testid="generic-filter-accordion-title-estabelecimentos"]')
                                    ));
                                    return !sidebar;
                                })()
                                """,
                                timeout=15.0,
                                step=0.25,
                            )
                        except Exception:
                            pass
                        await wait_for_portal_settle(
                            session_id,
                            tab_id=tab_id,
                            timeout=20.0,
                            context_label=f"aplicar o estabelecimento {', '.join(desired_ids)} na aba {tab_id}",
                        )
                        return len(desired_ids)
                    if state in {"ready", "no-estabelecimentos"}:
                        return len(desired_ids)
                    if state in {"no-filter", "missing-content"}:
                        return 0
                    await asyncio.sleep(0.5)
                raise RuntimeError(
                    f"A Caixa nao concluiu a selecao de estabelecimento(s) na aba {tab_id}."
                )

            async def wait_for_export_ready(
                session_id: str,
                kind: str,
                *,
                tab_id: str | None = None,
                timeout: float = 60.0,
            ) -> None:
                export_selector = '[data-testid="exportar-pix-van"]' if kind == "pix" else '[data-testid^="exportar-"]'
                content_id = f"{tab_id}Content" if tab_id else ("PixContent" if kind == "pix" else "HistoricoVendasContent")
                expression = f"""
                (() => {{
                    const text = (document.body?.innerText || '')
                        .normalize('NFD')
                        .replace(/[\\u0300-\\u036f]/g, '')
                        .toUpperCase();
                    const href = String(location.href || '').toUpperCase();
                    if (href.includes('/_ERROR.HTML') || text.includes('ERROR PROCESSING YOUR REQUEST')) return 'portal_error';
                    const content = document.querySelector("#{content_id} [role='tabpanel']");
                    if (!content || content.getAttribute('aria-hidden') === 'true') return '';
                    const visible = (el) => {{
                        if (!el) return false;
                        const rect = el.getBoundingClientRect();
                        const style = getComputedStyle(el);
                        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                    }};
                    const busyNodes = [...content.querySelectorAll('[aria-busy="true"], .ph-item, .ph-picture, .ph-picture-small, .spinner-border, .spinner-grow, .loading, .skeleton, .ant-skeleton')]
                        .filter(visible);
                    if (busyNodes.length) return '';
                    const exportBtn = [...content.querySelectorAll({export_selector!r})].find(visible);
                    if (!exportBtn) return '';
                    return !exportBtn.disabled ? 'ready' : '';
                }})()
                """
                try:
                    state = await wait_for_condition(
                        session_id,
                        expression,
                        timeout=timeout,
                        step=0.5,
                    )
                    if state == "ready":
                        return
                except Exception:
                    try:
                        html_debug = await eval_js(
                            session_id,
                            "document.documentElement ? document.documentElement.outerHTML : ''",
                            timeout=15.0,
                        )
                        if html_debug:
                            debug_path = artifacts_dir / f"azulzinha_export_ready_debug_{kind}.html"
                            debug_path.write_text(str(html_debug), encoding="utf-8")
                    except Exception:
                        pass
                    try:
                        state_debug = await eval_js(
                            session_id,
                            f"""
                            (() => {{
                                const content = document.querySelector("#{content_id} [role='tabpanel']");
                                const exportBtn = content ? [...content.querySelectorAll({export_selector!r})].find((el) => {{
                                    const rect = el.getBoundingClientRect();
                                    const style = getComputedStyle(el);
                                    return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                }}) : null;
                                const placeholderNodes = content ? [...content.querySelectorAll('.ph-item, .ph-picture, .ph-picture-small')] : [];
                                return {{
                                    contentFound: !!content,
                                    contentHidden: content ? content.getAttribute('aria-hidden') : null,
                                    exportFound: !!exportBtn,
                                    exportDisabled: exportBtn ? !!exportBtn.disabled : null,
                                    exportText: exportBtn ? (exportBtn.innerText || exportBtn.textContent || '').trim() : '',
                                    placeholderCount: placeholderNodes.length,
                                    visibleButtons: [...document.querySelectorAll('button, a, [role="button"]')].map((el) => {{
                                        const rect = el.getBoundingClientRect();
                                        const style = getComputedStyle(el);
                                        return {{
                                            text: (el.innerText || el.textContent || '').trim(),
                                            testid: el.getAttribute('data-testid') || '',
                                            disabled: !!el.disabled,
                                            visible: rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none',
                                        }};
                                    }}).filter((item) => item.visible),
                                }};
                            }})()
                            """,
                            timeout=15.0,
                        )
                        if state_debug is not None:
                            debug_state_path = artifacts_dir / f"azulzinha_export_ready_state_{kind}.json"
                            debug_state_path.write_text(json.dumps(state_debug, ensure_ascii=False, indent=2), encoding="utf-8")
                    except Exception:
                        pass
                    raise
                try:
                    html_debug = await eval_js(
                        session_id,
                        "document.documentElement ? document.documentElement.outerHTML : ''",
                        timeout=15.0,
                    )
                    if html_debug:
                        debug_path = artifacts_dir / f"azulzinha_export_error_{kind}.html"
                        debug_path.write_text(str(html_debug), encoding="utf-8")
                except Exception:
                    pass
                raise RuntimeError(
                    f"A Caixa abriu a pagina de erro ao preparar a exportacao do relatorio de {'PIX' if kind == 'pix' else 'cartoes'}."
                )

            async def click_export(session_id: str, kind: str, tab_id: str | None = None) -> None:
                tab_content_selector = f"#{tab_id}Content [role='tabpanel']" if tab_id else None
                await eval_js(
                    session_id,
                    """
                    (() => {
                        if (window.__codexExportHookInstalled) {
                            if (window.__codexExportSignals) {
                                window.__codexExportSignals.openedUrls = [];
                                window.__codexExportSignals.anchorUrls = [];
                                window.__codexExportSignals.iframeUrls = [];
                                window.__codexExportSignals.lastUrl = '';
                            }
                            return true;
                        }
                        const data = window.__codexExportSignals = {
                            openedUrls: [],
                            anchorUrls: [],
                            iframeUrls: [],
                            lastUrl: '',
                        };
                        const pushUrl = (value) => {
                            const text = String(value || '').trim();
                            if (!text) return;
                            data.lastUrl = text;
                            if (/^https?:/i.test(text) || text.startsWith('/')) {
                                data.openedUrls.push(text);
                            }
                        };
                        const originalOpen = window.open;
                        window.open = function(url, ...args) {
                            pushUrl(url);
                            return originalOpen ? originalOpen.call(this, url, ...args) : null;
                        };
                        const anchorClick = HTMLAnchorElement.prototype.click;
                        HTMLAnchorElement.prototype.click = function(...args) {
                            pushUrl(this.href || this.getAttribute('href') || '');
                            return anchorClick.apply(this, args);
                        };
                        const observer = new MutationObserver(() => {
                            document.querySelectorAll('iframe, embed, object, a[href], a[download]').forEach((el) => {
                                const url = el.src || el.data || el.href || el.getAttribute('href') || '';
                                if (!url) return;
                                if (el.tagName === 'A') data.anchorUrls.push(url);
                                else data.iframeUrls.push(url);
                                data.lastUrl = String(url);
                            });
                        });
                        observer.observe(document.documentElement || document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ['src', 'data', 'href'] });
                        window.__codexExportHookInstalled = true;
                        return true;
                    })()
                    """,
                )
                await eval_js(
                    session_id,
                    """
                    (() => {
                        const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                        const visible = (el) => {
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        };
                        const buttons = [...document.querySelectorAll('button, a, [role="button"], div.btn')].filter(visible);
                        const applyButton = buttons.find((el) => {
                            const text = norm(el.innerText || el.textContent || el.getAttribute('aria-label') || '');
                            return text.includes('MOSTRAR RESULTADOS') || text === 'APLICAR';
                        });
                        if (!applyButton) return false;
                        applyButton.scrollIntoView({ block: 'center', inline: 'center' });
                        applyButton.click();
                        return true;
                    })()
                    """,
                )
                try:
                    await wait_for_export_ready(session_id, kind, tab_id=tab_id, timeout=45.0)
                except Exception:
                    pass
                direct_selectors = (
                    ['button[data-testid="exportar-pix-van"]', '[data-testid="exportar-pix-van"]']
                    if kind == "pix"
                    else [
                        'button[data-testid="exportar-historicovendas"]',
                        '[data-testid="exportar-historicovendas"]',
                        'button[data-testid^="exportar-"]',
                        '[data-testid^="exportar-"]',
                    ]
                )
                for selector in direct_selectors:
                    if await click_selector_native(session_id, selector, root_selector=tab_content_selector):
                        if kind == "pix":
                            try:
                                await wait_for_condition(
                                    session_id,
                                    """
                                    (() => {
                                        const text = (document.body?.innerText || '')
                                            .normalize('NFD')
                                            .replace(/[\\u0300-\\u036f]/g, '')
                                            .toUpperCase();
                                        return text.includes('ESCOLHA COMO DESEJA EXPORTAR O RELATORIO') ||
                                            !!document.querySelector('[data-testid="pix-van-gerar-arquivo"]');
                                    })()
                                    """,
                                    timeout=8.0,
                                    step=0.25,
                                )
                                await eval_js(
                                    session_id,
                                    """
                                    (() => {
                                        const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                                        const visible = (el) => {
                                            const rect = el.getBoundingClientRect();
                                            const style = getComputedStyle(el);
                                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                        };
                                        const all = [...document.querySelectorAll('button, a, [role="button"], [role="option"], [role="menuitem"], label, div, span')].filter(visible);
                                        const openSelector = all.find((el) => {
                                            const text = norm(el.innerText || el.textContent || el.getAttribute('aria-label') || '');
                                            return text.includes('TIPO DE ARQUIVO') || text === 'EXCEL' || text.includes('FORMATO');
                                        });
                                        (openSelector?.closest('button, a, [role="button"], label, div, span') || openSelector)?.click();
                                        return true;
                                    })()
                                    """,
                                )
                                await asyncio.sleep(0.5)
                                await eval_js(
                                    session_id,
                                    """
                                    (() => {
                                        const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                                        const visible = (el) => {
                                            const rect = el.getBoundingClientRect();
                                            const style = getComputedStyle(el);
                                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                        };
                                        const excelNode = [...document.querySelectorAll('button, a, [role="button"], [role="option"], [role="menuitem"], label, div, span, input')]
                                            .filter(visible)
                                            .find((el) => {
                                                const text = norm(el.innerText || el.textContent || el.getAttribute('aria-label') || el.getAttribute('value') || '');
                                                const attrs = norm(`${el.id || ''} ${el.name || ''} ${el.value || ''}`);
                                                return (
                                                    text === 'EXCEL' ||
                                                    text.includes('XLSX') ||
                                                    text.includes('PLANILHA') ||
                                                    attrs.includes('EXCEL') ||
                                                    attrs.includes('XLSX')
                                                );
                                            });
                                        const target = excelNode?.closest('button, a, [role="button"], [role="option"], [role="menuitem"], label, div, span') || excelNode;
                                        target?.click();
                                        if (excelNode && excelNode.tagName === 'INPUT') {
                                            excelNode.checked = true;
                                            excelNode.dispatchEvent(new Event('input', { bubbles: true }));
                                            excelNode.dispatchEvent(new Event('change', { bubbles: true }));
                                        }
                                        return !!target;
                                    })()
                                    """,
                                )
                                await asyncio.sleep(0.6)
                                clicked_generate = await click_selector_native(session_id, '[data-testid="pix-van-gerar-arquivo"]')
                                if not clicked_generate:
                                    clicked_generate = await click_by_text(session_id, ["Gerar arquivo", "Baixar arquivo", "Download"], timeout=4.0)
                                if clicked_generate:
                                    await asyncio.sleep(1.0)
                                    return
                                raise RuntimeError("Nao foi possivel confirmar a geracao do arquivo PIX na Azulzinha/Caixa.")
                            except Exception:
                                pass
                            await asyncio.sleep(1.0)
                            continue
                        else:
                            try:
                                await wait_for_condition(
                                    session_id,
                                    """
                                    (() => {
                                        const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                                        const visible = (el) => {
                                            const rect = el.getBoundingClientRect();
                                            const style = getComputedStyle(el);
                                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                        };
                                        const text = norm(document.body?.innerText || '');
                                        if (text.includes('ESCOLHA COMO DESEJA EXPORTAR')) return true;
                                        return [...document.querySelectorAll('button, a, [role="button"]')]
                                            .filter(visible)
                                            .some((el) => {
                                                const value = norm(el.innerText || el.textContent || el.getAttribute('aria-label') || el.title || '');
                                                return value.includes('PDF') || value.includes('GERAR ARQUIVO') || value.includes('BAIXAR ARQUIVO');
                                            });
                                    })()
                                    """,
                                    timeout=6.0,
                                    step=0.25,
                                )
                                await asyncio.sleep(0.5)
                                await eval_js(
                                    session_id,
                                    """
                                    (() => {
                                        const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                                        const visible = (el) => {
                                            const rect = el.getBoundingClientRect();
                                            const style = getComputedStyle(el);
                                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                        };
                                        const clickBest = (targets) => {
                                            const nodes = [...document.querySelectorAll('button, a, [role="button"], label, div, span')].filter(visible);
                                            for (const target of targets) {
                                                const node = nodes.find((el) => norm(el.innerText || el.textContent || el.getAttribute('aria-label') || '').includes(target));
                                                if (node) {
                                                    (node.closest('button, a, [role="button"], label, div') || node).click();
                                                    return true;
                                                }
                                            }
                                            return false;
                                        };
                                        return (
                                            clickBest(['PDF']) ||
                                            clickBest(['EXCEL', 'XLSX', 'PLANILHA']) ||
                                            false
                                        );
                                    })()
                                    """,
                                )
                                await asyncio.sleep(0.7)
                                if not await click_selector_native(session_id, '[data-testid*="gerar-arquivo"]'):
                                    if not await click_selector_native(session_id, '[data-testid*="baixar-arquivo"]'):
                                        if not await click_selector_native(session_id, '[data-testid*="download"]'):
                                            await click_by_text(session_id, ["Gerar arquivo", "Baixar arquivo", "Download"], timeout=4.0)
                                await asyncio.sleep(1.0)
                                return
                            except Exception:
                                try:
                                    html_debug = await eval_js(
                                        session_id,
                                        "document.documentElement ? document.documentElement.outerHTML : ''",
                                        timeout=15.0,
                                    )
                                    if html_debug:
                                        debug_path = artifacts_dir / "azulzinha_export_popup_cartoes.html"
                                        debug_path.write_text(str(html_debug), encoding="utf-8")
                                except Exception:
                                    pass
                                continue
                clicked = await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                        const visible = (el) => {{
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        }};
                        const wantCsv = {kind == 'pix'};
                        const directSelectors = wantCsv
                            ? ['button[data-testid="exportar-pix-van"]', '[data-testid="exportar-pix-van"]']
                            : ['button[data-testid^="exportar-"]', '[data-testid^="exportar-"]'];
                        const root = {tab_content_selector!r} ? document.querySelector({tab_content_selector!r}) : document;
                        if (!root) return false;
                        const explicit = directSelectors
                            .flatMap((selector) => [...root.querySelectorAll(selector)])
                            .find((el) => el && !el.disabled && visible(el));
                        if (explicit) {{
                            explicit.scrollIntoView({{ block: 'center', inline: 'center' }});
                            explicit.dispatchEvent(new MouseEvent('mousedown', {{ bubbles: true, cancelable: true, view: window }}));
                            explicit.click();
                            explicit.dispatchEvent(new MouseEvent('mouseup', {{ bubbles: true, cancelable: true, view: window }}));
                            return true;
                        }}
                        const buttons = [...root.querySelectorAll('button, a, [role="button"]')].filter(visible);
                        const preferred = buttons.find((el) => {{
                            const text = norm(el.innerText || el.textContent || el.getAttribute('aria-label') || el.getAttribute('mattooltip') || el.title || '');
                            return !el.disabled && (text.includes('EXPORTAR') || text.includes('IMPRIMIR') || text.includes(wantCsv ? 'CSV' : 'PDF'));
                        }}) || buttons.find((el) => {{
                            const text = norm(el.getAttribute('aria-label') || el.getAttribute('mattooltip') || el.title || '');
                            return !el.disabled && (wantCsv ? text.includes('CSV') || text.includes('DATABASE') : text.includes('PDF') || text.includes('DOWNLOAD'));
                        }});
                        if (!preferred) return false;
                        preferred.scrollIntoView({{ block: 'center', inline: 'center' }});
                        preferred.click();
                        return true;
                    }})()
                    """,
                )
                if not clicked:
                    try:
                        state_debug = await eval_js(
                            session_id,
                            """
                            (() => {
                                const items = [...document.querySelectorAll('button, a, [role="button"]')].map((el) => {
                                    const rect = el.getBoundingClientRect();
                                    const style = getComputedStyle(el);
                                    return {
                                        tag: el.tagName,
                                        text: (el.innerText || el.textContent || '').trim(),
                                        testid: el.getAttribute('data-testid') || '',
                                        disabled: !!el.disabled,
                                        ariaLabel: el.getAttribute('aria-label') || '',
                                        title: el.title || '',
                                        visible: rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none',
                                        width: rect.width,
                                        height: rect.height,
                                    };
                                });
                                return items.filter((item) =>
                                    (item.testid || '').toLowerCase().includes('export') ||
                                    (item.text || '').toUpperCase().includes('EXPORT') ||
                                    (item.text || '').toUpperCase().includes('CSV') ||
                                    (item.text || '').toUpperCase().includes('PDF')
                                );
                            })()
                            """,
                            timeout=15.0,
                        )
                        if state_debug is not None:
                            debug_state_path = artifacts_dir / f"azulzinha_export_state_{kind}.json"
                            debug_state_path.write_text(json.dumps(state_debug, ensure_ascii=False, indent=2), encoding="utf-8")
                    except Exception:
                        pass
                    try:
                        html_debug = await eval_js(
                            session_id,
                            "document.documentElement ? document.documentElement.outerHTML : ''",
                            timeout=15.0,
                        )
                        if html_debug:
                            debug_path = artifacts_dir / f"azulzinha_export_debug_{kind}.html"
                            debug_path.write_text(str(html_debug), encoding="utf-8")
                    except Exception:
                        pass
                    raise RuntimeError("Nao foi possivel acionar o botao de exportacao no portal Azulzinha/Caixa.")

            async def ensure_login(session_id: str, auth_restart_count: int = 0) -> None:
                _emit_pix_status(on_status, "Acessando Azulzinha/Caixa...")
                await navigate(session_id, credenciais["login_url"])
                if await eval_js(
                    session_id,
                    """
                    (() => {
                        const text = (document.body?.innerText || '')
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .toUpperCase();
                        const visible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        };
                        const hasSalesTabs = Boolean(
                            document.querySelector('header[role="tablist"]') ||
                            document.querySelector('#HistoricoVendas button[role="tab"]') ||
                            document.querySelector('#Pix button[role="tab"]')
                        );
                        const onLoginPage = (
                            visible(document.querySelector('#b2-b1-b4-InputMask')) &&
                            visible(document.querySelector('#b2-b1-Input_Password'))
                        ) || text.includes('ACESSE SUA CONTA');
                        return !onLoginPage && (
                            hasSalesTabs ||
                            text.includes('HISTORICO DE VENDAS') ||
                            text.includes('PAGAMENTO INSTANTANEO') ||
                            text.includes('PIX')
                        );
                    })()
                    """,
                ):
                    return
                _emit_pix_status(on_status, "Autenticando na Azulzinha/Caixa...")
                login_selectors = await wait_for_condition(
                    session_id,
                    """
                    (() => {
                        const visible = (el) => {
                            if (!el || el.disabled) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        };
                        const userCandidates = [
                            document.querySelector('#b2-b1-b4-InputMask'),
                            ...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])'),
                        ].filter(visible);
                        const passwordCandidates = [
                            document.querySelector('#b2-b1-Input_Password'),
                            ...document.querySelectorAll('input[type="password"]'),
                        ].filter(visible);
                        const user = userCandidates[0];
                        const password = passwordCandidates[0];
                        if (!user || !password) return null;
                        user.setAttribute('data-codex-login-user', '1');
                        password.setAttribute('data-codex-login-pass', '1');
                        return {
                            user: user.id ? `#${user.id}` : '[data-codex-login-user="1"]',
                            password: password.id ? `#${password.id}` : '[data-codex-login-pass="1"]',
                        };
                    })()
                    """,
                    timeout=60.0,
                    description="A Caixa nao exibiu a tela de login para informar CNPJ e senha",
                )
                login_user_selector = str((login_selectors or {}).get("user") or "#b2-b1-b4-InputMask")
                login_password_selector = str((login_selectors or {}).get("password") or "#b2-b1-Input_Password")
                cnpj_digits = re.sub(r"\D", "", str(credenciais["cnpj"] or ""))
                password_text = str(credenciais["password"] or "")
                try:
                    await insert_text(session_id, login_user_selector, cnpj_digits)
                except Exception:
                    pass
                try:
                    await insert_text(session_id, login_password_selector, password_text)
                except Exception:
                    pass
                await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const visible = (el) => {{
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        }};
                        const setNativeValue = (el, value) => {{
                            const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
                            const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
                            if (setter) setter.call(el, value); else el.value = value;
                            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            el.dispatchEvent(new KeyboardEvent('keyup', {{ bubbles: true, key: '0' }}));
                            el.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                        }};
                        const user = document.querySelector({login_user_selector!r}) || [...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])')].filter(visible)[0];
                        const password = document.querySelector({login_password_selector!r}) || [...document.querySelectorAll('input[type="password"]')].filter(visible)[0];
                        if (!user || !password) return false;
                        user.focus();
                        setNativeValue(user, {cnpj_digits!r});
                        password.focus();
                        setNativeValue(password, {password_text!r});
                        return true;
                    }})()
                    """,
                    timeout=15.0,
                )
                credential_fill_state = await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const visible = (el) => {{
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        }};
                        const user = document.querySelector({login_user_selector!r}) || [...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])')].filter(visible)[0];
                        const password = document.querySelector({login_password_selector!r}) || [...document.querySelectorAll('input[type="password"]')].filter(visible)[0];
                        const userValue = String(user?.value || user?.getAttribute('value') || '').replace(/\\D/g, '');
                        const passwordValue = String(password?.value || password?.getAttribute('value') || '');
                        return {{
                            userLen: userValue.length,
                            passwordLen: passwordValue.length,
                        }};
                    }})()
                    """,
                ) or {}
                if int(credential_fill_state.get("userLen") or 0) < len(cnpj_digits) or int(credential_fill_state.get("passwordLen") or 0) < len(password_text):
                    await eval_js(
                        session_id,
                        f"""
                        (() => {{
                            const visible = (el) => {{
                                if (!el) return false;
                                const rect = el.getBoundingClientRect();
                                const style = getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                            }};
                            const setNativeValue = (el, value) => {{
                                const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
                                const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
                                if (setter) setter.call(el, value); else el.value = value;
                                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                                el.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                            }};
                            const user = document.querySelector({login_user_selector!r}) || [...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])')].filter(visible)[0];
                            const password = document.querySelector({login_password_selector!r}) || [...document.querySelectorAll('input[type="password"]')].filter(visible)[0];
                            if (!user || !password) return false;
                            user.focus();
                            setNativeValue(user, {cnpj_digits!r});
                            password.focus();
                            setNativeValue(password, {password_text!r});
                            return true;
                        }})()
                        """,
                    )
                credential_fill_state = await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const visible = (el) => {{
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        }};
                        const user = document.querySelector({login_user_selector!r}) || [...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])')].filter(visible)[0];
                        const password = document.querySelector({login_password_selector!r}) || [...document.querySelectorAll('input[type="password"]')].filter(visible)[0];
                        const userValue = String(user?.value || user?.getAttribute('value') || '').replace(/\\D/g, '');
                        const passwordValue = String(password?.value || password?.getAttribute('value') || '');
                        return {{
                            userLen: userValue.length,
                            passwordLen: passwordValue.length,
                        }};
                    }})()
                    """,
                ) or {}
                if int(credential_fill_state.get("userLen") or 0) < len(cnpj_digits) or int(credential_fill_state.get("passwordLen") or 0) < len(password_text):
                    await capture_portal_html_debug_v2(session_id, "azulzinha_login_fill_debug.html")
                    raise RuntimeError("Nao foi possivel preencher o login e a senha da Azulzinha/Caixa.")
                await press_tab(session_id)
                await wait_for_condition(
                    session_id,
                    """
                    (() => {
                        const botao = document.querySelector('#b2-b1-confirmar');
                        return botao && !botao.disabled;
                    })()
                    """,
                    timeout=20.0,
                )
                clicou = await eval_js(
                    session_id,
                    """
                    (() => {
                        const botao = document.querySelector('#b2-b1-confirmar');
                        if (!botao || botao.disabled) return false;
                        botao.click();
                        return true;
                    })()
                    """,
                )
                if not clicou:
                    raise RuntimeError("Nao foi possivel confirmar o login da Azulzinha/Caixa.")

                state = await wait_for_condition(
                    session_id,
                    """
                    (() => {
                        const text = (document.body?.innerText || '')
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .toUpperCase();
                        const visible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        };
                        const hasSalesTabs = Boolean(
                            document.querySelector('header[role="tablist"]') ||
                            document.querySelector('#HistoricoVendas button[role="tab"]') ||
                            document.querySelector('#Pix button[role="tab"]')
                        );
                        const onLoginPage = (
                            visible(document.querySelector('#b2-b1-b4-InputMask')) &&
                            visible(document.querySelector('#b2-b1-Input_Password'))
                        ) || text.includes('ACESSE SUA CONTA');
                        const tokenInputs = [...document.querySelectorAll('input')].filter((el) => {
                            const type = (el.type || '').toLowerCase();
                            return type !== 'hidden' && type !== 'password' && !el.disabled && visible(el);
                        });
                        if (!onLoginPage && (hasSalesTabs || text.includes('HISTORICO DE VENDAS') || text.includes('PAGAMENTO INSTANTANEO') || text.includes('PIX'))) return 'logged';
                        if (!onLoginPage && text.includes('SELECIONE O DISPOSITIVO')) return 'device';
                        if (!onLoginPage && (text.includes('ESCOLHA POR ONDE DESEJA RECEBER') || text.includes('RECEBER POR E-MAIL') || text.includes('@GMAIL.COM'))) return 'token';
                        if (!onLoginPage && tokenInputs.length > 0 && (text.includes('TOKEN') || text.includes('CODIGO'))) return 'token';
                        return '';
                    })()
                    """,
                    timeout=90.0,
                    description="A Caixa nao concluiu a etapa inicial do login",
                )
                if state == "logged":
                    return
                if state == "device":
                    _emit_pix_status(on_status, "Selecionando dispositivo da Caixa...")
                    if not await click_card_by_text(session_id, _azulzinha_device_aliases(company_label), timeout=30.0):
                        raise RuntimeError(f"Nao foi possivel selecionar o dispositivo da {company_label} na Azulzinha/Caixa.")
                    state = await wait_for_condition(
                        session_id,
                        """
                        (() => {
                            const text = (document.body?.innerText || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .toUpperCase();
                            const visible = (el) => {
                                if (!el) return false;
                                const rect = el.getBoundingClientRect();
                                const style = getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                            };
                            const hasSalesTabs = Boolean(
                                document.querySelector('header[role="tablist"]') ||
                                document.querySelector('#HistoricoVendas button[role="tab"]') ||
                                document.querySelector('#Pix button[role="tab"]')
                            );
                            const onLoginPage = (
                                visible(document.querySelector('#b2-b1-b4-InputMask')) &&
                                visible(document.querySelector('#b2-b1-Input_Password'))
                            ) || text.includes('ACESSE SUA CONTA');
                            const tokenInputs = [...document.querySelectorAll('input')].filter((el) => {
                                const type = (el.type || '').toLowerCase();
                                return type !== 'hidden' && type !== 'password' && !el.disabled && visible(el);
                            });
                            if (!onLoginPage && (hasSalesTabs || text.includes('HISTORICO DE VENDAS') || text.includes('PAGAMENTO INSTANTANEO') || text.includes('PIX'))) return 'logged';
                            if (!onLoginPage && text.includes('INFORME O TOKEN DO APLICATIVO')) return 'token_app';
                            if (!onLoginPage && (text.includes('ESCOLHA POR ONDE DESEJA RECEBER') || text.includes('RECEBER POR E-MAIL') || text.includes('@GMAIL.COM'))) return 'token_delivery';
                            if (!onLoginPage && tokenInputs.length > 0 && (text.includes('TOKEN') || text.includes('CODIGO'))) return 'token';
                            return '';
                        })()
                        """,
                        timeout=45.0,
                        description="A Caixa nao avancou apos a selecao do dispositivo",
                    )
                if state == "logged":
                    return

                async def request_token_via_email(session_id: str, current_state: str | None = None) -> str:
                    state_local = str(current_state or "").strip()
                    if state_local in {"token", "token_app"}:
                        token_screen_ready = await eval_js(
                            session_id,
                            """
                            (() => {
                                const text = (document.body?.innerText || '')
                                    .normalize('NFD')
                                    .replace(/[\u0300-\u036f]/g, '')
                                    .toUpperCase();
                                const visible = (el) => {
                                    if (!el) return false;
                                    const rect = el.getBoundingClientRect();
                                    const style = getComputedStyle(el);
                                    return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                };
                                const onLoginPage = (
                                    visible(document.querySelector('#b2-b1-b4-InputMask')) &&
                                    visible(document.querySelector('#b2-b1-Input_Password'))
                                ) || text.includes('ACESSE SUA CONTA');
                                const tokenInputs = [...document.querySelectorAll('input')].filter((el) => {
                                    const type = (el.type || '').toLowerCase();
                                    return type !== 'hidden' && type !== 'password' && !el.disabled && visible(el);
                                });
                                return !onLoginPage && (
                                    text.includes('ESCOLHA POR ONDE DESEJA RECEBER') ||
                                    text.includes('RECEBER POR E-MAIL') ||
                                    text.includes('@GMAIL.COM') ||
                                    (tokenInputs.length > 0 && (text.includes('TOKEN') || text.includes('CODIGO')))
                                );
                            })()
                            """,
                        )
                        if not token_screen_ready:
                            state_local = ""
                    if state_local in {"token", "token_app"}:
                        abriu = await eval_js(
                            session_id,
                            """
                            (() => {
                                const link = document.querySelector('a.bold.font-universe.cor-preto');
                                if (!link) return false;
                                link.click();
                                return true;
                            })()
                            """,
                        )
                        if not abriu and not await click_by_text(
                            session_id,
                            [
                                "Receber codigo por e-mail ou SMS",
                                "Receber c??digo por e-mail ou SMS",
                                "Receber cÃ³digo por e-mail ou SMS",
                                "Reenviar codigo",
                                "Reenviar token",
                            ],
                            timeout=20.0,
                        ):
                            return state_local
                        state_local = await wait_for_condition(
                            session_id,
                            """
                            (() => {
                                const text = (document.body?.innerText || '').toUpperCase();
                                const inputs = [...document.querySelectorAll('input')]
                                    .filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled);
                                if (text.includes('ESCOLHA POR ONDE DESEJA RECEBER')) return 'token_delivery';
                                if (text.includes('RECEBER POR E-MAIL') || text.includes('@GMAIL.COM')) return 'token_delivery';
                                if (inputs.length > 0 && (text.includes('TOKEN') || text.includes('CODIGO'))) return 'token';
                                return '';
                            })()
                            """,
                            timeout=30.0,
                        )
                        await asyncio.sleep(1.0)

                    if state_local == "token_delivery":
                        selecionou_email = await eval_js(
                            session_id,
                            """
                            (() => {
                                const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                                const visible = (el) => {
                                    const rect = el.getBoundingClientRect();
                                    const style = getComputedStyle(el);
                                    return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                };
                                const candidates = [...document.querySelectorAll('[id$="-Content"], .card-content, .ph.card.card-content')]
                                    .filter(visible)
                                    .map((el) => {
                                        const text = norm(el.innerText || el.textContent || '');
                                        if (!text.includes('RECEBER POR E-MAIL') && !text.includes('@GMAIL.COM')) return null;
                                        const className = (el.className || '').toString().toUpperCase();
                                        const isCard = className.includes('CARD-CONTENT') || className.includes('PH CARD');
                                        const textLength = text.length || 9999;
                                        return { el, score: (isCard ? 1000 : 0) - textLength };
                                    })
                                    .filter(Boolean)
                                    .sort((a, b) => b.score - a.score);
                                const match = candidates.length ? candidates[0].el : null;
                                if (!match) return false;
                                const text = norm(match.innerText || match.textContent || '');
                                if (!text.includes('RECEBER POR E-MAIL') && !text.includes('@GMAIL.COM')) return false;
                                const clickable = match.closest('[id$="-Content"], .card-content, .ph.card.card-content') || match;
                                clickable.scrollIntoView({ block: 'center', inline: 'center' });
                                clickable.click();
                                return true;
                            })()
                            """,
                        )
                        if not selecionou_email and not await click_card_by_text(session_id, ["RECEBER POR E-MAIL", "@GMAIL.COM"], timeout=20.0):
                            if not await click_by_text(session_id, ["Receber por e-mail", "@gmail.com"], timeout=20.0):
                                raise RuntimeError("Nao foi possivel selecionar o envio do token por e-mail na Azulzinha/Caixa.")
                        await asyncio.sleep(1.0)
                        await wait_for_condition(
                            session_id,
                            """
                            (() => {
                                const text = (document.body?.innerText || '')
                                    .normalize('NFD')
                                    .replace(/[\\u0300-\\u036f]/g, '')
                                    .toUpperCase();
                                const inputs = [...document.querySelectorAll('input')].filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled);
                                return inputs.length > 0 && (text.includes('TOKEN') || text.includes('CODIGO'));
                            })()
                            """,
                            timeout=30.0,
                        )
                        return "token"

                    return state_local

                token_email_grace_seconds = 15.0

                async def wait_for_token_email_delivery_grace() -> None:
                    _emit_pix_status(
                        on_status,
                        f"Aguardando {int(token_email_grace_seconds)}s para o e-mail do token chegar...",
                    )
                    await asyncio.sleep(token_email_grace_seconds)

                _emit_pix_status(on_status, "Solicitando token por e-mail...")
                token_requested_at = time.time()
                state = await request_token_via_email(session_id, state)
                if state == "token":
                    await wait_for_token_email_delivery_grace()
                if False and state in {"token", "token_app"}:
                    abriu = await eval_js(
                        session_id,
                        """
                        (() => {
                            const link = document.querySelector('a.bold.font-universe.cor-preto');
                            if (!link) return false;
                            link.click();
                            return true;
                        })()
                        """,
                    )
                    if not abriu and not await click_by_text(
                        session_id,
                        [
                            "Receber codigo por e-mail ou SMS",
                            "Receber c??digo por e-mail ou SMS",
                            "Receber código por e-mail ou SMS",
                        ],
                        timeout=20.0,
                    ):
                        raise RuntimeError("Nao foi possivel abrir a escolha de envio do token na Azulzinha/Caixa.")
                    state = await wait_for_condition(
                        session_id,
                        """
                        (() => {
                            const text = (document.body?.innerText || '').toUpperCase();
                            if (text.includes('ESCOLHA POR ONDE DESEJA RECEBER')) return 'token_delivery';
                            if (text.includes('RECEBER POR E-MAIL') || text.includes('@GMAIL.COM')) return 'token_delivery';
                            return '';
                        })()
                        """,
                        timeout=30.0,
                    )
                    await asyncio.sleep(1.0)
                if False and state == "token_delivery":
                    selecionou_email = await eval_js(
                        session_id,
                        """
                        (() => {
                            const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                            const visible = (el) => {
                                const rect = el.getBoundingClientRect();
                                const style = getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                            };
                            const candidates = [...document.querySelectorAll('[id$="-Content"], .card-content, .ph.card.card-content')]
                                .filter(visible)
                                .map((el) => {
                                    const text = norm(el.innerText || el.textContent || '');
                                    if (!text.includes('RECEBER POR E-MAIL') && !text.includes('@GMAIL.COM')) return null;
                                    const className = (el.className || '').toString().toUpperCase();
                                    const isCard = className.includes('CARD-CONTENT') || className.includes('PH CARD');
                                    const textLength = text.length || 9999;
                                    return { el, score: (isCard ? 1000 : 0) - textLength };
                                })
                                .filter(Boolean)
                                .sort((a, b) => b.score - a.score);
                            const match = candidates.length ? candidates[0].el : null;
                            if (!match) return false;
                            const text = norm(match.innerText || match.textContent || '');
                            if (!text.includes('RECEBER POR E-MAIL') && !text.includes('@GMAIL.COM')) return false;
                            const clickable = match.closest('[id$="-Content"], .card-content, .ph.card.card-content') || match;
                            clickable.scrollIntoView({ block: 'center', inline: 'center' });
                            clickable.click();
                            return true;
                        })()
                        """,
                    )
                    if not selecionou_email and not await click_card_by_text(session_id, ["RECEBER POR E-MAIL", "@GMAIL.COM"], timeout=20.0):
                        if not await click_by_text(session_id, ["Receber por e-mail", "@gmail.com"], timeout=20.0):
                            await capture_portal_html_debug_v2(session_id, f"azulzinha_token_delivery_select_{company_norm}.html")
                            raise RuntimeError("Nao foi possivel selecionar o envio do token por e-mail na Azulzinha/Caixa.")
                    await asyncio.sleep(1.0)
                    await wait_for_condition(
                        session_id,
                        """
                        (() => {
                            const text = (document.body?.innerText || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .toUpperCase();
                            const inputs = [...document.querySelectorAll('input')].filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled);
                            return inputs.length > 0 && (text.includes('TOKEN') || text.includes('CODIGO'));
                        })()
                        """,
                        timeout=30.0,
                    )
                await wait_for_condition(
                    session_id,
                    """
                    (() => {
                        const text = (document.body?.innerText || '')
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .toUpperCase();
                        const visible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        };
                        const hasSalesTabs = Boolean(
                            document.querySelector('header[role="tablist"]') ||
                            document.querySelector('#HistoricoVendas button[role="tab"]') ||
                            document.querySelector('#Pix button[role="tab"]')
                        );
                        const onLoginPage = (
                            visible(document.querySelector('#b2-b1-b4-InputMask')) &&
                            visible(document.querySelector('#b2-b1-Input_Password'))
                        ) || text.includes('ACESSE SUA CONTA');
                        const inputs = [...document.querySelectorAll('input')].filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled);
                        return (!onLoginPage && (hasSalesTabs || text.includes('HISTORICO DE VENDAS') || text.includes('PAGAMENTO INSTANTANEO') || text.includes('PIX'))) || (!onLoginPage && inputs.length > 0 && (text.includes('TOKEN') || text.includes('CODIGO')));
                    })()
                    """,
                    timeout=60.0,
                )
                if await eval_js(
                    session_id,
                    """
                    (() => {
                        const text = (document.body?.innerText || '')
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .toUpperCase();
                        const visible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        };
                        const onLoginPage = (
                            visible(document.querySelector('#b2-b1-b4-InputMask')) &&
                            visible(document.querySelector('#b2-b1-Input_Password'))
                        ) || text.includes('ACESSE SUA CONTA');
                        return Boolean(
                            !onLoginPage && (
                                document.querySelector('header[role="tablist"]') ||
                                document.querySelector('#HistoricoVendas button[role="tab"]') ||
                                document.querySelector('#Pix button[role="tab"]') ||
                                text.includes('HISTORICO DE VENDAS') ||
                                text.includes('PAGAMENTO INSTANTANEO') ||
                                text.includes('PIX')
                            )
                        );
                    })()
                    """,
                ):
                    return
                ultimo_token_usado = ""
                tokens_descartados: set[str] = set()
                for tentativa_token in range(3):
                    if tentativa_token > 0:
                        espera_extra_apos_rejeicao = 5.0
                        _emit_pix_status(
                            on_status,
                            f"Token rejeitado pela Caixa; descartando o codigo anterior e aguardando {int(espera_extra_apos_rejeicao)}s por um novo e-mail...",
                        )
                        await asyncio.sleep(espera_extra_apos_rejeicao)
                        try:
                            state = await request_token_via_email(session_id, "token")
                            if state == "token":
                                token_requested_at = time.time()
                                await wait_for_token_email_delivery_grace()
                        except Exception:
                            pass
                    inicio_busca_token = token_requested_at
                    espera_token = 180.0 if tentativa_token == 0 else 150.0
                    token = str(
                        await asyncio.to_thread(
                            _fetch_fiserv_token_from_gmail,
                            inicio_busca_token,
                            espera_token,
                            on_status,
                            tokens_descartados,
                            tentativa_token == 0,
                        )
                        or ""
                    ).strip()
                    if (not token or token == ultimo_token_usado or token in tokens_descartados) and callable(token_callback):
                        token = str(token_callback("Informe o token enviado por e-mail pela Azulzinha/Caixa") or "").strip()
                    if token in tokens_descartados:
                        _emit_pix_status(on_status, "O token informado ja foi rejeitado anteriormente pela Caixa.")
                        token = ""
                    if not token:
                        if tentativa_token >= 2:
                            raise RuntimeError("Nao foi possivel obter um token novo enviado por e-mail pela Caixa.")
                        _emit_pix_status(on_status, "Ainda nao chegou um token novo da Caixa; aguardando mais um pouco antes da proxima tentativa.")
                        continue

                    ultimo_token_usado = token
                    _emit_pix_status(on_status, f"Confirmando token da Caixa: {token}...")
                    confirmed = False
                    token_inputs = await eval_js(
                        session_id,
                        """
                        (() => {
                            return [...document.querySelectorAll('input')]
                                .filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled)
                                .map((el) => ({
                                    selector: el.id ? `#${el.id}` : '',
                                    maxLength: Number(el.maxLength || 0),
                                }));
                        })()
                        """,
                    ) or []
                    token_selectors = [
                        str(item.get("selector") or "").strip()
                        for item in token_inputs
                        if str(item.get("selector") or "").strip()
                    ]
                    token_digitos = list(str(token))
                    if token_selectors and len(token_selectors) > 1 and len(token_digitos) >= len(token_selectors):
                        for idx, selector in enumerate(token_selectors):
                            if not await focus_selector(session_id, selector):
                                continue
                            await eval_js(
                                session_id,
                                f"""
                                (() => {{
                                    const el = document.querySelector({selector!r});
                                    if (!el) return false;
                                    el.value = '';
                                    el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                    return true;
                                }})()
                                """,
                            )
                            await cdp("Input.insertText", {"text": token_digitos[idx]}, session_id=session_id)
                            await asyncio.sleep(0.08)
                        confirmed = True
                    else:
                        confirmed = await eval_js(
                            session_id,
                            f"""
                            (() => {{
                                const token = {token!r};
                                const setNativeValue = (el, value) => {{
                                    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
                                    if (setter) setter.call(el, value); else el.value = value;
                                    el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                    el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                                    el.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                                }};
                                const inputs = [...document.querySelectorAll('input')].filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled);
                                if (inputs.length) {{
                                    setNativeValue(inputs[inputs.length - 1], '');
                                    setNativeValue(inputs[inputs.length - 1], token);
                                    return true;
                                }}
                                return false;
                            }})()
                            """,
                        )
                    if not confirmed:
                        raise RuntimeError("Nao foi possivel preencher o token da Azulzinha/Caixa.")
                    await asyncio.sleep(0.4)
                    await eval_js(
                        session_id,
                        """
                        (() => {
                            const buttons = [...document.querySelectorAll('button, input[type="submit"], a')];
                            const button = buttons.find((el) => /CONFIRMAR|VALIDAR|ENTRAR|CONTINUAR/i.test(el.innerText || el.value || '')) || buttons[0];
                            button?.click();
                            return true;
                        })()
                        """,
                    )

                    _emit_pix_status(on_status, "Aguardando a Caixa validar o token...")
                    await asyncio.sleep(1.5)
                    sales_ready_expression = """
                    (() => {
                        const text = (document.body?.innerText || '')
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .toUpperCase();
                        const hasSalesTabs = Boolean(
                            document.querySelector('header[role="tablist"]') ||
                            document.querySelector('#HistoricoVendas button[role="tab"]') ||
                            document.querySelector('#Pix button[role="tab"]')
                        );
                        return hasSalesTabs || text.includes('HISTORICO DE VENDAS') || text.includes('PAGAMENTO INSTANTANEO') || text.includes('PIX');
                    })()
                    """
                    post_token_state_expression = """
                    (() => {
                        const text = (document.body?.innerText || '')
                            .normalize('NFD')
                            .replace(/[\\u0300-\\u036f]/g, '')
                            .toUpperCase();
                        const visible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        };
                        const hasSalesTabs = Boolean(
                            document.querySelector('header[role="tablist"]') ||
                            document.querySelector('#HistoricoVendas button[role="tab"]') ||
                            document.querySelector('#Pix button[role="tab"]')
                        );
                        const loginInput = document.querySelector('#b2-b1-b4-InputMask');
                        const passwordInput = document.querySelector('#b2-b1-Input_Password');
                        const tokenInputs = [...document.querySelectorAll('input')]
                            .filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled && visible(el));
                        if (
                            text.includes('CODIGO INVALIDO') ||
                            text.includes('TOKEN INVALIDO') ||
                            text.includes('TOKEN EXPIRADO') ||
                            text.includes('TENTATIVAS')
                        ) return 'invalid';
                        if ((visible(loginInput) && visible(passwordInput)) || (text.includes('ACESSE SUA CONTA') && visible(loginInput))) return 'login';
                        if (hasSalesTabs || text.includes('HISTORICO DE VENDAS') || text.includes('PAGAMENTO INSTANTANEO') || text.includes('PIX')) return 'sales';
                        if (tokenInputs.length > 0 && (text.includes('TOKEN') || text.includes('CODIGO') || text.includes('E-MAIL') || text.includes('EMAIL'))) return 'token';
                        return '';
                    })()
                    """
                    try:
                        resultado_token = await wait_for_condition(
                            session_id,
                            post_token_state_expression,
                            timeout=150.0,
                            description="A Caixa nao concluiu a validacao do token",
                        )
                    except Exception:
                        resultado_token = ""
                    if resultado_token == "login":
                        if auth_restart_count >= 2:
                            raise RuntimeError("A Caixa voltou para a tela de login apos validar o token repetidas vezes.")
                        _emit_pix_status(
                            on_status,
                            "A Caixa voltou para a tela de login apos validar o token; refazendo a autenticacao...",
                        )
                        await asyncio.sleep(2.0)
                        await ensure_login(session_id, auth_restart_count + 1)
                        return
                    if resultado_token != "sales" and resultado_token != "invalid":
                        try:
                            await navigate(session_id, credenciais["sales_url"])
                            resultado_token = await wait_for_condition(
                                session_id,
                                post_token_state_expression,
                                timeout=60.0,
                                description="A Caixa nao abriu a area de vendas apos validar o token",
                            )
                        except Exception:
                            pass
                    if resultado_token == "sales":
                        break
                    if tentativa_token >= 2:
                        raise RuntimeError("O token informado pela Caixa nao foi aceito apos multiplas tentativas.")
                    tokens_descartados.add(token)
                await wait_for_condition(
                    session_id,
                    sales_ready_expression,
                    timeout=90.0,
                    description="A Caixa nao abriu a area de vendas apos validar o token",
                )

            sales_area_state_expression = """
            (() => {
                const text = (document.body?.innerText || '')
                    .normalize('NFD')
                    .replace(/[\u0300-\u036f]/g, '')
                    .toUpperCase();
                const href = String(location.href || '').toUpperCase();
                const visible = (el) => {
                    if (!el) return false;
                    const rect = el.getBoundingClientRect();
                    const style = getComputedStyle(el);
                    return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                };
                const hasAuthenticatedNav = Boolean(
                    [...document.querySelectorAll('a, button, span, div')]
                        .filter(visible)
                        .find((el) => {
                            if (el.closest('.om-footer, footer')) return false;
                            const label = String(el.innerText || el.textContent || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .toUpperCase();
                            const linkHref = String(el.getAttribute?.('href') || '').toUpperCase();
                            return (
                                linkHref.includes('/MINHASVENDAS') ||
                                linkHref.includes('ROUTER=0') ||
                                label.includes('RELATORIOS DE VENDA') ||
                                label.includes('MINHAS VENDAS') ||
                                label.includes('HISTORICO DE VENDAS') ||
                                label.includes('PAGAMENTO INSTANTANEO') ||
                                label.includes('PIX') ||
                                label.includes('SAIR')
                            );
                        })
                );
                const hasSalesTabs = Boolean(
                    visible(document.querySelector('header[role="tablist"]')) ||
                    [...document.querySelectorAll('header[role="tablist"] button[role="tab"], #HistoricoVendas button[role="tab"], #Pix button[role="tab"], #HistoricoVendas [role="tabpanel"], #Pix [role="tabpanel"]')]
                        .some(visible)
                );
                const loginInput = document.querySelector('#b2-b1-b4-InputMask');
                const passwordInput = document.querySelector('#b2-b1-Input_Password');
                const tokenInputs = [...document.querySelectorAll('input')]
                    .filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled && visible(el));
                const deviceVisible = text.includes('SELECIONE O DISPOSITIVO');
                const tokenDeliveryVisible = (
                    text.includes('ESCOLHA POR ONDE DESEJA RECEBER') ||
                    text.includes('RECEBER POR E-MAIL') ||
                    text.includes('@GMAIL.COM')
                );
                const invalidVisible = (
                    text.includes('CODIGO INVALIDO') ||
                    text.includes('TOKEN INVALIDO') ||
                    text.includes('TOKEN EXPIRADO') ||
                    text.includes('TENTATIVAS')
                );
                const tokenVisible = tokenInputs.length > 0 && (
                    text.includes('TOKEN') ||
                    text.includes('CODIGO') ||
                    text.includes('E-MAIL') ||
                    text.includes('EMAIL')
                );
                const loginTextVisible = (
                    text.includes('ACESSE SUA CONTA') ||
                    (text.includes('CNPJ, CPF OU USUARIO') && text.includes('SENHA'))
                );
                const loginVisible = (
                    href.includes('ISTIMEOUT=TRUE') ||
                    (visible(loginInput) && visible(passwordInput)) ||
                    (
                        loginTextVisible &&
                        !deviceVisible &&
                        !tokenDeliveryVisible &&
                        !tokenVisible &&
                        !invalidVisible
                    )
                );
                if (href.includes('/_ERROR.HTML') || text.includes('ERROR PROCESSING YOUR REQUEST')) return 'portal_error';
                if (hasSalesTabs) return 'sales';
                if (loginVisible) return 'login';
                if (deviceVisible) return 'device';
                if (tokenDeliveryVisible) return 'token';
                if (tokenVisible) return 'token';
                return '';
            })()
            """

            async def ensure_sales_area(session_id: str, kind: str, context_label: str, auth_retry_count: int = 0) -> None:
                try:
                    current_state = str(await eval_js(session_id, sales_area_state_expression, timeout=15.0) or "").strip()
                except Exception:
                    current_state = ""
                if current_state == "sales":
                    return
                if current_state == "portal_error":
                    if auth_retry_count >= 2:
                        raise RuntimeError(f"A Caixa abriu a pagina de erro ao acessar {context_label} repetidas vezes.")
                    _emit_pix_status(
                        on_status,
                        f"A Caixa abriu a pagina de erro ao acessar {context_label}; recarregando o portal...",
                    )
                    try:
                        html_debug = await eval_js(
                            session_id,
                            "document.documentElement ? document.documentElement.outerHTML : ''",
                            timeout=15.0,
                        )
                        if html_debug:
                            debug_path = artifacts_dir / f"azulzinha_portal_error_{kind}.html"
                            debug_path.write_text(str(html_debug), encoding="utf-8")
                    except Exception:
                        pass
                    await asyncio.sleep(2.0)
                    await navigate(session_id, credenciais["sales_url"])
                    await ensure_sales_area(session_id, kind, context_label, auth_retry_count + 1)
                    return
                if current_state in {"login", "device", "token"}:
                    if auth_retry_count >= 2:
                        raise RuntimeError(f"A Caixa perdeu a sessao ao abrir {context_label} repetidas vezes.")
                    _emit_pix_status(
                        on_status,
                        f"A sessao da Caixa expirou ao abrir {context_label}; refazendo a autenticacao...",
                    )
                    await ensure_login(session_id)
                    try:
                        current_state = str(await eval_js(session_id, sales_area_state_expression, timeout=15.0) or "").strip()
                    except Exception:
                        current_state = ""
                    if current_state == "sales":
                        return
                await navigate(session_id, credenciais["sales_url"])
                await wait_for_condition(session_id, "document.body && document.body.innerText.length > 20", timeout=60.0)
                try:
                    state_after_navigation = await wait_for_condition(
                        session_id,
                        sales_area_state_expression,
                        timeout=60.0,
                        step=0.5,
                        description=f"A Caixa nao abriu a area de vendas para {context_label}",
                    )
                except Exception:
                    try:
                        html_debug = await eval_js(
                            session_id,
                            "document.documentElement ? document.documentElement.outerHTML : ''",
                            timeout=15.0,
                        )
                        if html_debug:
                            debug_path = artifacts_dir / f"azulzinha_sales_area_debug_{kind}.html"
                            debug_path.write_text(str(html_debug), encoding="utf-8")
                    except Exception:
                        pass
                    raise
                if state_after_navigation == "sales":
                    return
                if state_after_navigation == "portal_error":
                    if auth_retry_count >= 2:
                        raise RuntimeError(f"A Caixa abriu a pagina de erro ao acessar {context_label} repetidas vezes.")
                    _emit_pix_status(
                        on_status,
                        f"A Caixa abriu a pagina de erro ao abrir {context_label}; tentando recarregar o portal...",
                    )
                    await asyncio.sleep(2.0)
                    await navigate(session_id, credenciais["sales_url"])
                    await ensure_sales_area(session_id, kind, context_label, auth_retry_count + 1)
                    return
                if state_after_navigation in {"login", "device", "token"}:
                    if auth_retry_count >= 2:
                        raise RuntimeError(f"A Caixa perdeu a sessao ao abrir {context_label} repetidas vezes.")
                    _emit_pix_status(
                        on_status,
                        f"A Caixa redirecionou para o login ao abrir {context_label}; refazendo a autenticacao...",
                    )
                    await ensure_login(session_id)
                    await ensure_sales_area(session_id, kind, context_label, auth_retry_count + 1)
                    return
                raise RuntimeError(f"A Caixa nao confirmou a area de vendas para {context_label}.")

            token_email_grace_seconds_v2 = 15.0
            tokens_descartados_globais_v2: set[str] = set()

            portal_state_expression_v2 = """
            (() => {
                const text = (document.body?.innerText || '')
                    .normalize('NFD')
                    .replace(/[\\u0300-\\u036f]/g, '')
                    .toUpperCase();
                const href = String(location.href || '').toUpperCase();
                const visible = (el) => {
                    if (!el) return false;
                    const rect = el.getBoundingClientRect();
                    const style = getComputedStyle(el);
                    return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                };
                const hasAuthenticatedNav = Boolean(
                    [...document.querySelectorAll('a, button, span, div')]
                        .filter(visible)
                        .find((el) => {
                            if (el.closest('.om-footer, footer')) return false;
                            const label = String(el.innerText || el.textContent || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .toUpperCase();
                            const linkHref = String(el.getAttribute?.('href') || '').toUpperCase();
                            return (
                                linkHref.includes('/MINHASVENDAS') ||
                                linkHref.includes('ROUTER=0') ||
                                label.includes('RELATORIOS DE VENDA') ||
                                label.includes('MINHAS VENDAS') ||
                                label.includes('HISTORICO DE VENDAS') ||
                                label.includes('PAGAMENTO INSTANTANEO') ||
                                label.includes('PIX') ||
                                label.includes('SAIR')
                            );
                        })
                );
                const hasSalesTabs = Boolean(
                    visible(document.querySelector('header[role="tablist"]')) ||
                    [...document.querySelectorAll('header[role="tablist"] button[role="tab"], #HistoricoVendas button[role="tab"], #Pix button[role="tab"], #HistoricoVendas [role="tabpanel"], #Pix [role="tabpanel"]')]
                        .some(visible)
                );
                const loginInput = document.querySelector('#b2-b1-b4-InputMask');
                const passwordInput = document.querySelector('#b2-b1-Input_Password');
                const tokenInputs = [...document.querySelectorAll('input')]
                    .filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled && visible(el));
                const deviceVisible = text.includes('SELECIONE O DISPOSITIVO');
                const tokenDeliveryVisible = (
                    text.includes('ESCOLHA POR ONDE DESEJA RECEBER') ||
                    text.includes('RECEBER POR E-MAIL') ||
                    text.includes('@GMAIL.COM')
                );
                const invalidVisible = (
                    text.includes('CODIGO INVALIDO') ||
                    text.includes('TOKEN INVALIDO') ||
                    text.includes('TOKEN EXPIRADO') ||
                    text.includes('TENTATIVAS')
                );
                const tokenVisible = tokenInputs.length > 0 && (
                    text.includes('TOKEN') ||
                    text.includes('CODIGO') ||
                    text.includes('E-MAIL') ||
                    text.includes('EMAIL')
                );
                const genericErrorVisible = (
                    text.includes('OPS, UM ERRO ACONTECEU') ||
                    text.includes('OPS UM ERRO ACONTECEU') ||
                    (text.includes('ERRO ACONTECEU') && text.includes('TENTE NOVAMENTE MAIS TARDE'))
                );
                const loginTextVisible = (
                    text.includes('ACESSE SUA CONTA') ||
                    (text.includes('CNPJ, CPF OU USUARIO') && text.includes('SENHA'))
                );
                const loginVisible = (
                    href.includes('ISTIMEOUT=TRUE') ||
                    (visible(loginInput) && visible(passwordInput)) ||
                    (
                        loginTextVisible &&
                        !deviceVisible &&
                        !tokenDeliveryVisible &&
                        !tokenVisible &&
                        !invalidVisible
                    )
                );
                const homeVisible = (
                    !loginVisible &&
                    !deviceVisible &&
                    !tokenVisible &&
                    !tokenDeliveryVisible &&
                    !invalidVisible &&
                    (href.includes('/HOME') || text.includes('RELATORIOS DE VENDA') || hasAuthenticatedNav)
                );
                const bodyLength = (document.body?.innerText || '').trim().length;
                if (href.includes('/_ERROR.HTML') || text.includes('ERROR PROCESSING YOUR REQUEST') || genericErrorVisible) return 'portal_error';
                if (hasSalesTabs) return 'sales';
                if (loginVisible) return 'login';
                if (homeVisible) return 'home';
                if (invalidVisible) return 'invalid';
                if (deviceVisible) return 'device';
                if (tokenVisible) return 'token';
                if (tokenDeliveryVisible) return 'token_delivery';
                if (document.readyState !== 'complete' || bodyLength < 20) return 'loading';
                return 'unknown';
            })()
            """

            async def capture_portal_html_debug_v2(session_id: str, filename: str) -> None:
                try:
                    html_debug = await eval_js(
                        session_id,
                        "document.documentElement ? document.documentElement.outerHTML : ''",
                        timeout=15.0,
                    )
                    if html_debug:
                        (artifacts_dir / filename).write_text(str(html_debug), encoding="utf-8")
                except Exception:
                    pass

            async def get_portal_state_v2(session_id: str, timeout: float = 15.0) -> str:
                try:
                    return str(await eval_js(session_id, portal_state_expression_v2, timeout=timeout) or "").strip()
                except Exception:
                    return ""

            async def wait_for_portal_state_v2(
                session_id: str,
                accepted_states: set[str],
                timeout: float,
                description: str,
                step: float = 0.5,
            ) -> str:
                deadline = time.time() + timeout
                last_state = ""
                while time.time() < deadline:
                    _check_cancelled()
                    state = await get_portal_state_v2(session_id)
                    last_state = state or ""
                    if state in accepted_states:
                        return state
                    await asyncio.sleep(step)
                raise TimeoutError(f"{description} :: ultimo estado={last_state or 'desconhecido'}")

            async def is_app_token_screen_v2(session_id: str) -> bool:
                return bool(
                    await eval_js(
                        session_id,
                        """
                        (() => {
                            const text = (document.body?.innerText || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .toUpperCase();
                            return text.includes('INFORME O TOKEN DO APLICATIVO');
                        })()
                        """,
                        timeout=10.0,
                    )
                )

            async def inspect_token_challenge_v2(session_id: str) -> dict:
                return (
                    await eval_js(
                        session_id,
                        """
                        (() => {
                            const text = (document.body?.innerText || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .toUpperCase();
                            const href = String(location.href || '').toUpperCase();
                            const visible = (el) => {
                                if (!el) return false;
                                const rect = el.getBoundingClientRect();
                                const style = getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                            };
                            const loginInput = document.querySelector('#b2-b1-b4-InputMask');
                            const passwordInput = document.querySelector('#b2-b1-Input_Password');
                            const tokenInputs = [...document.querySelectorAll('input')].filter((el) => {
                                const type = (el.type || '').toLowerCase();
                                return type !== 'hidden' && type !== 'password' && !el.disabled && visible(el);
                            });
                            const deliveryVisible = (
                                text.includes('ESCOLHA POR ONDE DESEJA RECEBER') ||
                                text.includes('RECEBER POR E-MAIL') ||
                                text.includes('@GMAIL.COM')
                            );
                            const appTokenVisible = text.includes('INFORME O TOKEN DO APLICATIVO');
                            const invalidVisible = (
                                text.includes('CODIGO INVALIDO') ||
                                text.includes('TOKEN INVALIDO') ||
                                text.includes('TOKEN EXPIRADO') ||
                                text.includes('TENTATIVAS')
                            );
                            const emailTokenReady = tokenInputs.length > 0 && (
                                text.includes('TOKEN') ||
                                text.includes('CODIGO') ||
                                text.includes('E-MAIL') ||
                                text.includes('EMAIL')
                            );
                            const loginTextVisible = (
                                text.includes('ACESSE SUA CONTA') ||
                                (text.includes('CNPJ, CPF OU USUARIO') && text.includes('SENHA'))
                            );
                            const onLoginPage = (
                                (visible(loginInput) && visible(passwordInput)) ||
                                (
                                    loginTextVisible &&
                                    !deliveryVisible &&
                                    !appTokenVisible &&
                                    !emailTokenReady &&
                                    !invalidVisible &&
                                    !text.includes('SELECIONE O DISPOSITIVO')
                                )
                            );
                            return {
                                onLoginPage,
                                tokenInputsVisible: !onLoginPage && tokenInputs.length > 0,
                                emailTokenReady: !onLoginPage && emailTokenReady,
                                deliveryVisible: !onLoginPage && deliveryVisible,
                                appTokenVisible: !onLoginPage && appTokenVisible,
                                invalidVisible: !onLoginPage && invalidVisible,
                            };
                        })()
                        """,
                        timeout=10.0,
                    )
                    or {}
                )

            async def wait_for_token_email_delivery_grace_v2() -> None:
                _emit_pix_status(
                    on_status,
                    f"Aguardando {int(token_email_grace_seconds_v2)}s para o e-mail do token chegar...",
                )
                await asyncio.sleep(token_email_grace_seconds_v2)

            async def wait_for_post_token_resolution_v2(session_id: str, timeout: float = 150.0) -> str:
                deadline = time.time() + timeout
                while time.time() < deadline:
                    _check_cancelled()
                    snapshot = await eval_js(
                        session_id,
                        """
                        (() => {
                            const text = (document.body?.innerText || '')
                                .normalize('NFD')
                                .replace(/[\\u0300-\\u036f]/g, '')
                                .toUpperCase();
                            const href = String(location.href || '').toUpperCase();
                            const visible = (el) => {
                                if (!el) return false;
                                const rect = el.getBoundingClientRect();
                                const style = getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                            };
                            const bodyLength = (document.body?.innerText || '').trim().length;
                            const tokenInputs = [...document.querySelectorAll('input')]
                                .filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled && visible(el));
                            const hasAuthenticatedNav = Boolean(
                                [...document.querySelectorAll('a, button, span, div')]
                                    .filter(visible)
                                    .find((el) => {
                                        if (el.closest('.om-footer, footer')) return false;
                                        const label = String(el.innerText || el.textContent || '')
                                            .normalize('NFD')
                                            .replace(/[\\u0300-\\u036f]/g, '')
                                            .toUpperCase();
                                        const linkHref = String(el.getAttribute?.('href') || '').toUpperCase();
                                        return (
                                            linkHref.includes('/MINHASVENDAS') ||
                                            linkHref.includes('ROUTER=0') ||
                                            label.includes('RELATORIOS DE VENDA') ||
                                            label.includes('MINHAS VENDAS') ||
                                            label.includes('HISTORICO DE VENDAS') ||
                                            label.includes('PAGAMENTO INSTANTANEO') ||
                                            label.includes('PIX') ||
                                            label.includes('SAIR')
                                        );
                                    })
                            );
                            const hasSalesTabs = Boolean(
                                visible(document.querySelector('header[role="tablist"]')) ||
                                [...document.querySelectorAll('header[role="tablist"] button[role="tab"], #HistoricoVendas button[role="tab"], #Pix button[role="tab"], #HistoricoVendas [role="tabpanel"], #Pix [role="tabpanel"]')]
                                    .some(visible)
                            );
                            const loginInput = document.querySelector('#b2-b1-b4-InputMask');
                            const passwordInput = document.querySelector('#b2-b1-Input_Password');
                            const deviceVisible = text.includes('SELECIONE O DISPOSITIVO');
                            const tokenDeliveryVisible = text.includes('ESCOLHA POR ONDE DESEJA RECEBER') || text.includes('RECEBER POR E-MAIL') || text.includes('@GMAIL.COM');
                            const invalidVisible = (
                                text.includes('CODIGO INVALIDO') ||
                                text.includes('TOKEN INVALIDO') ||
                                text.includes('TOKEN EXPIRADO') ||
                                text.includes('TENTATIVAS')
                            );
                            const tokenVisible = tokenInputs.length > 0 && (text.includes('TOKEN') || text.includes('CODIGO') || text.includes('E-MAIL') || text.includes('EMAIL'));
                            const genericErrorVisible = (
                                text.includes('OPS, UM ERRO ACONTECEU') ||
                                text.includes('OPS UM ERRO ACONTECEU') ||
                                (text.includes('ERRO ACONTECEU') && text.includes('TENTE NOVAMENTE MAIS TARDE'))
                            );
                            const loginTextVisible = text.includes('ACESSE SUA CONTA') || (text.includes('CNPJ, CPF OU USUARIO') && text.includes('SENHA'));
                            const loginVisible = (
                                href.includes('ISTIMEOUT=TRUE') ||
                                (visible(loginInput) && visible(passwordInput)) ||
                                (
                                    loginTextVisible &&
                                    !deviceVisible &&
                                    !tokenDeliveryVisible &&
                                    !tokenVisible &&
                                    !invalidVisible
                                )
                            );
                            return {
                                href,
                                hasSalesTabs,
                                homeVisible: !loginVisible && !deviceVisible && !tokenVisible && !tokenDeliveryVisible && !invalidVisible && (href.includes('/HOME') || text.includes('RELATORIOS DE VENDA') || hasAuthenticatedNav),
                                loginVisible,
                                portalErrorVisible: href.includes('/_ERROR.HTML') || text.includes('ERROR PROCESSING YOUR REQUEST'),
                                genericErrorVisible,
                                invalidVisible,
                                tokenVisible,
                                tokenDeliveryVisible,
                                salesVisible: hasSalesTabs,
                                bodyLength,
                            };
                        })()
                        """,
                        timeout=15.0,
                    ) or {}
                    if bool(snapshot.get("portalErrorVisible")) or bool(snapshot.get("genericErrorVisible")):
                        return "portal_error"
                    if bool(snapshot.get("salesVisible")):
                        return "sales"
                    if bool(snapshot.get("homeVisible")):
                        return "home"
                    if bool(snapshot.get("loginVisible")):
                        return "login"
                    if bool(snapshot.get("invalidVisible")) and (
                        bool(snapshot.get("tokenVisible")) or bool(snapshot.get("tokenDeliveryVisible"))
                    ):
                        return "invalid"
                    await asyncio.sleep(0.5)
                return ""

            async def confirm_sales_access_after_token_v2(session_id: str) -> str:
                try:
                    await navigate(session_id, credenciais["sales_url"])
                    return await wait_for_portal_state_v2(
                        session_id,
                        {"sales", "home", "token", "token_delivery", "login", "portal_error", "invalid"},
                        timeout=90.0,
                        description="A Caixa nao confirmou a sessao ao abrir a area de vendas apos o token",
                    )
                except Exception:
                    return ""

            async def fetch_token_with_portal_watch_v2(
                session_id: str,
                total_timeout: float,
                allow_recent_fallback: bool,
            ) -> tuple[str, str]:
                deadline = time.time() + total_timeout
                first_chunk = True
                while time.time() < deadline:
                    state_before = await get_portal_state_v2(session_id)
                    if state_before in {"sales", "home", "login", "portal_error"}:
                        return "", state_before
                    remaining = max(0.0, deadline - time.time())
                    chunk_timeout = min(12.0, remaining)
                    if chunk_timeout <= 0:
                        break
                    token = str(
                        await asyncio.to_thread(
                            _fetch_fiserv_token_from_gmail,
                            time.time(),
                            chunk_timeout,
                            on_status,
                            tokens_descartados_globais_v2,
                            allow_recent_fallback if first_chunk else False,
                        )
                        or ""
                    ).strip()
                    if token:
                        return token, ""
                    first_chunk = False
                    state_after = await get_portal_state_v2(session_id)
                    if state_after in {"sales", "home", "login", "portal_error"}:
                        return "", state_after
                return "", ""

            async def perform_login_step_v2(session_id: str) -> str:
                _emit_pix_status(on_status, "Autenticando na Azulzinha/Caixa...")
                try:
                    login_selectors = await wait_for_condition(
                        session_id,
                        """
                        (() => {
                            const user = document.querySelector('#b2-b1-b4-InputMask');
                            const password = document.querySelector('#b2-b1-Input_Password');
                            if (user && password) {
                                user.setAttribute('data-codex-login-user', '1');
                                password.setAttribute('data-codex-login-pass', '1');
                                return {
                                    user: '#b2-b1-b4-InputMask',
                                    password: '#b2-b1-Input_Password',
                                };
                            }
                            const visible = (el) => {
                                if (!el || el.disabled) return false;
                                const rect = el.getBoundingClientRect();
                                const style = getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                            };
                            const fallbackUser = [...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])')].filter(visible)[0];
                            const fallbackPassword = [...document.querySelectorAll('input[type="password"]')].filter(visible)[0];
                            if (!fallbackUser || !fallbackPassword) return null;
                            fallbackUser.setAttribute('data-codex-login-user', '1');
                            fallbackPassword.setAttribute('data-codex-login-pass', '1');
                            return {
                                user: fallbackUser.id ? `#${fallbackUser.id}` : '[data-codex-login-user="1"]',
                                password: fallbackPassword.id ? `#${fallbackPassword.id}` : '[data-codex-login-pass="1"]',
                            };
                        })()
                        """,
                        timeout=60.0,
                        description="A Caixa nao exibiu a tela de login para informar CNPJ e senha",
                    )
                except Exception:
                    login_selectors = await eval_js(
                        session_id,
                        """
                        (() => {
                            const user = document.querySelector('#b2-b1-b4-InputMask');
                            const password = document.querySelector('#b2-b1-Input_Password');
                            if (!user || !password) return null;
                            user.setAttribute('data-codex-login-user', '1');
                            password.setAttribute('data-codex-login-pass', '1');
                            return {
                                user: '#b2-b1-b4-InputMask',
                                password: '#b2-b1-Input_Password',
                            };
                        })()
                        """,
                        timeout=15.0,
                    )
                    if not login_selectors:
                        try:
                            login_wait_state = await eval_js(
                                session_id,
                                """
                                (() => ({
                                    href: String(location.href || ''),
                                    text: String(document.body?.innerText || '').slice(0, 2000),
                                    hasUser: !!document.querySelector('#b2-b1-b4-InputMask'),
                                    hasPassword: !!document.querySelector('#b2-b1-Input_Password'),
                                    bodyLength: String(document.body?.innerText || '').trim().length,
                                }))()
                                """,
                                timeout=15.0,
                            )
                            if login_wait_state is not None:
                                (artifacts_dir / "azulzinha_login_wait_state.json").write_text(
                                    json.dumps(login_wait_state, ensure_ascii=False, indent=2),
                                    encoding="utf-8",
                                )
                        except Exception:
                            pass
                        await capture_portal_html_debug_v2(session_id, "azulzinha_login_wait_debug.html")
                        raise
                login_user_selector = str((login_selectors or {}).get("user") or "#b2-b1-b4-InputMask")
                login_password_selector = str((login_selectors or {}).get("password") or "#b2-b1-Input_Password")
                cnpj_digits = re.sub(r"\D", "", str(credenciais["cnpj"] or ""))
                password_text = str(credenciais["password"] or "")
                try:
                    await insert_text(session_id, login_user_selector, cnpj_digits)
                except Exception:
                    pass
                try:
                    await insert_text(session_id, login_password_selector, password_text)
                except Exception:
                    pass
                await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const visible = (el) => {{
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        }};
                        const setNativeValue = (el, value) => {{
                            const proto = el instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
                            const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
                            if (setter) setter.call(el, value); else el.value = value;
                            el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                            el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                            el.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                        }};
                        const user = document.querySelector({login_user_selector!r}) || [...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])')].filter(visible)[0];
                        const password = document.querySelector({login_password_selector!r}) || [...document.querySelectorAll('input[type="password"]')].filter(visible)[0];
                        if (!user || !password) return null;
                        user.focus();
                        setNativeValue(user, {cnpj_digits!r});
                        password.focus();
                        setNativeValue(password, {password_text!r});
                        const userValue = String(user?.value || user?.getAttribute('value') || '').replace(/\\D/g, '');
                        const passwordValue = String(password?.value || password?.getAttribute('value') || '');
                        return {{
                            userLen: userValue.length,
                            passwordLen: passwordValue.length,
                        }};
                    }})()
                    """,
                    timeout=20.0,
                ) or {}
                credential_fill_state = await eval_js(
                    session_id,
                    f"""
                    (() => {{
                        const visible = (el) => {{
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        }};
                        const user = document.querySelector({login_user_selector!r}) || [...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])')].filter(visible)[0];
                        const password = document.querySelector({login_password_selector!r}) || [...document.querySelectorAll('input[type="password"]')].filter(visible)[0];
                        const userValue = String(user?.value || user?.getAttribute('value') || '').replace(/\\D/g, '');
                        const passwordValue = String(password?.value || password?.getAttribute('value') || '');
                        return {{
                            userLen: userValue.length,
                            passwordLen: passwordValue.length,
                        }};
                    }})()
                    """,
                    timeout=15.0,
                ) or {}
                if int(credential_fill_state.get("userLen") or 0) < len(cnpj_digits) or int(credential_fill_state.get("passwordLen") or 0) < len(password_text):
                    state_after_fill = await get_portal_state_v2(session_id)
                    if state_after_fill in {"device", "token_delivery", "token", "sales", "portal_error"}:
                        return state_after_fill
                    await capture_portal_html_debug_v2(session_id, "azulzinha_login_fill_debug.html")
                    raise RuntimeError("Nao foi possivel preencher o login e a senha da Azulzinha/Caixa.")
                await asyncio.sleep(0.4)
                await press_tab(session_id)
                try:
                    await wait_for_condition(
                        session_id,
                        """
                        (() => {
                            const botao = document.querySelector('#b2-b1-confirmar');
                            return botao && !botao.disabled;
                        })()
                        """,
                        timeout=8.0,
                    )
                except Exception:
                    await eval_js(
                        session_id,
                        """
                        (() => {
                            const visible = (el) => {
                                if (!el) return false;
                                const rect = el.getBoundingClientRect();
                                const style = getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                            };
                            const user = [...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])')].filter(visible)[0];
                            const password = [...document.querySelectorAll('input[type="password"]')].filter(visible)[0];
                            for (const el of [user, password]) {
                                if (!el) continue;
                                el.dispatchEvent(new Event('input', { bubbles: true }));
                                el.dispatchEvent(new Event('change', { bubbles: true }));
                                el.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: '0' }));
                                el.dispatchEvent(new Event('blur', { bubbles: true }));
                            }
                            return true;
                        })()
                        """,
                        timeout=10.0,
                    )
                    try:
                        await wait_for_condition(
                            session_id,
                            """
                            (() => {
                                const botao = document.querySelector('#b2-b1-confirmar');
                                return botao && !botao.disabled;
                            })()
                            """,
                            timeout=12.0,
                        )
                    except Exception:
                        state_after_confirm_wait = await get_portal_state_v2(session_id)
                        if state_after_confirm_wait in {"device", "token_delivery", "token", "sales", "home", "portal_error"}:
                            return state_after_confirm_wait
                        try:
                            button_state = await eval_js(
                                session_id,
                                """
                                (() => {
                                    const visible = (el) => {
                                        if (!el) return false;
                                        const rect = el.getBoundingClientRect();
                                        const style = getComputedStyle(el);
                                        return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                                    };
                                    const user = [...document.querySelectorAll('input:not([type="hidden"]):not([type="password"])')].filter(visible)[0];
                                    const password = [...document.querySelectorAll('input[type="password"]')].filter(visible)[0];
                                    const botao = document.querySelector('#b2-b1-confirmar');
                                    return {
                                        userValue: String(user?.value || ''),
                                        passwordLen: String(password?.value || '').length,
                                        buttonDisabled: botao ? !!botao.disabled : null,
                                        buttonText: botao ? (botao.innerText || botao.textContent || '').trim() : '',
                                    };
                                })()
                                """,
                                timeout=10.0,
                            )
                            if button_state is not None:
                                (artifacts_dir / "azulzinha_login_confirm_state.json").write_text(
                                    json.dumps(button_state, ensure_ascii=False, indent=2),
                                    encoding="utf-8",
                                )
                        except Exception:
                            pass
                        await capture_portal_html_debug_v2(session_id, "azulzinha_login_confirm_debug.html")
                        raise
                clicou = await eval_js(
                    session_id,
                    """
                    (() => {
                        const botao = document.querySelector('#b2-b1-confirmar');
                        if (!botao || botao.disabled) return false;
                        botao.click();
                        return true;
                    })()
                    """,
                    timeout=10.0,
                )
                if not clicou:
                    raise RuntimeError("Nao foi possivel confirmar o login da Azulzinha/Caixa.")
                return await wait_for_portal_state_v2(
                    session_id,
                    {"sales", "home", "device", "token_delivery", "token", "login", "portal_error", "invalid"},
                    timeout=90.0,
                    description="A Caixa nao concluiu a etapa inicial do login",
                )

            async def perform_device_selection_step_v2(session_id: str) -> str:
                _emit_pix_status(on_status, "Selecionando dispositivo da Caixa...")
                if not await click_card_by_text(session_id, _azulzinha_device_aliases(company_label), timeout=30.0):
                    raise RuntimeError(f"Nao foi possivel selecionar o dispositivo da {company_label} na Azulzinha/Caixa.")
                await asyncio.sleep(1.0)
                return await wait_for_portal_state_v2(
                    session_id,
                    {"sales", "home", "token_delivery", "token", "login", "portal_error", "invalid"},
                    timeout=45.0,
                    description="A Caixa nao avancou apos a selecao do dispositivo",
                )

            async def request_token_via_email_v2(
                session_id: str,
                current_state: str | None = None,
                force_refresh: bool = False,
            ) -> str:
                state_local = str(current_state or "").strip() or await get_portal_state_v2(session_id)
                token_challenge = await inspect_token_challenge_v2(session_id)
                if bool(token_challenge.get("emailTokenReady")) and not bool(token_challenge.get("appTokenVisible")):
                    return "token"
                if state_local not in {"token", "token_delivery", "invalid"}:
                    state_local = await wait_for_portal_state_v2(
                        session_id,
                        {"sales", "home", "login", "device", "token_delivery", "token", "portal_error", "invalid"},
                        timeout=30.0,
                        description="A Caixa nao exibiu a etapa de token por e-mail",
                    )
                    token_challenge = await inspect_token_challenge_v2(session_id)
                if state_local == "invalid":
                    if bool(token_challenge.get("deliveryVisible")) and not bool(token_challenge.get("emailTokenReady")):
                        state_local = "token_delivery"
                    else:
                        state_local = "token"
                if bool(token_challenge.get("emailTokenReady")) and not bool(token_challenge.get("appTokenVisible")):
                    return "token"
                if state_local == "token" and not force_refresh and not await is_app_token_screen_v2(session_id):
                    return "token"

                abriu = False
                if state_local == "token":
                    abriu = bool(
                        await eval_js(
                            session_id,
                            """
                            (() => {
                                const link = document.querySelector('a.bold.font-universe.cor-preto');
                                if (!link) return false;
                                link.click();
                                return true;
                            })()
                            """,
                            timeout=10.0,
                        )
                    )
                    if not abriu:
                        abriu = await click_by_text(
                            session_id,
                            ["Receber codigo por e-mail ou SMS", "Reenviar codigo", "Reenviar token"],
                            timeout=15.0,
                        )
                    if abriu:
                        state_local = await wait_for_portal_state_v2(
                            session_id,
                            {"sales", "home", "login", "token_delivery", "token", "portal_error", "invalid"},
                            timeout=30.0,
                            description="A Caixa nao abriu a escolha de envio do token",
                        )
                        token_challenge = await inspect_token_challenge_v2(session_id)
                        await asyncio.sleep(1.0)
                        if state_local == "invalid":
                            if bool(token_challenge.get("deliveryVisible")) and not bool(token_challenge.get("emailTokenReady")):
                                state_local = "token_delivery"
                            else:
                                state_local = "token"

                if state_local == "token_delivery":
                    token_challenge = await inspect_token_challenge_v2(session_id)
                    if bool(token_challenge.get("emailTokenReady")) and not bool(token_challenge.get("appTokenVisible")):
                        return "token"
                    selecionou_email = await eval_js(
                        session_id,
                        """
                        (() => {
                            const norm = (v) => (v || '').normalize('NFD').replace(/[\\u0300-\\u036f]/g, '').replace(/\\s+/g, ' ').trim().toUpperCase();
                            const visible = (el) => {
                                const rect = el.getBoundingClientRect();
                                const style = getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                            };
                            const candidates = [...document.querySelectorAll('[id$="-Content"], .card-content, .ph.card.card-content')]
                                .filter(visible)
                                .map((el) => {
                                    const text = norm(el.innerText || el.textContent || '');
                                    if (!text.includes('RECEBER POR E-MAIL') && !text.includes('@GMAIL.COM')) return null;
                                    const className = (el.className || '').toString().toUpperCase();
                                    const isCard = className.includes('CARD-CONTENT') || className.includes('PH CARD');
                                    const textLength = text.length || 9999;
                                    return { el, score: (isCard ? 1000 : 0) - textLength };
                                })
                                .filter(Boolean)
                                .sort((a, b) => b.score - a.score);
                            const match = candidates.length ? candidates[0].el : null;
                            if (!match) return false;
                            const clickable = match.closest('[id$="-Content"], .card-content, .ph.card.card-content') || match;
                            clickable.scrollIntoView({ block: 'center', inline: 'center' });
                            clickable.click();
                            return true;
                        })()
                        """,
                        timeout=10.0,
                    )
                    if not selecionou_email and not await click_card_by_text(session_id, ["RECEBER POR E-MAIL", "@GMAIL.COM"], timeout=20.0):
                        if not await click_by_text(session_id, ["Receber por e-mail", "@gmail.com"], timeout=20.0):
                            raise RuntimeError("Nao foi possivel selecionar o envio do token por e-mail na Azulzinha/Caixa.")
                    await asyncio.sleep(1.0)
                    try:
                        await click_by_text(
                            session_id,
                            ["Continuar", "Confirmar", "Receber codigo", "Enviar codigo"],
                            timeout=4.0,
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(0.8)
                    try:
                        state_local = await wait_for_portal_state_v2(
                            session_id,
                            {"sales", "home", "login", "token", "portal_error", "invalid"},
                            timeout=30.0,
                            description="A Caixa nao exibiu o campo para digitar o token",
                        )
                        token_challenge = await inspect_token_challenge_v2(session_id)
                        if state_local == "invalid":
                            if bool(token_challenge.get("deliveryVisible")) and not bool(token_challenge.get("emailTokenReady")):
                                state_local = "token_delivery"
                            else:
                                state_local = "token"
                    except Exception:
                        await capture_portal_html_debug_v2(session_id, "azulzinha_token_delivery_debug.html")
                        raise
                return state_local

            async def submit_token_value_v2(session_id: str, token: str) -> None:
                confirmed = False
                token_inputs = await eval_js(
                    session_id,
                    """
                    (() => {
                        const visible = (el) => {
                            if (!el) return false;
                            const rect = el.getBoundingClientRect();
                            const style = getComputedStyle(el);
                            return rect.width > 0 && rect.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                        };
                        return [...document.querySelectorAll('input')]
                            .filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled && visible(el))
                            .map((el) => ({
                                selector: el.id ? `#${el.id}` : '',
                                maxLength: Number(el.maxLength || 0),
                                className: String(el.className || ''),
                            }));
                    })()
                    """,
                    timeout=15.0,
                ) or []
                code_selectors = [
                    str(item.get("selector") or "").strip()
                    for item in token_inputs
                    if str(item.get("selector") or "").strip()
                    and (
                        int(item.get("maxLength") or 0) == 1
                        or "input-code" in str(item.get("className") or "").lower()
                    )
                ]
                token_selectors = code_selectors or [
                    str(item.get("selector") or "").strip()
                    for item in token_inputs
                    if str(item.get("selector") or "").strip()
                ]
                token_digitos = list(str(token))
                if token_selectors and len(token_selectors) > 1 and len(token_digitos) >= len(token_selectors):
                    for idx, selector in enumerate(token_selectors):
                        if not await focus_selector(session_id, selector):
                            continue
                        await eval_js(
                            session_id,
                            f"""
                            (() => {{
                                const el = document.querySelector({selector!r});
                                if (!el) return false;
                                el.value = '';
                                el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                return true;
                            }})()
                            """,
                            timeout=10.0,
                        )
                        await cdp("Input.insertText", {"text": token_digitos[idx]}, session_id=session_id)
                        await asyncio.sleep(0.08)
                    confirmed = True
                else:
                    confirmed = bool(
                        await eval_js(
                            session_id,
                            f"""
                            (() => {{
                                const token = {token!r};
                                const setNativeValue = (el, value) => {{
                                    const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')?.set;
                                    if (setter) setter.call(el, value); else el.value = value;
                                    el.dispatchEvent(new Event('input', {{ bubbles: true }}));
                                    el.dispatchEvent(new Event('change', {{ bubbles: true }}));
                                    el.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                                }};
                                const inputs = [...document.querySelectorAll('input')].filter((el) => (el.type || '').toLowerCase() !== 'hidden' && !el.disabled);
                                if (inputs.length) {{
                                    setNativeValue(inputs[inputs.length - 1], '');
                                    setNativeValue(inputs[inputs.length - 1], token);
                                    return true;
                                }}
                                return false;
                            }})()
                            """,
                            timeout=15.0,
                        )
                    )
                if not confirmed:
                    raise RuntimeError("Nao foi possivel preencher o token da Azulzinha/Caixa.")
                await asyncio.sleep(0.4)
                try:
                    await wait_for_condition(
                        session_id,
                        """
                        (() => {
                            const botao = document.querySelector('#b2-b1-confirmar');
                            return botao && !botao.disabled;
                        })()
                        """,
                        timeout=5.0,
                    )
                except Exception:
                    pass
                await eval_js(
                    session_id,
                    """
                    (() => {
                        const buttons = [...document.querySelectorAll('button, input[type="submit"], a')];
                        const button = buttons.find((el) => /CONFIRMAR|VALIDAR|ENTRAR|CONTINUAR/i.test(el.innerText || el.value || '')) || buttons[0];
                        button?.click();
                        return true;
                    })()
                    """,
                    timeout=10.0,
                )

            async def complete_token_challenge_v2(session_id: str, initial_state: str) -> str:
                state = str(initial_state or "").strip() or await get_portal_state_v2(session_id)
                ultimo_token_usado = ""
                _emit_pix_status(on_status, "Solicitando token por e-mail...")
                state = await request_token_via_email_v2(session_id, state, force_refresh=False)
                if state == "token":
                    await wait_for_token_email_delivery_grace_v2()
                    state = await get_portal_state_v2(session_id)
                if state in {"sales", "home", "login", "portal_error"}:
                    return state

                for tentativa_token in range(3):
                    if tentativa_token > 0:
                        espera_extra_apos_rejeicao = 5.0
                        _emit_pix_status(
                            on_status,
                            f"Token rejeitado pela Caixa; descartando o codigo anterior e aguardando {int(espera_extra_apos_rejeicao)}s por um novo e-mail...",
                        )
                        await asyncio.sleep(espera_extra_apos_rejeicao)
                        state = await get_portal_state_v2(session_id)
                        if state in {"sales", "home", "login", "portal_error"}:
                            return state
                        if state == "device":
                            state = await perform_device_selection_step_v2(session_id)
                            if state in {"sales", "home", "login", "portal_error"}:
                                return state
                        token_challenge = await inspect_token_challenge_v2(session_id)
                        precisa_reenviar_token = False
                        if state == "token_delivery":
                            precisa_reenviar_token = not bool(token_challenge.get("emailTokenReady"))
                        elif state == "token":
                            precisa_reenviar_token = bool(token_challenge.get("appTokenVisible"))
                        elif state == "invalid":
                            precisa_reenviar_token = bool(token_challenge.get("appTokenVisible")) or (
                                bool(token_challenge.get("deliveryVisible")) and not bool(token_challenge.get("emailTokenReady"))
                            )
                        if precisa_reenviar_token:
                            state = await request_token_via_email_v2(session_id, state, force_refresh=True)
                            if state == "token":
                                state = await get_portal_state_v2(session_id)
                            elif state in {"sales", "home", "login", "portal_error"}:
                                return state
                        else:
                            _emit_pix_status(
                                on_status,
                                "A Caixa manteve a tela de digitacao do token; aguardando um novo e-mail sem reenviar o codigo.",
                            )

                    espera_token = 180.0 if tentativa_token == 0 else 150.0
                    token, portal_state_during_fetch = await fetch_token_with_portal_watch_v2(
                        session_id,
                        espera_token,
                        tentativa_token == 0,
                    )
                    if portal_state_during_fetch in {"sales", "home", "login", "portal_error"}:
                        return portal_state_during_fetch
                    if (not token or token == ultimo_token_usado or token in tokens_descartados_globais_v2) and callable(token_callback):
                        token = str(token_callback("Informe o token enviado por e-mail pela Azulzinha/Caixa") or "").strip()
                    if token in tokens_descartados_globais_v2:
                        _emit_pix_status(on_status, "O token informado ja foi rejeitado anteriormente pela Caixa.")
                        token = ""
                    if not token:
                        if tentativa_token >= 2:
                            raise RuntimeError("Nao foi possivel obter um token novo enviado por e-mail pela Caixa.")
                        _emit_pix_status(on_status, "Ainda nao chegou um token novo da Caixa; aguardando mais um pouco antes da proxima tentativa.")
                        continue

                    state_before_submit = await get_portal_state_v2(session_id)
                    if state_before_submit in {"sales", "home", "login", "portal_error"}:
                        return state_before_submit
                    token_challenge_before_submit = await inspect_token_challenge_v2(session_id)
                    if state_before_submit == "token_delivery" and not bool(token_challenge_before_submit.get("emailTokenReady")):
                        state_before_submit = await request_token_via_email_v2(session_id, state_before_submit, force_refresh=False)
                        if state_before_submit in {"sales", "home", "login", "portal_error"}:
                            return state_before_submit

                    ultimo_token_usado = token
                    _emit_pix_status(on_status, f"Confirmando token da Caixa: {token}...")
                    await submit_token_value_v2(session_id, token)
                    _emit_pix_status(on_status, "Aguardando a Caixa validar o token...")
                    await asyncio.sleep(1.5)
                    resultado_token = await wait_for_post_token_resolution_v2(session_id)
                    if not resultado_token:
                        _emit_pix_status(
                            on_status,
                            "Validando a sessao da Caixa pela abertura de MinhasVendas...",
                        )
                        confirmacao_sessao = await confirm_sales_access_after_token_v2(session_id)
                        if confirmacao_sessao:
                            resultado_token = confirmacao_sessao

                    if resultado_token not in {"sales", "home", "invalid", "token", "token_delivery", "login", "portal_error"}:
                        resultado_token = await confirm_sales_access_after_token_v2(session_id)

                    if resultado_token == "sales":
                        return "sales"
                    if resultado_token == "home":
                        return "home"
                    if resultado_token == "login":
                        _emit_pix_status(
                            on_status,
                            "A Caixa voltou para a tela de login apos validar o token; refazendo a autenticacao...",
                        )
                        return "login"
                    if resultado_token == "portal_error":
                        _emit_pix_status(
                            on_status,
                            "A Caixa exibiu um erro do portal ao confirmar o token; refazendo a autenticacao...",
                        )
                        await capture_portal_html_debug_v2(session_id, "azulzinha_post_token_error.html")
                        return "portal_error"

                    tokens_descartados_globais_v2.add(token)
                    if tentativa_token >= 2:
                        raise RuntimeError("O token informado pela Caixa nao foi aceito apos multiplas tentativas.")
                raise RuntimeError("A Caixa nao concluiu a validacao do token.")

            async def open_sales_area_from_home_v2(session_id: str, context_label: str) -> str:
                _emit_pix_status(on_status, "Abrindo Relatorio de vendas da Caixa...")

                async def _click_sales_report_entry() -> bool:
                    return bool(
                        await eval_js(
                            session_id,
                            """
                            (() => {
                                const visible = (el) => {
                                    if (!el) return false;
                                    const rect = el.getBoundingClientRect();
                                    const style = getComputedStyle(el);
                                    return (
                                        rect.width > 0 &&
                                        rect.height > 0 &&
                                        rect.bottom > 0 &&
                                        rect.right > 0 &&
                                        rect.top < window.innerHeight &&
                                        rect.left < window.innerWidth &&
                                        style.visibility !== 'hidden' &&
                                        style.display !== 'none'
                                    );
                                };
                                const trigger = (el) => {
                                    if (!el) return false;
                                    const clickable = el.closest('li[opt], a, button, div') || el;
                                    clickable.scrollIntoView({ block: 'center', inline: 'center' });
                                    clickable.focus?.();
                                    clickable.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window }));
                                    clickable.click();
                                    clickable.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window }));
                                    return true;
                                };
                                const selectors = [
                                    'a[data-testid="menu-relatorio-vendas"]',
                                    'a[data-testid="m-menu-relatorio-vendas"]',
                                ];
                                for (const selector of selectors) {
                                    const el = document.querySelector(selector);
                                    if (!el || !visible(el)) continue;
                                    return trigger(el);
                                }
                                return false;
                            })()
                            """,
                            timeout=10.0,
                        )
                    )

                abriu_vendas = await _click_sales_report_entry()
                if not abriu_vendas:
                    expanded = bool(
                        await eval_js(
                            session_id,
                            """
                            (() => {
                                const visible = (el) => {
                                    if (!el) return false;
                                    const rect = el.getBoundingClientRect();
                                    const style = getComputedStyle(el);
                                    return (
                                        rect.width > 0 &&
                                        rect.height > 0 &&
                                        rect.bottom > 0 &&
                                        rect.right > 0 &&
                                        rect.top < window.innerHeight &&
                                        rect.left < window.innerWidth &&
                                        style.visibility !== 'hidden' &&
                                        style.display !== 'none'
                                    );
                                };
                                const trigger = (el) => {
                                    if (!el) return false;
                                    const clickable = el.closest('li[opt], a, button, div') || el;
                                    clickable.scrollIntoView({ block: 'center', inline: 'center' });
                                    clickable.focus?.();
                                    clickable.dispatchEvent(new MouseEvent('mousedown', { bubbles: true, cancelable: true, view: window }));
                                    clickable.click();
                                    clickable.dispatchEvent(new MouseEvent('mouseup', { bubbles: true, cancelable: true, view: window }));
                                    return true;
                                };
                                const selectors = [
                                    'a[data-testid="menu-vendas"]',
                                    'a[data-testid="m-menu-vendas"]',
                                    'a[data-testid="home-card-personalizar-link-vendas"]',
                                ];
                                for (const selector of selectors) {
                                    const el = document.querySelector(selector);
                                    if (!el || !visible(el)) continue;
                                    return trigger(el);
                                }
                                return false;
                            })()
                            """,
                            timeout=10.0,
                        )
                    )
                    if expanded:
                        await asyncio.sleep(1.0)
                        abriu_vendas = await _click_sales_report_entry()
                if not abriu_vendas:
                    try:
                        abriu_vendas = await click_by_text(
                            session_id,
                            ["Relatório de vendas", "Relatorio de vendas"],
                            timeout=10.0,
                        )
                    except Exception:
                        abriu_vendas = False
                if abriu_vendas:
                    await asyncio.sleep(1.0)
                    state_after_click = await wait_for_portal_state_v2(
                        session_id,
                        {"sales", "home", "login", "device", "token_delivery", "token", "portal_error", "invalid"},
                        timeout=60.0,
                        description=f"A Caixa nao abriu o Relatorio de vendas para {context_label}",
                    )
                    _emit_pix_status(on_status, f"Estado do portal apos abrir Relatorio de vendas: {state_after_click or 'desconhecido'}.")
                    if state_after_click == "sales":
                        await capture_portal_html_debug_v2(session_id, "azulzinha_sales_entry_debug.html")
                    if state_after_click != "home":
                        return state_after_click

                await navigate(session_id, credenciais["sales_url"])
                state_after_navigation = await wait_for_portal_state_v2(
                    session_id,
                    {"sales", "home", "login", "device", "token_delivery", "token", "portal_error", "invalid"},
                    timeout=60.0,
                    description=f"A Caixa nao abriu a area de vendas para {context_label}",
                )
                _emit_pix_status(on_status, f"Estado do portal apos abrir a URL da area de vendas: {state_after_navigation or 'desconhecido'}.")
                if state_after_navigation == "sales":
                    await capture_portal_html_debug_v2(session_id, "azulzinha_sales_entry_debug.html")
                return state_after_navigation

            async def ensure_authenticated_sales_area(session_id: str, kind: str, context_label: str) -> None:
                last_state = ""
                for cycle in range(4):
                    _check_cancelled()
                    state = await get_portal_state_v2(session_id)
                    last_state = state or last_state

                    if state in {"", "loading", "unknown"}:
                        if cycle == 0:
                            _emit_pix_status(on_status, "Acessando Azulzinha/Caixa...")
                            await navigate(session_id, credenciais["login_url"])
                        else:
                            await navigate(session_id, credenciais["sales_url"])
                        state = await wait_for_portal_state_v2(
                            session_id,
                            {"sales", "home", "login", "device", "token_delivery", "token", "portal_error", "invalid"},
                            timeout=90.0,
                            description=f"A Caixa nao exibiu a tela esperada para {context_label}",
                        )
                        last_state = state

                    if state == "portal_error":
                        _emit_pix_status(
                            on_status,
                            f"A Caixa abriu a pagina de erro ao acessar {context_label}; recarregando o portal...",
                        )
                        await capture_portal_html_debug_v2(session_id, f"azulzinha_portal_error_{kind}.html")
                        await asyncio.sleep(2.0)
                        await navigate(session_id, credenciais["login_url"])
                        continue

                    if state in {"login", "device", "token_delivery", "token", "invalid"}:
                        if state == "login":
                            state = await perform_login_step_v2(session_id)
                        if state == "device":
                            state = await perform_device_selection_step_v2(session_id)
                        if state in {"token_delivery", "token", "invalid"}:
                            state = await complete_token_challenge_v2(session_id, state)
                        last_state = state

                    if state == "home":
                        state = await open_sales_area_from_home_v2(session_id, context_label)
                        last_state = state or last_state

                    if state == "sales":
                        await capture_portal_html_debug_v2(session_id, "azulzinha_sales_before_settle.html")
                        try:
                            await wait_for_portal_settle(
                                session_id,
                                timeout=25.0,
                                context_label=f"abrir {context_label}",
                            )
                            return
                        except Exception:
                            state = await get_portal_state_v2(session_id)
                            last_state = state or last_state
                            if state in {"login", "portal_error"}:
                                continue
                            raise

                    if state == "portal_error":
                        _emit_pix_status(
                            on_status,
                            f"A Caixa abriu a pagina de erro durante a autenticacao de {context_label}; tentando novamente...",
                        )
                        await capture_portal_html_debug_v2(session_id, f"azulzinha_auth_error_{kind}.html")
                        await asyncio.sleep(2.0)
                        await navigate(session_id, credenciais["login_url"])
                        continue

                    if state == "login":
                        await asyncio.sleep(2.0)
                        continue

                    state = await open_sales_area_from_home_v2(session_id, context_label)
                    last_state = state
                    if state == "sales":
                        await capture_portal_html_debug_v2(session_id, "azulzinha_sales_before_settle.html")
                        try:
                            await wait_for_portal_settle(
                                session_id,
                                timeout=25.0,
                                context_label=f"abrir {context_label}",
                            )
                            return
                        except Exception:
                            state = await get_portal_state_v2(session_id)
                            last_state = state or last_state
                            if state in {"login", "portal_error"}:
                                continue
                            raise

                await capture_portal_html_debug_v2(session_id, f"azulzinha_sales_area_debug_{kind}.html")
                raise RuntimeError(
                    f"A Caixa nao concluiu a autenticacao para abrir {context_label}. Ultimo estado observado: {last_state or 'desconhecido'}."
                )

            async def download_report(session_id: str, kind: str) -> str | None:
                context_label = f"o relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'}"
                export_ready_timeout = 90.0 if kind == "pix" else 60.0
                captured_timeout = 60.0 if kind == "pix" else 35.0
                download_timeout = 150.0 if kind == "pix" else 100.0
                today_br = datetime.now().strftime("%d/%m/%Y")
                use_today_tab = kind == "cartoes" and data_br == today_br
                active_tab_id = "Hoje" if use_today_tab else ("HistoricoVendas" if kind == "cartoes" else "Pix")
                last_error = None

                def _copy_establishment_variant(saved_path: str, establishment_id: str) -> str | None:
                    try:
                        source = Path(saved_path)
                    except Exception:
                        return None
                    if not source.exists():
                        return None
                    target = source.with_name(f"{source.stem}_est{establishment_id}{source.suffix}")
                    try:
                        shutil.copy2(source, target)
                        return str(target)
                    except Exception:
                        return None

                def _combine_establishment_reports(
                    captured_paths: list[tuple[str, str]],
                    report_kind: str,
                ) -> str | None:
                    if not captured_paths:
                        return None
                    if len(captured_paths) == 1:
                        return captured_paths[0][0]
                    if report_kind == "cartoes":
                        columns = [
                            "Data da venda",
                            "Cód. de autorização",
                            "Comprovante da venda",
                            "Produto",
                            "Parcelado",
                            "Bandeira",
                            "Canal",
                            "Terminal",
                            "Valor bruto",
                            "Status",
                            "Número do estabelecimento",
                            "Final do cartão",
                            "Cód. Ref. Cartão",
                        ]
                        rows_out: list[dict[str, object]] = []
                        seen: set[tuple[str, str, float, str, str]] = set()
                        for path_value, establishment_id in captured_paths:
                            reports = _build_card_reports_from_caixa(path_value, data_br)
                            for report_key, produto in (
                                ("cartao_credito_caixa", "Crédito"),
                                ("cartao_debito_caixa", "Débito"),
                            ):
                                for item in list((reports.get(report_key) or {}).get("itens_autorizados") or []):
                                    numero = str(item.get("numero") or "").strip()
                                    data_venda = str(item.get("data_venda") or "").strip()
                                    valor = round(float(item.get("valor_bruto", 0.0) or 0.0), 2)
                                    dedupe_key = (numero, data_venda, valor, produto, str(establishment_id or "").strip())
                                    if dedupe_key in seen:
                                        continue
                                    seen.add(dedupe_key)
                                    rows_out.append(
                                        {
                                            "Data da venda": data_venda,
                                            "Cód. de autorização": numero,
                                            "Comprovante da venda": item.get("numero_exibicao") or numero,
                                            "Produto": produto,
                                            "Parcelado": "-",
                                            "Bandeira": "",
                                            "Canal": "",
                                            "Terminal": "",
                                            "Valor bruto": valor,
                                            "Status": "Autorizada",
                                            "Número do estabelecimento": establishment_id,
                                            "Final do cartão": "",
                                            "Cód. Ref. Cartão": "",
                                        }
                                    )
                        if not rows_out:
                            return None
                        final_path = artifacts_dir / f"Historico_Simplificado_de_vendas_{data_br.replace('/', '-')}_{company_norm}_auto.xlsx"
                        import pandas as pd

                        pd.DataFrame(rows_out, columns=columns).to_excel(final_path, index=False)
                        return str(final_path)

                    headers = ["Data da venda", "Cód. de autorização", "Valor bruto", "Status"]
                    rows_out: list[dict[str, object]] = []
                    seen: set[tuple[str, str, float, str]] = set()
                    for path_value, _establishment_id in captured_paths:
                        suffix = _effective_local_report_suffix(path_value)
                        if suffix == ".csv":
                            report = _build_pix_report_from_caixa_csv(path_value, data_br)
                        elif suffix == ".xlsx":
                            report = _build_pix_report_from_caixa_xlsx(path_value, data_br)
                        else:
                            report = _build_pix_report_from_caixa_pdf(path_value, data_br)
                        for item in list(report.get("itens_autorizados") or []):
                            data_venda = str(item.get("data_venda") or "").strip()
                            codigo = str(item.get("nome") or "").strip()
                            valor = round(float(item.get("valor_bruto", 0.0) or 0.0), 2)
                            status = str(item.get("situacao") or "").strip() or "APROVADA"
                            dedupe_key = (data_venda, codigo, valor, status)
                            if dedupe_key in seen:
                                continue
                            seen.add(dedupe_key)
                            rows_out.append(
                                {
                                    "Data da venda": data_venda,
                                    "Cód. de autorização": codigo,
                                    "Valor bruto": valor,
                                    "Status": status,
                                }
                            )
                    if not rows_out:
                        return None
                    final_path = artifacts_dir / f"Relatorio_de_Vendas_Pix_{data_br.replace('/', '-')}_{company_norm}_auto.csv"
                    with open(final_path, "w", encoding="utf-8-sig", newline="") as handle:
                        writer = csv.DictWriter(handle, fieldnames=headers, delimiter=";")
                        writer.writeheader()
                        for row in rows_out:
                            writer.writerow(row)
                    return str(final_path)

                async def export_current_selection() -> str | None:
                    await wait_for_export_ready(session_id, kind, tab_id=active_tab_id, timeout=export_ready_timeout)
                    await asyncio.sleep(1.0)
                    started_at = _download_start_time()
                    event_start_index = len(event_log)
                    _emit_pix_status(on_status, f"Solicitando arquivo de {'cartoes' if kind == 'cartoes' else 'PIX'} para a Caixa...")
                    await click_export(session_id, "pix" if kind == "pix" else "cartoes", tab_id=active_tab_id)
                    captured = await wait_for_captured_report(session_id, kind, event_start_index, timeout=captured_timeout)
                    if captured:
                        _emit_pix_status(on_status, f"Relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'} recebido e validado.")
                        return captured
                    _emit_pix_status(on_status, f"Aguardando o download final do relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'}...")
                    return _persist_downloaded_report(
                        _wait_for_downloaded_report(browser_download_dir, data_br, kind, started_at, timeout=download_timeout),
                        kind,
                    )

                for attempt in range(2):
                    try:
                        await ensure_authenticated_sales_area(session_id, kind, context_label)
                        if kind == "cartoes":
                            _emit_pix_status(on_status, "Baixando relatorio de cartoes da Caixa...")
                            await activate_sales_tab(session_id, active_tab_id, timeout=35.0)
                            await wait_for_tab_content(session_id, active_tab_id, timeout=45.0)
                            await wait_for_portal_settle(
                                session_id,
                                tab_id=active_tab_id,
                                timeout=20.0,
                                context_label=f"estabilizar a aba {active_tab_id}",
                            )
                        else:
                            _emit_pix_status(on_status, "Baixando relatorio PIX da Caixa...")
                            await activate_sales_tab(session_id, active_tab_id, timeout=35.0)
                            await wait_for_tab_content(session_id, active_tab_id, timeout=60.0)
                            await wait_for_portal_settle(
                                session_id,
                                tab_id=active_tab_id,
                                timeout=25.0,
                                context_label=f"estabilizar a aba {active_tab_id}",
                            )
                        await asyncio.sleep(1.0)
                        if use_today_tab:
                            _emit_pix_status(on_status, "Usando a aba Hoje da Caixa para baixar o relatorio de cartoes do dia corrente...")
                        else:
                            _emit_pix_status(on_status, f"Aplicando filtro de data do relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'}...")
                            await set_date_inputs(session_id)
                            await wait_for_portal_settle(
                                session_id,
                                tab_id=active_tab_id,
                                timeout=25.0 if kind == "pix" else 20.0,
                                context_label=f"aplicar a data do relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'}",
                            )
                        establishment_options = await list_establishment_filter_options(
                            session_id,
                            active_tab_id,
                            timeout=25.0,
                        )
                        if company_norm == "mva" and len(establishment_options) > 1:
                            _emit_pix_status(
                                on_status,
                                f"Baixando o relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'} separadamente para cada estabelecimento da MVA...",
                            )
                            captured_paths: list[tuple[str, str]] = []
                            for establishment_id in establishment_options:
                                _emit_pix_status(
                                    on_status,
                                    f"Aplicando o estabelecimento {establishment_id} na Caixa antes da exportacao...",
                                )
                                applied_count = await apply_establishment_filter_selection(
                                    session_id,
                                    active_tab_id,
                                    [establishment_id],
                                    timeout=25.0,
                                )
                                if applied_count <= 0:
                                    continue
                                _emit_pix_status(on_status, f"Preparando exportacao do relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'}...")
                                saved = await export_current_selection()
                                if not saved:
                                    continue
                                variant_path = _copy_establishment_variant(saved, establishment_id)
                                if variant_path:
                                    captured_paths.append((variant_path, establishment_id))
                            combined = _combine_establishment_reports(captured_paths, kind)
                            if combined:
                                return combined
                            raise RuntimeError(
                                f"A Caixa nao entregou um arquivo valido do relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'} para os estabelecimentos da MVA."
                            )
                        establishments_applied = await ensure_all_establishments_selected(
                            session_id,
                            active_tab_id,
                            timeout=25.0,
                        )
                        if establishments_applied:
                            _emit_pix_status(
                                on_status,
                                "Aplicados todos os estabelecimentos disponiveis no filtro da Caixa antes da exportacao...",
                            )
                        _emit_pix_status(on_status, f"Preparando exportacao do relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'}...")
                        saved = await export_current_selection()
                        if saved:
                            return saved
                        raise RuntimeError(
                            f"A Caixa nao entregou o arquivo final do relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'}."
                        )
                    except Exception as exc:
                        last_error = exc
                        if attempt >= 1:
                            raise
                        try:
                            current_state = str(await eval_js(session_id, sales_area_state_expression, timeout=10.0) or "").strip()
                        except Exception:
                            current_state = ""
                        if current_state == "portal_error" or "_error.html" in str(exc).lower():
                            retry_message = (
                                f"A Caixa abriu a pagina de erro ao gerar o relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'}; tentando novamente..."
                            )
                        else:
                            retry_message = (
                                f"A Caixa demorou para gerar o relatorio de {'cartoes' if kind == 'cartoes' else 'PIX'}; tentando novamente..."
                            )
                        _emit_pix_status(
                            on_status,
                            retry_message,
                        )
                        await asyncio.sleep(2.0)
                if last_error:
                    raise last_error
                return None

            try:
                target_id = (await cdp("Target.createTarget", {"url": "about:blank"})).get("targetId")
                if not target_id:
                    raise RuntimeError("Nao foi possivel abrir a aba da Azulzinha/Caixa.")
                try:
                    await cdp("Target.activateTarget", {"targetId": target_id}, timeout=5.0)
                except Exception:
                    pass
                session_id = (await cdp("Target.attachToTarget", {"targetId": target_id, "flatten": True})).get("sessionId")
                if not session_id:
                    raise RuntimeError("Nao foi possivel anexar a aba da Azulzinha/Caixa.")
                await cdp("Page.enable", session_id=session_id)
                await cdp("Runtime.enable", session_id=session_id)
                try:
                    await cdp("Page.bringToFront", session_id=session_id, timeout=5.0)
                except Exception:
                    pass
                try:
                    await cdp("Network.enable", {}, session_id=session_id)
                except Exception:
                    pass
                try:
                    await cdp("Browser.setDownloadBehavior", {"behavior": "allow", "downloadPath": browser_download_dir, "eventsEnabled": True})
                except Exception:
                    pass
                try:
                    await cdp(
                        "Page.setDownloadBehavior",
                        {"behavior": "allow", "downloadPath": browser_download_dir},
                        session_id=session_id,
                    )
                except Exception:
                    pass
                await ensure_authenticated_sales_area(session_id, company_norm, "a area de vendas")
                resultado = {"pix": None, "cartoes": None, "avisos": []}
                if need_cartoes:
                    resultado["cartoes"] = await download_report(session_id, "cartoes")
                    if not resultado["cartoes"]:
                        resultado["avisos"].append("Nao foi possivel baixar o relatorio de cartoes da Azulzinha/Caixa.")
                if need_pix:
                    resultado["pix"] = await download_report(session_id, "pix")
                    if not resultado["pix"]:
                        resultado["avisos"].append("Nao foi possivel baixar o relatorio PIX da Azulzinha/Caixa.")
                return resultado
            finally:
                recv_task.cancel()
                try:
                    await recv_task
                except BaseException:
                    pass

    _emit_pix_status(on_status, "Abrindo portal da Caixa...")
    proc = _launch_browser_process(chrome_args)
    try:
        return asyncio.run(asyncio.wait_for(_run(), timeout=480.0))
    finally:
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        shutil.rmtree(profile_dir, ignore_errors=True)
        shutil.rmtree(browser_download_dir, ignore_errors=True)
        _cleanup_azulzinha_debug_artifacts()


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
        "tab_title": "PIX",
        "menu_text": "Abrir PIX CAIXA",
        "summary_label": "PIX",
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
        "tab_title": "PIX",
        "menu_text": "Abrir PIX CAIXA",
        "summary_label": "PIX",
        "total_label": "Total PIX CAIXA",
        "section_label": "Transações PIX recebidas na CAIXA",
        "empty_message": "Nenhuma transação PIX recebida encontrada para este dia.",
        "table_headers": ("Data da venda", "Valor bruto"),
        "table_mode": "data_valor",
        "categoria": "pix_caixa",
    }


def _build_pix_report_from_caixa_xlsx(caminho_xlsx: str, data_br: str) -> dict:
    itens = []
    itens_todos = []
    for row in _collect_card_rows_from_caixa_xlsx(caminho_xlsx):
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
        "arquivo": os.path.basename(caminho_xlsx),
        "caminho": caminho_xlsx,
        "periodo": periodo,
        "quantidade_autorizados": len(itens),
        "total_autorizado": total,
        "itens_autorizados": itens,
        "quantidade_relatorio": len(itens),
        "total_relatorio": total,
        "consistente": True,
        "origem": "caixa_pix_xlsx",
        "mensagem": None if itens else "Nenhuma transação PIX recebida encontrada no relatório local para este dia.",
        "itens_todos": itens_todos,
        "tab_title": "PIX",
        "menu_text": "Abrir PIX CAIXA",
        "summary_label": "PIX",
        "total_label": "Total PIX CAIXA",
        "section_label": "Transações PIX recebidas na CAIXA",
        "empty_message": "Nenhuma transação PIX recebida encontrada para este dia.",
        "table_headers": ("Data da venda", "Valor bruto"),
        "table_mode": "data_valor",
        "categoria": "pix_caixa",
    }


def _convert_pix_xlsx_to_csv(caminho_xlsx: str) -> str | None:
    try:
        rows = _collect_card_rows_from_caixa_xlsx(caminho_xlsx)
    except Exception:
        return None
    if not rows:
        return None
    csv_path = str(Path(caminho_xlsx).with_suffix(".csv"))
    ordered_headers = [
        "Data da venda",
        "Cód. de autorização",
        "Valor bruto",
        "Terminal",
        "Número do estabelecimento",
        "Status",
    ]
    try:
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=ordered_headers, delimiter=";")
            writer.writeheader()
            for row in rows:
                writer.writerow({header: str(row.get(header, "") or "").strip() for header in ordered_headers})
        return csv_path
    except Exception:
        return None


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


def _collect_card_rows_from_caixa_xlsx(caminho_xlsx: str) -> list[dict[str, str]]:
    import pandas as pd

    rows_out: list[dict[str, str]] = []
    try:
        xls = pd.ExcelFile(caminho_xlsx)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str).fillna("")
            for _, row in df.iterrows():
                rows_out.append({str(key or ""): str(value or "").strip() for key, value in row.items()})
        if rows_out:
            return rows_out
    except Exception:
        pass

    for _sheet_name, rows in _read_xlsx_rows_fallback(caminho_xlsx):
        if not rows:
            continue
        headers: list[str] = []
        header_idx = -1
        for idx, row in enumerate(rows):
            normalized = [_normalize_ascii_text(cell) for cell in row]
            if "data da venda" in normalized and "valor bruto" in normalized and "status" in normalized:
                headers = [str(cell or "").strip() for cell in row]
                header_idx = idx
                break
        if header_idx < 0 or not headers:
            continue
        for row in rows[header_idx + 1 :]:
            if not any(str(value or "").strip() for value in row):
                continue
            row_map = {
                headers[idx]: str(row[idx] or "").strip()
                for idx in range(min(len(headers), len(row)))
                if str(headers[idx] or "").strip()
            }
            if any(str(value or "").strip() for value in row_map.values()):
                rows_out.append(row_map)
    return rows_out


def _build_card_reports_from_caixa_xlsx(caminho_xlsx: str, data_br: str) -> dict[str, dict]:
    import pandas as pd

    sheet_rows = _collect_card_rows_from_caixa_xlsx(caminho_xlsx)
    buckets = {
        "cartao_credito_caixa": [],
        "cartao_debito_caixa": [],
    }

    for row in sheet_rows:
        normalized_row = {_normalize_ascii_text(key): str(value or "").strip() for key, value in row.items()}
        data_raw = normalized_row.get("data da venda", "")
        valor_raw = normalized_row.get("valor bruto", "")
        status_raw = normalized_row.get("status", "")
        produto_raw = normalized_row.get("produto", "")
        numero_raw = normalized_row.get("cod. de autorizacao", "") or normalized_row.get("comprovante de venda", "")
        if not data_raw or not valor_raw:
            continue
        match = re.match(r"^(\d{2}/\d{2}/\d{4})\s+(?:as|a?s)\s+(\d{2}:\d{2})(?::(\d{2}))?$", _normalize_ascii_text(data_raw))
        if not match:
            match = re.match(r"^(\d{2}/\d{2}/\d{4})\s+(\d{2}:\d{2})(?::(\d{2}))?$", _normalize_ascii_text(data_raw))
        if not match:
            continue
        data_venda = match.group(1)
        if data_venda != data_br:
            continue
        if _normalize_ascii_text(status_raw) not in {
            _normalize_ascii_text("Aprovada"),
            _normalize_ascii_text("Autorizada"),
        }:
            continue
        produto_norm = _normalize_ascii_text(produto_raw)
        if "debito" in produto_norm:
            key = "cartao_debito_caixa"
        elif "credito" in produto_norm or "parcelado" in produto_norm:
            key = "cartao_credito_caixa"
        else:
            continue
        hora = f"{match.group(2)}:{match.group(3) or '00'}"
        numero = str(numero_raw or "").strip()
        buckets[key].append(
            {
                    "numero": numero,
                    "numero_exibicao": numero.lstrip("0") or numero or "-",
                    "data_venda": f"{data_venda} às {hora[:5]}",
                    "valor_bruto": round(parse_number(valor_raw), 2),
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
            "arquivo": os.path.basename(caminho_xlsx),
            "caminho": caminho_xlsx,
            "periodo": periodo,
            "quantidade_autorizados": len(itens),
            "total_autorizado": total,
            "itens_autorizados": itens,
            "quantidade_relatorio": len(itens),
            "total_relatorio": total,
            "consistente": True,
            "origem": "caixa_cartoes_xlsx",
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


def _build_card_reports_from_caixa(caminho: str, data_br: str) -> dict[str, dict]:
    suffix = _effective_local_report_suffix(caminho)
    if suffix == ".xlsx":
        return _build_card_reports_from_caixa_xlsx(caminho, data_br)
    return _build_card_reports_from_caixa_pdf(caminho, data_br)


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
    cancelados_rows: list[tuple[str, str]] | None = None,
    allow_empty: bool = False,
) -> dict | None:
    pix_fechamento_rows = list(pix_fechamento_rows or [])
    pix_maquina_rows = list(pix_maquina_rows or [])
    cartao_fechamento_rows = list(cartao_fechamento_rows or [])
    cartao_maquina_rows = list(cartao_maquina_rows or [])
    cancelados_rows = list(cancelados_rows or [])
    tipos_alerta_exibidos = {"Cupom cancelado"}

    observacao_rows = [
        row
        for row in rows
        if row and row[0] not in {"CF sem Transação Bancária", "Transação Bancária sem CF/NF"}
    ]
    observacao_rows = [row for row in observacao_rows if row and row[0] not in tipos_alerta_exibidos]
    total_rows = (
        len(pix_fechamento_rows)
        + len(pix_maquina_rows)
        + len(cartao_fechamento_rows)
        + len(cartao_maquina_rows)
        + len(cancelados_rows)
        + len(observacao_rows)
    )
    if total_rows <= 0 and not allow_empty:
        return None

    table_rows: list[tuple[str, ...]] = []
    if cancelados_rows:
        table_rows.append(("Cupons Cancelados", "", ""))
        table_rows.extend(cancelados_rows)
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
        for row in collection:
            if not row:
                continue
            valor = row[-1]
            total += parse_number(valor)
    for row in cancelados_rows:
        if not row:
            continue
        total += parse_number(row[-1])

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
    report["cancelados_rows"] = cancelados_rows
    report["observacao_rows"] = observacao_rows
    return report


def _count_visible_alert_rows(report: dict | None) -> int:
    report = report or {}
    return sum(
        len(report.get(key) or [])
        for key in (
            "cancelados_rows",
            "pix_fechamento_rows",
            "cartao_fechamento_rows",
            "pix_maquina_rows",
            "cartao_maquina_rows",
        )
    )


def _build_eh_card_mismatch_report(
    periodo: str | None,
    itens_fechamento: list[dict],
    itens_maquina: list[dict],
) -> dict | None:
    if not itens_fechamento and not itens_maquina:
        return None

    def _tipo_curto(titulo: str) -> str:
        titulo = corrigir_texto(titulo)
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
    fechamento_janelas: list[dict] = []

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
        fechamento_janela = _build_scope_window(match.group("abertura"), match.group("fechamento"))
        if fechamento_janela:
            fechamento_janelas.append(fechamento_janela)
        scope_abertura = str((fechamento_janela or {}).get("abertura") or "").strip()
        scope_fechamento = str((fechamento_janela or {}).get("fechamento") or "").strip()
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
                    "scope_abertura": scope_abertura,
                    "scope_fechamento": scope_fechamento,
                    "valor": valor,
                }
            if bucket_pagamento is not None:
                for valor_parcela in valores_linha:
                    bucket_pagamento["itens"].append(
                        {
                            "numero": numero_normalizado,
                            "numero_exibicao": _display_fiscal_number(numero_normalizado),
                            "data_venda": data_exibicao,
                            "scope_abertura": scope_abertura,
                            "scope_fechamento": scope_fechamento,
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

    fechamento_janelas = _normalize_scope_windows(fechamento_janelas)
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
        "fechamento_janelas": fechamento_janelas,
        "fechamento_parcial": _is_partial_scope_windows(fechamento_janelas),
        "nfces": itens_nfce,
        "nfces_faltantes_sequencia": _find_missing_fiscal_numbers([item["numero"] for item in itens_nfce]),
        "fiscal_status_map": {},
    }


def _is_mva_clipp_fechamento_text(texto: str) -> bool:
    texto_normalizado = _normalize_caixa_client(texto)
    return (
        "FECHAMENTO DE CAIXA" in texto_normalizado
        and "DOCUMENTOS GERADOS" in texto_normalizado
        and "PAGAMENTO INSTANTANEO" in texto_normalizado
        and "MVA COMERCIO" in texto_normalizado
    )


def analisar_pdf_fechamento_caixa_mva_clipp(
    caminho_pdf: str,
    *,
    company: str = "MVA",
    avisos_usuario: list[str] | None = None,
    auto_download_missing: bool = False,
    force_refresh_payments: bool = False,
    on_status=None,
    cancel_event: threading.Event | None = None,
    token_callback=None,
) -> dict:
    texto = _read_pdf_text(caminho_pdf)
    if not _is_mva_clipp_fechamento_text(texto):
        avisos = list(avisos_usuario or [])
        avisos.append(
            "O PDF informado como Fechamento de Caixa da MVA nao corresponde ao layout esperado do fechamento Clipp. "
            "Ele sera tratado apenas como relatorio local, e a busca automatica dos pagamentos na Azulzinha/Caixa nao sera acionada por este arquivo."
        )
        return {
            "arquivo": os.path.basename(caminho_pdf),
            "arquivo_tipo": "mva_desconhecido",
            "resumo_modelo": "MVA",
            "periodo": None,
            "quantidade_nfce": 0,
            "total_nfce": 0.0,
            "relatorios_pagamento": {},
            "fechamento_janelas": [],
            "fechamento_parcial": False,
            "nfces": [],
            "nfces_faltantes_sequencia": [],
            "fiscal_status_map": {},
            "avisos_usuario": list(dict.fromkeys(avisos)),
        }

    linhas_brutas = [corrigir_texto(linha.strip()) for linha in texto.splitlines()]
    linhas: list[str] = []
    for linha in linhas_brutas:
        if not linha:
            continue
        linha_normalizada = _normalize_caixa_client(linha)
        if linha.lower().startswith("file:///"):
            continue
        if linha_normalizada == "(PIX)":
            continue
        if re.fullmatch(r"\d{2}/\d{2}/\d{2},\s+\d{2}:\d{2}\s+clipp_exportado\.htm", linha, re.IGNORECASE):
            continue
        linhas.append(linha)

    periodo = None
    match_periodo = re.search(
        r"PER[IÍ]ODO ANALISADO,\s*DE\s*(\d{2}/\d{2}/\d{4})\s*AT[ÉE]\s*(\d{2}/\d{2}/\d{4})",
        _normalize_caixa_client(texto),
    )
    if match_periodo:
        periodo = f"{match_periodo.group(1)} - {match_periodo.group(2)}"
    data_base = _extract_period_range(periodo or "")[0] or ""
    fechamento_janelas = _normalize_scope_windows(
        [
            {
                "abertura": match.group(1),
                "fechamento": match.group(2),
            }
            for match in re.finditer(
                r"\b\d+\s*-\s*Abertura\s*:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})\s*-\s*Fechamento\s*:\s*(\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}:\d{2})",
                corrigir_texto(texto),
                re.IGNORECASE,
            )
        ]
    )

    payment_aliases = [
        ("Pagamento Instantâneo (PIX)", ("PAGAMENTO INSTANTANEO (PIX)", "PAGAMENTO INSTANTANEO")),
        ("Cartão de Crédito", ("CARTAO DE CREDITO",)),
        ("Cartão de Débito", ("CARTAO DE DEBITO",)),
        ("Dinheiro", ("DINHEIRO",)),
    ]

    totais_pagamento: dict[str, float] = {}
    nfces_map: dict[str, dict] = {}
    payment_buckets: dict[str, dict] = {}

    for linha in linhas:
        linha_normalizada = _normalize_caixa_client(linha)
        total_match = re.match(
            r"^(DINHEIRO|CARTAO DE CREDITO|CARTAO DE DEBITO|PAGAMENTO INSTANTANEO(?: \(PIX\))?)\s*:?\s*([-\d\.,]+)$",
            linha_normalizada,
        )
        if total_match:
            label_ascii = total_match.group(1)
            valor = round(parse_number(total_match.group(2)), 2)
            canonical_label = next(
                (
                    display
                    for display, aliases in payment_aliases
                    if any(alias == label_ascii for alias in aliases)
                ),
                None,
            )
            if canonical_label:
                totais_pagamento[canonical_label] = valor
            continue

        doc_match = re.match(r"^(?P<numero>\d{6,})\s+NFCE\s+(?P<hora>\d{2}:\d{2}:\d{2})\s+(?P<resto>.+)$", linha_normalizada)
        if not doc_match:
            continue

        numero = _normalize_fiscal_number(doc_match.group("numero"))
        hora = doc_match.group("hora")
        resto_original = linha[doc_match.end("hora"):].strip()
        valor_match = re.search(r"([-\d\.,]+)\s*$", resto_original)
        if not valor_match:
            continue
        valor = round(parse_number(valor_match.group(1)), 2)
        corpo_original = resto_original[: valor_match.start()].strip()
        corpo_normalizado = _normalize_caixa_client(corpo_original)

        forma_pagamento = ""
        idx_forma = -1
        for display_label, aliases in payment_aliases:
            for alias in aliases:
                current_idx = corpo_normalizado.rfind(alias)
                if current_idx > idx_forma:
                    idx_forma = current_idx
                    forma_pagamento = display_label

        cliente = corpo_original
        if idx_forma >= 0 and forma_pagamento:
            forma_ascii = _normalize_caixa_client(forma_pagamento)
            idx_original = _normalize_caixa_client(corpo_original).rfind(forma_ascii)
            if idx_original >= 0:
                cliente = corpo_original[:idx_original].strip()
            else:
                cliente = corpo_original

        data_venda = f"{data_base} {hora}".strip() if data_base else hora
        existente = nfces_map.get(numero)
        if existente is None:
            nfces_map[numero] = {
                "numero": numero,
                "numero_exibicao": _display_fiscal_number(numero),
                "descricao": forma_pagamento or "NFC-e",
                "data_venda": data_venda,
                "cliente": cliente,
                "valor": valor,
            }
        else:
            existente["valor"] = round(float(existente.get("valor", 0.0)) + valor, 2)
            if forma_pagamento and forma_pagamento not in str(existente.get("descricao") or ""):
                existente["descricao"] = f"{existente['descricao']} + {forma_pagamento}"

        if not forma_pagamento:
            continue
        meta_pagamento = _build_zweb_payment_report_meta(forma_pagamento)
        if not meta_pagamento:
            continue
        bucket = payment_buckets.setdefault(
            meta_pagamento["key"],
            {
                **meta_pagamento,
                "itens": [],
            },
        )
        bucket["itens"].append(
            {
                "numero": numero,
                "numero_exibicao": _display_fiscal_number(numero),
                "data_venda": data_venda,
                "valor_bruto": valor,
            }
        )

    itens_nfce = sorted(nfces_map.values(), key=lambda item: item["numero"])
    total_nfce = round(sum(float(item.get("valor", 0.0)) for item in itens_nfce), 2)

    relatorios_pagamento = {}
    for key, bucket in payment_buckets.items():
        itens = sorted(
            bucket.get("itens", []),
            key=lambda item: (str(item.get("data_venda") or ""), str(item.get("numero") or "")),
        )
        total_itens = round(sum(float(item.get("valor_bruto", 0.0)) for item in itens), 2)
        total_reportado = round(float(totais_pagamento.get(bucket["forma_pagamento"], total_itens)), 2)
        relatorios_pagamento[key] = {
            "arquivo": os.path.basename(caminho_pdf),
            "caminho": caminho_pdf,
            "periodo": periodo,
            "quantidade_autorizados": len(itens),
            "total_autorizado": total_reportado,
            "itens_autorizados": itens,
            "quantidade_relatorio": len(itens),
            "total_relatorio": total_reportado,
            "consistente": abs(total_itens - total_reportado) < 0.01,
            "origem": "clipp_fechamento_caixa_mva",
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

    report = {
        "arquivo": os.path.basename(caminho_pdf),
        "arquivo_tipo": "fechamento_caixa_clipp_mva",
        "arquivo_resumo_titulo": "Arquivo Fechamento",
        "total_resumo_titulo": "Total Fechamento de caixa",
        "subtitle": "Compara os DAVs finalizados com o fechamento do Clipp e cruza as formas de pagamento com os relatórios da Caixa.",
        "resumo_modelo": "MVA",
        "periodo": periodo,
        "quantidade_nfce": len(itens_nfce),
        "total_nfce": total_nfce,
        "total_geral": total_nfce,
        "totalizadores": totais_pagamento,
        "relatorios_pagamento": relatorios_pagamento,
        "fechamento_janelas": fechamento_janelas,
        "fechamento_parcial": _is_partial_scope_windows(fechamento_janelas),
        "nfces": itens_nfce,
        "nfces_faltantes_sequencia": _find_missing_fiscal_numbers([item["numero"] for item in itens_nfce]),
        "fiscal_status_map": {},
    }
    data_br = _extract_period_range(periodo or "")[0]
    avisos = list(avisos_usuario or [])
    if data_br and auto_download_missing:
        local_payment_reports = _find_eh_local_payment_reports(data_br, company=company)
        need_pix = not bool(local_payment_reports.get("pix"))
        need_cartoes = not bool(local_payment_reports.get("cartoes"))
        if force_refresh_payments or need_pix or need_cartoes:
            try:
                baixados = baixar_relatorios_caixa_eh_azulzinha(
                    data_br,
                    on_status=on_status,
                    cancel_event=cancel_event,
                    token_callback=token_callback,
                    need_pix=force_refresh_payments or need_pix,
                    need_cartoes=force_refresh_payments or need_cartoes,
                    company=company,
                )
                avisos.extend(list(baixados.get("avisos") or []))
            except Exception as exc:
                if str(exc).strip() == "__cancelled__":
                    raise
                avisos.append(f"Nao foi possivel baixar automaticamente os relatorios da Caixa da {company}: {exc}")
    if data_br:
        report, _relatorio_pix, _avisos = _integrate_local_payment_reports(
            report,
            data_br,
            avisos_usuario=avisos,
            company=company,
        )
    return report


def gerar_relatorios_caixa_eh_manuais(
    data_br: str,
    caminho_pedidos_html: str,
    caminho_fechamento_html: str,
) -> tuple[dict, dict, dict | None]:
    texto_pedidos = _read_text_file(caminho_pedidos_html)
    texto_fechamento = _read_text_file(caminho_fechamento_html)

    relatorio = _analisar_html_pedidos_importados_eh(
        texto_pedidos,
        arquivo=os.path.basename(caminho_pedidos_html),
    )
    relatorio_fechamento = _analisar_html_fechamento_caixa_eh(
        texto_fechamento,
        arquivo=os.path.basename(caminho_fechamento_html),
    )
    relatorio_fechamento["fiscal_status_map"] = {}
    relatorio = _aplicar_filtro_canceladas_pedidos_eh(relatorio, {})
    relatorio_fechamento, relatorio_pix, _avisos = _integrate_local_payment_reports(
        relatorio_fechamento,
        data_br,
    )
    return relatorio, relatorio_fechamento, relatorio_pix




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
            key=lambda item: (str(item.get("ordem") or ""), item.get("pedido", "")),
        ),
        "itens_excluidos": sorted(
            itens_excluidos,
            key=lambda item: (
                str(item.get("ordem") or ""),
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


def _parse_scope_datetime(value: object) -> datetime | None:
    text = corrigir_texto(str(value or "")).strip()
    if not text:
        return None

    normalized = _normalize_ascii_text(text)
    for fmt in (
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue

    match = re.search(
        r"(\d{2}/\d{2}/\d{4})\s*(?:as\s*)?(\d{2}:\d{2})(?::(\d{2}))?",
        normalized,
        re.IGNORECASE,
    )
    if match:
        seconds = match.group(3) or "00"
        try:
            return datetime.strptime(
                f"{match.group(1)} {match.group(2)}:{seconds}",
                "%d/%m/%Y %H:%M:%S",
            )
        except ValueError:
            return None

    return None


def _build_scope_window(abertura: object, fechamento: object) -> dict | None:
    abertura_dt = _parse_scope_datetime(abertura)
    fechamento_dt = _parse_scope_datetime(fechamento)
    if not abertura_dt or not fechamento_dt or fechamento_dt < abertura_dt:
        return None
    return {
        "abertura": abertura_dt.strftime("%d/%m/%Y %H:%M:%S"),
        "fechamento": fechamento_dt.strftime("%d/%m/%Y %H:%M:%S"),
    }


def _normalize_scope_windows(windows: object) -> list[dict]:
    normalized: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for raw in list(windows or []):
        if isinstance(raw, dict):
            opening = raw.get("abertura")
            closing = raw.get("fechamento")
        elif isinstance(raw, (list, tuple)) and len(raw) >= 2:
            opening, closing = raw[0], raw[1]
        else:
            continue
        window = _build_scope_window(opening, closing)
        if not window:
            continue
        key = (window["abertura"], window["fechamento"])
        if key in seen:
            continue
        seen.add(key)
        normalized.append(window)
    normalized.sort(key=lambda item: (item["abertura"], item["fechamento"]))
    return normalized


def _scope_window_datetimes(window: dict) -> tuple[datetime | None, datetime | None]:
    return _parse_scope_datetime(window.get("abertura")), _parse_scope_datetime(window.get("fechamento"))


def _is_partial_scope_windows(windows: object) -> bool:
    normalized = _normalize_scope_windows(windows)
    if not normalized:
        return False
    if len(normalized) > 1:
        return False
    opening_dt, closing_dt = _scope_window_datetimes(normalized[0])
    if not opening_dt or not closing_dt:
        return False
    duration_hours = max(0.0, (closing_dt - opening_dt).total_seconds() / 3600.0)
    if duration_hours >= 8.5 and opening_dt.hour <= 10 and closing_dt.hour >= 16:
        return False
    return True


def _report_scope_windows(report: dict | None) -> list[dict]:
    return _normalize_scope_windows((report or {}).get("fechamento_janelas"))


def _scope_window_key(window: dict | None) -> tuple[str, str]:
    window = window or {}
    return (
        str(window.get("abertura") or "").strip(),
        str(window.get("fechamento") or "").strip(),
    )


def _scope_windows_for_mode(windows: object, mode: str | None) -> list[dict]:
    normalized = _normalize_scope_windows(windows)
    mode_norm = _normalize_ascii_text(mode or "")
    if not normalized or mode_norm in {"", "daily", "diario"}:
        return normalized
    if mode_norm in {"morning", "manha"}:
        return normalized[:1]
    if mode_norm in {"afternoon", "tarde"}:
        return normalized[1:] if len(normalized) > 1 else []
    return normalized


def _detect_closing_scope_kind(report: dict | None) -> str:
    windows = _report_scope_windows(report)
    if not windows:
        return "daily"
    if len(windows) > 1:
        return "daily"
    opening_dt, closing_dt = _scope_window_datetimes(windows[0])
    if opening_dt and closing_dt and closing_dt.hour < 15:
        return "morning"
    return "partial"


def describe_closing_scope(report: dict | None) -> dict:
    windows = _report_scope_windows(report)
    detected = _detect_closing_scope_kind(report)
    available_modes = ["daily"]
    if windows:
        available_modes = ["morning"] if len(windows) == 1 else ["daily", "afternoon", "morning"]
    return {
        "detected": detected,
        "windows": windows,
        "available_modes": available_modes,
        "has_morning_only": detected == "morning",
        "has_full_day": len(windows) > 1,
    }


def _scope_mode_label(mode: str | None) -> str:
    mode_norm = _normalize_ascii_text(mode or "")
    if mode_norm in {"morning", "manha"}:
        return "Manhã"
    if mode_norm in {"afternoon", "tarde"}:
        return "Tarde"
    return "Diário"


def _item_within_scope_windows(item: dict, windows: list[dict]) -> bool:
    if not windows:
        return True
    item_scope_key = _scope_window_key(
        {
            "abertura": (item or {}).get("scope_abertura"),
            "fechamento": (item or {}).get("scope_fechamento"),
        }
    )
    if any(item_scope_key):
        valid_scope_keys = {_scope_window_key(window) for window in windows}
        return item_scope_key in valid_scope_keys
    item_dt = _parse_scope_datetime((item or {}).get("ordem")) or _parse_scope_datetime((item or {}).get("data_venda"))
    if item_dt is None:
        return True
    for window in windows:
        opening_dt, closing_dt = _scope_window_datetimes(window)
        if opening_dt and closing_dt and opening_dt <= item_dt <= closing_dt:
            return True
    return False


def _filter_payment_report_to_scope(report: dict | None, windows: list[dict]) -> dict | None:
    if not report:
        return report
    normalized_windows = _normalize_scope_windows(windows)
    if not normalized_windows:
        return report

    filtered = dict(report)
    items = [dict(item) for item in list(report.get("itens_autorizados") or []) if _item_within_scope_windows(item, normalized_windows)]
    filtered["itens_autorizados"] = items
    filtered["quantidade_autorizados"] = len(items)
    filtered["total_autorizado"] = round(sum(float(item.get("valor_bruto", 0.0) or 0.0) for item in items), 2)

    if "itens_todos" in report:
        all_items = [dict(item) for item in list(report.get("itens_todos") or []) if _item_within_scope_windows(item, normalized_windows)]
        filtered["itens_todos"] = all_items

    if "quantidade_relatorio" in filtered:
        filtered["quantidade_relatorio"] = len(items)
    if "total_relatorio" in filtered:
        filtered["total_relatorio"] = filtered["total_autorizado"]

    if filtered.get("quantidade_autorizados", 0) <= 0 and filtered.get("empty_message"):
        filtered["mensagem"] = filtered.get("empty_message")
    filtered["escopo_horario_aplicado"] = list(normalized_windows)
    return filtered


def _filter_items_to_scope(items: list[dict] | None, windows: list[dict]) -> list[dict]:
    normalized_windows = _normalize_scope_windows(windows)
    if not normalized_windows:
        return [dict(item) for item in list(items or [])]
    return [
        dict(item)
        for item in list(items or [])
        if _item_within_scope_windows(item, normalized_windows)
    ]


def _derive_scope_numbers(items: list[dict]) -> set[str]:
    numbers: set[str] = set()
    for item in items:
        numero = _normalize_fiscal_number(item.get("numero") or item.get("pedido") or "")
        if numero:
            numbers.add(numero)
    return numbers


def _filter_eh_caixa_report_to_scope(relatorio_caixa: dict, relatorio_fechamento: dict, windows: list[dict]) -> dict:
    filtered = dict(relatorio_caixa or {})
    scoped_nfces = list(relatorio_fechamento.get("nfces") or [])
    scoped_numbers = _derive_scope_numbers(scoped_nfces)
    scoped_ints = sorted(int(numero) for numero in scoped_numbers if numero.isdigit())
    min_num = scoped_ints[0] if scoped_ints else None
    max_num = scoped_ints[-1] if scoped_ints else None

    def _item_in_scope(item: dict) -> bool:
        numero = _normalize_fiscal_number(item.get("pedido", ""))
        if not numero or not numero.isdigit():
            return False
        if numero in scoped_numbers:
            return True
        if min_num is None or max_num is None:
            return False
        numero_int = int(numero)
        return min_num <= numero_int <= max_num

    itens_caixa = [dict(item) for item in list(relatorio_caixa.get("itens_caixa") or []) if _item_in_scope(item)]
    itens_excluidos = [dict(item) for item in list(relatorio_caixa.get("itens_excluidos") or []) if _item_in_scope(item)]

    pedidos_balcao = len(
        [
            item
            for item in itens_caixa
            if _is_eh_counter_client(item.get("cliente", ""))
        ]
    )
    pedidos_excluidos_cliente = len(
        [
            item
            for item in itens_excluidos
            if "CLIENTE DIFERENTE" in _normalize_caixa_client(item.get("motivo", ""))
        ]
    )
    pedidos_excluidos_documento = len(
        [
            item
            for item in itens_excluidos
            if "NF-E" in _normalize_caixa_client(item.get("motivo", ""))
        ]
    )
    total_caixa = round(sum(float(item.get("valor", 0.0) or 0.0) for item in itens_caixa), 2)
    total_excluido = round(sum(float(item.get("valor", 0.0) or 0.0) for item in itens_excluidos), 2)

    filtered.update(
        {
            "pedidos_total": len(itens_caixa) + len(itens_excluidos),
            "pedidos_balcao": pedidos_balcao,
            "pedidos_caixa": len(itens_caixa),
            "pedidos_excluidos": len(itens_excluidos),
            "pedidos_excluidos_cliente": pedidos_excluidos_cliente,
            "pedidos_excluidos_documento": pedidos_excluidos_documento,
            "total_documento": round(total_caixa + total_excluido, 2),
            "total_excluido": total_excluido,
            "total_caixa": total_caixa,
            "itens_caixa": sorted(
                itens_caixa,
                key=lambda item: (item.get("pedido", ""), str(item.get("cliente", "")).casefold()),
            ),
            "itens_excluidos": sorted(
                itens_excluidos,
                key=lambda item: (-float(item.get("valor", 0.0) or 0.0), str(item.get("cliente", "")).casefold(), item.get("pedido", "")),
            ),
            "escopo_horario_aplicado": list(_normalize_scope_windows(windows)),
        }
    )
    return filtered


def _filter_generic_caixa_report_to_scope(relatorio_caixa: dict, windows: list[dict]) -> dict:
    filtered = dict(relatorio_caixa or {})
    itens_caixa = _filter_items_to_scope(relatorio_caixa.get("itens_caixa") or [], windows)
    itens_excluidos = _filter_items_to_scope(relatorio_caixa.get("itens_excluidos") or [], windows)
    total_caixa = round(sum(float(item.get("valor", 0.0) or 0.0) for item in itens_caixa), 2)
    total_excluido = round(sum(float(item.get("valor", 0.0) or 0.0) for item in itens_excluidos), 2)
    pedidos_editando = len(
        [
            item
            for item in itens_excluidos
            if _normalize_caixa_client(item.get("documento", "")) == "EDITANDO"
        ]
    )
    pedidos_outros_status = max(0, len(itens_excluidos) - pedidos_editando)
    filtered.update(
        {
            "pedidos_total": len(itens_caixa) + len(itens_excluidos),
            "pedidos_caixa": len(itens_caixa),
            "pedidos_excluidos": len(itens_excluidos),
            "pedidos_editando": pedidos_editando,
            "pedidos_outros_status": pedidos_outros_status,
            "total_documento": round(total_caixa + total_excluido, 2),
            "total_excluido": total_excluido,
            "total_caixa": total_caixa,
            "itens_caixa": sorted(itens_caixa, key=lambda item: item.get("pedido", "")),
            "itens_excluidos": sorted(
                itens_excluidos,
                key=lambda item: (item.get("documento", ""), item.get("origem_mva", ""), item.get("pedido", "")),
            ),
            "escopo_horario_aplicado": list(_normalize_scope_windows(windows)),
        }
    )
    return filtered


def _filter_fechamento_report_to_scope(relatorio_fechamento: dict, windows: list[dict]) -> dict:
    normalized_windows = _normalize_scope_windows(windows)
    if not normalized_windows:
        return dict(relatorio_fechamento or {})

    filtered = dict(relatorio_fechamento or {})
    nfces = _filter_items_to_scope(relatorio_fechamento.get("nfces") or [], normalized_windows)
    relatorios_pagamento = {}
    for key, report in dict(relatorio_fechamento.get("relatorios_pagamento") or {}).items():
        relatorios_pagamento[key] = _filter_payment_report_to_scope(report, normalized_windows)

    totalizadores = {}
    for report in relatorios_pagamento.values():
        if not isinstance(report, dict):
            continue
        titulo = str(report.get("forma_pagamento") or report.get("summary_label") or "").strip()
        if titulo:
            totalizadores[titulo] = round(float(report.get("total_autorizado", 0.0) or 0.0), 2)

    filtered.update(
        {
            "quantidade_nfce": len(nfces),
            "total_nfce": round(sum(float(item.get("valor", 0.0) or 0.0) for item in nfces), 2),
            "total_geral": round(sum(float(item.get("valor", 0.0) or 0.0) for item in nfces), 2),
            "nfces": sorted(
                nfces,
                key=lambda item: (
                    str(item.get("data_venda") or ""),
                    str(item.get("numero") or ""),
                ),
            ),
            "nfces_faltantes_sequencia": _find_missing_fiscal_numbers(
                [_normalize_fiscal_number(item.get("numero", "")) for item in nfces]
            ),
            "relatorios_pagamento": relatorios_pagamento,
            "fechamento_janelas": list(normalized_windows),
            "fechamento_parcial": _is_partial_scope_windows(normalized_windows),
            "totalizadores": totalizadores or dict(relatorio_fechamento.get("totalizadores") or {}),
            "escopo_horario_aplicado": list(normalized_windows),
        }
    )
    return filtered


def aplicar_escopo_relatorio_caixa(
    relatorio_caixa: dict,
    relatorio_fechamento: dict,
    relatorio_pix: dict | None = None,
    *,
    scope_mode: str | None = None,
) -> tuple[dict, dict, dict | None]:
    windows = _scope_windows_for_mode((relatorio_fechamento or {}).get("fechamento_janelas"), scope_mode)
    mode_norm = _normalize_ascii_text(scope_mode or "")
    if not windows or mode_norm in {"", "daily", "diario"}:
        caixa_copy = dict(relatorio_caixa or {})
        fechamento_copy = dict(relatorio_fechamento or {})
        pix_copy = dict(relatorio_pix) if isinstance(relatorio_pix, dict) else relatorio_pix
        for report in (caixa_copy, fechamento_copy, pix_copy):
            if isinstance(report, dict):
                report["escopo_relatorio"] = _scope_mode_label(scope_mode)
        return caixa_copy, fechamento_copy, pix_copy

    fechamento_filtrado = _filter_fechamento_report_to_scope(relatorio_fechamento, windows)
    if str((relatorio_caixa or {}).get("caixa_modelo") or "").upper() == "MVA":
        caixa_filtrado = _filter_generic_caixa_report_to_scope(relatorio_caixa, windows)
    else:
        caixa_filtrado = _filter_eh_caixa_report_to_scope(relatorio_caixa, fechamento_filtrado, windows)
    pix_filtrado = _filter_payment_report_to_scope(relatorio_pix, windows) if relatorio_pix else relatorio_pix

    escopo_label = _scope_mode_label(scope_mode)
    for report in (caixa_filtrado, fechamento_filtrado, pix_filtrado):
        if isinstance(report, dict):
            report["escopo_relatorio"] = escopo_label
            report["escopo_horario_aplicado"] = list(windows)

    return caixa_filtrado, fechamento_filtrado, pix_filtrado


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
    return _project_base_dir()


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
    base_dir = _runtime_user_dir()
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


def _wait_for_devtools_ready(port: int, timeout: float = 60.0) -> dict:
    import urllib.request
    import urllib.error
    deadline = time.time() + timeout
    last_exc = None

    while time.time() < deadline:
        try:
            resposta = requests.get(f"http://127.0.0.1:{port}/json/version", timeout=3)
            resposta.raise_for_status()
            return resposta.json() or {}
        except Exception as exc:
            last_exc = exc
            time.sleep(1.0)

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

    for lock_name in (
        "SingletonCookie",
        "SingletonLock",
        "SingletonSocket",
        "DevToolsActivePort",
        "lockfile",
    ):
        lock_path = os.path.join(profile_dir, lock_name)
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
        except OSError:
            pass
        default_lock_path = os.path.join(default_dir, lock_name)
        try:
            if os.path.exists(default_lock_path):
                os.remove(default_lock_path)
        except OSError:
            pass

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
    return subprocess.Popen(chrome_args, **popen_kwargs)


async def _hide_chromium_window(cdp, target_id: str) -> None:
    if not target_id:
        return
    try:
        window_info = await cdp("Browser.getWindowForTarget", {"targetId": target_id}, timeout=5.0)
    except Exception:
        return

    window_id = window_info.get("windowId")
    if not window_id:
        return

    try:
        await cdp(
            "Browser.setWindowBounds",
            {
                "windowId": window_id,
                "bounds": {
                    "left": -32000,
                    "top": 0,
                    "width": 1400,
                    "height": 900,
                },
            },
            timeout=5.0,
        )
    except Exception:
        pass

    try:
        await cdp(
            "Browser.setWindowBounds",
            {"windowId": window_id, "bounds": {"state": "minimized"}},
            timeout=5.0,
        )
    except Exception:
        pass


def gerar_relatorios_caixa_eh_zweb(
    data_br: str,
    on_status=None,
    cancel_event: threading.Event | None = None,
    token_callback=None,
    force_refresh_payments: bool = False,
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
    avisos_usuario: list[str] = list(local_payment_reports.get("avisos") or [])

    if force_refresh_payments or not local_pix_pdf or not local_card_pdf:
        try:
            baixados = baixar_relatorios_caixa_eh_azulzinha(
                data_br,
                on_status=on_status,
                cancel_event=cancel_event,
                token_callback=token_callback,
                need_pix=force_refresh_payments or not bool(local_pix_pdf),
                need_cartoes=force_refresh_payments or not bool(local_card_pdf),
            )
            if baixados.get("pix"):
                local_pix_pdf = baixados.get("pix")
            if baixados.get("cartoes"):
                local_card_pdf = baixados.get("cartoes")
            avisos_usuario.extend(baixados.get("avisos") or [])
        except Exception as exc:
            if str(exc).strip() == "__cancelled__":
                raise
            avisos_usuario.append(f"Nao foi possivel baixar automaticamente os relatorios da Caixa: {exc}")

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
    caminho_html_pedidos = ""
    caminho_html_fechamento = ""
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

    _emit_pix_status(on_status, "Salvando relatorios do Zweb na pasta atual...")
    caminho_html_pedidos = _save_zweb_html_report(data_br, "pedidos_importados", html_pedidos)
    caminho_html_fechamento = _save_zweb_html_report(data_br, "fechamento_caixa", html_fechamento)

    _emit_pix_status(on_status, "Processando Pedidos importados...")
    relatorio = _analisar_html_pedidos_importados_eh(html_pedidos, arquivo=os.path.basename(caminho_html_pedidos))
    relatorio["caminho"] = caminho_html_pedidos
    _emit_pix_status(on_status, "Processando Fechamento de caixa...")
    relatorio_fechamento = _analisar_html_fechamento_caixa_eh(html_fechamento, arquivo=os.path.basename(caminho_html_fechamento))
    relatorio_fechamento["caminho"] = caminho_html_fechamento
    for report_pagamento in (relatorio_fechamento.get("relatorios_pagamento") or {}).values():
        if not isinstance(report_pagamento, dict):
            continue
        report_pagamento["arquivo"] = os.path.basename(caminho_html_fechamento)
        report_pagamento["caminho"] = caminho_html_fechamento
    relatorio_fechamento["fiscal_status_map"] = fiscal_status_map or {}
    relatorio = _aplicar_filtro_canceladas_pedidos_eh(relatorio, fiscal_status_map or {})
    scope_windows = _report_scope_windows(relatorio_fechamento)
    scope_label = " dentro do escopo horário do fechamento" if scope_windows else ""

    if local_card_pdf:
        _emit_pix_status(on_status, "Lendo relatorio local de cartoes...")
        try:
            relatorios_cartao = _build_card_reports_from_caixa(local_card_pdf, data_br)
            relatorios_validos = {}
            for key, report in relatorios_cartao.items():
                report_filtrado = _filter_payment_report_to_scope(report, scope_windows)
                if report_filtrado.get("itens_autorizados"):
                    relatorios_validos[key] = report_filtrado
            if relatorios_validos:
                relatorio_fechamento["relatorios_pagamento"].update(relatorios_validos)
            else:
                avisos_usuario.append(
                    f'O arquivo "{os.path.basename(local_card_pdf)}" não trouxe transações de cartão para {data_br}{scope_label} e foi ignorado.'
                )
        except Exception as exc:
            avisos_usuario.append(
                f'Não foi possível ler o arquivo "{os.path.basename(local_card_pdf)}" para {data_br}: {exc}'
            )

    if local_pix_pdf:
        _emit_pix_status(on_status, "Lendo relatorio local de PIX...")
        try:
            pix_suffix = _effective_local_report_suffix(local_pix_pdf)
            if pix_suffix == ".csv":
                relatorio_pix = _build_pix_report_from_caixa_csv(local_pix_pdf, data_br)
            elif pix_suffix == ".xlsx":
                relatorio_pix = _build_pix_report_from_caixa_xlsx(local_pix_pdf, data_br)
            else:
                relatorio_pix = _build_pix_report_from_caixa_pdf(local_pix_pdf, data_br)
            relatorio_pix = _filter_payment_report_to_scope(relatorio_pix, scope_windows)
            if relatorio_pix.get("quantidade_autorizados", 0) <= 0:
                avisos_usuario.append(
                    f'O arquivo "{os.path.basename(local_pix_pdf)}" não trouxe transações PIX para {data_br}{scope_label} e foi ignorado.'
                )
                relatorio_pix = _filter_payment_report_to_scope(
                    _build_pix_report_from_zweb_movimentations(data_iso, pix_transactions, erro=pix_error),
                    scope_windows,
                )
        except Exception as exc:
            avisos_usuario.append(
                f'Não foi possível ler o arquivo "{os.path.basename(local_pix_pdf)}" para {data_br}: {exc}'
            )
            relatorio_pix = _filter_payment_report_to_scope(
                _build_pix_report_from_zweb_movimentations(data_iso, pix_transactions, erro=str(exc)),
                scope_windows,
            )
    else:
        relatorio_pix = _filter_payment_report_to_scope(
            _build_pix_report_from_zweb_movimentations(data_iso, pix_transactions, erro=pix_error),
            scope_windows,
        )

    if avisos_usuario:
        relatorio_fechamento["avisos_usuario"] = list(dict.fromkeys(avisos_usuario))
        if relatorio_pix is not None:
            relatorio_pix["avisos_usuario"] = list(dict.fromkeys(avisos_usuario))

    if relatorio_pix:
        relatorio_fechamento.setdefault("relatorios_pagamento", {})
        relatorio_fechamento["relatorios_pagamento"][str(relatorio_pix.get("categoria") or "pagamentos_digitais_nfce")] = relatorio_pix

    _cleanup_eh_auto_payment_reports(local_pix_pdf, local_card_pdf)
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
    else:
        mensagem = "Relatório da Caixa/Azulzinha não utilizado. Valores confirmados via Financeiro > Movimentações do Zweb."

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
        "origem_label": "Financeiro > Movimentações do Zweb",
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

    base_dir = _runtime_user_dir()
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
    *,
    model: str = "55",
    cache_namespace: str = "",
) -> list[dict]:
    model_text = str(model or "55").strip() or "55"
    cache_key = ((cache_namespace or access_token[-8:]).lower(), data_iso, model_text)
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
                "model": model_text,
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
                "model": model_text,
                "tipo": str(item.get("type") or "").strip(),
                "cliente": str(item.get("customerName") or "").strip(),
                "cpf_cnpj": str(item.get("customerIdentification") or "").strip(),
                "emissao": str(item.get("emission") or "").strip(),
                "cancelada": bool(item.get("canceledXmlFile")) or int(item.get("status") or 0) == 2,
                "canceled_xml": str(item.get("canceledXmlFile") or "").strip(),
            }
        )

    _MINHAS_NOTAS_CACHE[cache_key] = [dict(item) for item in normalizados]
    return [dict(item) for item in normalizados]


def _load_minhas_notas_mva_context(
    periodo: str,
) -> tuple[list[dict], dict[str, dict], str | None]:
    data_iso = _period_to_iso_date(periodo)
    if not data_iso:
        return [], {}, None

    credenciais = _load_minhas_notas_credentials()
    if not credenciais:
        return [], {}, None

    try:
        login, password = credenciais
        access_token = _authenticate_minhas_notas(login, password)
        nfes_modelo_55 = _fetch_minhas_notas_nfes(
            access_token,
            data_iso,
            model="55",
            cache_namespace=login,
        )
        nfces_modelo_65 = _fetch_minhas_notas_nfes(
            access_token,
            data_iso,
            model="65",
            cache_namespace=login,
        )
    except Exception as exc:
        return [], {}, str(exc)

    nfes_ativas = [
        item
        for item in nfes_modelo_55
        if item.get("status") == 1 and item.get("tipo") == "1"
    ]

    fiscal_status_map: dict[str, dict] = {}
    for item in sorted(
        nfces_modelo_65,
        key=lambda dado: (
            str(dado.get("numero") or ""),
            str(dado.get("emissao") or ""),
        ),
    ):
        numero = _normalize_fiscal_number(item.get("numero", ""))
        if not numero or numero in fiscal_status_map:
            continue
        fiscal_status_map[numero] = {
            "numero": numero,
            "numero_exibicao": _display_fiscal_number(numero),
            "valor": round(float(item.get("valor", 0.0) or 0.0), 2),
            "cancelada": bool(item.get("cancelada")),
            "status_codigo": int(item.get("status") or 0),
            "emissao": str(item.get("emissao") or "").strip(),
        }

    return nfes_ativas, fiscal_status_map, None


def _match_davs_with_minhas_notas_nfes_from_items(
    davs_sem_cupom: list[dict],
    nfes: list[dict],
) -> tuple[list[dict], list[dict]]:
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

    return restantes, encontrados


def _match_davs_with_minhas_notas_nfes(
    davs_sem_cupom: list[dict],
    periodo: str,
) -> tuple[list[dict], list[dict], str | None]:
    data_iso = _period_to_iso_date(periodo)
    if not data_iso or not davs_sem_cupom:
        return list(davs_sem_cupom), [], None

    nfes_ativas, _fiscal_status_map, erro = _load_minhas_notas_mva_context(periodo)
    if erro:
        return list(davs_sem_cupom), [], erro

    restantes, encontrados = _match_davs_with_minhas_notas_nfes_from_items(
        davs_sem_cupom,
        nfes_ativas,
    )
    return restantes, encontrados, None


def _build_mva_cancelled_coupon_pool(
    itens_nfce: list[dict],
    fiscal_status_map: dict[str, dict],
) -> dict[str, dict]:
    numeros_presentes = {
        _normalize_fiscal_number(item.get("numero", ""))
        for item in itens_nfce or []
        if _normalize_fiscal_number(item.get("numero", ""))
    }
    return {
        numero: info
        for numero, info in (fiscal_status_map or {}).items()
        if numero not in numeros_presentes and bool((info or {}).get("cancelada"))
    }


def _match_davs_with_mva_cancelled_coupons(
    davs_sem_cupom: list[dict],
    cancelados_ausentes: dict[str, dict],
) -> tuple[list[dict], dict[str, dict]]:
    cancelados_por_valor: dict[float, list[tuple[str, dict]]] = {}
    for numero, info in sorted(cancelados_ausentes.items(), key=lambda item: int(item[0])):
        valor = round(float((info or {}).get("valor", 0.0) or 0.0), 2)
        cancelados_por_valor.setdefault(valor, []).append((numero, info))

    restantes: list[dict] = []
    cancelados_correspondentes: dict[str, dict] = {}
    for item in sorted(davs_sem_cupom, key=lambda dado: dado.get("pedido", "")):
        valor = round(float(item.get("valor", 0.0) or 0.0), 2)
        candidatos = cancelados_por_valor.get(valor) or []
        if not candidatos:
            restantes.append(item)
            continue
        numero, info = candidatos.pop(0)
        cancelados_correspondentes[numero] = info

    return restantes, cancelados_correspondentes


def _build_mva_cancelled_coupon_registros(
    cfs_faltantes: list[str],
    cancelados_ausentes: dict[str, dict],
) -> list[dict]:
    registros: list[dict] = []
    numeros_registrados: set[str] = set()

    for numero in cfs_faltantes:
        if not numero or numero in numeros_registrados:
            continue
        info = cancelados_ausentes.get(numero) or {}
        observacao = "Cupom cancelado" if info.get("cancelada") else "Cupom faltante na sequencia"
        valor = info.get("valor") if info.get("cancelada") else None
        registros.append(
            {
                "numero": numero,
                "numero_exibicao": f"CF {_display_fiscal_number(numero)}",
                "origem": "CF",
                "observacao": observacao,
                "valor": valor,
            }
        )
        numeros_registrados.add(numero)

    for numero, info in sorted(cancelados_ausentes.items(), key=lambda item: int(item[0])):
        if numero in numeros_registrados:
            continue
        registros.append(
            {
                "numero": numero,
                "numero_exibicao": f"CF {_display_fiscal_number(numero)}",
                "origem": "CF",
                "observacao": "Cupom cancelado",
                "valor": info.get("valor"),
            }
        )
        numeros_registrados.add(numero)

    return registros


def _build_cancelados_rows_from_pendencias(pendencias: list[dict]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    for item in pendencias or []:
        numero_exibicao = str(item.get("numero_exibicao") or item.get("numero") or "-").strip()
        if numero_exibicao.upper().startswith("CF "):
            cf_label = numero_exibicao
        else:
            cf_label = f"CF {numero_exibicao}"
        valor = item.get("valor")
        valor_texto = "-" if valor in (None, "") else f"R$ {format_number_br(valor)}"
        rows.append((cf_label, valor_texto))
    return rows


def _split_cancelled_payment_items(
    itens_fechamento: list[dict],
    fiscal_status_map: dict[str, dict],
) -> tuple[list[dict], list[dict]]:
    itens_ativos: list[dict] = []
    itens_cancelados: list[dict] = []
    for item in itens_fechamento or []:
        numero = _normalize_fiscal_number(item.get("numero", "") or item.get("numero_exibicao", ""))
        if numero and (fiscal_status_map.get(numero) or {}).get("cancelada"):
            info = fiscal_status_map.get(numero) or {}
            itens_cancelados.append(
                {
                    "numero": numero,
                    "numero_exibicao": item.get("numero_exibicao")
                    or info.get("numero_exibicao")
                    or _display_fiscal_number(numero),
                    "valor": round(float(item.get("valor_bruto", item.get("valor", 0.0)) or 0.0), 2),
                }
            )
            continue
        itens_ativos.append(item)
    return itens_ativos, itens_cancelados


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
    pedidos_cancelados_map: dict[str, dict] = {}
    pedidos_cancelados_numeros: set[str] = set()
    numeros_pedidos_conferidos = set()
    numeros_pedidos_pendentes = set()

    for item in relatorio_caixa.get("itens_excluidos", []) or []:
        motivo_item = corrigir_texto(str(item.get("motivo", ""))).strip()
        documento_item = _normalize_caixa_client(item.get("documento", ""))
        if "cupom cancelado" not in motivo_item.casefold() and "CANCELADA" not in documento_item:
            continue
        numero = _normalize_fiscal_number(item.get("pedido", ""))
        if not numero or numero in pedidos_cancelados_numeros:
            continue
        valor = round(float(item.get("valor", 0.0)), 2)
        pendencia = {
            "numero": numero,
            "numero_exibicao": _display_fiscal_number(numero),
            "valor": valor,
            "motivo": motivo_item or "Cupom cancelado",
        }
        pedidos_cancelados.append(pendencia)
        pedidos_cancelados_map[numero] = dict(pendencia)
        pedidos_cancelados_numeros.add(numero)
        _add_alert("Cupom cancelado", f"CF {pendencia['numero_exibicao']}: {pendencia['motivo']}", _money_text(valor))

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
            if numero in pedidos_cancelados_numeros:
                continue
            pedidos_cancelados.append(pendencia)
            pedidos_cancelados_map[numero] = dict(pendencia)
            pedidos_cancelados_numeros.add(numero)
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

    correlacao_rows = []

    dinheiro_total = round(float(dinheiro_report.get("total_autorizado", 0.0) or 0.0), 2)
    correlacao_rows.append(
        (
            "Dinheiro",
            f"R$ {format_number_br(dinheiro_total)}",
            "-",
            "Interno",
        )
    )

    comparacoes = [
        (
            "PIX",
            relatorios_pagamento.get("pix_caixa") or relatorios_pagamento.get("pagamentos_digitais_nfce"),
            relatorios_pagamento.get("pix_fechamento"),
            "valor_bruto",
        ),
        (
            "Cart\u00e3o Cr\u00e9dito",
            relatorios_pagamento.get("cartao_credito_caixa"),
            relatorios_pagamento.get("cartao_credito"),
            "valor_bruto",
        ),
        (
            "Cart\u00e3o D\u00e9bito",
            relatorios_pagamento.get("cartao_debito_caixa"),
            relatorios_pagamento.get("cartao_debito"),
            "valor_bruto",
        ),
    ]
    comparacoes_cartao = {"Cart\u00e3o Cr\u00e9dito", "Cart\u00e3o D\u00e9bito"}


    for titulo_pagamento, report_externo, report_fechamento, campo_valor in comparacoes:
        if not report_fechamento:
            continue
        itens_fechamento = list(report_fechamento.get("itens_autorizados") or [])
        itens_fechamento, itens_fechamento_cancelados = _split_cancelled_payment_items(
            itens_fechamento,
            fiscal_status_map,
        )
        for item_cancelado in itens_fechamento_cancelados:
            numero_cancelado = str(item_cancelado.get("numero") or "")
            if numero_cancelado in pedidos_cancelados_map:
                continue
            pedidos_cancelados_map[numero_cancelado] = {
                "numero": numero_cancelado,
                "numero_exibicao": item_cancelado.get("numero_exibicao") or _display_fiscal_number(numero_cancelado),
                "valor": item_cancelado.get("valor"),
                "motivo": f"{titulo_pagamento} cancelado permaneceu no fechamento",
            }
            _add_alert(
                "Cupom cancelado",
                f"CF {pedidos_cancelados_map[numero_cancelado]['numero_exibicao']}: {pedidos_cancelados_map[numero_cancelado]['motivo']}",
                _money_text(item_cancelado.get("valor")),
            )
        total_caixa_pagamento = round(float(report_fechamento.get("total_autorizado", 0.0) or 0.0), 2)
        origem_externa = str((report_externo or {}).get("origem") or "").strip()
        usa_fallback_zweb = origem_externa == "zweb_movimentacoes"

        if not report_externo:
            correlacao_rows.append(
                (
                    titulo_pagamento,
                    f"R$ {format_number_br(total_caixa_pagamento)}",
                    "R$ 0,00",
                    "Divergente",
                )
            )
            _add_alert(
                "Relat?rio ausente",
                f"{titulo_pagamento}: relat?rio local n?o encontrado na pasta atual de execucao.",
                "-",
            )
            continue

        if False and usa_fallback_zweb:
            _add_alert(
                "Origem alternativa",
                f"{titulo_pagamento}: valor confirmado via Financeiro > Movimentações do Zweb, sem relatório da Caixa/Azulzinha.",
                f"R$ {format_number_br(total_pagamentos)}",
            )

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
        total_pagamentos = round(
            sum(float(item.get(campo_valor, 0.0) or 0.0) for item in itens_externos)
            - sum(float(item.get(campo_valor, 0.0) or 0.0) for item, _nf in _matched_nf),
            2,
        )
        if usa_fallback_zweb:
            _add_alert(
                "Origem alternativa",
                f"{titulo_pagamento}: valor confirmado via Financeiro > MovimentaÃ§Ãµes do Zweb, sem relatÃ³rio da Caixa/Azulzinha.",
                f"R$ {format_number_br(total_pagamentos)}",
            )
        status_correlacao = "Finalizado" if abs(total_caixa_pagamento - total_pagamentos) < 0.01 else "Divergente"
        if usa_fallback_zweb:
            status_correlacao = f"{status_correlacao} (Financeiro Zweb)"
        correlacao_rows.append(
            (
                titulo_pagamento,
                f"R$ {format_number_br(total_caixa_pagamento)}",
                f"R$ {format_number_br(total_pagamentos)}",
                status_correlacao,
            )
        )

        for item in externos_restantes:
            valor = round(float(item.get(campo_valor, 0.0)), 2)
            detail = item.get("data_venda") or item.get("numero_exibicao") or item.get("numero") or "-"
            if titulo_pagamento == "PIX":
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
                    "Transa??o Banc?ria sem CF/NF",
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
                f"{titulo_pagamento} sem pagamento correspondente na m?quina",
            )
            if titulo_pagamento == "PIX":
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
                    "CF sem Transa??o Banc?ria",
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

    pedidos_cancelados = list(pedidos_cancelados_map.values())
    registros = list(registros_map.values())
    valor_canceladas_pendentes = round(sum(item.get("valor", 0.0) for item in pedidos_cancelados), 2)
    valor_registros_pendentes = round(
        sum(float(item.get("valor", 0.0)) for item in registros if item.get("valor") not in (None, "")),
        2,
    )
    valor_faltantes = round(valor_registros_pendentes + valor_canceladas_pendentes, 2)
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
        cancelados_rows=_build_cancelados_rows_from_pendencias(pedidos_cancelados),
    )
    if alertas_report:
        alertas_report["hidden_in_menu"] = True
        alertas_report["summary_items"] = [
            ("Per?odo", str(relatorio_caixa.get("periodo") or "N?o identificado")),
            ("Pend?ncias", str(_count_visible_alert_rows(alertas_report))),
            ("Total Pend?ncias", f"R$ {format_number_br(valor_faltantes)}"),
        ]
        alertas_report["correlacao_rows"] = correlacao_rows
        alertas_report["valor_total_vendas"] = total_caixa
        alertas_report["valor_pendente"] = valor_faltantes
        alertas_report["hidden_pending_count"] = 0
        alertas_report["hidden_pending_value"] = 0.0
        alertas_report["texto_informativo"] = ""
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
    if registros or alert_rows:
        status = "Faltante"
    if alertas_report:
        alertas_report["status"] = status

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
        "canceladas_ignoradas_valor": valor_canceladas_pendentes,
        "canceladas_pendentes_count": len(pedidos_cancelados),
        "canceladas_pendentes_valor": valor_canceladas_pendentes,
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


def _build_mva_conferencia_observation_rows(registros: list[dict]) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for item in registros or []:
        origem = _normalize_caixa_client(item.get("origem", ""))
        numero_exibicao = str(item.get("numero_exibicao") or "-").strip()
        observacao = str(item.get("observacao") or "").strip()
        valor = item.get("valor")
        valor_texto = "-" if valor in (None, "") else f"R$ {format_number_br(valor)}"
        if origem == "DAV":
            tipo = "DAV pendente"
        elif origem == "CF":
            tipo = "CF pendente"
        else:
            tipo = "Pendência"
        detalhe = f"{numero_exibicao}: {observacao}" if observacao else numero_exibicao
        rows.append((tipo, detalhe, valor_texto))
    return rows


def _comparar_caixa_fechamento_mva_com_pagamentos(relatorio_caixa: dict, relatorio_fechamento: dict) -> dict:
    itens_caixa = relatorio_caixa.get("itens_caixa", [])
    itens_nfce = relatorio_fechamento.get("nfces", [])
    relatorios_pagamento = dict(relatorio_fechamento.get("relatorios_pagamento") or {})
    _nfes_ativas, fiscal_status_map_mva, erro_minhas_notas = _load_minhas_notas_mva_context(
        relatorio_caixa.get("periodo", "")
    )

    davs_sem_cupom = _infer_mva_davs_sem_cupom(itens_caixa, itens_nfce)
    cancelados_ausentes = _build_mva_cancelled_coupon_pool(itens_nfce, fiscal_status_map_mva)
    davs_sem_cupom, _cancelados_correspondentes = _match_davs_with_mva_cancelled_coupons(
        davs_sem_cupom,
        cancelados_ausentes,
    )
    cfs_faltantes = sorted(
        {
            _normalize_fiscal_number(numero)
            for numero in relatorio_fechamento.get("nfces_faltantes_sequencia", [])
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
                "observacao": "DAV sem cupom no fechamento",
                "valor": round(float(item.get("valor", 0.0)), 2),
            }
        )
    registros.extend(
        _build_mva_cancelled_coupon_registros(
            cfs_faltantes,
            cancelados_ausentes,
        )
    )
    cupons_cancelados = [
        item for item in registros
        if item.get("origem") == "CF" and str(item.get("observacao") or "").strip() == "Cupom cancelado"
    ]
    registros_alerta = [
        item for item in registros
        if not (item.get("origem") == "CF" and str(item.get("observacao") or "").strip() == "Cupom cancelado")
    ]

    cancelados_pagamento_map = {str(item.get("numero") or ""): dict(item) for item in cupons_cancelados}

    correlacao_rows = []
    pix_fechamento_only: list[dict] = []
    pix_machine_only: list[dict] = []
    card_fechamento_only: list[dict] = []
    card_machine_only: list[dict] = []
    alert_rows: list[tuple[str, str, str]] = _build_mva_conferencia_observation_rows(registros_alerta)
    if erro_minhas_notas:
        avisos_minhas_notas = [
            "Nao foi possivel consultar o status dos cupons da MVA no Minhas Notas nesta analise."
        ]
    else:
        avisos_minhas_notas = []

    dinheiro_report = relatorios_pagamento.get("dinheiro") or {}
    dinheiro_total = round(float(dinheiro_report.get("total_autorizado", 0.0) or 0.0), 2)
    correlacao_rows.append(
        (
            "Dinheiro",
            f"R$ {format_number_br(dinheiro_total)}",
            "-",
            "Interno",
        )
    )


    comparacoes = [
        (
            "PIX",
            relatorios_pagamento.get("pix_caixa"),
            relatorios_pagamento.get("pix_fechamento"),
        ),
        (
            "Cartão Crédito",
            relatorios_pagamento.get("cartao_credito_caixa"),
            relatorios_pagamento.get("cartao_credito"),
        ),
        (
            "Cartão Débito",
            relatorios_pagamento.get("cartao_debito_caixa"),
            relatorios_pagamento.get("cartao_debito"),
        ),
    ]

    for titulo_pagamento, report_externo, report_fechamento in comparacoes:
        if not report_fechamento:
            continue
        itens_fechamento = list(report_fechamento.get("itens_autorizados") or [])
        itens_fechamento, itens_fechamento_cancelados = _split_cancelled_payment_items(
            itens_fechamento,
            fiscal_status_map_mva,
        )
        for item_cancelado in itens_fechamento_cancelados:
            numero_cancelado = str(item_cancelado.get("numero") or "")
            if numero_cancelado not in cancelados_pagamento_map:
                cancelados_pagamento_map[numero_cancelado] = {
                    "numero": numero_cancelado,
                    "numero_exibicao": item_cancelado.get("numero_exibicao") or _display_fiscal_number(numero_cancelado),
                    "valor": item_cancelado.get("valor"),
                    "observacao": "Cupom cancelado",
                }
                alert_rows.append(
                    (
                        "Cupom cancelado",
                        f"{titulo_pagamento}: CF {cancelados_pagamento_map[numero_cancelado]['numero_exibicao']} cancelado permaneceu no fechamento",
                        f"R$ {format_number_br(item_cancelado.get('valor', 0.0))}",
                    )
                )
        itens_externos = list((report_externo or {}).get("itens_autorizados") or [])
        total_fechamento = round(float(report_fechamento.get("total_autorizado", 0.0) or 0.0), 2)
        total_externo = round(float((report_externo or {}).get("total_autorizado", 0.0) or 0.0), 2)
        status_correlacao = "Finalizado" if report_externo and abs(total_fechamento - total_externo) < 0.01 else "Divergente"
        correlacao_rows.append(
            (
                titulo_pagamento,
                f"R$ {format_number_br(total_fechamento)}",
                f"R$ {format_number_br(total_externo)}",
                status_correlacao,
            )
        )

        if not report_externo:
            alert_rows.append(
                (
                    "Relatório ausente",
                    f"{titulo_pagamento}: relatório local não encontrado na pasta atual de execução.",
                    "-",
                )
            )
            continue

        _matched, externos_sem_fechamento, fechamento_sem_externo = _multiset_match_by_value(
            itens_externos,
            itens_fechamento,
            campo_esquerda="valor_bruto",
            campo_direita="valor_bruto",
        )

        for item in externos_sem_fechamento:
            valor = round(float(item.get("valor_bruto", 0.0)), 2)
            detalhe = item.get("data_venda") or item.get("numero_exibicao") or item.get("numero") or "-"
            payload = {
                "titulo": titulo_pagamento,
                "data_venda": detalhe,
                "valor": valor,
            }
            if titulo_pagamento == "PIX":
                pix_machine_only.append(payload)
            else:
                card_machine_only.append(payload)

        for item in fechamento_sem_externo:
            numero = _normalize_fiscal_number(item.get("numero", "") or item.get("numero_exibicao", ""))
            if numero and (fiscal_status_map_mva.get(numero) or {}).get("cancelada"):
                continue
            valor = round(float(item.get("valor_bruto", 0.0)), 2)
            payload = {
                "titulo": titulo_pagamento,
                "numero_exibicao": item.get("numero_exibicao") or _display_fiscal_number(item.get("numero", "")),
                "valor": valor,
            }
            if titulo_pagamento == "PIX":
                pix_fechamento_only.append(payload)
            else:
                card_fechamento_only.append(payload)

    for item in pix_fechamento_only:
        alert_rows.append(
            (
                "CF sem Transação Bancária",
                f"PIX: CF {item.get('numero_exibicao') or '-'}",
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
        )
    for item in card_fechamento_only:
        alert_rows.append(
            (
                "CF sem Transação Bancária",
                f"{item.get('titulo')}: CF {item.get('numero_exibicao') or '-'}",
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
        )
    for item in pix_machine_only:
        alert_rows.append(
            (
                "Transação Bancária sem CF/NF",
                f"PIX: {item.get('data_venda') or '-'}",
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
        )
    for item in card_machine_only:
        alert_rows.append(
            (
                "Transação Bancária sem CF/NF",
                f"{item.get('titulo')}: {item.get('data_venda') or '-'}",
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
        )

    cancelados_visiveis = list(cancelados_pagamento_map.values())
    numeros_cancelados_registro = {
        str(item.get("numero") or "")
        for item in cupons_cancelados
        if str(item.get("numero") or "")
    }
    valor_cancelados_pagamento_adicionais = round(
        sum(
            float(item.get("valor", 0.0) or 0.0)
            for item in cancelados_visiveis
            if str(item.get("numero") or "") not in numeros_cancelados_registro
        ),
        2,
    )

    alertas_report = _build_eh_alerts_report(
        relatorio_caixa.get("periodo"),
        alert_rows,
        pix_fechamento_rows=[
            (f"CF {item.get('numero_exibicao') or '-'}", f"R$ {format_number_br(item.get('valor', 0.0))}")
            for item in pix_fechamento_only
        ],
        pix_maquina_rows=[
            ("PIX", str(item.get("data_venda") or "-"), f"R$ {format_number_br(item.get('valor', 0.0))}")
            for item in pix_machine_only
        ],
        cartao_fechamento_rows=[
            (f"{item.get('titulo')}: CF {item.get('numero_exibicao') or '-'}", f"R$ {format_number_br(item.get('valor', 0.0))}")
            for item in card_fechamento_only
        ],
        cartao_maquina_rows=[
            (str(item.get("titulo") or "Cartão"), str(item.get("data_venda") or "-"), f"R$ {format_number_br(item.get('valor', 0.0))}")
            for item in card_machine_only
        ],
        cancelados_rows=_build_cancelados_rows_from_pendencias(cancelados_visiveis),
        allow_empty=True,
    )

    total_caixa = round(float(relatorio_caixa.get("total_caixa", 0.0)), 2)
    total_resumo = round(float(relatorio_fechamento.get("total_nfce", 0.0)), 2)
    valor_davs_pendentes = round(
        sum(float(item.get("valor", 0.0)) for item in registros if item.get("valor") not in (None, "")),
        2,
    )
    valor_banco_pendente = round(
        sum(float(item.get("valor", 0.0)) for item in pix_fechamento_only + card_fechamento_only + pix_machine_only + card_machine_only),
        2,
    )
    valor_faltantes = round(valor_davs_pendentes + valor_banco_pendente + valor_cancelados_pagamento_adicionais, 2)

    if alertas_report:
        alertas_report["hidden_in_menu"] = True
        alertas_report["caixa_modelo"] = "MVA"
        alertas_report["summary_items"] = [
            ("Período", str(relatorio_caixa.get("periodo") or "Não identificado")),
            ("Pendências", str(len(alert_rows))),
            ("Total Pendências", f"R$ {format_number_br(valor_faltantes)}"),
        ]
        alertas_report["summary_items"][1] = (
            str(alertas_report["summary_items"][1][0]),
            str(_count_visible_alert_rows(alertas_report)),
        )
        alertas_report["correlacao_rows"] = correlacao_rows
        alertas_report["valor_total_vendas"] = total_caixa
        alertas_report["valor_pendente"] = valor_faltantes
        alertas_report["texto_informativo"] = ""
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

    status = "Confere" if not registros and not alert_rows else "Faltante"
    if alertas_report:
        alertas_report["status"] = status

    periodo_unico, _ = _extract_period_range(relatorio_caixa.get("periodo", ""))
    cupons_cancelados = [
        item for item in registros
        if item.get("origem") == "CF" and str(item.get("observacao") or "").strip() == "Cupom cancelado"
    ]
    subtitle = str(relatorio_fechamento.get("subtitle") or "").strip()
    if cupons_cancelados:
        subtitle = (
            (subtitle + " ") if subtitle else ""
        ) + f"Cupons cancelados identificados no Minhas Notas: {len(cupons_cancelados)}."
    return {
        "fechamento_modelo": "MVA",
        "caixa_modelo": "MVA",
        "subtitle": subtitle,
        "arquivo_caixa": relatorio_caixa.get("arquivo"),
        "arquivo_resumo": relatorio_fechamento.get("arquivo"),
        "arquivo_resumo_titulo": relatorio_fechamento.get("arquivo_resumo_titulo") or "Arquivo Fechamento",
        "periodo": periodo_unico or relatorio_caixa.get("periodo"),
        "total_caixa": total_caixa,
        "total_caixa_titulo": "Total DAVs finalizados",
        "total_resumo_nfce": total_resumo,
        "total_resumo_titulo": relatorio_fechamento.get("total_resumo_titulo") or "Total Fechamento de caixa",
        "nfces_faltantes_count": len(registros),
        "faltantes_titulo": "DAVs/CF faltantes",
        "valor_faltantes": valor_faltantes,
        "status": status,
        "cupons_cancelados_count": len(cupons_cancelados),
        "cupons_cancelados_valor": round(
            sum(float(item.get("valor", 0.0) or 0.0) for item in cupons_cancelados),
            2,
        ),
        "secao_titulo": "DAVs/CF para conferência",
        "empty_message": "Nenhum DAV/CF faltante encontrado.",
        "registros_conferencia": sorted(
            registros,
            key=lambda item: (
                0 if item.get("origem") == "DAV" else 1,
                int(item["numero"]) if item.get("numero") else 0,
            ),
        ),
        "relatorios_pagamento": relatorios_pagamento,
        "alertas_count": len(alert_rows),
        "avisos_usuario": list(relatorio_fechamento.get("avisos_usuario") or []) + avisos_minhas_notas,
    }


def _comparar_caixa_resumo_nfce_mva(relatorio_caixa: dict, relatorio_nfce: dict) -> dict:
    if relatorio_nfce.get("arquivo_tipo") == "fechamento_caixa_clipp_mva":
        return _comparar_caixa_fechamento_mva_com_pagamentos(relatorio_caixa, relatorio_nfce)

    itens_caixa = relatorio_caixa.get("itens_caixa", [])
    itens_nfce = relatorio_nfce.get("nfces", [])
    itens_cupom_base = [
        item
        for item in itens_caixa
        if _is_mva_cupom_client(item.get("cliente", ""))
    ]

    davs_sem_cupom = _infer_mva_davs_sem_cupom(itens_caixa, itens_nfce)
    nfes_ativas, fiscal_status_map_mva, erro_minhas_notas = _load_minhas_notas_mva_context(
        relatorio_caixa.get("periodo", "")
    )
    if nfes_ativas:
        davs_sem_cupom, nfes_identificadas = _match_davs_with_minhas_notas_nfes_from_items(
            davs_sem_cupom,
            nfes_ativas,
        )
    else:
        nfes_identificadas = []
    cancelados_ausentes = _build_mva_cancelled_coupon_pool(itens_nfce, fiscal_status_map_mva)
    davs_sem_cupom, _cancelados_correspondentes = _match_davs_with_mva_cancelled_coupons(
        davs_sem_cupom,
        cancelados_ausentes,
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
    registros.extend(
        _build_mva_cancelled_coupon_registros(
            cfs_faltantes,
            cancelados_ausentes,
        )
    )

    total_caixa = round(sum(float(item.get("valor", 0.0)) for item in itens_cupom_base), 2)
    total_resumo = round(float(relatorio_nfce.get("total_nfce", 0.0)), 2)
    cupons_cancelados = [
        item for item in registros
        if item.get("origem") == "CF" and str(item.get("observacao") or "").strip() == "Cupom cancelado"
    ]
    valor_cupons_cancelados = round(
        sum(float(item.get("valor", 0.0) or 0.0) for item in cupons_cancelados),
        2,
    )
    if nfes_identificadas:
        valor_faltantes = round(
            sum(float(item.get("valor", 0.0)) for item in davs_sem_cupom) + valor_cupons_cancelados,
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
    if cupons_cancelados:
        subtitle += f" Cupons cancelados identificados no Minhas Notas: {len(cupons_cancelados)}."
    elif erro_minhas_notas:
        subtitle += " Consulta ao Minhas Notas indisponivel nesta analise."

    registros_alerta = [
        item for item in registros
        if not (item.get("origem") == "CF" and str(item.get("observacao") or "").strip() == "Cupom cancelado")
    ]
    alert_rows = _build_mva_conferencia_observation_rows(registros_alerta)
    if erro_minhas_notas:
        alert_rows.append(("Minhas Notas", "Consulta indisponível nesta análise.", "-"))

    relatorios_pagamento: dict[str, dict] = {}
    alertas_report = _build_eh_alerts_report(
        relatorio_caixa.get("periodo"),
        alert_rows,
        cancelados_rows=_build_cancelados_rows_from_pendencias(cupons_cancelados),
        allow_empty=False,
    )
    if alertas_report:
        alertas_report["hidden_in_menu"] = True
        alertas_report["caixa_modelo"] = "MVA"
        alertas_report["summary_items"] = [
            ("Período", str(relatorio_caixa.get("periodo") or "Não identificado")),
            ("Pendências", str(len(alert_rows))),
            ("Total Pendências", f"R$ {format_number_br(valor_faltantes)}"),
        ]
        alertas_report["summary_items"][1] = (
            str(alertas_report["summary_items"][1][0]),
            str(_count_visible_alert_rows(alertas_report)),
        )
        alertas_report["correlacao_rows"] = []
        alertas_report["valor_total_vendas"] = total_caixa
        alertas_report["valor_pendente"] = valor_faltantes
        alertas_report["texto_informativo"] = ""
        alertas_report["status"] = status
        relatorios_pagamento[alertas_report["categoria"]] = alertas_report

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
        "cupons_cancelados_count": len(cupons_cancelados),
        "cupons_cancelados_valor": valor_cupons_cancelados,
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
        "relatorios_pagamento": relatorios_pagamento,
        "nfes_identificadas": nfes_identificadas,
        "erro_minhas_notas": erro_minhas_notas,
        "avisos_usuario": (
            ["Nao foi possivel consultar o status dos cupons da MVA no Minhas Notas nesta analise."]
            if erro_minhas_notas
            else []
        ),
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
        print(f"Erro ao analisar período de vendas: {e}")
        SALES_PERIOD = None
        return None

# ----------------- Conexão com Supabase -----------------

from api_client import (
    get_supabase,
    listar_vendedores_db,
    registrar_vendedor_db,
    excluir_ultimo_feedback,
    atualizar_ultimo_feedback,
    salvar_feedback_db,
    carregar_feedbacks_db
)


# Importações delegadas ao pdf_parser
from pdf_parser import (_get_pd, _get_pdfplumber, _inspect_pdf_text_layer, _pdf_sem_texto_message, _analisar_pdf_caixa_eh, _analisar_pdf_caixa_mva, analisar_pdf_caixa, _analisar_pdf_resumo_nfce_eh, _analisar_pdf_resumo_nfce_mva, analisar_pdf_resumo_nfce, source_pdf_async, adicionar_pdf, processar_pdf_sem_ui)
