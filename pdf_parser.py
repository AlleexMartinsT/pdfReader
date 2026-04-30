import os
import re
import queue
import threading
from tkinter import filedialog, messagebox

# Importar o globo de funÃ§Ãµes legadas do utils
from utils import *
from utils import (
    _UI_REFS,
    _classify_caixa_document,
    _display_fiscal_number,
    _empty_caixa_report,
    _empty_resumo_nfce_report,
    _extract_mva_column_bounds,
    _extract_mva_description_from_row,
    _extract_mva_word_rows,
    _extract_sale_amount,
    _extract_total_vendas,
    _extract_vendor_name,
    _find_missing_fiscal_numbers,
    _is_eh_counter_client,
    _is_sale_entry_line,
    _line_has_d_marker,
    _mva_report_label,
    _mva_report_type_from_text,
    _normalize_caixa_client,
    _normalize_fiscal_number,
    _normalize_mva_description,
    _poll_queue,
)


def _get_pd():
    import pandas as pd
    return pd


def _get_pdfplumber():
    import pdfplumber
    return pdfplumber


def _inspect_pdf_text_layer(pdf) -> dict:
    primeiro_texto = ""
    total_imagens = 0
    encontrou_texto = False
    paginas = list(pdf.pages[:3])

    for pagina in paginas:
        if pagina.chars:
            encontrou_texto = True
        total_imagens += len(pagina.images or [])
        if not primeiro_texto:
            texto = (pagina.extract_text() or "").strip()
            if texto:
                primeiro_texto = texto[:240]
                encontrou_texto = True
        if encontrou_texto and primeiro_texto:
            break

    return {
        "pdf_sem_texto": not encontrou_texto,
        "pdf_tem_imagens": total_imagens > 0,
        "pdf_paginas": len(pdf.pages),
        "pdf_primeiro_texto": primeiro_texto,
        "pdf_produtor": str((pdf.metadata or {}).get("Producer") or "").strip(),
    }


def _pdf_sem_texto_message(arquivo: str | None = None) -> str:
    nome = os.path.basename(arquivo or "").strip()
    prefixo = f'O arquivo "{nome}"' if nome else "O PDF selecionado"
    return (
        f"{prefixo} não possui texto selecionável.\n\n"
        "Ele parece ter sido salvo como imagem ou escaneado, e por isso o aplicativo não consegue ler os dados.\n\n"
        "Exporte o relatorio novamente em PDF com texto selecionavel e tente de novo."
    )


def _analisar_pdf_caixa_eh(caminho_pdf: str) -> dict:
    pdfplumber = _get_pdfplumber()
    pedidos = []
    cliente_atual = None
    periodo = None
    total_documento = None
    itens_caixa = []
    itens_excluidos = []

    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text() or ""
            for linha in texto.splitlines():
                if periodo is None:
                    linha_normalizada = _normalize_caixa_client(linha)
                    match_periodo = re.search(r"(\d{2}/\d{2}/\d{4})\s+ATE\s+(\d{2}/\d{2}/\d{4})", linha_normalizada)
                    if match_periodo:
                        periodo = f"{match_periodo.group(1)} - {match_periodo.group(2)}"

                match_cliente = re.match(r"^Cliente:\s*(.*?)\s+\d{2}/\d{2}/\d{4}\s*$", linha)
                if match_cliente:
                    cliente_atual = match_cliente.group(1).strip()
                    continue

                match_pedido = re.match(r"^(N\S*\s+\d+\s+.+?)\s+([-\d\.,]+)\s*$", linha)
                if match_pedido and cliente_atual:
                    descricao = match_pedido.group(1).strip()
                    valor = parse_number(match_pedido.group(2))
                    numero_match = re.search(r"\b(\d{5,})\b", descricao)
                    pedidos.append({
                        "cliente": cliente_atual,
                        "valor": valor,
                        "pedido": numero_match.group(1) if numero_match else "",
                        "documento": _classify_caixa_document(descricao),
                    })
                    cliente_atual = None
                    continue

                match_total = re.match(r"^Total R\$\s+([-\d\.,]+)\s*$", linha)
                if match_total:
                    total_documento = parse_number(match_total.group(1))

    total_caixa_bruto = 0.0
    total_excluido = 0.0
    pedidos_caixa = 0
    pedidos_balcao = 0
    pedidos_excluidos = 0
    pedidos_excluidos_cliente = 0
    pedidos_excluidos_documento = 0

    for pedido in pedidos:
        cliente = pedido["cliente"]
        valor = pedido["valor"]
        is_balcao = _is_eh_counter_client(cliente)
        is_nfe = pedido["documento"] == "Nota Fiscal Eletronica"
        motivos = []

        if is_balcao:
            pedidos_balcao += 1
        if not is_balcao:
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
                    "valor": round(valor, 2),
                }
            )
            continue

        total_caixa_bruto += valor
        pedidos_caixa += 1
        itens_caixa.append(
            {
                "pedido": pedido["pedido"],
                "cliente": cliente,
                "documento": pedido["documento"],
                "valor": round(valor, 2),
            }
        )

    if total_documento is None:
        total_documento = total_caixa_bruto + total_excluido

    total_documento = round(total_documento, 2)
    total_excluido = round(total_excluido, 2)
    total_caixa = round(total_documento - total_excluido, 2)

    return {
        "arquivo": os.path.basename(caminho_pdf),
        "caixa_modelo": "EH",
        "arquivo_tipo": "pedidos_importados_eh",
        "periodo": periodo,
        "pedidos_total": len(pedidos),
        "pedidos_balcao": pedidos_balcao,
        "pedidos_caixa": pedidos_caixa,
        "pedidos_excluidos": pedidos_excluidos,
        "pedidos_excluidos_cliente": pedidos_excluidos_cliente,
        "pedidos_excluidos_documento": pedidos_excluidos_documento,
        "total_documento": total_documento,
        "total_excluido": total_excluido,
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


def _analisar_pdf_caixa_mva(caminho_pdf: str) -> dict:
    from datetime import datetime

    pdfplumber = _get_pdfplumber()
    padrao_linha = re.compile(
        r"^(?:(?P<pedido>\d{6,})\s+)?"
        r"(?:(?P<data>\d{2}/\d{2}/\d{4})"
        r"(?:\s+(?P<hora>\d{2}:\d{2}:\d{2}))?\s+)?"
        r"(?P<resto>.*?)"
        r"\s+(?P<status>[^\W\d_]+)"
        r"\s+(?P<valor>[-\d\.,]+)\s*$"
    )
    padrao_total = re.compile(r"^[-\d\.,]+$")

    registros = []
    totais_documento = []
    datas_encontradas = []
    arquivo_tipo = "mva_desconhecido"

    with pdfplumber.open(caminho_pdf) as pdf:
        primeira_pagina = pdf.pages[0].extract_text() or ""
        arquivo_tipo = _mva_report_type_from_text(primeira_pagina)
        last_bounds = None

        for pagina in pdf.pages:
            texto = pagina.extract_text() or ""
            linhas_palavras = _extract_mva_word_rows(pagina)
            bounds = _extract_mva_column_bounds(pagina)
            if bounds.get("status_x0") is None and last_bounds is not None:
                bounds = last_bounds
            elif bounds.get("status_x0") is not None:
                last_bounds = bounds
            data_row_idx = 0

            for linha in texto.splitlines():
                linha = linha.strip()
                match = padrao_linha.match(linha)
                if match:
                    data = match.group("data")
                    hora = match.group("hora") or ""
                    data_venda = ""
                    ordem = ""
                    if data:
                        datas_encontradas.append(datetime.strptime(data, "%d/%m/%Y"))
                        if hora:
                            data_venda = f"{data} {hora}"
                            ordem = datetime.strptime(f"{data} {hora}", "%d/%m/%Y %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                        else:
                            data_venda = data
                    status = match.group("status").strip()
                    fallback_descricao = _normalize_mva_description(match.group("resto"))
                    row_words = linhas_palavras[data_row_idx] if data_row_idx < len(linhas_palavras) else []
                    descricao = _extract_mva_description_from_row(row_words, bounds, fallback_descricao)
                    data_row_idx += 1
                    registros.append(
                        {
                            "pedido": match.group("pedido"),
                            "cliente": descricao,
                            "status": status,
                            "valor": round(parse_number(match.group("valor")), 2),
                            "documento": status,
                            "origem_mva": _mva_report_label(arquivo_tipo),
                            "data_venda": data_venda,
                            "ordem": ordem,
                        }
                    )
                    continue
                if padrao_total.fullmatch(linha):
                    totais_documento.append(round(parse_number(linha), 2))

    periodo = None
    if datas_encontradas:
        data_inicial = min(datas_encontradas).strftime("%d/%m/%Y")
        data_final = max(datas_encontradas).strftime("%d/%m/%Y")
        periodo = f"{data_inicial} - {data_final}"

    itens_caixa = []
    itens_excluidos = []
    total_caixa_bruto = 0.0
    total_excluido = 0.0
    pedidos_editando = 0
    pedidos_outros_status = 0

    for registro in registros:
        item_base = {
            "pedido": registro["pedido"],
            "cliente": registro["cliente"],
            "documento": registro["status"],
            "valor": registro["valor"],
            "data_venda": registro.get("data_venda", ""),
            "ordem": registro.get("ordem", ""),
            "origem_mva": registro.get("origem_mva", ""),
        }
        if registro["status"] == "Finalizado":
            total_caixa_bruto += registro["valor"]
            itens_caixa.append(item_base)
            continue

        total_excluido += registro["valor"]
        itens_excluidos.append(item_base)
        if registro["status"] == "Editando":
            pedidos_editando += 1
        else:
            pedidos_outros_status += 1

    total_documento = totais_documento[-1] if totais_documento else round(
        sum(item["valor"] for item in registros),
        2,
    )
    total_documento = round(total_documento, 2)
    total_excluido = round(total_excluido, 2)
    total_caixa = round(total_caixa_bruto, 2)

    return {
        "arquivo": os.path.basename(caminho_pdf),
        "caixa_modelo": "MVA",
        "arquivo_tipo": arquivo_tipo,
        "periodo": periodo,
        "pedidos_total": len(registros),
        "pedidos_balcao": 0,
        "pedidos_caixa": len(itens_caixa),
        "pedidos_excluidos": len(itens_excluidos),
        "pedidos_excluidos_cliente": 0,
        "pedidos_excluidos_documento": 0,
        "pedidos_editando": pedidos_editando,
        "pedidos_outros_status": pedidos_outros_status,
        "total_documento": total_documento,
        "total_excluido": total_excluido,
        "total_caixa": total_caixa,
        "itens_caixa": sorted(
            itens_caixa,
            key=lambda item: (str(item.get("ordem") or ""), item["pedido"]),
        ),
        "itens_excluidos": sorted(
            itens_excluidos,
            key=lambda item: (str(item.get("ordem") or ""), item["documento"], item["pedido"]),
        ),
    }


def analisar_pdf_caixa(caminho_pdf: str | list[str] | tuple[str, ...]) -> dict:
    if isinstance(caminho_pdf, (list, tuple)):
        relatorios = [analisar_pdf_caixa(caminho) for caminho in caminho_pdf if caminho]
        if relatorios and all((rel.get("caixa_modelo") or "").upper() == "MVA" for rel in relatorios):
            return combinar_relatorios_caixa_mva(relatorios)
        if len(relatorios) == 1:
            return relatorios[0]
        raise ValueError("Não foi possível combinar os relatórios de Caixa informados.")

    pdfplumber = _get_pdfplumber()
    with pdfplumber.open(caminho_pdf) as pdf:
        pdf_info = _inspect_pdf_text_layer(pdf)
        primeira_pagina = pdf.pages[0].extract_text() or pdf_info.get("pdf_primeiro_texto") or ""

    if pdf_info.get("pdf_sem_texto"):
        return _empty_caixa_report(caminho_pdf, pdf_info)

    if _mva_report_type_from_text(primeira_pagina) != "mva_desconhecido":
        return _analisar_pdf_caixa_mva(caminho_pdf)
    return _analisar_pdf_caixa_eh(caminho_pdf)


def _analisar_pdf_resumo_nfce_eh(caminho_pdf: str) -> dict:
    pdfplumber = _get_pdfplumber()
    itens_nfce = []
    periodo = None
    total_nfce = None
    total_reportado = None

    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text() or ""
            for linha in texto.splitlines():
                linha = linha.strip()
                linha_normalizada = _normalize_caixa_client(linha)

                if periodo is None:
                    match_periodo = re.search(
                        r"(\d{2}/\d{2}/\d{4})\s+ATE\s+(\d{2}/\d{2}/\d{4})",
                        linha_normalizada,
                    )
                    if match_periodo:
                        periodo = f"{match_periodo.group(1)} - {match_periodo.group(2)}"

                match_nfce_fechamento = re.match(
                    r"^(\d{6,})\s+\d{2}/\d{2}/\d{2}\s+R\$\s+([-\d\.,]+)\s*$",
                    linha,
                )
                if match_nfce_fechamento:
                    numero = _normalize_fiscal_number(match_nfce_fechamento.group(1))
                    itens_nfce.append(
                        {
                            "numero": numero,
                            "numero_exibicao": _display_fiscal_number(numero),
                            "descricao": "Fechamento de caixa",
                            "valor": round(parse_number(match_nfce_fechamento.group(2)), 2),
                        }
                    )
                    continue

                match_nfce = re.match(
                    r"^(\d{6,})\s+Autorizada\s+\d+\s+65\s+\d{2}/\d{2}/\d{4}\s+(.+?)\s+([-\d\.,]+)\s*$",
                    linha,
                )
                if match_nfce:
                    numero = _normalize_fiscal_number(match_nfce.group(1))
                    itens_nfce.append(
                        {
                            "numero": numero,
                            "numero_exibicao": _display_fiscal_number(numero),
                            "descricao": match_nfce.group(2).strip(),
                            "valor": round(parse_number(match_nfce.group(3)), 2),
                        }
                    )
                    continue

                match_total = re.match(r"^Totais?\s+R\$\s+([-\d\.,]+)\s*$", linha)
                if not match_total:
                    match_total = re.match(r"^Total\s+R\$\s+([-\d\.,]+)\s*$", linha)
                if match_total:
                    total_reportado = round(parse_number(match_total.group(1)), 2)

    total_itens = round(sum(item["valor"] for item in itens_nfce), 2)
    if itens_nfce:
        total_nfce = total_itens
    elif total_reportado is not None:
        total_nfce = total_reportado
    else:
        total_nfce = 0.0

    faltantes_sequencia = _find_missing_fiscal_numbers([item["numero"] for item in itens_nfce])

    return {
        "arquivo": os.path.basename(caminho_pdf),
        "resumo_modelo": "EH",
        "periodo": periodo,
        "quantidade_nfce": len(itens_nfce),
        "total_nfce": total_nfce,
        "nfces": sorted(itens_nfce, key=lambda item: item["numero"]),
        "nfces_faltantes_sequencia": faltantes_sequencia,
    }


def _analisar_pdf_resumo_nfce_mva(caminho_pdf: str) -> dict:
    pdfplumber = _get_pdfplumber()
    itens_nfce = []
    periodo = None
    total_nfce = None
    quantidade_documentos = None

    with pdfplumber.open(caminho_pdf) as pdf:
        for pagina in pdf.pages:
            texto = pagina.extract_text() or ""
            for linha in texto.splitlines():
                linha = linha.strip()
                linha_normalizada = _normalize_caixa_client(linha)

                if periodo is None:
                    match_periodo = re.search(
                        r"PERIODO ANALISADO,\s+DE\s+(\d{2}/\d{2}/\d{4})\s+ATE\s+(\d{2}/\d{2}/\d{4})",
                        linha_normalizada,
                    )
                    if match_periodo:
                        periodo = f"{match_periodo.group(1)} - {match_periodo.group(2)}"

                match_nfce = re.match(
                    r"^(\d{6,})\s+\d+\s+\d+\s+\d{2}/\d{2}/\d{4}\s+\d+\s+(.+?)\s+([-\d\.,]+)\s+[-\d\.,]+\s*$",
                    linha,
                )
                if match_nfce:
                    numero = _normalize_fiscal_number(match_nfce.group(1))
                    itens_nfce.append(
                        {
                            "numero": numero,
                            "numero_exibicao": _display_fiscal_number(numero),
                            "descricao": match_nfce.group(2).strip(),
                            "valor": round(parse_number(match_nfce.group(3)), 2),
                        }
                    )
                    continue

                match_total = re.search(
                    r"TOTAL DE DOCUMENTOS:\s*(\d+)\s+TOTAIS:\s*([-\d\.,]+)",
                    linha_normalizada,
                )
                if match_total:
                    quantidade_documentos = int(match_total.group(1))
                    total_nfce = round(parse_number(match_total.group(2)), 2)

    if total_nfce is None:
        total_nfce = round(sum(item["valor"] for item in itens_nfce), 2)
    if quantidade_documentos is None:
        quantidade_documentos = len(itens_nfce)

    faltantes_sequencia = _find_missing_fiscal_numbers([item["numero"] for item in itens_nfce])

    return {
        "arquivo": os.path.basename(caminho_pdf),
        "resumo_modelo": "MVA",
        "periodo": periodo,
        "quantidade_nfce": quantidade_documentos,
        "total_nfce": total_nfce,
        "nfces": sorted(itens_nfce, key=lambda item: item["numero"]),
        "nfces_faltantes_sequencia": faltantes_sequencia,
    }


def analisar_pdf_resumo_nfce(caminho_pdf: str) -> dict:
    pdfplumber = _get_pdfplumber()
    with pdfplumber.open(caminho_pdf) as pdf:
        pdf_info = _inspect_pdf_text_layer(pdf)
        primeira_pagina = pdf.pages[0].extract_text() or pdf_info.get("pdf_primeiro_texto") or ""

    if pdf_info.get("pdf_sem_texto"):
        return _empty_resumo_nfce_report(caminho_pdf, pdf_info)

    primeira_pagina_normalizada = _normalize_caixa_client(primeira_pagina)
    if "RELATORIO DE VENDAS" in primeira_pagina_normalizada and "PERIODO ANALISADO" in primeira_pagina_normalizada:
        return _analisar_pdf_resumo_nfce_mva(caminho_pdf)
    return _analisar_pdf_resumo_nfce_eh(caminho_pdf)


def source_pdf_async(tree, progress_var, progress_bar, root, label_files_var, btn_cancel, caminho, origem):
    """
    Inicia processamento do PDF jÃ¡ com o caminho e a origem (MVA/EH) escolhidos.
    NÃ£o pergunta nada ao usuÃ¡rio â€” a escolha jÃ¡ veio do tk.py.
    """
    global listFiles, list_results

    if not caminho:
        return False

    # evita duplicados
    if caminho in listFiles:
        messagebox.showerror("Erro", "Arquivo jÃ¡ importado!")
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

    # habilita botÃ£o cancelar (na main thread via chamada)
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
        messagebox.showerror("Erro", "Arquivo jÃ¡ importado!")
        return
    
    try:
        pdfplumber = _get_pdfplumber()
        with pdfplumber.open(caminho) as pdf:
            if pdf.metadata.get("encrypted", False):
                messagebox.showerror("Erro", "Este PDF estÃ¡ protegido por senha.")
                return
    except Exception as e:
        messagebox.showerror("Erro", f"NÃ£o foi possÃ­vel abrir o PDF: {e}")
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

    # Atualiza o label imediatamente com a origem atribuÃ­da
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
            # embala jÃ¡ com a origem
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
                        messagebox.showinfo("Cancelado", "Processamento cancelado pelo usuÃ¡rio.")
                    elif resultados.get("__empty__"):
                        messagebox.showwarning(
                            "Aviso",
                            resultados.get("__warning__") or "Nenhum dado foi encontrado neste PDF.",
                        )
                    else:
                        # Armazena os dados
                        listFiles.append(caminho)
                        list_results.append(resultados)
                        results_by_source[origem].append((caminho, resultados))  # salva tambÃ©m o caminho para referÃªncia

                        # ðŸ”¹ Monta o texto do label com todos os arquivos e origens
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
                        messagebox.showinfo("ConcluÃ­do", f"PDF adicional processado e atribuÃ­do a {origem}!")
                    return

                elif kind == "error":
                    set_btn_cancel()
                    messagebox.showerror("Erro", payload)
                    return
        except queue.Empty:
            pass
        root.after(10, poll_queue_add)

    poll_queue_add()


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

    # se nÃ£o vier nada, cria versÃµes "neutras"
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
    on_progress("progress", 1)
    with pdfplumber.open(caminho_pdf) as pdf:
        pdf_info = _inspect_pdf_text_layer(pdf)
        if pdf_info.get("pdf_sem_texto"):
            return {
                "__empty__": True,
                "__pdf_sem_texto__": True,
                "__warning__": _pdf_sem_texto_message(caminho_pdf),
            }

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

                # Atualiza o progresso a cada pÃ¡gina
                progresso = int(i * 100 / max(1, total))
                on_progress("progress", progresso)
            except Exception as e:
                return {"__error__": str(e)}

        # Garante que o progresso chegue a 100% apÃ³s o loop
        fechar_vendedor()
    if not resultados:
        return {"__empty__": True}
    
    return resultados

