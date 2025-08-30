from tk import *
from library import *
from globalVar import *

def escolher_pdf(tree, progress_var, progress_bar, root):
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    if not caminho:
        return
    
    for item in tree.get_children():
        tree.delete(item)

    resultados = processar_pdf(caminho, progress_var, progress_bar, root)

    for vendedor, dados in resultados.items():
        tree.insert("", "end", values=(
            vendedor,
            dados['atendidos'],
            dados['devolucoes'],
            dados['total_clientes'],
            dados['total_vendas']
        ))

    messagebox.showinfo("Concluído", "Processamento finalizado com sucesso!")
  
def processar_pdf(caminho_pdf, progress_var, progress_bar, root):
    resultados = {}
    vendedor_atual = None

    def fechar_vendedor():
        nonlocal vendedor_atual
        if vendedor_atual and vendedor_atual in resultados:
            dados = resultados[vendedor_atual]
            dados["total_clientes"] = dados["atendidos"] - dados["devolucoes"]

    with pdfplumber.open(caminho_pdf) as pdf:
        total_paginas = len(pdf.pages)
        for i, pagina in enumerate(pdf.pages, start=1):
            texto = pagina.extract_text()
            if not texto:
                continue
            linhas = texto.splitlines()
            for linha in linhas:
                # Detecta vendedor
                if "Vendedor: " in linha:
                    fechar_vendedor()
                    partes = linha.split("Vendedor: ", 1)[1].strip()
                    if partes:
                        palavras = partes.split()
                        if palavras[0].isdigit():
                            vendedor_atual = " ".join(palavras[1:])
                        else:
                            vendedor_atual = " ".join(palavras)
                        resultados[vendedor_atual] = {
                            "atendidos": 0,
                            "devolucoes": 0,
                            "total_clientes": 0,
                            "total_vendas": ""
                        }
                    continue

                # Detecta linha de cliente
                if regex_data.match(linha):
                    if not vendedor_atual:
                        continue
                    resultados[vendedor_atual]["atendidos"] += 1
                    if regex_negativo.search(linha):
                        resultados[vendedor_atual]["devolucoes"] += 1

                # Detecta linha Totais
                if "Totais" in linha and vendedor_atual:
                    match = re.search(r"Totais:\s*([\d\.\,]+)", linha)
                    if match:
                        resultados[vendedor_atual]["total_vendas"] = match.group(1)

            fechar_vendedor()
            # Atualiza barra de progresso
            progress = int((i / total_paginas) * 100)
            progress_var.set(progress)
            progress_bar.update_idletasks()
            root.update_idletasks()
    return resultados

def ordenar_coluna(tree, col, reverse):
    # Pega os valores da coluna
    dados = [(tree.set(k, col), k) for k in tree.get_children('')]

    # Converte números para float/int, caso possível
    def try_num(v):
        try:
            return float(v.replace(".", "").replace(",", "."))
        except:
            return v.lower()
    dados.sort(key=lambda t: try_num(t[0]), reverse=reverse)

    # Reorganiza na Treeview
    for index, (val, k) in enumerate(dados):
        tree.move(k, '', index)

    # Inverte a ordem para o próximo clique
    tree.heading(col, command=lambda: ordenar_coluna(tree, col, not reverse))

def check_for_updates():  # Função de atualização com confirmação
    try:
        response = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest")
        response.raise_for_status()
        latest_version = response.json()["tag_name"].lstrip("v")  # Ex: "1.0.1"
        if latest_version > APP_VERSION:
            # Confirmação antes de atualizar
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
