import pdfplumber
import logging
import re
import tkinter as tk
from tkinter import filedialog, ttk, messagebox
from PIL import Image, ImageTk, ImageEnhance
import requests
import tkinter.messagebox as messagebox


# Desativa logs chatos
logging.getLogger("pdfplumber").setLevel(logging.ERROR)
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("PyPDF2").setLevel(logging.ERROR)

def check_for_updates(): # Função de atualização
    try:
        response = requests.get(f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest")
        response.raise_for_status()
        latest_version = response.json()["tag_name"].lstrip("v")  # Ex: "1.0.1"
        if latest_version > APP_VERSION:
            messagebox.showinfo("Atualização Disponível", f"Uma nova versão ({latest_version}) está disponível! Baixe em: https://github.com/{GITHUB_REPO}/releases/latest")
            # Ainda em teste, para atualizar automaticamente.
            # asset_url = response.json()["assets"][0]["browser_download_url"]
            # with open("novo_app.exe", "wb") as f:
            #     f.write(requests.get(asset_url).content)
            # messagebox.showinfo("Atualizado", "Nova versão baixada. Reinicie o app.")
        else:
            print("App atualizado.") 
    except Exception as e:
        print(f"Erro ao checar updates: {e}") 
# Regex
regex_data = re.compile(r"^\s*\d{2}/\d{2}/\d{4}")
regex_negativo = re.compile(r"[-−–]\s*\d")

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

def escolher_pdf():
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

# GUI
root = tk.Tk()
root.title("Relatório de Clientes por Vendedor")

window_width = 750  
window_height = 500  
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()
x_coordinate = (screen_width - window_width) // 2
y_coordinate = (screen_height - window_height) // 2
root.geometry(f"{window_width}x{window_height}+{x_coordinate}+{y_coordinate}")

# Upload do icone
try:
    root.iconbitmap("icone.ico")
except Exception as e:
    print("Não consegui carregar ícone:", e)

# Fundo cinza
bg_color = "#4E4E4E"   
fg_color = "#FA7F2D"  
root.configure(bg=bg_color)

style = ttk.Style(root)
style.theme_use("default")

# Estilo tabela
style.configure("Treeview",
                background=bg_color,
                foreground="white",
                rowheight=25,
                fieldbackground=bg_color,
                font=("Segoe UI", 10))
style.configure("Treeview.Heading",
                background=bg_color,
                foreground=fg_color,
                font=("Segoe UI", 11, "bold"))
style.map("Treeview", background=[("selected", "#FF6600")],
                        foreground=[("selected", "black")])

# Botão
btn = tk.Button(root, text="Selecionar PDF", command=escolher_pdf,
                bg=fg_color, fg="black", font=("Segoe UI", 11, "bold"))
btn.pack(pady=10)

# Barra de progresso
style.configure("custom.Horizontal.TProgressbar",
                troughcolor=bg_color,
                background=fg_color,
                thickness=20)

progress_var = tk.IntVar()
progress_bar = ttk.Progressbar(root, variable=progress_var,
                               maximum=100, length=400,
                               style="custom.Horizontal.TProgressbar")
progress_bar.pack(pady=5)

# Frame que vai segurar a tabela
frame_tabela = tk.Frame(root, bg=bg_color)
frame_tabela.pack(fill="both", expand=True, padx=10, pady=10)

cols = ("Vendedor", "Atendidos", "Devoluções", "Total Final", "Total Vendas")
tree = ttk.Treeview(frame_tabela, columns=cols, show="headings", height=15)
for col in cols:
    tree.heading(col, text=col)
    tree.column(col, anchor="center", width=150)
tree.pack(fill="both", expand=True)

root.mainloop()

APP_VERSION = "1.0.0"  # Atualiza isso a cada release
GITHUB_REPO = "AlleexMartinsT/pdfReader" 

check_for_updates()