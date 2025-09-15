from utils import (
    escolher_pdf_async, adicionar_pdf, ordenar_coluna,
    cancelar_processamento, carregar_planilha_async,
    limpar_tabelas, check_for_updates, resource_path, mesclar_tabelas,
    _exportar_pdf, _exportar_excel
)
import customtkinter
from tkinter import filedialog, ttk, messagebox
import sys, os

# GUI
root = customtkinter.CTk()
root.title("Relatório de Vendedor")

window_width = 1050
window_height = 800
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()
x_coordinate = (screen_width - window_width) // 2
y_coordinate = (screen_height - window_height) // 2
root.geometry(f"{window_width}x{window_height}+{x_coordinate}+{y_coordinate}")

arquivos_label_var = customtkinter.StringVar(value="Nenhum arquivo carregado ainda")
progress_var = customtkinter.IntVar(value=0)

customtkinter.set_default_color_theme(resource_path("basedTheme.json"))
customtkinter.set_appearance_mode("dark")  # força dark mode no windows

if hasattr(sys, "_MEIPASS"):
    # Quando empacotado, o ícone está na pasta temporária do PyInstaller
    icon_path = os.path.join(sys._MEIPASS, "icone.ico")
else:
    icon_path = "icone.ico"  # Caminho durante o desenvolvimento
    
root.iconbitmap(icon_path)

# ----------------- Barra de progresso com botão Cancelar -----------------
frame_progress = customtkinter.CTkFrame(root)
frame_progress.pack(pady=10)

progress_bar = ttk.Progressbar(
    frame_progress, variable=progress_var, maximum=100, length=400, style="custom.Horizontal.TProgressbar"
    ) # progress_bar = customtkinter.CTkProgressBar(frame_progress, width=400, height=17) Um dia eu mexo nisso, muita dor de cabeça.
progress_var.set(0)
progress_bar.grid(padx=10, pady=5)

btn_cancelar = customtkinter.CTkButton(
    root,
    text="Cancelar",
    command=cancelar_processamento,
    fg_color="red",
    hover_color="#e66161",
    font=("Segoe UI", 15, "bold"),
    text_color="#FFFFFF",
    text_color_disabled="#1a1818",

    state="disabled"
)
btn_cancelar.pack(pady=10)

# ----------------- Frame principal com tabela -----------------
frame_tabela = customtkinter.CTkFrame(root)
frame_tabela.pack(fill="both", expand=True, padx=10, pady=10)

label_arquivos = customtkinter.CTkLabel(
    frame_tabela,
    textvariable=arquivos_label_var,
    font=("Segoe UI", 13, "italic"),
    text_color="white"
)
label_arquivos.pack(anchor="w", padx=5, pady=(0, 5))

cols = ("Vendedor", "Atendidos", "Devoluções", "Total Final", "Total Vendas")
tree = ttk.Treeview(frame_tabela, columns=cols, show="headings", height=10,)

for col in cols:
    tree.heading(col, text=col, command=lambda _col=col: ordenar_coluna(tree, _col, False))
    tree.column(col, anchor="center", width=150, minwidth=50)

tree.pack(fill="both", expand=True)

style = ttk.Style()
style.theme_use("default")

# Corpo da tabela
style.configure(
    "Treeview",
    background="#1e1e1e",   # fundo (dark)
    foreground="white",     # cor do texto
    rowheight=28,
    fieldbackground="#1e1e1e",  # fundo atrás das linhas
    font=("Segoe UI", 11)   # fonte e tamanho
)

# Cabeçalho da tabela
style.configure(
    "Treeview.Heading",
    background="#2d2d2d",   # fundo do cabeçalho
    foreground="orange",    # cor do texto do cabeçalho
    font=("Segoe UI", 12, "bold")
)

# Cor quando seleciona uma linha
style.map(
    "Treeview",
    background=[("selected", "#ff6600")],
    foreground=[("selected", "black")]
)

style.configure(
    "custom.Horizontal.TProgressbar",
    troughcolor="#2d2d2d",   # cor do fundo (trilha)
    background="#59C734",    # cor da barra preenchida
    thickness=20
)

# ----------------- Frame da planilha online -----------------

frame_planilha = customtkinter.CTkFrame(root)
frame_planilha.pack(fill="both", expand=True, padx=10, pady=5)

label_planilha = customtkinter.CTkLabel(
    frame_planilha,
    text="Planilha Online (MVA + EH)",
    font=("Segoe UI", 13, "italic"),
    text_color="white"
)
label_planilha.pack(anchor="center", padx=3, pady=(0, 5))

cols_planilha = ("Vendedor", "Clientes Atendidos", "Valor Total")
tree_planilha = ttk.Treeview(frame_planilha, columns=cols_planilha, show="headings", height=5)

for col in cols_planilha:
    tree_planilha.heading(col, text=col, command=lambda _col=col: ordenar_coluna(tree_planilha, _col, False))
    tree_planilha.column(col, anchor="center", width=150, minwidth=100)

tree_planilha.pack(fill="y", expand=False)

# --- Função para escolher arquivo e origem antes do processamento ---
def selecionar_pdf_flow():
    # 1) Escolhe arquivo
    caminho = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    if not caminho:
        return

    # 2) Janela modal para escolher origem (MVA ou EH) ANTES de iniciar o processamento
    escolha = {"origem": None}
    janela_origem = customtkinter.CTkToplevel(root)
    janela_origem.title("Origem do PDF")
    janela_origem.transient(root)
    janela_origem.grab_set()  # modal
    
    # centraliza a janela modal
    width, height = 300, 140
    sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
    x = (sw - width) // 2
    y = (sh - height) // 2
    janela_origem.geometry(f"{width}x{height}+{x}+{y}")

    customtkinter.CTkLabel(janela_origem, text="Este PDF pertence a qual empresa?").pack(pady=(12,6))

    def escolher(e):
        escolha["origem"] = e
        janela_origem.destroy()

    btn_mva_modal = customtkinter.CTkButton(janela_origem, text="MVA", command=lambda: escolher("MVA"))
    btn_mva_modal.pack(pady=6, padx=20, fill="x")
    btn_eh_modal = customtkinter.CTkButton(janela_origem, text="EH", command=lambda: escolher("EH"), text_color="#F2F2F2", fg_color='#1145AB', hover_color='#4A6FBA')
    btn_eh_modal.pack(pady=6, padx=20, fill="x")

    janela_origem.wait_window()  # espera o usuário escolher

    origem = escolha["origem"]
    if not origem:
        return

    escolher_pdf_async(tree, progress_var, progress_bar, root, arquivos_label_var, btn_cancelar, caminho, origem)

# ----------------- Janela lateral de botões -----------------
janela_botoes = customtkinter.CTkToplevel(root)
janela_botoes.title("Opções")
janela_botoes.geometry(f"250x300+{x_coordinate + window_width + 10}+{y_coordinate}")
janela_botoes.iconbitmap(icon_path)

btn = customtkinter.CTkButton(
    janela_botoes,
    text="Selecionar PDF",
    command=selecionar_pdf_flow,  # <--- aqui
    text_color_disabled="#D92525"
)

btn.pack(pady=5)

btn_add_mais = customtkinter.CTkButton(
    janela_botoes,
    text="Adicionar mais um PDF",
    command=lambda: adicionar_pdf(tree, progress_var, progress_bar, root, arquivos_label_var), text_color_disabled="#D92525"   
)
btn_add_mais.pack(pady=5)

btn_planilha = customtkinter.CTkButton(
    janela_botoes,
    text="Extrair dados da planilha online",
    command=lambda: carregar_planilha_async(tree_planilha, progress_var, progress_bar, root),
)
btn_planilha.pack(pady=5)

btn_exportar = customtkinter.CTkButton(
    janela_botoes,
    text="Exportar",
    command=lambda: exportar_dados(tree),
)
btn_exportar.pack(pady=5)

btn_limpar = customtkinter.CTkButton(
    janela_botoes,
    text="Limpar Tabelas",
    command=lambda: limpar_tabelas(tree, tree_planilha, arquivos_label_var, progress_var),
)
btn_limpar.pack(pady=5)

btn_mesclar_planilhas = customtkinter.CTkButton(
    janela_botoes,
    text="Mesclar planilhas",
    command=lambda: mesclar_tabelas(tree, progress_var, progress_bar, root, arquivos_label_var, tree_planilha), text_color_disabled="#D92525"
)
btn_mesclar_planilhas.pack(pady=5)

# ----------------------- Janela de Exportação ---------------------------
def exportar_dados(tree):
    janela_export = customtkinter.CTkToplevel(root)
    janela_export.title("Exportar")

    # Dimensões desejadas da janela
    export_width = 250
    export_height = 150

    # Dimensões da tela
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    # Coordenadas para centralizar
    x_coordinate = (screen_width - export_width) // 2
    y_coordinate = (screen_height - export_height) // 2

    janela_export.geometry(f"{export_width}x{export_height}+{x_coordinate}+{y_coordinate}")

    def exportar_excel():
            try:
                _exportar_excel(tree)
                janela_export.destroy()
            except Exception as e:
                messagebox.showerror("Erro", f"Erro ao exportar Excel: {e}")

    def exportar_pdf():
            try:
                _exportar_pdf(tree)
                janela_export.destroy()
            except Exception as e:
                messagebox.showerror("Erro", f"Erro ao exportar PDF: {e}")
                
    customtkinter.CTkLabel(janela_export, text="Escolha o formato para exportar:").pack(pady=10)
    customtkinter.CTkButton(janela_export, text="Excel", command=exportar_excel).pack(pady=5)
    customtkinter.CTkButton(janela_export, text="PDF", command=exportar_pdf).pack(pady=5)

# ----------------- Checagem de updates + loop principal -----------------
check_for_updates(root)
root.mainloop()
