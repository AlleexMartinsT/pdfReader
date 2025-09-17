from utils import (
    source_pdf_async, adicionar_pdf, ordenar_coluna,
    process_cancel, carregar_planilha_async,
    limpar_tabelas, check_for_updates, resource_path, mesclar_tabelas,
    _pdf_export, _excel_export, criar_etiquetas
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

label_files_var = customtkinter.StringVar(value="Nenhum arquivo carregado ainda")
progress_var = customtkinter.IntVar(value=0)

customtkinter.set_default_color_theme(resource_path("basedTheme.json"))
customtkinter.set_appearance_mode("dark")  # força dark mode no windows

if hasattr(sys, "_MEIPASS"):
    # Quando empacotado, o ícone está na pasta temporária do PyInstaller
    icon_path = os.path.join(sys._MEIPASS, "icone.ico")
else:
    icon_path = "icone.ico"  # path durante o desenvolvimento
    
root.iconbitmap(icon_path)

# ----------------- Barra de progresso com botão Cancelar -----------------
frame_progress = customtkinter.CTkFrame(root)
frame_progress.pack(pady=10)

progress_bar = ttk.Progressbar(
    frame_progress, variable=progress_var, maximum=100, length=400, style="custom.Horizontal.TProgressbar"
    ) # progress_bar = customtkinter.CTkProgressBar(frame_progress, width=400, height=17) Um dia eu mexo nisso, muita dor de cabeça.
progress_var.set(0)
progress_bar.grid(padx=10, pady=5)

btn_cancel = customtkinter.CTkButton(
    root,
    text="Cancelar",
    command=process_cancel,
    fg_color="red",
    hover_color="#e66161",
    font=("Segoe UI", 15, "bold"),
    text_color="#FFFFFF",
    text_color_disabled="#1a1818",

    state="disabled"
)
btn_cancel.pack(pady=10)

# ----------------- Frame principal com tabela -----------------
frame_table = customtkinter.CTkFrame(root)
frame_table.pack(fill="both", expand=True, padx=10, pady=10)

files_label = customtkinter.CTkLabel(
    frame_table,
    textvariable=label_files_var,
    font=("Segoe UI", 13, "italic"),
    text_color="white"
)
files_label.pack(anchor="w", padx=5, pady=(0, 5))

cols = ("Vendedor", "Atendidos", "Devoluções", "Total Final", "Total Vendas")
tree = ttk.Treeview(frame_table, columns=cols, show="headings", height=10,)

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

frame_table = customtkinter.CTkFrame(root)
frame_table.pack(fill="both", expand=True, padx=10, pady=5)

spreadsheet_label = customtkinter.CTkLabel(
    frame_table,
    text="Planilha Online (MVA + EH)",
    font=("Segoe UI", 13, "italic"),
    text_color="white"
)
spreadsheet_label.pack(anchor="center", padx=3, pady=(0, 5))

cols_spreadsheet = ("Vendedor", "Clientes Atendidos", "Valor Total")
tree_spreadsheet = ttk.Treeview(frame_table, columns=cols_spreadsheet, show="headings", height=5)

for col in cols_spreadsheet:
    tree_spreadsheet.heading(col, text=col, command=lambda _col=col: ordenar_coluna(tree_spreadsheet, _col, False))
    tree_spreadsheet.column(col, anchor="center", width=150, minwidth=100)

tree_spreadsheet.pack(fill="y", expand=False)

# --- Função para source arquivo e origem antes do processamento ---

def pdf_select_flow():
    # 1) Escolhe arquivo
    path = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
    if not path:
        return

    # 2) Janela modal para source origem (MVA ou EH) ANTES de iniciar o processamento
    choice = {"origem": None}
    source_window = customtkinter.CTkToplevel(root)
    source_window.title("Origem do PDF")
    source_window.transient(root)
    source_window.grab_set()  # modal
    
    # centraliza a janela modal
    width, height = 300, 140
    sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
    x = (sw - width) // 2
    y = (sh - height) // 2
    source_window.geometry(f"{width}x{height}+{x}+{y}")

    customtkinter.CTkLabel(source_window, text="Este PDF pertence a qual empresa?").pack(pady=(12,6))

    def source(e):
        choice["origem"] = e
        source_window.destroy()

    btn_mva_modal = customtkinter.CTkButton(source_window, text="MVA", command=lambda: source("MVA"))
    btn_mva_modal.pack(pady=6, padx=20, fill="x")
    btn_eh_modal = customtkinter.CTkButton(source_window, text="EH", command=lambda: source("EH"), text_color="#F2F2F2", fg_color='#1145AB', hover_color='#4A6FBA')
    btn_eh_modal.pack(pady=6, padx=20, fill="x")

    source_window.wait_window()  # espera o usuário source

    origem = choice["origem"]
    if not origem:
        return

    source_pdf_async(tree, progress_var, progress_bar, root, label_files_var, btn_cancel, path, origem)

# ----------------- Janela lateral de botões -----------------
btn_window = customtkinter.CTkToplevel(root)
btn_window.title("Opções")
btn_window.geometry(f"250x300+{x_coordinate + window_width + 10}+{y_coordinate}")
btn_window.iconbitmap(icon_path)

btn = customtkinter.CTkButton(
    btn_window,
    text="Selecionar PDF",
    command=pdf_select_flow,  # <--- aqui
    text_color_disabled="#D92525"
)

btn.pack(pady=5)

btn_add_mais = customtkinter.CTkButton(
    btn_window,
    text="Adicionar mais um PDF",
    command=lambda: adicionar_pdf(tree, progress_var, progress_bar, root, label_files_var), text_color_disabled="#D92525"   
)
btn_add_mais.pack(pady=5)

btn_spreadsheet = customtkinter.CTkButton(
    btn_window,
    text="Extrair dados da planilha online",
    command=lambda: carregar_planilha_async(tree_spreadsheet, progress_var, progress_bar, root),
)
btn_spreadsheet.pack(pady=5)

btn_export = customtkinter.CTkButton(
    btn_window,
    text="Exportar",
    command=lambda: data_export(tree),
)
btn_export.pack(pady=5)

btn_clear = customtkinter.CTkButton(
    btn_window,
    text="Limpar Tabelas",
    command=lambda: limpar_tabelas(tree, tree_spreadsheet, label_files_var, progress_var),
)
btn_clear.pack(pady=5)

btn_merge_spreadsheet = customtkinter.CTkButton(
    btn_window,
    text="Mesclar planilhas",
    command=lambda: mesclar_tabelas(tree, progress_var, progress_bar, root, label_files_var, tree_spreadsheet), text_color_disabled="#D92525"
)
btn_merge_spreadsheet.pack(pady=5)

btn_tag = customtkinter.CTkButton(
    btn_window,
    text="Criar Etiquetas",
    command=lambda: criar_etiquetas(),
    state="disabled",
    hover_color="#97d8a6",
    fg_color="#44cc64",
)

btn_tag.pack(pady=5)

if str(btn_tag.cget("state")) == "disabled":
    btn_tag.configure(fg_color="#EE9919")
    
# ----------------------- Janela de Exportação ---------------------------
def data_export(tree):
    window_export = customtkinter.CTkToplevel(root)
    window_export.title("Exportar")

    # Dimensões desejadas da janela
    export_width = 250
    export_height = 150

    # Dimensões da tela
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()

    # Coordenadas para centralizar
    x_coordinate = (screen_width - export_width) // 2
    y_coordinate = (screen_height - export_height) // 2

    window_export.geometry(f"{export_width}x{export_height}+{x_coordinate}+{y_coordinate}")

    def excel_export():
            try:
                _excel_export(tree)
                window_export.destroy()
            except Exception as e:
                messagebox.showerror("Erro", f"Erro ao exportar Excel: {e}")

    def pdf_export():
            try:
                _pdf_export(tree)
                window_export.destroy()
            except Exception as e:
                messagebox.showerror("Erro", f"Erro ao exportar PDF: {e}")
                
    customtkinter.CTkLabel(window_export, text="Escolha o formato para exportar:").pack(pady=10)
    customtkinter.CTkButton(window_export, text="Excel", command=excel_export).pack(pady=5)
    customtkinter.CTkButton(window_export, text="PDF", command=pdf_export).pack(pady=5)

# ----------------- Checagem de updates + loop principal -----------------
check_for_updates(root)
root.mainloop()
