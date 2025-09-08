<<<<<<< HEAD
from utils import _poll_queue, escolher_pdf_async, adicionar_pdf, ordenar_coluna, cancelar_processamento, carregar_planilha, exportar_para_excel, limpar_tabelas, check_for_updates
=======
from utils import *
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
from library import *
from globalVar import *

# GUI
root = tk.Tk()
root.title("Relatório de Clientes por Vendedor")

<<<<<<< HEAD
window_width = 1050
window_height = 800
=======
window_width = 750
window_height = 500
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()
x_coordinate = (screen_width - window_width) // 2
y_coordinate = (screen_height - window_height) // 2
root.geometry(f"{window_width}x{window_height}+{x_coordinate}+{y_coordinate}")

# Fundo cinza
bg_color = "#4E4E4E"
fg_color = "#FA7F2D"
root.configure(bg=bg_color)

<<<<<<< HEAD
arquivos_label_var = tk.StringVar(value="Nenhum arquivo carregado ainda")
progress_var = tk.IntVar(value=0)

=======
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
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

<<<<<<< HEAD
# Barra de progresso com botão Cancelar
=======
# Botão Selecionar PDF
btn = tk.Button(root, text="Selecionar PDF", 
                command=lambda: escolher_pdf(tree, progress_var, progress_bar, root),
                bg=fg_color, fg="black", font=("Segoe UI", 11, "bold"))
btn.pack(pady=10)

# Botão Adicionar mais um PDF
btn_add_mais = tk.Button(root, text="Adicionar mais um PDF", 
                         command=lambda: adicionar_pdf(tree, progress_var, progress_bar, root),
                         bg=fg_color, fg="black", font=("Segoe UI", 11, "bold"))
btn_add_mais.pack(pady=5)

# Barra de progresso
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
style.configure("custom.Horizontal.TProgressbar",
                troughcolor=bg_color,
                background=fg_color,
                thickness=20)

<<<<<<< HEAD
frame_progress = tk.Frame(root, bg=bg_color)
frame_progress.pack(pady=5)

progress_bar = ttk.Progressbar(root, variable=progress_var,
                               maximum=100, length=400,
                               style="custom.Horizontal.TProgressbar")
progress_bar.pack(padx=5)

# Botão cancelar
cancelado = tk.BooleanVar(value=False)

def cancelar_extracao():
    cancelado.set(True)
    print("❌ Extração cancelada pelo usuário!")

btn_cancelar = tk.Button(root, text="Cancelar",
                         command=cancelar_processamento,
                         bg="red", fg="white", font=("Segoe UI", 11, "bold"))
btn_cancelar.pack(pady=5)


# Frame que vai segurar a tabela principal
frame_tabela = tk.Frame(root, bg=bg_color)
frame_tabela.pack(fill="both", expand=True, padx=10, pady=10)

label_arquivos = tk.Label(frame_tabela, textvariable=arquivos_label_var,
                          bg=bg_color, fg="white", font=("Segoe UI", 10, "italic"))
label_arquivos.pack(anchor="w", padx=5, pady=(0,5))

cols = ("Vendedor", "Atendidos", "Devoluções", "Total Final", "Total Vendas")
tree = ttk.Treeview(frame_tabela, columns=cols, show="headings", height=10)
=======
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
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b

for col in cols:
    tree.heading(col, text=col, command=lambda _col=col: ordenar_coluna(tree, _col, False))
    tree.column(col, anchor="center", width=150, minwidth=50)

<<<<<<< HEAD
tree.pack(fill="both", expand=True)

# Frame para segunda tabela (planilha online)
frame_planilha = tk.Frame(root, bg=bg_color)
frame_planilha.pack(fill="both", expand=True, padx=10, pady=20)


label_planilha = tk.Label(frame_planilha, text="Planilha Online (MVA + EH)",
                          bg=bg_color, fg="white", font=("Segoe UI", 10, "italic"))
label_planilha.pack(anchor="center", padx=3, pady=(0,5))

cols_planilha = ("Vendedor", "Clientes Atendidos", "Valor Total") # ajusta depois conforme os headers da planilha

tree_planilha = ttk.Treeview(frame_planilha, columns=cols_planilha, show="headings", height=5)

for col in cols_planilha:
    tree_planilha.heading(col, text=col, command=lambda _col=col: ordenar_coluna(tree_planilha, _col, False))
    tree_planilha.column(col, anchor="center", width=150, minwidth=100)

tree_planilha.pack(fill="y", expand=False) # largura total fixa (em px)

_poll_queue(root, tree, progress_var, progress_bar)

# ------------------ JANELA PARA BOTÕES ------------------

janela_botoes = tk.Toplevel(root)
janela_botoes.title("Opções")
janela_botoes.configure(bg=bg_color)
janela_botoes.geometry(f"250x300+{x_coordinate + window_width + 10}+{y_coordinate}")

btn = tk.Button(janela_botoes, text="Selecionar PDF", 
                        command=lambda: escolher_pdf_async(tree, progress_var, progress_bar, root, arquivos_label_var),
                        bg=fg_color, fg="black", font=("Segoe UI", 11, "bold"))
btn.pack(pady=10)

# -----------------------

btn_add_mais = tk.Button(
    janela_botoes,
    text="Adicionar mais um PDF", 
    command=lambda: adicionar_pdf(tree, progress_var,  progress_bar, root, arquivos_label_var),
    bg=fg_color, fg="black", font=("Segoe UI", 11, "bold")
)

btn_add_mais.pack(pady=5)

# ----------------------

btn_planilha = tk.Button(
    janela_botoes,
    text="Extrair dados da planilha online", 
    command=lambda: carregar_planilha(tree_planilha),
    bg=fg_color, fg="black", font=("Segoe UI", 11, "bold")
)
btn_planilha.pack(pady=5)

# ----------------------

btn_exportar = tk.Button(
    janela_botoes,
    text="Exportar relatório em Excel",
    command=lambda: exportar_para_excel(tree),
    bg=fg_color, fg="black", font=("Segoe UI", 11, "bold")
)
btn_exportar.pack(pady=5)

# ----------------------

btn_limpar = tk.Button(
    janela_botoes,
    text="Limpar Tabelas",
    command=lambda: limpar_tabelas(tree, tree_planilha, arquivos_label_var, progress_var),
    bg=fg_color, fg="black", font=("Segoe UI", 11, "bold")
)
btn_limpar.pack(pady=5)

check_for_updates()
root.mainloop()
=======
tree.pack(fill="both", expand=True)
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
