from utils import *
from library import *
from globalVar import *

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
    tree.heading(col, text=col, command=lambda _col=col: ordenar_coluna(tree, _col, False))
    tree.column(col, anchor="center", width=150, minwidth=50)

tree.pack(fill="both", expand=True)