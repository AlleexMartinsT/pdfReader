from utils import (
    source_pdf_async, adicionar_pdf, ordenar_coluna,
    process_cancel, carregar_planilhas_duplas_async,
    limpar_tabelas_duplas, check_for_updates, resource_path, mesclar_tabelas_duplas,
    _pdf_export, _excel_export, criar_etiquetas, salvar_feedback_db,
    carregar_feedbacks_db, excluir_ultimo_feedback, atualizar_ultimo_feedback,
)
import customtkinter
from PIL import Image, ImageTk
from tkinter import filedialog, ttk, messagebox
import sys, os

# GUI
root = customtkinter.CTk()
root.title("Relatório de Vendedor")
root.resizable(False, False)

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
pdf_img = Image.open(resource_path("pdf_icon.png")).resize((26, 26))
pdf_icon = ImageTk.PhotoImage(pdf_img)


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

# ----------------- Função Feedback -----------------

def abrir_feedback():
    # Cria janela lateral à esquerda
    feedback_window = customtkinter.CTkToplevel(root)
    feedback_window.title("Feedback dos Vendedores")
    feedback_window.geometry(f"250x450+{x_coordinate - 260}+{y_coordinate}")  # lado esquerdo
    feedback_window.resizable(False, False)
    feedback_window.iconbitmap(icon_path)

    customtkinter.CTkLabel(
        feedback_window,
        text="Selecione o Vendedor",
        font=("Segoe UI", 13, "bold")
    ).pack(pady=10)

    # --- Área rolável com botões dos vendedores ---
    try:
        container = customtkinter.CTkScrollableFrame(feedback_window, width=230, height=260)
        container.pack(padx=10, pady=5, fill="both", expand=True)
    except Exception:
        container = customtkinter.CTkFrame(feedback_window)
        container.pack(padx=10, pady=5, fill="both", expand=True)

    # frame inferior
    btn_bottom = customtkinter.CTkFrame(feedback_window)
    btn_bottom.pack(pady=10, fill="x")

    # Botão fechar (fixo, sempre presente)
    btn_fechar = customtkinter.CTkButton(
        btn_bottom,
        text="Fechar",
        command=feedback_window.destroy
    )
    btn_fechar.pack(pady=(6,8))
    
    # guarda último snapshot dos vendedores
    state = {"last_vendedores": []}
    btn_exportar = {"widget": None}

    def abrir_observacoes(vendedor):
        obs_window = customtkinter.CTkToplevel(root)
        obs_window.title(f"Observações - {vendedor}")
        obs_window.resizable(False, False)
        obs_window.iconbitmap(icon_path)
        
        obs_window.transient(feedback_window)
        obs_window.grab_set()
        # Centraliza janela Observações
        largura, altura = 650, 420
        sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
        x = (sw - largura) // 2
        y = (sh - altura) // 2
        obs_window.geometry(f"{largura}x{altura}+{x}+{y}")

        customtkinter.CTkLabel(obs_window, text=f"Feedback para {vendedor}", font=("Segoe UI", 14, "bold")).pack(pady=10)

        # Caixa para mostrar histórico
        historico_box = customtkinter.CTkTextbox(obs_window, width=480, height=120, state="disabled", text_color="white")
        historico_box.pack(padx=10, pady=5, fill="both", expand=False)

        def atualizar_historico():
            historico = carregar_feedbacks_db(vendedor)
            historico_box.configure(state="normal")
            historico_box.delete("1.0", "end")
            if historico:
                for fb in historico:
                    ts = fb.get("created_at") or fb.get("created_at_ts", "")
                    historico_box.insert("end", f"[{str(ts)[:19]}]\n{fb.get('feedback','')}\n\n")
            else:
                historico_box.insert("end", "Nenhum feedback registrado ainda.\n")
            historico_box.configure(state="disabled")

        atualizar_historico()

        # Caixa para novo feedback
        customtkinter.CTkLabel(obs_window, text="Novo Feedback:", font=("Segoe UI", 12, "bold")).pack(pady=(10, 0))
        textbox = customtkinter.CTkTextbox(obs_window, width=480, height=80, text_color="white")
        textbox.pack(padx=10, pady=5, fill="both", expand=False)

        def salvar_feedback():
            texto = textbox.get("1.0", "end").strip()
            if not texto:
                messagebox.showwarning("Aviso", "Digite algum feedback antes de salvar.")
                return
            if salvar_feedback_db(vendedor, texto):
                messagebox.showinfo("Salvo", f"Feedback de {vendedor} registrado com sucesso!")
                textbox.delete("1.0", "end")
                atualizar_historico()
            else:
                messagebox.showerror("Erro", "Falha ao salvar feedback.")

        def exportar_pdf():
            feedbacks = carregar_feedbacks_db(vendedor)
            if not feedbacks:
                messagebox.showwarning("Aviso", "Nenhum feedback para exportar.")
                return
            exportar_feedbacks_pdf(vendedor, feedbacks)

        # Botões
        btn_frame = customtkinter.CTkFrame(obs_window)
        btn_frame.pack(pady=10, fill="x")
        
        def editar_feedback():
            historico = carregar_feedbacks_db(vendedor)
            if not historico:
                messagebox.showwarning("Aviso", "Nenhum feedback encontrado para exportar.")
                return
            
            choice_window = customtkinter.CTkToplevel(obs_window)
            choice_window.title("Editor")
            choice_window.transient(obs_window)
            choice_window.grab_set()

            largura, altura = 300, 200
            sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
            x = (sw - largura) // 2
            y = (sh - altura) // 2
            
            choice_window.geometry(f"{largura}x{altura}+{x}+{y}")
            choice_window.resizable(False, False)
            
            customtkinter.CTkLabel(choice_window, text="O que deseja fazer?").pack(pady=10)
        
            def acao_editar():
                choice_window.destroy()
                historico = carregar_feedbacks_db(vendedor)
                if historico:
                    ultimo = historico[-1]["feedback"]
                    textbox.delete("1.0", "end")
                    textbox.insert("1.0", ultimo)

                    def confirmar_edicao():
                        novo = textbox.get("1.0", "end").strip()
                        if atualizar_ultimo_feedback(vendedor, novo):
                            messagebox.showinfo("Sucesso", "Feedback atualizado com sucesso!")
                            textbox.delete("1.0", "end")
                            atualizar_historico()
                            obs_window.lift()      
                            obs_window.focus_force()
                            btn_confirmar_edicao.destroy()
                            
                    btn_confirmar_edicao = customtkinter.CTkButton(
                    obs_window,
                    text="Confirmar Edição",
                    command=confirmar_edicao,
                    fg_color="#4CAF50"
                )
                btn_confirmar_edicao.pack(pady=5)
            def acao_excluir():
                choice_window.destroy()
                if excluir_ultimo_feedback(vendedor):
                    messagebox.showinfo("Sucesso", "Último feedback excluído.")
                    atualizar_historico()
        
            customtkinter.CTkButton(choice_window, text="Editar último", command=acao_editar).pack(pady=5)
            customtkinter.CTkButton(choice_window, text="Excluir último", command=acao_excluir).pack(pady=5)
            customtkinter.CTkButton(choice_window, text="Cancelar", command=choice_window.destroy).pack(pady=5)
            
        customtkinter.CTkButton(btn_frame, text="Salvar", command=salvar_feedback).pack(side="left", padx=10)
        customtkinter.CTkButton(btn_frame, text="Exportar PDF", command=exportar_pdf, fg_color="#2196F3", hover_color="#1976D2").pack(side="left", padx=10)
        customtkinter.CTkButton(btn_frame, text="Fechar", command=obs_window.destroy).pack(side="left", padx=10)
        customtkinter.CTkButton(btn_frame, text="Editar", command=editar_feedback).pack(side="left", padx=10)

    def atualizar_lista_vendedores():
        from utils import listar_vendedores_db

        vendedores_tree = [tree.item(item)["values"][0] for item in tree.get_children()]
        vendedores_db = listar_vendedores_db()
        vendedores = sorted(set(vendedores_tree + vendedores_db))

        # só atualiza se mudou
        if vendedores != state["last_vendedores"]:
            state["last_vendedores"] = vendedores

            # limpa botões antigos
            for widget in container.winfo_children():
                widget.destroy()

            if not vendedores:
                customtkinter.CTkLabel(container, text="Nenhum vendedor carregado.").pack(pady=20)
            else:
                for vendedor in vendedores:
                    b = customtkinter.CTkButton(
                        container,
                        text=vendedor,
                        command=lambda v=vendedor: abrir_observacoes(v),
                        anchor="center",
                        corner_radius=6
                    )
                    b.pack(fill="x", pady=4, padx=6)

            # gerencia botão exportar
            if btn_exportar["widget"]:
                btn_exportar["widget"].destroy()
                btn_exportar["widget"] = None
           
            if vendedores:  
                def exportar_pdf_todos():
                    feedbacks_all = []
                    for v in vendedores:
                        feedbacks_all.extend(carregar_feedbacks_db(v))
                    if not feedbacks_all:
                        messagebox.showwarning("Aviso", "Nenhum feedback encontrado para exportar.")
                        return
                    exportar_feedbacks_pdf("Todos os Vendedores", feedbacks_all)

                btn_exportar["widget"] = customtkinter.CTkButton(
                    btn_bottom,
                    text="Exportar PDF",
                    command=exportar_pdf_todos,
                    fg_color="#2196F3",
                    hover_color="#1976D2"
                )
                btn_exportar["widget"].pack(pady=(6,8), before=btn_fechar)                
        
        # agenda próxima checagem
        feedback_window.after(2000, atualizar_lista_vendedores)

    # inicia atualização periódica
    atualizar_lista_vendedores()

# ---------------- Função Exportar ------------------

def exportar_planilha_pdf(tree, titulo):
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from tkinter import filedialog, messagebox

    rows = [tree.item(i)["values"] for i in tree.get_children()]
    cols = [tree.heading(col)["text"] for col in tree["columns"]]

    if not rows:
        messagebox.showwarning("Aviso", "Não há dados para exportar.")
        return

    caminho = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("PDF", "*.pdf")],
        title=f"Exportar {titulo}"
    )
    if not caminho:
        return

    c = canvas.Canvas(caminho, pagesize=A4)
    largura, altura = A4
    y = altura - 40

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, titulo)
    y -= 30

    c.setFont("Helvetica-Bold", 10)
    for i, col in enumerate(cols):
        c.drawString(50 + i*150, y, col)
    y -= 20

    c.setFont("Helvetica", 9)
    for row in rows:
        for i, val in enumerate(row):
            c.drawString(50 + i*150, y, str(val))
        y -= 18
        if y < 40:
            c.showPage()
            y = altura - 40
            c.setFont("Helvetica", 9)

    c.save()
    messagebox.showinfo("Sucesso", f"PDF salvo em:\n{caminho}")
# ----------------- Frame da planilha online (MVA e EH lado a lado) -----------------

frame_online = customtkinter.CTkFrame(root)
frame_online.pack(fill="both", expand=True, padx=10, pady=5)

# Frame esquerdo (MVA)
frame_mva = customtkinter.CTkFrame(frame_online)
frame_mva.pack(side="left", fill="both", expand=True, padx=(0, 5))

frame_mva_top = customtkinter.CTkFrame(frame_mva, fg_color="transparent")
frame_mva_top.pack(fill="x")

# --- Configura grid para centralizar ---
frame_mva_top.grid_columnconfigure(0, weight=1)   # espaço esquerdo
frame_mva_top.grid_columnconfigure(3, weight=1)   # espaço direito

spreadsheet_label_mva = customtkinter.CTkLabel(
    frame_mva_top,
    text="Planilha Online - MVA",
    font=("Segoe UI", 13, "italic"),
    text_color="white"
)
spreadsheet_label_mva.grid(row=0, column=1, padx=(0,5))

btn_export_mva = customtkinter.CTkButton(
    frame_mva_top,
    text="",
    width=30,
    height=30,
    image=pdf_icon,
    command=lambda: exportar_planilha_pdf(tree_spreadsheet_mva, "Planilha MVA"),
    fg_color="transparent",
    hover_color="#3b3b3b"
)

btn_export_mva.grid(row=0, column=2, padx=6)

cols_spreadsheet = ("Vendedor", "Clientes Atendidos", "Valor Total")
tree_spreadsheet_mva = ttk.Treeview(frame_mva, columns=cols_spreadsheet, show="headings", height=5)

for col in cols_spreadsheet:
    tree_spreadsheet_mva.heading(col, text=col, command=lambda _col=col: ordenar_coluna(tree_spreadsheet_mva, _col, False))
    tree_spreadsheet_mva.column(col, anchor="center", width=150, minwidth=100)

tree_spreadsheet_mva.pack(fill="both", expand=True)

# Frame direito (EH)
frame_eh = customtkinter.CTkFrame(frame_online)
frame_eh.pack(side="left", fill="both", expand=True, padx=(5, 0))

frame_eh_top = customtkinter.CTkFrame(frame_eh, fg_color="transparent")
frame_eh_top.pack(fill="x")

frame_eh_top.grid_columnconfigure(0, weight=1)   # espaço esquerdo
frame_eh_top.grid_columnconfigure(3, weight=1)   # espaço direito

spreadsheet_label_eh = customtkinter.CTkLabel(
    frame_eh_top,
    text="Planilha Online - EH",
    font=("Segoe UI", 13, "italic"),
    text_color="white"
)
spreadsheet_label_eh.grid(row=0, column=1, padx=(0,5))

btn_export_eh = customtkinter.CTkButton(
    frame_eh_top,
    text="",
    width=30,
    height=30,
    image=pdf_icon,
    command=lambda: exportar_planilha_pdf(tree_spreadsheet_eh, "Planilha EH"),
    fg_color="transparent",
    hover_color="#3b3b3b"
)
btn_export_eh.grid(row=0, column=2, padx=6)

tree_spreadsheet_eh = ttk.Treeview(frame_eh, columns=cols_spreadsheet, show="headings", height=5)

for col in cols_spreadsheet:
    tree_spreadsheet_eh.heading(col, text=col, command=lambda _col=col: ordenar_coluna(tree_spreadsheet_eh, _col, False))
    tree_spreadsheet_eh.column(col, anchor="center", width=150, minwidth=100)

tree_spreadsheet_eh.pack(fill="both", expand=True)

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
    btn.configure(state="disabled")

# ----------------- Janela lateral de botões -----------------

btn_window = customtkinter.CTkToplevel(root)
btn_window.title("Opções")
btn_window.geometry(f"250x320+{x_coordinate + window_width + 10}+{y_coordinate}")
btn_window.resizable(False, False)
btn_window.iconbitmap(icon_path)

def impedir_fechamento():
    pass  

btn_window.protocol("WM_DELETE_WINDOW", impedir_fechamento)

_syncing = False

def sync_on_minimize(event):
    global _syncing
    if _syncing:
        return

    _syncing = True
    try:
        # Se a principal foi minimizada
        if root.state() == "iconic":
            btn_window.iconify()

        # Se a lateral foi minimizada
        elif btn_window.state() == "iconic":
            root.iconify()

    finally:
        _syncing = False

root.bind("<Unmap>", sync_on_minimize)
btn_window.bind("<Unmap>", sync_on_minimize)

btn = customtkinter.CTkButton(
    btn_window,
    text="Selecionar PDF",
    command=pdf_select_flow,  
    text_color_disabled="#D92525"
)

btn.pack(pady=5)

btn_add_mais = customtkinter.CTkButton(
    btn_window,
    text="Adicionar mais um PDF", 
    font=("Segoe UI", 11, "bold"),
    command=lambda: adicionar_pdf(tree, progress_var, progress_bar, root, label_files_var), text_color_disabled="#D92525"   
)
btn_add_mais.pack(pady=5)

btn_spreadsheet = customtkinter.CTkButton(
    btn_window,
    text="Planilha online",
    command=lambda: carregar_planilhas_duplas_async(
        tree_spreadsheet_mva, tree_spreadsheet_eh, progress_var, progress_bar, root
    ),
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
    command=lambda: limpar_tabelas_duplas(
        tree, 
        tree_spreadsheet_mva, 
        tree_spreadsheet_eh, 
        label_files_var, 
        progress_var
    ),
)
btn_clear.pack(pady=5)

btn_merge_spreadsheet = customtkinter.CTkButton(
    btn_window,
    text="Mesclar Planilhas",
    command=lambda: mesclar_tabelas_duplas(
        tree,
        progress_var,
        progress_bar,
        root,
        label_files_var,
        tree_spreadsheet_mva,
        tree_spreadsheet_eh
    ),
    text_color_disabled="#D92525"
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
   
btn_feedback = customtkinter.CTkButton(
    btn_window,
    text="Feedback",
    command=abrir_feedback,
)
btn_feedback.pack(pady=5)
   
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

def exportar_feedbacks_pdf(vendedor: str, feedbacks: list):
    """Exporta feedbacks para PDF — suporta um ou vários vendedores."""
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from tkinter import filedialog, messagebox

    caminho = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("Arquivo PDF", "*.pdf")],
        title=f"Exportar feedbacks de {vendedor}"
    )
    if not caminho:
        return

    c = canvas.Canvas(caminho, pagesize=A4)
    largura, altura = A4
    y = altura - 50

    # título principal
    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, f"Feedbacks - {vendedor}")
    y -= 30
    c.setFont("Helvetica", 10)

    # agrupa feedbacks por vendedor, se vierem misturados
    feedbacks_por_vendedor = {}
    for fb in feedbacks:
        nome = fb.get("vendedor", "Desconhecido")
        feedbacks_por_vendedor.setdefault(nome, []).append(fb)

    for vendedor_nome, lista in feedbacks_por_vendedor.items():
        # rótulo do vendedor (se houver vários)
        if len(feedbacks_por_vendedor) > 1:
            c.setFont("Helvetica-Bold", 12)
            c.drawString(50, y, f"Vendedor: {vendedor_nome}")
            y -= 20
            c.setFont("Helvetica", 10)

        for fb in lista:
            linha = f"{fb['created_at'][:19]} - {fb['feedback']}"
            for parte in linha.split("\n"):
                c.drawString(50, y, parte)
                y -= 15
                if y < 50:
                    c.showPage()
                    c.setFont("Helvetica", 10)
                    y = altura - 50

        y -= 10  # espaço entre vendedores

    c.save()
    messagebox.showinfo("Exportado", f"✅ Feedbacks exportados para:\n{caminho}")

# ----------------- Checagem de updates + loop principal -----------------
check_for_updates(root)
root.mainloop()
