import os
import json
from tkinter import messagebox
from utils import resource_path

_supabase = None

def get_supabase():
    global _supabase
    if _supabase is None:
        try:
            from supabase import create_client
            cred_path = resource_path(os.path.join("data", "credenciaisDB.json"))
            with open(cred_path, "r", encoding="utf-8") as f:
                creds = json.load(f)

            url = creds["SUPABASE_URL"]
            key = creds["SUPABASE_KEY"]
            _supabase = create_client(url, key)
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao conectar ao Supabase: {e}")
            raise
    return _supabase

def listar_vendedores_db():
    """Retorna a lista de vendedores cadastrados no Supabase."""
    try:
        supabase = get_supabase()
        data = supabase.table("vendedores").select("nome").order("nome").execute()
        return [v["nome"] for v in data.data] if data and hasattr(data, "data") else []
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao listar vendedores: {e}")
        return []

def registrar_vendedor_db(nome: str):
    """Registra um vendedor no banco (evita duplicados)."""
    try:
        supabase = get_supabase()
        existe = supabase.table("vendedores").select("id").eq("nome", nome).execute()
        if not existe.data:
            supabase.table("vendedores").insert({"nome": nome}).execute()
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao registrar vendedor: {e}")

def excluir_ultimo_feedback(vendedor: str):
    """Exclui o último feedback de um vendedor"""
    try:
        supabase = get_supabase()
        # pega o último registro
        data = supabase.table("feedbacks").select("*").eq("vendedor", vendedor).order("created_at", desc=True).limit(1).execute()
        if data.data:
            fid = data.data[0]["id"]
            supabase.table("feedbacks").delete().eq("id", fid).execute()
            return True
        return False
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao excluir feedback: {e}")
        return False

def atualizar_ultimo_feedback(vendedor: str, novo_texto: str):
    """Atualiza o último feedback de um vendedor"""
    try:
        supabase = get_supabase()
        data = supabase.table("feedbacks").select("*").eq("vendedor", vendedor).order("created_at", desc=True).limit(1).execute()
        if data.data:
            fid = data.data[0]["id"]
            supabase.table("feedbacks").update({"feedback": novo_texto}).eq("id", fid).execute()
            return True
        return False
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao editar feedback: {e}")
        return False

def salvar_feedback_db(vendedor: str, texto: str):
    from datetime import datetime
    """Salva o feedback de um vendedor no Supabase."""
    try:
        supabase = get_supabase()
        agora = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        data, count = supabase.table("feedbacks").insert({
            "vendedor": vendedor,
            "feedback": texto,
            "created_at": agora   # sobrescreve no formato desejado
        }).execute()
        return True
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao salvar feedback no banco: {e}")
        return False

def carregar_feedbacks_db(vendedor: str):
    """Carrega todos os feedbacks de um vendedor do Supabase."""
    try:
        supabase = get_supabase()
        data = supabase.table("feedbacks").select("*").eq("vendedor", vendedor).order("created_at").execute()
        return data.data if data and hasattr(data, "data") else []
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao carregar feedbacks: {e}")
        return []
