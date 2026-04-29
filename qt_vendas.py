from __future__ import annotations

import sys
import os
import re
import time
import datetime as dt
import tempfile
import queue
import threading
import difflib
from typing import List, Optional
from pathlib import Path

from PySide6 import QtCore, QtGui, QtWidgets, QtPrintSupport

from utils import (
    source_pdf_async,
    adicionar_pdf,
    analisar_pdf_caixa,
    analisar_pdf_resumo_nfce,
    analisar_pdf_fechamento_caixa_mva_clipp,
    combinar_relatorios_caixa_mva,
    comparar_caixa_resumo_nfce,
    gerar_relatorios_caixa_eh_zweb,
    validar_periodo_relatorios_caixa,
    validar_arquivo_caixa_mva,
    validar_relatorio_pedidos_importados,
    validar_relatorio_resumo_nfce,
    ordenar_coluna,
    process_cancel,
    carregar_planilhas_duplas_async,
    limpar_tabelas_duplas,
    check_for_updates,
    resource_path,
    mesclar_tabelas_duplas,
    _pdf_export,
    _excel_export,
    parse_number,
    format_number_br,
    corrigir_texto,
    corrigir_estrutura_texto,
    criar_etiquetas,
    salvar_feedback_db,
    carregar_feedbacks_db,
    excluir_ultimo_feedback,
    atualizar_ultimo_feedback,
    listar_vendedores_db,
    set_ui_refs,
    _active_report_dir,
    cleanup_generated_auto_reports,
)
from ui_dialogs import messagebox, filedialog, set_parent
from qt_adapters import (
    QtRootAdapter,
    QtVar,
    QtProgressBarAdapter,
    QtButtonAdapter,
    QtTreeAdapter,
)


_LEXEND_REPORTLAB_FONT_NAME: str | None = None


def _get_reportlab_font_names() -> tuple[str, str]:
    global _LEXEND_REPORTLAB_FONT_NAME

    if _LEXEND_REPORTLAB_FONT_NAME:
        return (_LEXEND_REPORTLAB_FONT_NAME, _LEXEND_REPORTLAB_FONT_NAME)

    try:
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont

        font_path = resource_path(os.path.join("data", "Lexend-Regular.ttf"))
        if os.path.exists(font_path):
            font_name = "Lexend"
            if font_name not in pdfmetrics.getRegisteredFontNames():
                pdfmetrics.registerFont(TTFont(font_name, font_path))
            _LEXEND_REPORTLAB_FONT_NAME = font_name
            return (font_name, font_name)
    except Exception:
        pass

    _LEXEND_REPORTLAB_FONT_NAME = "Helvetica"
    return ("Helvetica-Bold", "Helvetica")


def _create_a4_printer() -> QtPrintSupport.QPrinter:
    printer = QtPrintSupport.QPrinter(QtPrintSupport.QPrinter.HighResolution)
    printer.setPageSize(QtGui.QPageSize(QtGui.QPageSize.A4))
    printer.setPageMargins(QtCore.QMarginsF(6, 6, 6, 6), QtGui.QPageLayout.Millimeter)
    return printer


def _resolve_default_printer() -> QtPrintSupport.QPrinter:
    printer_info = QtPrintSupport.QPrinterInfo.defaultPrinter()
    printer_name = str(printer_info.printerName() or "").strip()
    if printer_info.isNull() or not printer_name:
        raise RuntimeError("Nenhuma impressora padrao esta configurada no Windows.")

    printer = _create_a4_printer()
    printer.setPrinterName(printer_name)
    if not printer.isValid():
        raise RuntimeError(
            f"A impressora padrao '{printer_name}' nao esta disponivel para impressao."
        )
    return printer


def _render_html_document_to_printer(
    html: str,
    printer: QtPrintSupport.QPrinter,
    font_family: str | None = None,
) -> None:
    document = QtGui.QTextDocument()
    document.setDefaultFont(
        QtGui.QFont(font_family or QtWidgets.QApplication.font().family() or "Lexend", 8)
    )
    page_rect = printer.pageRect(QtPrintSupport.QPrinter.Point)
    document.setPageSize(page_rect.size())
    document.setHtml(html)
    document.print_(printer)


class SourceDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget) -> None:
        super().__init__(parent)
        self.setWindowTitle("Origem do PDF")
        self.setModal(True)
        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(QtWidgets.QLabel("Este PDF pertence a qual empresa?"))
        self._choice = None

        btn_mva = QtWidgets.QPushButton("MVA")
        btn_eh = QtWidgets.QPushButton("HORIZONTE")
        btn_mva.clicked.connect(lambda: self._set_choice("MVA"))
        btn_eh.clicked.connect(lambda: self._set_choice("EH"))
        layout.addWidget(btn_mva)
        layout.addWidget(btn_eh)

        self.setFixedSize(280, 140)

    def _set_choice(self, value: str) -> None:
        self._choice = value
        self.accept()

    def choice(self) -> Optional[str]:
        if self.exec() == QtWidgets.QDialog.Accepted:
            return self._choice
        return None


class InstructionDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, title: str, message: str) -> None:
        super().__init__(parent)
        self.setWindowTitle(corrigir_texto(title))
        self.setModal(True)
        self.resize(430, 170)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setSpacing(12)

        label = QtWidgets.QLabel(corrigir_texto(message))
        label.setWordWrap(True)
        label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(label, 1)

        buttons = QtWidgets.QHBoxLayout()
        buttons.addStretch()

        btn_continue = QtWidgets.QPushButton("Continuar")
        btn_continue.setStyleSheet("text-align:center;")
        btn_continue.setMinimumWidth(120)

        btn_continue.clicked.connect(self.accept)

        buttons.addWidget(btn_continue)
        buttons.addStretch()
        layout.addLayout(buttons)

    def confirmed(self) -> bool:
        return self.exec() == QtWidgets.QDialog.Accepted


class LoadingStatusDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, title: str, message: str) -> None:
        super().__init__(parent)
        self._cancelled = False
        self._closing_programmatically = False
        self._last_log_message = ""
        self.setWindowTitle(corrigir_texto(title))
        self.setModal(True)
        self.setWindowFlag(QtCore.Qt.WindowCloseButtonHint, True)
        self.resize(620, 340)
        self.setMinimumSize(620, 340)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setSpacing(12)

        self._label = QtWidgets.QLabel(corrigir_texto(message))
        self._label.setWordWrap(True)
        self._label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(self._label, 1)

        self._bar = QtWidgets.QProgressBar()
        self._bar.setRange(0, 0)
        self._bar.setTextVisible(False)
        self._bar.setMinimumHeight(16)
        layout.addWidget(self._bar)

        debug_title = QtWidgets.QLabel("Debug em tempo real")
        debug_title.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        layout.addWidget(debug_title)

        self._debug_log = QtWidgets.QPlainTextEdit()
        self._debug_log.setReadOnly(True)
        self._debug_log.setLineWrapMode(QtWidgets.QPlainTextEdit.WidgetWidth)
        self._debug_log.setMinimumHeight(180)
        layout.addWidget(self._debug_log, 2)

        self.append_log(message)

    def set_status(self, message: str) -> None:
        normalized = corrigir_texto(message)
        self._label.setText(normalized)
        self.append_log(normalized)

    def append_log(self, message: str) -> None:
        normalized = corrigir_texto(message)
        if not normalized:
            return
        if normalized == self._last_log_message:
            return
        self._last_log_message = normalized
        timestamp = time.strftime("%H:%M:%S")
        self._debug_log.appendPlainText(f"[{timestamp}] {normalized}")
        scrollbar = self._debug_log.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def was_cancelled(self) -> bool:
        return self._cancelled

    def close_gracefully(self) -> None:
        self._closing_programmatically = True
        self.close()

    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        if not self._closing_programmatically:
            self._cancelled = True
        super().closeEvent(event)


class AutomationTimeDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, current_time: QtCore.QTime) -> None:
        super().__init__(parent)
        self.setWindowTitle("Horario da automacao")
        self.setModal(True)
        self.setFixedSize(320, 150)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        label = QtWidgets.QLabel(
            corrigir_texto("Escolha o horario diario da automacao.")
        )
        label.setWordWrap(True)
        label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(label)

        self._time_edit = QtWidgets.QTimeEdit(current_time)
        self._time_edit.setDisplayFormat("HH:mm")
        self._time_edit.setAlignment(QtCore.Qt.AlignCenter)
        self._time_edit.setCalendarPopup(False)
        self._time_edit.setMinimumWidth(120)
        layout.addWidget(self._time_edit, alignment=QtCore.Qt.AlignHCenter)

        buttons = QtWidgets.QDialogButtonBox()
        btn_confirm = buttons.addButton("Confirmar", QtWidgets.QDialogButtonBox.AcceptRole)
        btn_cancel = buttons.addButton("Cancelar", QtWidgets.QDialogButtonBox.RejectRole)
        btn_confirm.setStyleSheet("text-align:center;")
        btn_cancel.setStyleSheet("text-align:center;")
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def selected_time(self) -> QtCore.QTime | None:
        if self.exec() == QtWidgets.QDialog.Accepted:
            return self._time_edit.time()
        return None


class CaixaCnpjDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget) -> None:
        super().__init__(parent)
        self.setWindowTitle("Caixa - CNPJ")
        self.setModal(True)
        self._choice = None
        self.resize(300, 150)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setSpacing(12)

        label = QtWidgets.QLabel("Selecione o CNPJ para o fluxo de Caixa.")
        label.setWordWrap(True)
        label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(label)

        btn_mva = QtWidgets.QPushButton("MVA")
        btn_eh = QtWidgets.QPushButton("EH")
        for btn in (btn_mva, btn_eh):
            btn.setStyleSheet("text-align:center;")
            btn.setMinimumHeight(36)
            layout.addWidget(btn)

        btn_mva.clicked.connect(lambda: self._set_choice("MVA"))
        btn_eh.clicked.connect(lambda: self._set_choice("EH"))

    def _set_choice(self, value: str) -> None:
        self._choice = value
        self.accept()

    def choice(self) -> Optional[str]:
        if self.exec() == QtWidgets.QDialog.Accepted:
            return self._choice
        return None


class CaixaDateDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, title: str, message: str) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.resize(300, 160)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setSpacing(12)

        label = QtWidgets.QLabel(message)
        label.setWordWrap(True)
        label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(label)

        self._date_edit = QtWidgets.QDateEdit(QtCore.QDate.currentDate())
        self._date_edit.setCalendarPopup(True)
        self._date_edit.setDisplayFormat("dd/MM/yyyy")
        self._date_edit.setAlignment(QtCore.Qt.AlignCenter)
        self._date_edit.setMinimumHeight(34)
        layout.addWidget(self._date_edit, alignment=QtCore.Qt.AlignCenter)

        buttons = QtWidgets.QHBoxLayout()
        buttons.addStretch()
        btn_continue = QtWidgets.QPushButton("Continuar")
        btn_continue.setMinimumWidth(120)
        btn_continue.setStyleSheet("text-align:center;")
        btn_continue.clicked.connect(self.accept)
        buttons.addWidget(btn_continue)
        buttons.addStretch()
        layout.addLayout(buttons)

    def selected_date(self) -> str | None:
        if self.exec() != QtWidgets.QDialog.Accepted:
            return None
        return self._date_edit.date().toString("dd/MM/yyyy")


class FeedbackDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, tree_adapter: QtTreeAdapter) -> None:
        super().__init__(parent)
        self._tree = tree_adapter
        self._last_vendedores: List[str] = []
        self.setWindowTitle("Feedback dos Vendedores")
        self.setFixedSize(250, 450)
        layout = QtWidgets.QVBoxLayout(self)

        title = QtWidgets.QLabel("Selecione o Vendedor")
        title.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(title)

        self._scroll = QtWidgets.QScrollArea()
        self._scroll.setWidgetResizable(True)
        self._container = QtWidgets.QWidget()
        self._buttons_layout = QtWidgets.QVBoxLayout(self._container)
        self._buttons_layout.setAlignment(QtCore.Qt.AlignTop)
        self._scroll.setWidget(self._container)
        layout.addWidget(self._scroll)

        self._bottom = QtWidgets.QWidget()
        bottom_layout = QtWidgets.QVBoxLayout(self._bottom)
        self._btn_export_all = None
        btn_fechar = QtWidgets.QPushButton("Fechar")
        btn_fechar.setStyleSheet("text-align:center;")
        btn_fechar.clicked.connect(self.close)
        bottom_layout.addWidget(btn_fechar)
        layout.addWidget(self._bottom)

        self._timer = QtCore.QTimer(self)
        self._timer.timeout.connect(self._refresh)
        self._timer.start(2000)
        self._refresh()

    def _refresh(self) -> None:
        vendedores_tree = [self._tree.item(i)["values"][0] for i in self._tree.get_children()]
        vendedores_db = listar_vendedores_db()
        vendedores = sorted(set(vendedores_tree + vendedores_db))

        if vendedores == self._last_vendedores:
            return

        self._last_vendedores = vendedores
        while self._buttons_layout.count():
            item = self._buttons_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

        if not vendedores:
            self._buttons_layout.addWidget(QtWidgets.QLabel("Nenhum vendedor carregado."))
        else:
            for vendedor in vendedores:
                btn = QtWidgets.QPushButton(vendedor)
                btn.setStyleSheet("text-align:center;")
                btn.clicked.connect(lambda _checked=False, v=vendedor: self._open_obs(v))
                self._buttons_layout.addWidget(btn)

        if self._btn_export_all:
            self._btn_export_all.deleteLater()
            self._btn_export_all = None

        if vendedores:
            self._btn_export_all = QtWidgets.QPushButton("Exportar PDF")
            self._btn_export_all.setStyleSheet("text-align:center;")
            self._btn_export_all.clicked.connect(lambda: self._export_all(vendedores))
            self._bottom.layout().insertWidget(0, self._btn_export_all)

    def _export_all(self, vendedores: List[str]) -> None:
        feedbacks_all = []
        for v in vendedores:
            feedbacks_all.extend(carregar_feedbacks_db(v))
        if not feedbacks_all:
            messagebox.showwarning("Aviso", "Nenhum feedback encontrado para exportar.")
            return
        exportar_feedbacks_pdf("Todos os Vendedores", feedbacks_all)

    def _open_obs(self, vendedor: str) -> None:
        dlg = ObservacoesDialog(self, vendedor)
        dlg.exec()


class ObservacoesDialog(QtWidgets.QDialog):
    def __init__(self, parent: QtWidgets.QWidget, vendedor: str) -> None:
        super().__init__(parent)
        self._vendedor = vendedor
        self.setWindowTitle(f"Observações - {vendedor}")
        self.setFixedSize(650, 420)
        layout = QtWidgets.QVBoxLayout(self)

        title = QtWidgets.QLabel(f"Feedback para {vendedor}")
        font = title.font()
        font.setBold(True)
        font.setPointSize(12)
        title.setFont(font)
        title.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(title)

        self._history = QtWidgets.QTextEdit()
        self._history.setReadOnly(True)
        layout.addWidget(self._history)

        label_novo = QtWidgets.QLabel("Novo Feedback:")
        label_novo.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(label_novo)
        self._new_text = QtWidgets.QTextEdit()
        layout.addWidget(self._new_text)

        btn_row = QtWidgets.QHBoxLayout()
        btn_salvar = QtWidgets.QPushButton("Salvar")
        btn_export = QtWidgets.QPushButton("Exportar PDF")
        btn_fechar = QtWidgets.QPushButton("Fechar")
        btn_editar = QtWidgets.QPushButton("Editar")
        for btn in (btn_salvar, btn_export, btn_fechar, btn_editar):
            btn.setStyleSheet("text-align:center;")

        btn_salvar.clicked.connect(self._save_feedback)
        btn_export.clicked.connect(self._export_pdf)
        btn_fechar.clicked.connect(self.close)
        btn_editar.clicked.connect(self._edit_feedback)

        btn_row.addWidget(btn_salvar)
        btn_row.addWidget(btn_export)
        btn_row.addWidget(btn_fechar)
        btn_row.addWidget(btn_editar)
        layout.addLayout(btn_row)

        self._confirm_edit = QtWidgets.QPushButton("Confirmar Edição")
        self._confirm_edit.setStyleSheet("text-align:center;")
        self._confirm_edit.clicked.connect(self._confirm_edit_feedback)
        self._confirm_edit.setVisible(False)
        layout.addWidget(self._confirm_edit)

        self._refresh_history()

    def _refresh_history(self) -> None:
        historico = carregar_feedbacks_db(self._vendedor)
        self._history.setPlainText("")
        if historico:
            lines = []
            for fb in historico:
                ts = fb.get("created_at") or fb.get("created_at_ts", "")
                lines.append(f"[{str(ts)[:19]}]\n{fb.get('feedback', '')}\n")
            self._history.setPlainText("\n".join(lines))
        else:
            self._history.setPlainText("Nenhum feedback registrado ainda.\n")

    def _save_feedback(self) -> None:
        texto = self._new_text.toPlainText().strip()
        if not texto:
            messagebox.showwarning("Aviso", "Digite algum feedback antes de salvar.")
            return
        if salvar_feedback_db(self._vendedor, texto):
            messagebox.showinfo("Salvo", f"Feedback de {self._vendedor} registrado com sucesso!")
            self._new_text.clear()
            self._refresh_history()
        else:
            messagebox.showerror("Erro", "Falha ao salvar feedback.")

    def _export_pdf(self) -> None:
        feedbacks = carregar_feedbacks_db(self._vendedor)
        if not feedbacks:
            messagebox.showwarning("Aviso", "Nenhum feedback para exportar.")
            return
        exportar_feedbacks_pdf(self._vendedor, feedbacks)

    def _edit_feedback(self) -> None:
        historico = carregar_feedbacks_db(self._vendedor)
        if not historico:
            messagebox.showwarning("Aviso", "Nenhum feedback encontrado para exportar.")
            return

        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Editor")
        dlg.setFixedSize(300, 200)
        layout = QtWidgets.QVBoxLayout(dlg)
        label = QtWidgets.QLabel("O que deseja fazer?")
        label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(label)
        btn_editar = QtWidgets.QPushButton("Editar último")
        btn_excluir = QtWidgets.QPushButton("Excluir último")
        btn_cancelar = QtWidgets.QPushButton("Cancelar")
        for btn in (btn_editar, btn_excluir, btn_cancelar):
            btn.setStyleSheet("text-align:center;")
        layout.addWidget(btn_editar)
        layout.addWidget(btn_excluir)
        layout.addWidget(btn_cancelar)

        def acao_editar() -> None:
            dlg.accept()
            historico_local = carregar_feedbacks_db(self._vendedor)
            if historico_local:
                ultimo = historico_local[-1]["feedback"]
                self._new_text.setPlainText(ultimo)
                self._confirm_edit.setVisible(True)

        def acao_excluir() -> None:
            dlg.accept()
            if excluir_ultimo_feedback(self._vendedor):
                messagebox.showinfo("Sucesso", "Último feedback excluído.")
                self._refresh_history()

        btn_editar.clicked.connect(acao_editar)
        btn_excluir.clicked.connect(acao_excluir)
        btn_cancelar.clicked.connect(dlg.reject)
        dlg.exec()

    def _confirm_edit_feedback(self) -> None:
        novo = self._new_text.toPlainText().strip()
        if atualizar_ultimo_feedback(self._vendedor, novo):
            messagebox.showinfo("Sucesso", "Feedback atualizado com sucesso!")
            self._new_text.clear()
            self._refresh_history()
            self._confirm_edit.setVisible(False)


class CaixaReportDialog(QtWidgets.QDialog):
    def __init__(
        self,
        parent: QtWidgets.QWidget,
        relatorio_caixa: dict,
        fechamento: dict | None = None,
        relatorio_pix: dict | None = None,
    ) -> None:
        super().__init__(parent)
        self._relatorio_caixa = relatorio_caixa
        self._fechamento = fechamento
        self._relatorio_pix = relatorio_pix
        self._payment_tab_widgets: dict[str, QtWidgets.QWidget] = {}
        self._payment_reports: dict[str, dict] = {}
        self._tabs: QtWidgets.QTabWidget | None = None
        if relatorio_pix:
            payment_key = str(relatorio_pix.get("categoria") or "pagamentos_digitais_nfce").strip() or "pagamentos_digitais_nfce"
            self._payment_reports[payment_key] = relatorio_pix
        if fechamento:
            for key, report in (fechamento.get("relatorios_pagamento") or {}).items():
                if report:
                    self._payment_reports[str(key)] = report
        self.setWindowTitle("Relatório de Caixa")
        self.resize(840, 640)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(16, 14, 16, 14)
        layout.setSpacing(10)

        title = QtWidgets.QLabel("Relatório de Caixa")
        title_font = title.font()
        title_font.setBold(True)
        title_font.setPointSize(15)
        title.setFont(title_font)
        title.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(title)

        tabs = QtWidgets.QTabWidget()
        self._tabs = tabs
        tabs.setTabsClosable(True)
        tabs.tabCloseRequested.connect(self._handle_tab_close_requested)
        tabs.addTab(self._build_davs_tab(relatorio_caixa), self._build_davs_tab_title(relatorio_caixa))
        if fechamento:
            tabs.addTab(self._build_fechamento_tab(fechamento), "Fechamento Caixa")
        self._hide_static_tab_close_buttons()
        layout.addWidget(tabs, 1)

        btn_close = QtWidgets.QPushButton("Fechar")
        btn_close.setStyleSheet("text-align:center;")
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close, alignment=QtCore.Qt.AlignHCenter)

    def _build_davs_tab_title(self, relatorio: dict) -> str:
        if relatorio.get("caixa_modelo") == "EH":
            return "Pedidos Caixa"
        return "DAVs Importados"

    def _build_caixa_dialog_title(self, relatorio: dict) -> str:
        if relatorio.get("caixa_modelo") == "EH":
            return "Fechamento de Caixa - Eletrônica Horizonte"
        return "Fechamento de Caixa - MVA"


    def _display_periodo(self, periodo: str | None) -> str:
        periodo_texto = str(periodo or "").strip()
        if not periodo_texto:
            return "Não identificado"
        partes = [parte.strip() for parte in periodo_texto.split(" - ", 1)]
        if len(partes) == 2 and partes[0] == partes[1]:
            return partes[0]
        return periodo_texto

    def _bank_row_has_explicit_origin(self, row: tuple[str, ...] | list[str]) -> bool:
        if len(row) < 3:
            return False
        origem = corrigir_texto(str(row[0] or "")).strip().casefold()
        return origem == "pix" or origem.startswith("cart")

    def _wrap_centered(self, widget: QtWidgets.QWidget, max_width: int | None = None) -> QtWidgets.QWidget:
        if max_width is not None:
            widget.setMaximumWidth(max_width)
        widget.setSizePolicy(QtWidgets.QSizePolicy.Maximum, widget.sizePolicy().verticalPolicy())
        container = QtWidgets.QWidget()
        row = QtWidgets.QHBoxLayout(container)
        row.setContentsMargins(0, 0, 0, 0)
        row.setSpacing(0)
        row.addStretch()
        row.addWidget(widget)
        row.addStretch()
        return container

    def _ordered_payment_reports(self) -> list[tuple[str, dict]]:
        ordem = {
            "pix_caixa": 0,
            "pix_fechamento": 1,
            "pagamentos_digitais_nfce": 1,
            "dinheiro": 2,
            "alertas_eh": 3,
            "cartao_credito": 5,
            "cartao_debito": 6,
            "cartao_credito_caixa": 7,
            "cartao_debito_caixa": 8,
            "nf_pedidos_eh": 9,
        }
        return sorted(
            [
                (key, report)
                for key, report in self._payment_reports.items()
                if not (report or {}).get("hidden_in_menu")
            ],
            key=lambda item: (
                ordem.get(item[0], 99),
                str((item[1] or {}).get("tab_title") or item[0]).casefold(),
            ),
        )

    def _hide_static_tab_close_buttons(self) -> None:
        if self._tabs is None:
            return
        tab_bar = self._tabs.tabBar()
        static_count = 1 + int(self._tabs.count() > 1)
        for index in range(min(static_count, self._tabs.count())):
            tab_bar.setTabButton(index, QtWidgets.QTabBar.LeftSide, None)
            tab_bar.setTabButton(index, QtWidgets.QTabBar.RightSide, None)

    def _build_davs_summary_items(self, relatorio: dict):
        if relatorio.get("caixa_modelo") == "MVA":
            return (
                ("Período", self._display_periodo(relatorio.get("periodo"))),
                ("Pedidos totais", str(relatorio.get("pedidos_total", 0))),
                ("Finalizados", str(relatorio.get("pedidos_caixa", 0))),
                ("Editando", str(relatorio.get("pedidos_editando", 0))),
                ("Outros status", str(relatorio.get("pedidos_outros_status", 0))),
                ("Pedidos excluídos", str(relatorio.get("pedidos_excluidos", 0))),
                ("Total do documento", f"R$ {format_number_br(relatorio.get('total_documento', 0.0))}"),
                ("Total excluído", f"R$ {format_number_br(relatorio.get('total_excluido', 0.0))}"),
                ("Total Caixa", f"R$ {format_number_br(relatorio.get('total_caixa', 0.0))}"),
            )

        return (
            ("Período", self._display_periodo(relatorio.get("periodo"))),
            ("Pedidos totais", str(relatorio.get("pedidos_total", 0))),
            ("Pedidos Caixa", str(relatorio.get("pedidos_caixa", 0))),
            ("Fora do balcão", str(relatorio.get("pedidos_excluidos_cliente", 0))),
            ("NF-e excluídas", str(relatorio.get("pedidos_excluidos_documento", 0))),
            ("Cupons cancelados", str(relatorio.get("pedidos_excluidos_cancelados", 0))),
            ("Pedidos excluídos", str(relatorio.get("pedidos_excluidos", 0))),
            ("Total do documento", f"R$ {format_number_br(relatorio.get('total_documento', 0.0))}"),
            ("Total excluído", f"R$ {format_number_br(relatorio.get('total_excluido', 0.0))}"),
            ("Total Caixa", f"R$ {format_number_br(relatorio.get('total_caixa', 0.0))}"),
        )

    def _build_davs_table_headers(self, relatorio: dict) -> tuple[str, ...]:
        if relatorio.get("caixa_modelo") == "MVA":
            return ("Pedido", "Descrição", "Status", "Valor")
        return ("Pedido", "Cliente", "Documento", "Valor")

    def _build_davs_table_widths(self, relatorio: dict) -> list[int]:
        if relatorio.get("caixa_modelo") == "MVA":
            return [100, 250, 110, 120]
        return [90, 280, 190, 120]

    def _build_davs_section_title(self, relatorio: dict) -> str:
        if relatorio.get("caixa_modelo") == "MVA":
            return "Pedidos não finalizados"
        return "Pedidos excluídos do cálculo"

    def _build_davs_empty_message(self, relatorio: dict) -> str:
        if relatorio.get("caixa_modelo") == "MVA":
            return "Nenhum pedido não finalizado encontrado."
        return "Nenhum pedido excluído encontrado."

    def _build_fechamento_subtitle(self, fechamento: dict) -> str:
        return ""

    def _build_fechamento_summary_items(self, fechamento: dict):
        return (
            ("Período", self._display_periodo(fechamento.get("periodo"))),
            (
                fechamento.get("total_resumo_titulo", "Total Resumo NFC-e"),
                f"R$ {format_number_br(fechamento.get('total_resumo_nfce', 0.0))}",
            ),
            ("Valor das faltantes", f"R$ {format_number_br(fechamento.get('valor_faltantes', 0.0))}"),
            ("Status", fechamento.get("status", "-")),
        )

    def _build_fechamento_section_title(self, fechamento: dict) -> str:
        return fechamento.get("secao_titulo", "NFC-e faltantes")

    def _build_fechamento_empty_message(self, fechamento: dict) -> str:
        return fechamento.get("empty_message", "Nenhuma NFC-e faltante encontrada.")

    def _get_fechamento_bank_report(self, fechamento: dict) -> dict | None:
        relatorios_pagamento = fechamento.get("relatorios_pagamento") or {}
        report = relatorios_pagamento.get("alertas_eh")
        if report:
            return report
        return self._payment_reports.get("alertas_eh")

    def _enrich_eh_bank_report(self, fechamento: dict, relatorio_pagamento: dict) -> dict:
        report = corrigir_estrutura_texto(dict(relatorio_pagamento or {}))
        periodo = fechamento.get("periodo") or report.get("periodo")
        valor_total_vendas = round(float(report.get("valor_total_vendas", fechamento.get("total_caixa", 0.0)) or 0.0), 2)
        valor_pendente = round(float(report.get("valor_pendente", fechamento.get("valor_faltantes", 0.0)) or 0.0), 2)
        status = str(fechamento.get("status") or report.get("status") or "-")
        report["periodo"] = periodo
        report["valor_total_vendas"] = valor_total_vendas
        report["valor_pendente"] = valor_pendente
        report["status"] = status
        report["caixa_modelo"] = fechamento.get("caixa_modelo") or report.get("caixa_modelo") or "EH"
        report["texto_informativo"] = ""
        pendencias_count = int(report.get("alertas_count", 0) or 0)
        if pendencias_count <= 0:
            pendencias_count = sum(
                len(report.get(key) or [])
                for key in (
                    "pix_fechamento_rows",
                    "cartao_fechamento_rows",
                    "pix_maquina_rows",
                    "cartao_maquina_rows",
                    "observacao_rows",
                )
            )
        report["pendencias_count"] = pendencias_count
        report["summary_items"] = (
            ("Período", self._display_periodo(periodo)),
            ("Pendências", str(int(report.get("pendencias_count", 0) or 0))),
            ("Total Pendências", f"R$ {format_number_br(valor_pendente)}"),
        )
        return report

    def _build_eh_bank_sections(
        self,
        relatorio_pagamento: dict,
    ) -> list[tuple[str, tuple[str, ...], list[tuple[str, ...]], list[int], str]]:
        relatorio_pagamento = corrigir_estrutura_texto(relatorio_pagamento or {})
        correlation_rows = [
            tuple(corrigir_texto(str(value)) for value in row)
            for row in (relatorio_pagamento.get("correlacao_rows") or [])
            if row
        ]
        pix_rows = [
            tuple(corrigir_texto(str(value)) for value in row)
            for row in (relatorio_pagamento.get("pix_fechamento_rows") or [])
            if row
        ]
        cartao_rows = [
            tuple(corrigir_texto(str(value)) for value in row)
            for row in (relatorio_pagamento.get("cartao_fechamento_rows") or [])
            if row
        ]

        bank_rows: list[tuple[str, str, str]] = []
        for row in (relatorio_pagamento.get("pix_maquina_rows") or []):
            if not row:
                continue
            origem = corrigir_texto(str(row[0] or "")).strip() if len(row) >= 1 else ""
            if self._bank_row_has_explicit_origin(row):
                bank_rows.append(tuple(corrigir_texto(str(value)) for value in row[:3]))
            elif len(row) >= 2:
                bank_rows.append(("PIX", corrigir_texto(str(row[0])), corrigir_texto(str(row[1]))))
        for row in (relatorio_pagamento.get("cartao_maquina_rows") or []):
            if not row:
                continue
            origem = corrigir_texto(str(row[0] or "")).strip() if len(row) >= 1 else ""
            if self._bank_row_has_explicit_origin(row):
                bank_rows.append(tuple(corrigir_texto(str(value)) for value in row[:3]))
            elif len(row) >= 2:
                bank_rows.append(("Cartão", corrigir_texto(str(row[0])), corrigir_texto(str(row[1]))))

        sections = [
            (
                "Correlação de Valores",
                ("Pagamento", "Caixa", "Pagamentos", "Status"),
                correlation_rows,
                [220, 110, 110, 110],
                "Nenhuma correlação de valores disponível.",
            ),
            (
                "CF sem Transação Bancária - PIX",
                ("CF", "Valor"),
                pix_rows,
                [340, 140],
                "Nenhum CF PIX sem transação bancária encontrado.",
            ),
            (
                "CF sem Transação Bancária - Cartão",
                ("CF", "Valor"),
                cartao_rows,
                [340, 140],
                "Nenhum CF de cartão sem transação bancária encontrado.",
            ),
            (
                "Transações Bancárias sem CF/NF",
                ("Origem", "Detalhe", "Valor"),
                bank_rows,
                [110, 400, 120],
                "Nenhuma transação bancária sem CF/NF encontrada.",
            ),
        ]
        return sections

    def _build_bank_sections(
        self,
        relatorio_pagamento: dict,
    ) -> list[tuple[str, tuple[str, ...], list[tuple[str, ...]], list[int], str]]:
        relatorio_pagamento = corrigir_estrutura_texto(relatorio_pagamento or {})
        caixa_modelo = str(relatorio_pagamento.get("caixa_modelo") or "EH").upper()
        fechamento_label = "Fechamento EH" if caixa_modelo == "EH" else "Fechamento MVA"

        correlation_rows = [
            tuple(corrigir_texto(str(value)) for value in row)
            for row in (relatorio_pagamento.get("correlacao_rows") or [])
            if row
        ]
        pix_rows = [
            tuple(corrigir_texto(str(value)) for value in row)
            for row in (relatorio_pagamento.get("pix_fechamento_rows") or [])
            if row
        ]
        cartao_rows = [
            tuple(corrigir_texto(str(value)) for value in row)
            for row in (relatorio_pagamento.get("cartao_fechamento_rows") or [])
            if row
        ]

        bank_rows: list[tuple[str, str, str]] = []
        for row in (relatorio_pagamento.get("pix_maquina_rows") or []):
            if not row:
                continue
            origem = corrigir_texto(str(row[0] or "")).strip() if len(row) >= 1 else ""
            if self._bank_row_has_explicit_origin(row):
                bank_rows.append(tuple(corrigir_texto(str(value)) for value in row[:3]))
            elif len(row) >= 2:
                bank_rows.append(("PIX", corrigir_texto(str(row[0])), corrigir_texto(str(row[1]))))
        for row in (relatorio_pagamento.get("cartao_maquina_rows") or []):
            if not row:
                continue
            origem = corrigir_texto(str(row[0] or "")).strip() if len(row) >= 1 else ""
            if self._bank_row_has_explicit_origin(row):
                bank_rows.append(tuple(corrigir_texto(str(value)) for value in row[:3]))
            elif len(row) >= 2:
                bank_rows.append(("Cartão", corrigir_texto(str(row[0])), corrigir_texto(str(row[1]))))

        return [
            (
                "Correlação de Valores",
                ("Pagamento", "Caixa", "Pagamentos", "Status"),
                correlation_rows,
                [220, 110, 110, 110],
                "Nenhuma correlação de valores disponível.",
            ),
            (
                "CF sem Transação Bancária - PIX",
                (fechamento_label, "Valor"),
                pix_rows,
                [340, 140],
                "Nenhum CF PIX sem transação bancária encontrado.",
            ),
            (
                "CF sem Transação Bancária - Cartão",
                (fechamento_label, "Valor"),
                cartao_rows,
                [340, 140],
                "Nenhum CF de cartão sem transação bancária encontrado.",
            ),
            (
                "Transações Bancárias sem CF/NF",
                ("Origem", "Detalhe", "Valor"),
                bank_rows,
                [110, 400, 120],
                "Nenhuma transação bancária sem CF/NF encontrada.",
            ),
        ]

    def _build_pix_summary_items(self, relatorio_pix: dict):
        summary_label = relatorio_pix.get("summary_label") or "Pagamentos digitais"
        total_label = relatorio_pix.get("total_label") or f"Total {str(summary_label).casefold()}"
        return (
            ("Período", self._display_periodo(relatorio_pix.get("periodo"))),
            (summary_label, str(relatorio_pix.get("quantidade_autorizados", 0))),
            (
                total_label,
                f"R$ {format_number_br(relatorio_pix.get('total_autorizado', 0.0))}",
            ),
        )

    def _build_pix_empty_message(self, relatorio_pix: dict) -> str:
        return relatorio_pix.get(
            "mensagem",
            relatorio_pix.get("empty_message") or "Nenhum pagamento encontrado para este dia.",
        )

    def _build_davs_tab(self, relatorio: dict) -> QtWidgets.QWidget:
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        summary_items = self._build_davs_summary_items(relatorio)
        dav_actions: dict[str, QtWidgets.QWidget] | None = None
        if (
            relatorio.get("caixa_modelo") == "EH"
            and "nf_pedidos_eh" in self._payment_reports
        ):
            dav_actions = {
                "NF-e excluídas": self._create_filtered_payment_toggle_button({"nf_pedidos_eh"}),
            }
        layout.addWidget(
            self._wrap_centered(
                self._build_summary_frame(summary_items, {"Total Caixa"}, action_widgets=dav_actions),
                560,
            )
        )

        actions = QtWidgets.QHBoxLayout()
        actions.addStretch()
        actions.addWidget(
            self._create_export_button(
                "Imprimir",
                lambda: self._export_davs_pdf(relatorio),
            )
        )
        actions.addStretch()
        layout.addLayout(actions)

        section_label = QtWidgets.QLabel(self._build_davs_section_title(relatorio))
        section_label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(section_label)

        table = QtWidgets.QTableWidget()
        table.setColumnCount(4)
        table.setHorizontalHeaderLabels(self._build_davs_table_headers(relatorio))
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        table.setAlternatingRowColors(True)
        table.verticalHeader().setVisible(False)
        table.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Expanding)
        table.setMinimumSize(0, 0)
        table.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self._configure_resizable_table(table, self._build_davs_table_widths(relatorio))

        itens_excluidos = relatorio.get("itens_excluidos", [])
        table.setRowCount(len(itens_excluidos))
        for row, item in enumerate(itens_excluidos):
            pedido_item = QtWidgets.QTableWidgetItem(self._display_numero(item.get("pedido", "")))
            cliente_item = QtWidgets.QTableWidgetItem(item.get("cliente", ""))
            documento_item = QtWidgets.QTableWidgetItem(item.get("documento", ""))
            total_item = QtWidgets.QTableWidgetItem(f"R$ {format_number_br(item.get('valor', 0.0))}")

            pedido_item.setTextAlignment(QtCore.Qt.AlignCenter)
            cliente_item.setTextAlignment(QtCore.Qt.AlignCenter)
            documento_item.setTextAlignment(QtCore.Qt.AlignCenter)
            total_item.setTextAlignment(QtCore.Qt.AlignCenter)

            table.setItem(row, 0, pedido_item)
            table.setItem(row, 1, cliente_item)
            table.setItem(row, 2, documento_item)
            table.setItem(row, 3, total_item)

        if not itens_excluidos:
            table.setRowCount(1)
            empty_item = QtWidgets.QTableWidgetItem(self._build_davs_empty_message(relatorio))
            empty_item.setTextAlignment(QtCore.Qt.AlignCenter)
            table.setSpan(0, 0, 1, 4)
            table.setItem(0, 0, empty_item)

        self._fit_table_width(table)
        table_wrap = QtWidgets.QHBoxLayout()
        table_wrap.addStretch()
        table_wrap.addWidget(table)
        table_wrap.addStretch()
        layout.addLayout(table_wrap, 1)

        return widget

    def _build_fechamento_tab(self, fechamento: dict) -> QtWidgets.QWidget:
        bank_report = self._get_fechamento_bank_report(fechamento)
        if bank_report:
            return self._build_bank_reconciliation_tab(self._enrich_eh_bank_report(fechamento, bank_report))

        widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        subtitle_text = self._build_fechamento_subtitle(fechamento)
        if subtitle_text:
            subtitle = QtWidgets.QLabel(subtitle_text)
            subtitle.setWordWrap(True)
            subtitle.setAlignment(QtCore.Qt.AlignCenter)
            layout.addWidget(self._wrap_centered(subtitle, 620))

        summary_items = self._build_fechamento_summary_items(fechamento)
        action_widgets: dict[str, QtWidgets.QWidget] | None = None
        if self._payment_reports:
            action_widgets = {
                fechamento.get("total_resumo_titulo", "Total Resumo NFC-e"): self._create_payment_toggle_button(),
            }
        layout.addWidget(
            self._wrap_centered(
                self._build_summary_frame(
                    summary_items,
                    {"Total Pendências"},
                    fechamento=fechamento,
                    action_widgets=action_widgets,
                ),
                560,
            )
        )

        actions = QtWidgets.QHBoxLayout()
        actions.addStretch()
        actions.addWidget(
            self._create_export_button(
                "Imprimir",
                lambda: self._export_fechamento_pdf(fechamento),
            )
        )
        actions.addStretch()
        layout.addLayout(actions)

        section_label = QtWidgets.QLabel(self._build_fechamento_section_title(fechamento))
        section_label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(section_label)

        table = QtWidgets.QTableWidget()
        table.setColumnCount(2)
        table.setHorizontalHeaderLabels(("Número", "Valor"))
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        table.setSortingEnabled(False)
        table.setAlternatingRowColors(True)
        table.verticalHeader().setVisible(False)
        table.setFocusPolicy(QtCore.Qt.NoFocus)
        table.setWordWrap(False)
        table.setTextElideMode(QtCore.Qt.ElideRight)
        table.setShowGrid(True)
        table.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Expanding)
        table.setMinimumSize(0, 0)
        table.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        table.verticalHeader().setDefaultSectionSize(32)
        header = table.horizontalHeader()
        header.setSectionResizeMode(0, QtWidgets.QHeaderView.Fixed)
        header.setSectionResizeMode(1, QtWidgets.QHeaderView.Fixed)
        header.setDefaultAlignment(QtCore.Qt.AlignCenter)
        table.setColumnWidth(0, 185)
        table.setColumnWidth(1, 145)
        table_box = QtWidgets.QFrame()
        table_box.setFrameShape(QtWidgets.QFrame.StyledPanel)
        table_box.setStyleSheet("QFrame{background-color:#1e1e1e;border:1px solid #3b3b3b;}")
        table_box.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Expanding)
        table_box_layout = QtWidgets.QVBoxLayout(table_box)
        table_box_layout.setContentsMargins(0, 0, 0, 0)
        table_box_layout.setSpacing(0)
        table.setFrameShape(QtWidgets.QFrame.NoFrame)
        table_box_layout.addWidget(table, 1)

        registros = fechamento.get("registros_conferencia", [])
        table.setRowCount(len(registros) if registros else 1)
        for row, item in enumerate(registros):
            numero_item = QtWidgets.QTableWidgetItem(item.get("numero_exibicao", ""))
            valor = item.get("valor")
            valor_texto = "-" if valor in (None, "") else f"R$ {format_number_br(valor)}"
            valor_item = QtWidgets.QTableWidgetItem(valor_texto)

            numero_item.setTextAlignment(QtCore.Qt.AlignCenter)
            valor_item.setTextAlignment(QtCore.Qt.AlignCenter)
            numero_item.setFlags(QtCore.Qt.ItemIsEnabled)
            valor_item.setFlags(QtCore.Qt.ItemIsEnabled)

            table.setItem(row, 0, numero_item)
            table.setItem(row, 1, valor_item)

        if not registros:
            empty_item = QtWidgets.QTableWidgetItem(self._build_fechamento_empty_message(fechamento))
            empty_item.setTextAlignment(QtCore.Qt.AlignCenter)
            empty_item.setFlags(QtCore.Qt.ItemIsEnabled)
            table.setSpan(0, 0, 1, 2)
            table.setItem(0, 0, empty_item)

        total_frame = QtWidgets.QFrame()
        total_frame.setStyleSheet("QFrame{background-color:#2a2a2a;border-top:1px solid #3b3b3b;}")
        total_frame.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        total_layout = QtWidgets.QHBoxLayout(total_frame)
        total_layout.setContentsMargins(10, 6, 10, 6)
        total_layout.setSpacing(0)
        total_label = QtWidgets.QLabel(
            f"Total faltante: R$ {format_number_br(fechamento.get('valor_faltantes', 0.0))}"
        )
        total_font = total_label.font()
        total_font.setBold(True)
        total_label.setFont(total_font)
        total_label.setAlignment(QtCore.Qt.AlignCenter)
        total_color = "#59C734" if fechamento.get("status") == "Confere" else "#FF4D4F"
        total_label.setStyleSheet(f"color:{total_color};")
        total_layout.addStretch()
        total_layout.addWidget(total_label)
        total_layout.addStretch()

        self._fit_table_width(table)
        table_box_layout.addWidget(total_frame, 0)
        table_box.setFixedWidth(table.width() + table_box.frameWidth() * 2)

        table_wrap = QtWidgets.QHBoxLayout()
        table_wrap.addStretch()
        table_wrap.addWidget(table_box)
        table_wrap.addStretch()
        layout.addLayout(table_wrap, 1)
        return widget

    def _create_payment_toggle_button(self) -> QtWidgets.QWidget:
        return self._create_filtered_payment_toggle_button(None)

    def _create_filtered_payment_toggle_button(self, allowed_keys: set[str] | None) -> QtWidgets.QWidget:
        button = QtWidgets.QToolButton()
        button.setArrowType(QtCore.Qt.DownArrow)
        button.setPopupMode(QtWidgets.QToolButton.InstantPopup)
        button.setToolButtonStyle(QtCore.Qt.ToolButtonIconOnly)
        button.setAutoRaise(True)
        button.setFixedSize(16, 16)
        button.setIconSize(QtCore.QSize(8, 8))
        button.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Fixed)
        button.setStyleSheet(
            "QToolButton{padding:0px;margin:0px;border:none;}"
            "QToolButton::menu-indicator{image:none;width:0px;}"
        )
        button.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        button.setToolTip("Abrir detalhes de pagamentos")

        menu = QtWidgets.QMenu(button)
        for report_key, report in self._ordered_payment_reports():
            if allowed_keys is not None and report_key not in allowed_keys:
                continue
            action = menu.addAction(
                report.get("menu_text") or f"Abrir {str(report.get('tab_title') or report_key).casefold()}"
            )
            action.triggered.connect(
                lambda _checked=False, key=report_key: self._open_payment_tab(key)
            )
        button.setMenu(menu)
        return button

    def _open_payment_tab(self, report_key: str) -> None:
        if self._tabs is None:
            return
        report = self._payment_reports.get(report_key)
        if report is None:
            return
        if report_key not in self._payment_tab_widgets:
            widget = self._build_pix_tab(report)
            self._payment_tab_widgets[report_key] = widget
            self._tabs.addTab(widget, report.get("tab_title") or "Pagamentos")
        self._tabs.setCurrentWidget(self._payment_tab_widgets[report_key])

    def _close_payment_tab(self, report_key: str) -> None:
        if self._tabs is None:
            return
        widget = self._payment_tab_widgets.pop(report_key, None)
        if widget is None:
            return
        index = self._tabs.indexOf(widget)
        if index >= 0:
            self._tabs.removeTab(index)
        widget.deleteLater()

    def _handle_tab_close_requested(self, index: int) -> None:
        if self._tabs is None:
            return
        widget = self._tabs.widget(index)
        for report_key, report_widget in list(self._payment_tab_widgets.items()):
            if report_widget is widget:
                self._close_payment_tab(report_key)
                return

    def _build_payment_table_rows(self, relatorio_pagamento: dict) -> list[tuple[str, ...]]:
        if relatorio_pagamento.get("table_rows"):
            return [tuple(str(value) for value in row) for row in relatorio_pagamento.get("table_rows", [])]
        headers = tuple(relatorio_pagamento.get("table_headers") or ())
        mode = relatorio_pagamento.get("table_mode") or ("numero_data_valor" if len(headers) == 3 else "data_valor")
        rows: list[tuple[str, ...]] = []
        for item in relatorio_pagamento.get("itens_autorizados", []):
            valor_texto = f"R$ {format_number_br(item.get('valor_bruto', 0.0))}"
            if mode == "numero_data_valor":
                rows.append(
                    (
                        item.get("numero_exibicao", ""),
                        item.get("data_venda", ""),
                        valor_texto,
                    )
                )
            else:
                rows.append(
                    (
                        item.get("data_venda", ""),
                        valor_texto,
                    )
                )
        return rows

    def _build_payment_table_widths(self, relatorio_pagamento: dict) -> list[int]:
        if relatorio_pagamento.get("table_widths"):
            return list(relatorio_pagamento.get("table_widths") or [])
        headers = tuple(relatorio_pagamento.get("table_headers") or ())
        mode = relatorio_pagamento.get("table_mode") or ("numero_data_valor" if len(headers) == 3 else "data_valor")
        if mode == "numero_data_valor":
            return [110, 120, 130]
        return [230, 140]

    def _build_simple_centered_table(
        self,
        headers: tuple[str, ...],
        rows: list[tuple[str, ...]],
        widths: list[int],
        empty_message: str,
    ) -> QtWidgets.QWidget:
        headers = tuple(corrigir_texto(str(value)) for value in headers)
        rows = [tuple(corrigir_texto(str(value)) for value in row) for row in rows]
        empty_message = corrigir_texto(empty_message)
        table = QtWidgets.QTableWidget()
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        table.setAlternatingRowColors(True)
        table.verticalHeader().setVisible(False)
        table.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Preferred)
        table.setMinimumSize(0, 0)
        table.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self._configure_resizable_table(table, widths)

        table.setRowCount(len(rows) if rows else 1)
        for row_index, values in enumerate(rows):
            for col_index, value in enumerate(values):
                item = QtWidgets.QTableWidgetItem(corrigir_texto(str(value)))
                item.setTextAlignment(QtCore.Qt.AlignCenter)
                table.setItem(row_index, col_index, item)

        if not rows:
            empty_item = QtWidgets.QTableWidgetItem(empty_message)
            empty_item.setTextAlignment(QtCore.Qt.AlignCenter)
            table.setSpan(0, 0, 1, len(headers))
            table.setItem(0, 0, empty_item)

        self._fit_table_width(table)
        wrap = QtWidgets.QWidget()
        wrap_layout = QtWidgets.QHBoxLayout(wrap)
        wrap_layout.setContentsMargins(0, 0, 0, 0)
        wrap_layout.addStretch()
        wrap_layout.addWidget(table)
        wrap_layout.addStretch()
        return wrap

    def _build_bank_reconciliation_tab(self, relatorio_pagamento: dict) -> QtWidgets.QWidget:
        relatorio_pagamento = corrigir_estrutura_texto(relatorio_pagamento or {})
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        if relatorio_pagamento.get("categoria") == "alertas_eh":
            summary_items = relatorio_pagamento.get("summary_items") or ()
            layout.addWidget(
                self._wrap_centered(
                    self._build_summary_frame(summary_items, {"Total Pendências"}),
                    560,
                )
            )
            sections = self._build_bank_sections(relatorio_pagamento)
            keep_empty_sections = True

        else:
            summary_items = self._build_pix_summary_items(relatorio_pagamento)
            layout.addWidget(
                self._wrap_centered(
                    self._build_summary_frame(
                        summary_items,
                        {corrigir_texto(relatorio_pagamento.get("total_label") or "Total pendências")},
                    ),
                    560,
                )
            )
            sections = [
                (
                    "PIX - CF sem Transação Bancária",
                    ("Fechamento EH", "Valor EH"),
                    list(relatorio_pagamento.get("pix_fechamento_rows") or []),
                    [250, 110],
                    "Nenhum CF PIX sem transa??o banc?ria encontrado.",
                ),
                (
                    "PIX - Transação Bancária sem CF/NF",
                    ("Máquina", "Valor Banco"),
                    list(relatorio_pagamento.get("pix_maquina_rows") or []),
                    [250, 110],
                    "Nenhuma transa??o PIX sem CF/NF encontrada.",
                ),
                (
                    "Cartões - CF sem Transação Bancária",
                    ("Fechamento EH", "Valor EH"),
                    list(relatorio_pagamento.get("cartao_fechamento_rows") or []),
                    [250, 110],
                    "Nenhum CF de cart?o sem transa??o banc?ria encontrado.",
                ),
                (
                    "Cartões - Transação Bancária sem CF/NF",
                    ("Máquina", "Valor Banco"),
                    list(relatorio_pagamento.get("cartao_maquina_rows") or []),
                    [250, 110],
                    "Nenhuma transa??o de cart?o sem CF/NF encontrada.",
                ),
                (
                    "Observações",
                    ("Tipo", "Detalhe", "Valor"),
                    list(relatorio_pagamento.get("observacao_rows") or []),
                    [170, 360, 110],
                    "Nenhuma observa??o adicional encontrada.",
                ),
            ]
            keep_empty_sections = False

        actions = QtWidgets.QHBoxLayout()
        actions.addStretch()
        actions.addWidget(
            self._create_export_button(
                "Imprimir",
                lambda: self._export_pix_pdf(relatorio_pagamento),
            )
        )
        actions.addStretch()
        layout.addLayout(actions)

        for title, headers, rows, widths, empty_message in sections:
            if not rows and not keep_empty_sections and title != "Observações":
                continue
            label = QtWidgets.QLabel(corrigir_texto(title))
            label.setAlignment(QtCore.Qt.AlignCenter)
            label_font = label.font()
            label_font.setPointSize(max(8, label_font.pointSize() - 1))
            label.setFont(label_font)
            layout.addWidget(label)
            layout.addWidget(
                self._build_simple_centered_table(
                    headers,
                    rows,
                    widths,
                    empty_message,
                )
            )

        layout.addStretch()
        return widget

    def _build_pix_tab(self, relatorio_pix: dict) -> QtWidgets.QWidget:
        relatorio_pix = corrigir_estrutura_texto(relatorio_pix or {})
        if relatorio_pix.get("categoria") == "alertas_eh":
            return self._build_bank_reconciliation_tab(relatorio_pix)

        widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(10)

        summary_items = self._build_pix_summary_items(relatorio_pix)
        layout.addWidget(
            self._wrap_centered(
                self._build_summary_frame(
                    summary_items,
                    {corrigir_texto(relatorio_pix.get("total_label") or "Total pagamentos digitais")},
                ),
                560,
            )
        )

        if relatorio_pix.get("arquivo"):
            actions = QtWidgets.QHBoxLayout()
            actions.addStretch()
            actions.addWidget(
                self._create_export_button(
                    "Imprimir",
                    lambda: self._export_pix_pdf(relatorio_pix),
                )
            )
            actions.addStretch()
            layout.addLayout(actions)

        section_label = QtWidgets.QLabel(relatorio_pix.get("section_label") or "Transações de pagamento")
        section_label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(section_label)

        headers = tuple(corrigir_texto(str(value)) for value in (relatorio_pix.get("table_headers") or ("Data da venda", "Valor bruto")))
        rows = self._build_payment_table_rows(relatorio_pix)
        table = QtWidgets.QTableWidget()
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        table.setAlternatingRowColors(True)
        table.verticalHeader().setVisible(False)
        table.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Expanding)
        table.setMinimumSize(0, 0)
        table.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self._configure_resizable_table(table, self._build_payment_table_widths(relatorio_pix))

        table.setRowCount(len(rows))
        for row_index, values in enumerate(rows):
            for col_index, value in enumerate(values):
                item = QtWidgets.QTableWidgetItem(corrigir_texto(str(value)))
                item.setTextAlignment(QtCore.Qt.AlignCenter)
                table.setItem(row_index, col_index, item)

        if not rows:
            table.setRowCount(1)
            empty_item = QtWidgets.QTableWidgetItem(corrigir_texto(self._build_pix_empty_message(relatorio_pix)))
            empty_item.setTextAlignment(QtCore.Qt.AlignCenter)
            table.setSpan(0, 0, 1, len(headers))
            table.setItem(0, 0, empty_item)

        self._fit_table_width(table)
        table_wrap = QtWidgets.QHBoxLayout()
        table_wrap.addStretch()
        table_wrap.addWidget(table)
        table_wrap.addStretch()
        layout.addLayout(table_wrap, 1)

        return widget

    def _build_summary_frame(
        self,
        items,
        highlighted_labels: set[str] | None = None,
        fechamento: dict | None = None,
        action_widgets: dict[str, QtWidgets.QWidget] | None = None,
    ) -> QtWidgets.QFrame:
        highlighted_labels = {corrigir_texto(str(label)) for label in (highlighted_labels or set())}
        action_widgets = action_widgets or {}
        frame = QtWidgets.QFrame()
        frame.setSizePolicy(QtWidgets.QSizePolicy.Maximum, QtWidgets.QSizePolicy.Fixed)
        layout = QtWidgets.QGridLayout(frame)
        layout.setContentsMargins(6, 0, 6, 0)
        layout.setHorizontalSpacing(12)
        layout.setVerticalSpacing(8)
        layout.setAlignment(QtCore.Qt.AlignCenter)

        for row, (label_text, value_text) in enumerate(items):
            label_text = corrigir_texto(str(label_text))
            value_text = corrigir_texto(str(value_text))
            label = QtWidgets.QLabel(f"{label_text}:")
            value = QtWidgets.QLabel(value_text)
            label.setAlignment(QtCore.Qt.AlignCenter)
            value.setAlignment(QtCore.Qt.AlignCenter)
            label_font = label.font()
            label_font.setPointSize(max(10, label_font.pointSize() + 1))
            label.setFont(label_font)
            value_font_base = value.font()
            value_font_base.setPointSize(max(10, value_font_base.pointSize() + 1))
            value.setFont(value_font_base)

            if label_text in highlighted_labels:
                value_font = value.font()
                value_font.setBold(True)
                value_font.setPointSize(value_font.pointSize())
                value.setFont(value_font)

            if label_text in highlighted_labels and label_text not in {"Total Pendências"}:
                value.setStyleSheet("color:#59C734;")
            if label_text in {"Total Pendências"}:
                cor = "#59C734"
                if fechamento and fechamento.get("status") != "Confere":
                    cor = "#FF4D4F"
                value.setStyleSheet(f"color:{cor};")

            value_box = QtWidgets.QWidget()
            value_layout = QtWidgets.QHBoxLayout(value_box)
            value_layout.setContentsMargins(0, 0, 0, 0)
            value_layout.setSpacing(4)
            value_layout.addStretch()
            value_layout.addWidget(value, 0, QtCore.Qt.AlignCenter)
            if label_text in action_widgets:
                value_layout.addWidget(action_widgets[label_text], 0, QtCore.Qt.AlignCenter)
            value_layout.addStretch()

            layout.addWidget(label, row, 0, QtCore.Qt.AlignCenter)
            layout.addWidget(value_box, row, 1, QtCore.Qt.AlignCenter)

        return frame

    def _create_export_button(self, text: str, callback) -> QtWidgets.QPushButton:
        button = QtWidgets.QPushButton(text)
        button.setStyleSheet("text-align:center;")
        button.clicked.connect(callback)
        return button

    def _configure_resizable_table(self, table: QtWidgets.QTableWidget, widths: list[int]) -> None:
        header = table.horizontalHeader()
        header.setSectionResizeMode(QtWidgets.QHeaderView.Interactive)
        table.setHorizontalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        table.setVerticalScrollMode(QtWidgets.QAbstractItemView.ScrollPerPixel)
        table.setMinimumSize(0, 0)
        for index, width in enumerate(widths):
            table.setColumnWidth(index, width)
        table.verticalHeader().setSectionResizeMode(QtWidgets.QHeaderView.ResizeToContents)

    def _fit_table_width(self, table: QtWidgets.QTableWidget) -> None:
        width = table.horizontalHeader().length() + table.frameWidth() * 2 + 2
        if table.verticalScrollBar().isVisible():
            width += table.verticalScrollBar().sizeHint().width()
        table.setFixedWidth(width)

    def _create_configured_printer(self) -> QtPrintSupport.QPrinter:
        return _create_a4_printer()

    def _get_default_printer(self) -> QtPrintSupport.QPrinter:
        return _resolve_default_printer()

    def _print_section_widths(
        self,
        section_title: object,
        headers: tuple[str, ...],
        widths: object,
    ) -> object:
        normalized_title = corrigir_texto(str(section_title or "")).strip().casefold()
        if normalized_title == "transacoes bancarias sem cf/nf" and len(headers) == 3:
            return [85, 325, 170]
        return widths

    def _column_width_percentages(self, widths: object, column_count: int) -> list[float]:
        values: list[float] = []
        for raw in list(widths or [])[:column_count]:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                value = 0.0
            values.append(max(0.0, value))
        if len(values) < column_count:
            values.extend([0.0] * (column_count - len(values)))
        total = sum(values)
        if total <= 0:
            return []
        return [(value / total) * 100.0 for value in values]

    def _build_print_document_html(
        self,
        title: str,
        summary_items,
        sections,
    ) -> str:
        from html import escape

        def _cell(value: object) -> str:
            return escape(corrigir_texto(str(value or "")))

        font_family = escape(corrigir_texto(self.font().family() or "Lexend"))
        html_parts = [
            "<html><head><meta charset='utf-8'>",
            "<style>",
            f"body{{font-family:'{font_family}',Arial,Helvetica,sans-serif;font-size:7.4pt;color:#000;margin:0;padding:0;}}",
            ".page{width:100%;max-width:none;margin:0 auto;padding:0;text-align:center;}",
            "h1{font-size:9pt;text-align:center;margin:0 0 4px 0;white-space:nowrap;overflow-wrap:normal;word-break:normal;}",
            "h2{font-size:7pt;text-align:center;margin:3px 0 2px 0;font-weight:600;}",
            "table{width:100%;border-collapse:collapse;table-layout:fixed;margin:0 0 6px 0;}",
            ".section-table th:last-child,.section-table td:last-child{white-space:nowrap;overflow-wrap:normal;word-break:keep-all;}",
            "th{background:#000;color:#fff;font-weight:bold;}",
            "th,td{border:1px solid #777;padding:2px 3px;text-align:center;vertical-align:middle;word-wrap:break-word;overflow-wrap:anywhere;font-size:6.6pt;}",
            "td{background:#fff;color:#000;}",
            "</style></head><body><div class='page'>",
            f"<h1>{_cell(title)}</h1>",
            "<table><thead><tr><th>Campo</th><th>Valor</th></tr></thead><tbody>",
        ]

        for label, value in summary_items:
            html_parts.append(f"<tr><td>{_cell(label)}</td><td>{_cell(value)}</td></tr>")
        html_parts.append("</tbody></table>")

        for section_title, headers, rows, widths, empty_message in sections:
            headers = tuple(headers or ())
            rows = list(rows or [])
            if section_title:
                html_parts.append(f"<h2>{_cell(section_title)}</h2>")
            width_percentages = self._column_width_percentages(
                self._print_section_widths(section_title, headers, widths),
                len(headers),
            )
            html_parts.append("<table class='section-table'>")
            if width_percentages:
                html_parts.append("<colgroup>")
                for width_pct in width_percentages:
                    html_parts.append(f"<col style='width:{width_pct:.2f}%'>")
                html_parts.append("</colgroup>")
            html_parts.append("<thead><tr>")
            for header in headers:
                html_parts.append(f"<th>{_cell(header)}</th>")
            html_parts.append("</tr></thead><tbody>")
            if rows:
                for row in rows:
                    normalized_row = list(row[: len(headers)])
                    if len(normalized_row) < len(headers):
                        normalized_row.extend([""] * (len(headers) - len(normalized_row)))
                    html_parts.append("<tr>")
                    for value in normalized_row:
                        html_parts.append(f"<td>{_cell(value)}</td>")
                    html_parts.append("</tr>")
            else:
                col_span = max(1, len(headers))
                html_parts.append(f"<tr><td colspan='{col_span}'>{_cell(empty_message)}</td></tr>")
            html_parts.append("</tbody></table>")

        html_parts.append("</div></body></html>")
        return "".join(html_parts)

    def _print_html_document(self, html: str, printer: QtPrintSupport.QPrinter) -> None:
        _render_html_document_to_printer(html, printer, self.font().family() or "Lexend")

    def _print_simple_report_to_default_printer(
        self,
        title: str,
        summary_items,
        section_title: str,
        headers,
        rows,
        widths,
        empty_message: str,
    ) -> None:
        html = self._build_print_document_html(
            title=title,
            summary_items=summary_items,
            sections=[(section_title, headers, rows, widths, empty_message)],
        )
        self._print_html_document(html, self._get_default_printer())

    def _print_sectioned_report_to_default_printer(
        self,
        title: str,
        summary_items,
        sections,
    ) -> None:
        html = self._build_print_document_html(
            title=title,
            summary_items=summary_items,
            sections=sections,
        )
        self._print_html_document(html, self._get_default_printer())

    def build_automation_bundle_jobs(self) -> list[tuple[str, str]]:
        jobs: list[tuple[str, str]] = []
        if not self._fechamento:
            return jobs

        fechamento = self._fechamento
        fechamento_title = (
            "Fechamento de Caixa - Eletronica Horizonte"
            if str(fechamento.get("caixa_modelo") or "").upper() == "EH"
            else "Fechamento de Caixa - MVA"
        )
        bank_report = self._get_fechamento_bank_report(fechamento)
        if bank_report:
            report = self._enrich_eh_bank_report(fechamento, bank_report)
            html = self._build_print_document_html(
                title=fechamento_title,
                summary_items=report.get("summary_items") or (),
                sections=self._build_bank_sections(report),
            )
            jobs.append((fechamento_title, html))
            return jobs

        fechamento_rows = [
            (
                item.get("numero_exibicao", ""),
                "-" if item.get("valor") in (None, "") else f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
            for item in fechamento.get("registros_conferencia", [])
        ]
        if fechamento_rows:
            fechamento_rows.append(
                (
                    "Total faltante",
                    f"R$ {format_number_br(fechamento.get('valor_faltantes', 0.0))}",
                )
            )
        html = self._build_print_document_html(
            title=fechamento_title,
            summary_items=self._build_fechamento_summary_items(fechamento),
            sections=[
                (
                    self._build_fechamento_section_title(fechamento),
                    ("Numero", "Valor"),
                    fechamento_rows,
                    [260, 140],
                    self._build_fechamento_empty_message(fechamento),
                )
            ],
        )
        jobs.append((fechamento_title, html))
        return jobs

    def print_automation_jobs(self, jobs: list[tuple[str, str]]) -> list[str]:
        printed_titles: list[str] = []
        for title, html in jobs:
            self._print_html_document(html, self._get_default_printer())
            printed_titles.append(title)
        return printed_titles

    def print_automation_bundle(self) -> list[str]:
        return self.print_automation_jobs(self.build_automation_bundle_jobs())

    def _export_davs_pdf(self, relatorio: dict) -> None:
        summary_items = self._build_davs_summary_items(relatorio)
        rows = [
            (
                self._display_numero(item.get("pedido", "")),
                item.get("cliente", ""),
                item.get("documento", ""),
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
            for item in relatorio.get("itens_excluidos", [])
        ]
        title = "Relatório de Caixa - DAVs Importados"
        if relatorio.get("caixa_modelo") == "EH":
            title = "Relatório de Caixa - Pedidos Caixa"
        self._export_report_pdf(
            title=title,
            default_name="relatorio_caixa_davs.pdf",
            summary_items=summary_items,
            headers=self._build_davs_table_headers(relatorio),
            rows=rows,
            empty_message=self._build_davs_empty_message(relatorio),
        )

    def _export_fechamento_pdf(self, fechamento: dict) -> None:
        bank_report = self._get_fechamento_bank_report(fechamento)
        if bank_report:
            report = self._enrich_eh_bank_report(fechamento, bank_report)
            self._export_sectioned_report_pdf(
                title="Fechamento de Caixa - Eletrônica Horizonte" if str(fechamento.get("caixa_modelo") or "").upper() == "EH" else "Fechamento de Caixa - MVA",
                default_name="relatorio_caixa_fechamento_eh.pdf" if str(fechamento.get("caixa_modelo") or "").upper() == "EH" else "relatorio_caixa_fechamento_mva.pdf",
                summary_items=report.get("summary_items") or (),
                sections=self._build_bank_sections(report),
            )
            return

        summary_items = self._build_fechamento_summary_items(fechamento)
        rows = [
            (
                item.get("numero_exibicao", ""),
                "-" if item.get("valor") in (None, "") else f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
            for item in fechamento.get("registros_conferencia", [])
        ]
        if rows:
            rows.append(
                (
                    "Total faltante",
                    f"R$ {format_number_br(fechamento.get('valor_faltantes', 0.0))}",
                )
            )
        self._export_report_pdf(
            title=f"Fechamento de Caixa - {'MVA' if fechamento.get('caixa_modelo') == 'MVA' else 'Eletrônica Horizonte'}",
            default_name="relatorio_caixa_fechamento.pdf",
            summary_items=summary_items,
            headers=("Número", "Valor"),
            rows=rows,
            empty_message=self._build_fechamento_empty_message(fechamento),
        )

    def _export_pix_pdf(self, relatorio_pix: dict) -> None:
        if relatorio_pix.get("categoria") == "alertas_eh":
            self._export_sectioned_report_pdf(
                title="Fechamento de Caixa - Eletrônica Horizonte" if str(relatorio_pix.get("caixa_modelo") or "EH").upper() == "EH" else "Fechamento de Caixa - MVA",
                default_name="relatorio_caixa_fechamento_eh.pdf" if str(relatorio_pix.get("caixa_modelo") or "EH").upper() == "EH" else "relatorio_caixa_fechamento_mva.pdf",
                summary_items=relatorio_pix.get("summary_items") or (),
                sections=self._build_bank_sections(relatorio_pix),
            )
            return

        summary_items = self._build_pix_summary_items(relatorio_pix)
        headers = tuple(corrigir_texto(str(value)) for value in (relatorio_pix.get("table_headers") or ("Data da venda", "Valor bruto")))
        rows = self._build_payment_table_rows(relatorio_pix)
        categoria = re.sub(r"[^a-z0-9]+", "_", str(relatorio_pix.get("categoria") or "pagamento").casefold()).strip("_")
        self._export_report_pdf(
            title=relatorio_pix.get("export_title") or f"Fechamento de Caixa - {relatorio_pix.get('tab_title') or 'Pagamentos'}",
            default_name=relatorio_pix.get("export_name") or f"relatorio_caixa_{categoria or 'pagamento'}.pdf",
            summary_items=summary_items,
            headers=headers,
            rows=rows,
            empty_message=self._build_pix_empty_message(relatorio_pix),
        )

    def _export_report_pdf(
        self,
        title: str,
        default_name: str,
        summary_items,
        headers,
        rows,
        empty_message: str,
    ) -> None:
        from reportlab.lib.pagesizes import A4
        from reportlab.pdfgen import canvas

        title = corrigir_texto(title)
        summary_items = [(corrigir_texto(str(label)), corrigir_texto(str(value))) for label, value in summary_items]
        headers = tuple(corrigir_texto(str(header)) for header in headers)
        rows = [tuple(corrigir_texto(str(value)) for value in row) for row in rows]
        empty_message = corrigir_texto(str(empty_message))

        path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf")],
            title=title,
        )
        if not path:
            return
        if os.path.isdir(path):
            path = os.path.join(path, default_name)

        pdf = canvas.Canvas(path, pagesize=A4)
        width, height = A4
        y = height - 40
        title_font_name, body_font_name = _get_reportlab_font_names()

        def new_page():
            nonlocal y
            pdf.showPage()
            y = height - 40
            pdf.setFont(body_font_name, 9)

        pdf.setFont(title_font_name, 12)
        pdf.drawString(40, y, title)
        y -= 24

        pdf.setFont(body_font_name, 9)
        for label, value in summary_items:
            text = f"{label}: {value}"
            for part in self._split_pdf_text(text, 95):
                if y < 45:
                    new_page()
                pdf.drawString(40, y, part)
                y -= 13
        y -= 8

        if y < 60:
            new_page()

        pdf.setFont(title_font_name, 9)
        pdf.drawString(40, y, " | ".join(headers))
        y -= 14
        pdf.setFont(body_font_name, 8.5)

        if not rows:
            pdf.drawString(40, y, empty_message)
        else:
            for row in rows:
                line = " | ".join(str(value) for value in row)
                for part in self._split_pdf_text(line, 120):
                    if y < 45:
                        new_page()
                    pdf.drawString(40, y, part)
                    y -= 11

        pdf.save()
        messagebox.showinfo("Exportado", f"PDF salvo em:\n{path}")

    def _export_sectioned_report_pdf(
        self,
        title: str,
        default_name: str,
        summary_items,
        sections,
    ) -> None:
        from html import escape
        from PySide6 import QtPrintSupport

        def _print_section_widths(
            section_title: object,
            headers: tuple[str, ...],
            widths: object,
        ) -> object:
            normalized_title = corrigir_texto(str(section_title or "")).strip().casefold()
            if normalized_title == "transações bancárias sem cf/nf" and len(headers) == 3:
                return [85, 325, 170]
            return widths

        def _column_width_percentages(widths: object, column_count: int) -> list[float]:
            values: list[float] = []
            for raw in list(widths or [])[:column_count]:
                try:
                    value = float(raw)
                except (TypeError, ValueError):
                    value = 0.0
                values.append(max(0.0, value))
            if len(values) < column_count:
                values.extend([0.0] * (column_count - len(values)))
            total = sum(values)
            if total <= 0:
                return []
            return [(value / total) * 100.0 for value in values]

        printer = QtPrintSupport.QPrinter(QtPrintSupport.QPrinter.HighResolution)
        printer.setPageSize(QtGui.QPageSize(QtGui.QPageSize.A4))
        printer.setPageMargins(QtCore.QMarginsF(6, 6, 6, 6), QtGui.QPageLayout.Millimeter)
        print_dialog = QtPrintSupport.QPrintDialog(printer, self)
        print_dialog.setWindowTitle(corrigir_texto("Selecionar impressora"))
        if print_dialog.exec() != QtWidgets.QDialog.Accepted:
            return

        def _cell(value: object) -> str:
            return escape(corrigir_texto(str(value or "")))

        font_family = escape(corrigir_texto(self.font().family() or "Lexend"))

        html_parts = [
            "<html><head><meta charset='utf-8'>",
            "<style>",
            f"body{{font-family:'{font_family}',Arial,Helvetica,sans-serif;font-size:7.4pt;color:#000;margin:0;padding:0;}}",
            ".page{width:100%;max-width:none;margin:0 auto;padding:0;text-align:center;}",
            "h1{font-size:9pt;text-align:center;margin:0 0 4px 0;white-space:nowrap;overflow-wrap:normal;word-break:normal;}",
            "h2{font-size:7pt;text-align:center;margin:3px 0 2px 0;font-weight:600;}",
            "table{width:100%;border-collapse:collapse;table-layout:fixed;margin:0 0 6px 0;}",
            ".section-table th:last-child,.section-table td:last-child{white-space:nowrap;overflow-wrap:normal;word-break:keep-all;}",
            "th{background:#000;color:#fff;font-weight:bold;}",
            "th,td{border:1px solid #777;padding:2px 3px;text-align:center;vertical-align:middle;word-wrap:break-word;overflow-wrap:anywhere;font-size:6.6pt;}",
            "td{background:#fff;color:#000;}",
            "</style></head><body><div class='page'>",
            f"<h1>{_cell(title)}</h1>",
            "<table><thead><tr><th>Campo</th><th>Valor</th></tr></thead><tbody>",
        ]

        for label, value in summary_items:
            html_parts.append(f"<tr><td>{_cell(label)}</td><td>{_cell(value)}</td></tr>")
        html_parts.append("</tbody></table>")

        for section_title, headers, rows, _widths, empty_message in sections:
            headers = tuple(headers or ())
            rows = list(rows or [])
            html_parts.append(f"<h2>{_cell(section_title)}</h2>")
            width_percentages = _column_width_percentages(
                _print_section_widths(section_title, headers, _widths),
                len(headers),
            )
            html_parts.append("<table class='section-table'>")
            if width_percentages:
                html_parts.append("<colgroup>")
                for width_pct in width_percentages:
                    html_parts.append(f"<col style='width:{width_pct:.2f}%'>")
                html_parts.append("</colgroup>")
            html_parts.append("<thead><tr>")
            for header in headers:
                html_parts.append(f"<th>{_cell(header)}</th>")
            html_parts.append("</tr></thead><tbody>")
            if rows:
                for row in rows:
                    normalized_row = list(row[: len(headers)])
                    if len(normalized_row) < len(headers):
                        normalized_row.extend([""] * (len(headers) - len(normalized_row)))
                    html_parts.append("<tr>")
                    for value in normalized_row:
                        html_parts.append(f"<td>{_cell(value)}</td>")
                    html_parts.append("</tr>")
            else:
                col_span = max(1, len(headers))
                html_parts.append(f"<tr><td colspan='{col_span}'>{_cell(empty_message)}</td></tr>")
            html_parts.append("</tbody></table>")

        html_parts.append("</div></body></html>")

        document = QtGui.QTextDocument()
        document.setDefaultFont(QtGui.QFont(self.font().family() or "Lexend", 8))
        page_rect = printer.pageRect(QtPrintSupport.QPrinter.Point)
        document.setPageSize(page_rect.size())
        document.setHtml("".join(html_parts))
        document.print_(printer)
        messagebox.showinfo(corrigir_texto("Impress\u00e3o"), corrigir_texto("Relat\u00f3rio enviado para impress\u00e3o."))

    def _split_pdf_text(self, text: str, max_chars: int) -> list[str]:
        text = str(text or "")
        if len(text) <= max_chars:
            return [text]
        parts = []
        current = []
        current_len = 0
        for word in text.split():
            projected = current_len + len(word) + (1 if current else 0)
            if projected > max_chars and current:
                parts.append(" ".join(current))
                current = [word]
                current_len = len(word)
            else:
                current.append(word)
                current_len = projected
        if current:
            parts.append(" ".join(current))
        return parts or [text]

    def _display_numero(self, numero: str) -> str:
        digits = re.sub(r"\D", "", str(numero or ""))
        if not digits:
            return ""
        return str(int(digits))


class BarChartWidget(QtWidgets.QWidget):
    def __init__(self) -> None:
        super().__init__()
        self._data = []
        self._bar_color = QtGui.QColor("#59C734")
        self._bar_gradient = None
        self._dual_mode = False
        self._dual_colors = (QtGui.QColor("#FF8A00"), QtGui.QColor("#2F6BFF"))
        self._label_color = QtGui.QColor("#f2f2f2")
        self._row_height = 28
        self._label_width = 190
        self._value_padding = 120
        self._padding = 16

    def set_data(
        self,
        data,
        color: QtGui.QColor,
        show_decimals: bool = False,
        gradient_colors: tuple[QtGui.QColor, QtGui.QColor] | None = None,
    ) -> None:
        self._data = data
        self._bar_color = color
        self._show_decimals = show_decimals
        self._bar_gradient = gradient_colors
        self._dual_mode = False
        self._update_size()
        self.update()

    def set_data_dual(
        self,
        data,
        colors: tuple[QtGui.QColor, QtGui.QColor],
        show_decimals: bool = False,
    ) -> None:
        self._data = data
        self._dual_colors = colors
        self._show_decimals = show_decimals
        self._bar_gradient = None
        self._dual_mode = True
        self._update_size()
        self.update()

    def _update_size(self) -> None:
        height = max(1, len(self._data)) * self._row_height + self._padding * 2
        self.setMinimumHeight(height)

    def paintEvent(self, event: QtGui.QPaintEvent) -> None:
        painter = QtGui.QPainter(self)
        painter.setRenderHint(QtGui.QPainter.Antialiasing)
        rect = self.rect()
        painter.fillRect(rect, QtGui.QColor("#1e1e1e"))

        if not self._data:
            painter.setPen(self._label_color)
            painter.drawText(rect, QtCore.Qt.AlignCenter, "Sem dados para exibir.")
            return

        if self._dual_mode:
            max_value = max(max(v1, v2) for _, v1, v2 in self._data) if self._data else 1.0
        else:
            max_value = max(v for _, v in self._data) if self._data else 1.0
        max_value = max(max_value, 1.0)

        bar_area_width = rect.width() - self._label_width - self._padding * 2 - self._value_padding
        y = self._padding
        for entry in self._data:
            if self._dual_mode:
                label, value_a, value_b = entry
            else:
                label, value_a = entry
                value_b = 0.0
            label_rect = QtCore.QRect(
                self._padding,
                y,
                self._label_width - self._padding,
                self._row_height,
            )
            painter.setPen(self._label_color)
            painter.drawText(label_rect, QtCore.Qt.AlignVCenter | QtCore.Qt.AlignLeft, str(label))

            if self._dual_mode:
                bar_width_a = int((value_a / max_value) * bar_area_width)
                bar_width_b = int((value_b / max_value) * bar_area_width)
                bar_height = (self._row_height - 12) // 2
                bar_rect_a = QtCore.QRect(
                    self._label_width + self._padding,
                    y + 4,
                    max(2, bar_width_a) if value_a > 0 else 0,
                    bar_height,
                )
                bar_rect_b = QtCore.QRect(
                    self._label_width + self._padding,
                    y + 6 + bar_height,
                    max(2, bar_width_b) if value_b > 0 else 0,
                    bar_height,
                )
                painter.setPen(QtCore.Qt.NoPen)
                painter.setBrush(self._dual_colors[0])
                if bar_rect_a.width() > 0:
                    painter.drawRoundedRect(bar_rect_a, 4, 4)
                painter.setBrush(self._dual_colors[1])
                if bar_rect_b.width() > 0:
                    painter.drawRoundedRect(bar_rect_b, 4, 4)
            else:
                bar_width = int((value_a / max_value) * bar_area_width)
                bar_rect = QtCore.QRect(
                    self._label_width + self._padding,
                    y + 6,
                    max(2, bar_width),
                    self._row_height - 12,
                )
                if self._bar_gradient:
                    grad = QtGui.QLinearGradient(bar_rect.topLeft(), bar_rect.topRight())
                    grad.setColorAt(0.0, self._bar_gradient[0])
                    grad.setColorAt(1.0, self._bar_gradient[1])
                    painter.setBrush(grad)
                else:
                    painter.setBrush(self._bar_color)
                painter.setPen(QtCore.Qt.NoPen)
                painter.drawRoundedRect(bar_rect, 4, 4)
            painter.setPen(self._label_color)
            if self._dual_mode:
                if getattr(self, "_show_decimals", False):
                    text_a = f"R$ {value_a:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                    text_b = f"R$ {value_b:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                else:
                    text_a = f"{int(round(value_a))}"
                    text_b = f"{int(round(value_b))}"

                if bar_rect_a.width() > 0:
                    text_rect_a = QtCore.QRect(
                        bar_rect_a.right() + 6,
                        bar_rect_a.top() - 2,
                        self._value_padding - self._padding,
                        bar_rect_a.height() + 4,
                    )
                    painter.drawText(text_rect_a, QtCore.Qt.AlignVCenter | QtCore.Qt.AlignLeft, text_a)
                if bar_rect_b.width() > 0:
                    text_rect_b = QtCore.QRect(
                        bar_rect_b.right() + 6,
                        bar_rect_b.top() - 2,
                        self._value_padding - self._padding,
                        bar_rect_b.height() + 4,
                    )
                    painter.drawText(text_rect_b, QtCore.Qt.AlignVCenter | QtCore.Qt.AlignLeft, text_b)
            else:
                value_rect = QtCore.QRect(
                    rect.width() - self._value_padding,
                    y,
                    self._value_padding - self._padding,
                    self._row_height,
                )
                if getattr(self, "_show_decimals", False):
                    text = f"R$ {value_a:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                else:
                    text = f"{int(round(value_a))}"
                painter.drawText(value_rect, QtCore.Qt.AlignVCenter | QtCore.Qt.AlignLeft, text)

            y += self._row_height


def exportar_planilha_pdf(tree: QtTreeAdapter, titulo: str) -> None:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    rows = [tree.item(i)["values"] for i in tree.get_children()]
    cols = [tree.heading(col)["text"] for col in tree["columns"]]

    if not rows:
        messagebox.showwarning("Aviso", "Não há dados para exportar.")
        return

    caminho = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("PDF", "*.pdf")],
        title=f"Exportar {titulo}",
    )
    if not caminho:
        return

    c = canvas.Canvas(caminho, pagesize=A4)
    largura, altura = A4
    y = altura - 40
    title_font_name, body_font_name = _get_reportlab_font_names()

    c.setFont(title_font_name, 12)
    c.drawString(50, y, titulo)
    y -= 24

    c.setFont(title_font_name, 9)
    for i, col in enumerate(cols):
        c.drawString(50 + i * 150, y, col)
    y -= 16

    c.setFont(body_font_name, 8.5)
    for row in rows:
        for i, val in enumerate(row):
            c.drawString(50 + i * 150, y, str(val))
        y -= 18
        if y < 40:
            c.showPage()
            y = altura - 40
            c.setFont(body_font_name, 8.5)

    c.save()
    messagebox.showinfo("Sucesso", f"PDF salvo em:\n{caminho}")


def exportar_feedbacks_pdf(vendedor: str, feedbacks: list) -> None:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    caminho = filedialog.asksaveasfilename(
        defaultextension=".pdf",
        filetypes=[("Arquivo PDF", "*.pdf")],
        title=f"Exportar feedbacks de {vendedor}",
    )
    if not caminho:
        return

    c = canvas.Canvas(caminho, pagesize=A4)
    largura, altura = A4
    y = altura - 50
    title_font_name, body_font_name = _get_reportlab_font_names()

    c.setFont(title_font_name, 12)
    c.drawString(50, y, f"Feedbacks - {vendedor}")
    y -= 24
    c.setFont(body_font_name, 9)

    feedbacks_por_vendedor = {}
    for fb in feedbacks:
        nome = fb.get("vendedor", "Desconhecido")
        feedbacks_por_vendedor.setdefault(nome, []).append(fb)

    for vendedor_nome, lista in feedbacks_por_vendedor.items():
        if len(feedbacks_por_vendedor) > 1:
            c.setFont(title_font_name, 10)
            c.drawString(50, y, f"Vendedor: {vendedor_nome}")
            y -= 16
            c.setFont(body_font_name, 9)

        for fb in lista:
            linha = f"{fb['created_at'][:19]} - {fb['feedback']}"
            for parte in linha.split("\n"):
                c.drawString(50, y, parte)
                y -= 15
                if y < 50:
                    c.showPage()
                    c.setFont(body_font_name, 9)
                    y = altura - 50

        y -= 10

    c.save()
    messagebox.showinfo("Exportado", f"Feedbacks exportados para:\n{caminho}")


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Relatório de Vendedor")
        self.setMinimumSize(1050, 800)
        self.resize(1200, 860)
        self._setup_icon()
        self._load_app_font()
        self._sort_state = {}
        self._edit_mode = False
        self._table_dirty = False
        self._neon_on = False
        self._neon_hue = 0
        self._showing_graphs = False
        self._automation_enabled = True
        self._automation_time = QtCore.QTime(8, 0)
        self._automation_running = False
        self._automation_next_run: dt.datetime | None = None
        self._automation_test_run_at: dt.datetime | None = None
        self._automation_last_status = "Automacao diaria pronta. Sempre usa o dia anterior."

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        main_layout = QtWidgets.QHBoxLayout(central)
        main_layout.setContentsMargins(10, 10, 10, 10)

        self._setup_styles()

        left_panel = QtWidgets.QVBoxLayout()
        left_panel.setAlignment(QtCore.Qt.AlignTop)
        left_panel.setSpacing(8)
        left_panel.addStretch(1)

        self.btn_select_pdf = QtWidgets.QPushButton("Importar")
        self.btn_select_pdf.setObjectName("btn_import")
        btn_policy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Fixed)
        self.btn_caixa = QtWidgets.QPushButton("Caixa")
        self.btn_spreadsheet = QtWidgets.QPushButton("Planilha online")
        self.btn_export = QtWidgets.QPushButton("Exportar")
        self.btn_edit_table = QtWidgets.QPushButton("Editar")
        self.btn_graphs = QtWidgets.QPushButton("Gráficos")
        self.btn_clear = QtWidgets.QPushButton("Limpar")
        self.btn_merge = QtWidgets.QPushButton("Mesclar Planilhas")
        self.btn_tag = QtWidgets.QPushButton("Criar Etiquetas")
        self.btn_feedback = QtWidgets.QPushButton("Feedback")
        self.btn_automation_test = QtWidgets.QPushButton("Teste automacao (5s)")
        for btn in (
            self.btn_select_pdf,
            self.btn_caixa,
            self.btn_spreadsheet,
            self.btn_export,
            self.btn_edit_table,
            self.btn_graphs,
            self.btn_clear,
            self.btn_merge,
            self.btn_tag,
            self.btn_feedback,
            self.btn_automation_test,
        ):
            btn.setSizePolicy(btn_policy)
            btn.setMinimumHeight(36)

        for btn in (
            self.btn_tag,
            self.btn_edit_table,
            self.btn_export,
            self.btn_feedback,
            self.btn_select_pdf,
            self.btn_caixa,
            self.btn_automation_test,
            self.btn_clear,
            self.btn_merge,
            self.btn_spreadsheet,
        ):
            left_panel.addWidget(btn)
        left_panel.addStretch(3)
        left_panel.addWidget(self.btn_graphs)

        main_layout.addLayout(left_panel, 0)

        right_panel = QtWidgets.QVBoxLayout()
        right_panel.setSpacing(10)

        self.progress_bar = QtWidgets.QProgressBar()
        self.progress_bar.setMinimumHeight(20)
        self.progress_var = QtVar(self.progress_bar.setValue, 0)
        self.progress_bar_adapter = QtProgressBarAdapter(self.progress_bar)
        right_panel.addWidget(self.progress_bar)

        self.progress_bar_online = QtWidgets.QProgressBar()
        self.progress_bar_online.setMinimumHeight(14)
        self.progress_var_online = QtVar(self.progress_bar_online.setValue, 0)
        self.progress_bar_online_adapter = QtProgressBarAdapter(self.progress_bar_online)
        self.progress_bar_online.setVisible(False)
        right_panel.addWidget(self.progress_bar_online)

        self.btn_cancel = QtWidgets.QPushButton("Cancelar")
        self.btn_cancel.setSizePolicy(btn_policy)
        self.btn_cancel.setEnabled(False)
        self.btn_cancel.setStyleSheet("text-align:center;")
        self.btn_automation_power = QtWidgets.QPushButton()
        self.btn_automation_power.setFixedSize(36, 36)
        self.btn_automation_power.setStyleSheet("text-align:center;padding:4px;")
        self.btn_automation_power.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))

        cancel_row = QtWidgets.QHBoxLayout()
        cancel_row.addStretch()
        cancel_row.addWidget(self.btn_cancel)
        cancel_row.addWidget(self.btn_automation_power)
        cancel_row.addStretch()
        right_panel.addLayout(cancel_row)

        self.label_files = QtWidgets.QLabel("Nenhum arquivo carregado ainda")
        self.label_files.setAlignment(QtCore.Qt.AlignCenter)
        right_panel.addWidget(self.label_files)

        self.cols_main = ("Vendedor", "Atendidos", "Devoluções", "Total Final", "Total Vendas")
        self.cols_online = ("Vendedor", "Clientes Atendidos", "Valor Total")
        self.table_main = QtWidgets.QTableWidget()
        self.table_main.setSortingEnabled(False)
        self.table_main.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        self.table_main.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table_main.horizontalHeader().setDefaultAlignment(QtCore.Qt.AlignCenter)
        self.table_main.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        right_panel.addWidget(self.table_main)

        self.tree_main = QtTreeAdapter(self.table_main, self.cols_main)
        self.table_main.horizontalHeader().sectionClicked.connect(
            lambda idx: self._sort_table(self.tree_main, self.cols_main, idx)
        )
        self.table_main.itemChanged.connect(self._on_table_item_changed)

        self.online_container = QtWidgets.QWidget()
        self.online_container.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        online_frame = QtWidgets.QHBoxLayout(self.online_container)
        right_panel.addWidget(self.online_container)

        self._setup_planilha_section(
            online_frame,
            "Planilha Online - MVA",
            self._export_planilha_mva,
            is_left=True,
        )
        self._setup_planilha_section(
            online_frame,
            "Planilha Online - EH",
            self._export_planilha_eh,
            is_left=False,
        )

        self.graphs_view = self._build_graphs_view()
        self.graphs_view.setVisible(False)
        right_panel.addWidget(self.graphs_view)

        self.label_files_var = QtVar(self.label_files.setText, "Nenhum arquivo carregado ainda")
        self.root_adapter = QtRootAdapter(self)
        self._bind_actions()
        self._setup_import_neon()
        self._setup_progress_visibility_timer()
        self._setup_graphs_refresh_timer()
        self._setup_daily_automation_timer()

        set_ui_refs(
            btn_cancel=QtButtonAdapter(self.btn_cancel),
            progress_var=self.progress_var,
            progress_bar=self.progress_bar_adapter,
            progress_var_online=self.progress_var_online,
            progress_bar_online=self.progress_bar_online_adapter,
            btn_tag=QtButtonAdapter(self.btn_tag),
            btn_add_mais=QtButtonAdapter(self.btn_select_pdf),
            btn_merge_spreadsheet=QtButtonAdapter(self.btn_merge),
            btn_select_pdf=QtButtonAdapter(self.btn_select_pdf),
        )
        set_parent(self)
        check_for_updates(self.root_adapter)

        main_layout.addLayout(right_panel, 1)

    def _setup_icon(self) -> None:
        icon_path = resource_path("icone.ico")
        if os.path.exists(icon_path):
            self.setWindowIcon(QtGui.QIcon(icon_path))

    def _load_app_font(self) -> None:
        font_path = resource_path(os.path.join("data", "Lexend-Regular.ttf"))
        if not os.path.exists(font_path):
            return
        font_id = QtGui.QFontDatabase.addApplicationFont(font_path)
        if font_id == -1:
            return
        families = QtGui.QFontDatabase.applicationFontFamilies(font_id)
        if families:
            self.setFont(QtGui.QFont(families[0]))

    def _setup_styles(self) -> None:
        self.setStyleSheet(
            "QWidget{background-color:#1e1e1e;color:#f2f2f2;font-family:'Lexend';}"
            "QPushButton{background-color:#2d2d2d;border-radius:6px;padding:6px 10px;text-align:center;}"
            "QLabel{qproperty-alignment: AlignCenter;}"
            "QPushButton:hover{background-color:#3b3b3b;}"
            "QPushButton:disabled{color:#777;background-color:#2a2a2a;}"
            "QTableWidget{background-color:#1e1e1e;gridline-color:#3b3b3b;border:1px solid #e6e6e6;border-radius:6px;}"
            "QTableWidget::item{text-align:center;}"
            "QHeaderView::section{background-color:#2d2d2d;color:orange;font-weight:bold;text-align:center;}"
            "QProgressBar{background:#2d2d2d;color:white;border:1px solid #333;text-align:center;}"
            "QProgressBar::chunk{background-color:#59C734;}"
        )

    def _setup_planilha_section(self, parent_layout, title, export_fn, is_left: bool) -> None:
        frame = QtWidgets.QFrame()
        frame_layout = QtWidgets.QVBoxLayout(frame)
        header = QtWidgets.QHBoxLayout()

        label = QtWidgets.QLabel(title)
        label.setAlignment(QtCore.Qt.AlignCenter)
        btn_export = QtWidgets.QPushButton()
        btn_export.setFixedSize(30, 30)
        pdf_icon_path = resource_path("pdf_icon.png")
        if os.path.exists(pdf_icon_path):
            btn_export.setIcon(QtGui.QIcon(pdf_icon_path))
        btn_export.clicked.connect(export_fn)
        header.addStretch()
        header.addWidget(label)
        header.addWidget(btn_export)
        header.addStretch()
        frame_layout.addLayout(header)

        cols = self.cols_online
        table = QtWidgets.QTableWidget()
        table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        frame_layout.addWidget(table)

        adapter = QtTreeAdapter(table, cols)
        table.horizontalHeader().sectionClicked.connect(
            lambda idx: self._sort_table(adapter, cols, idx)
        )

        if is_left:
            self.tree_mva = adapter
            self.table_mva = table
        else:
            self.tree_eh = adapter
            self.table_eh = table

        parent_layout.addWidget(frame)

    def _export_planilha_mva(self) -> None:
        exportar_planilha_pdf(self.tree_mva, "Planilha MVA")

    def _export_planilha_eh(self) -> None:
        exportar_planilha_pdf(self.tree_eh, "Planilha EH")

    def _bind_actions(self) -> None:
        self.btn_cancel.clicked.connect(process_cancel)
        self.btn_select_pdf.clicked.connect(self._handle_pdf_button)
        self.btn_caixa.clicked.connect(self._handle_caixa_report)
        self.btn_spreadsheet.clicked.connect(self._handle_load_planilhas)
        self.btn_export.clicked.connect(lambda: self._export_dialog())
        self.btn_edit_table.clicked.connect(self._toggle_table_edit)
        self.btn_graphs.clicked.connect(self._toggle_graphs_view)
        self.btn_clear.clicked.connect(self._handle_clear_tables)
        self.btn_merge.clicked.connect(self._handle_merge_tables)
        self.btn_tag.clicked.connect(lambda: criar_etiquetas(self.tree_main))
        self.btn_tag.setEnabled(False)
        self.btn_feedback.clicked.connect(self._open_feedback)
        self.btn_automation_power.clicked.connect(self._toggle_automation_power)
        self.btn_automation_test.clicked.connect(self._schedule_automation_test_run)

    def _setup_daily_automation_timer(self) -> None:
        self._automation_next_run = self._compute_next_automation_run()
        self._automation_timer = QtCore.QTimer(self)
        self._automation_timer.timeout.connect(self._on_automation_timer_tick)
        self._automation_timer.start(1000)
        self._refresh_automation_controls_ui()

    def _compute_next_automation_run(self, now: dt.datetime | None = None) -> dt.datetime:
        now = now or dt.datetime.now()
        target_time = self._automation_time if self._automation_time.isValid() else QtCore.QTime(8, 0)
        target = now.replace(
            hour=target_time.hour(),
            minute=target_time.minute(),
            second=0,
            microsecond=0,
        )
        if now >= target:
            target += dt.timedelta(days=1)
        return target

    def _automation_target_date_br(self) -> str:
        return QtCore.QDate.currentDate().addDays(-1).toString("dd/MM/yyyy")

    def _automation_time_text(self) -> str:
        return self._automation_time.toString("HH:mm") if self._automation_time.isValid() else "08:00"

    def _format_countdown(self, total_seconds: int) -> str:
        total_seconds = max(0, int(total_seconds))
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _refresh_automation_controls_ui(self) -> None:
        play_icon = self.style().standardIcon(QtWidgets.QStyle.SP_MediaPlay)
        stop_icon = self.style().standardIcon(QtWidgets.QStyle.SP_MediaStop)
        if self._automation_enabled:
            next_run = self._automation_next_run or self._compute_next_automation_run()
            remaining = self._format_countdown(int((next_run - dt.datetime.now()).total_seconds()))
            tooltip = (
                f"Automacao ativa para {self._automation_time_text()}.\n"
                f"Proxima execucao: {next_run.strftime('%d/%m/%Y %H:%M:%S')} (em {remaining}).\n"
                "Clique para pausar."
            )
            self.btn_automation_power.setIcon(stop_icon)
            self.btn_automation_power.setToolTip(corrigir_texto(tooltip))
        else:
            tooltip = (
                f"Automacao pausada. Ultimo horario configurado: {self._automation_time_text()}.\n"
                "Clique para escolher o horario e ligar novamente."
            )
            self.btn_automation_power.setIcon(play_icon)
            self.btn_automation_power.setToolTip(corrigir_texto(tooltip))
        self.btn_automation_power.setEnabled(not self._automation_running)

        if self._automation_test_run_at:
            test_remaining = self._format_countdown(
                int((self._automation_test_run_at - dt.datetime.now()).total_seconds())
            )
            self.btn_automation_test.setText(f"Teste em {test_remaining}")
            self.btn_automation_test.setToolTip(
                corrigir_texto(f"Teste manual agendado para iniciar em {test_remaining}.")
            )
        else:
            self.btn_automation_test.setText("Teste automacao (5s)")
            self.btn_automation_test.setToolTip(
                corrigir_texto("Dispara a automacao do dia anterior em 5 segundos.")
            )

    def _choose_automation_time(self) -> QtCore.QTime | None:
        dialog = AutomationTimeDialog(self, self._automation_time if self._automation_time.isValid() else QtCore.QTime(8, 0))
        try:
            return dialog.selected_time()
        finally:
            dialog.deleteLater()

    def _toggle_automation_power(self) -> None:
        if self._automation_running:
            return
        if self._automation_enabled:
            self._automation_enabled = False
            self._automation_next_run = None
            self._automation_last_status = "Automacao pausada pelo usuario."
            self._refresh_automation_controls_ui()
            return

        selected_time = self._choose_automation_time()
        if selected_time is None:
            return

        self._automation_time = selected_time
        self._automation_enabled = True
        self._automation_next_run = self._compute_next_automation_run()
        self._automation_last_status = (
            f"Automacao retomada para {self._automation_time_text()}. Proximo alvo: {self._automation_target_date_br()}."
        )
        self._refresh_automation_controls_ui()

    def _schedule_automation_test_run(self) -> None:
        if self._automation_running:
            return
        self._automation_test_run_at = dt.datetime.now() + dt.timedelta(seconds=5)
        self._automation_last_status = (
            f"Teste de automacao armado para {self._automation_target_date_br()}."
        )
        self._refresh_automation_controls_ui()

    def _on_automation_timer_tick(self) -> None:
        now = dt.datetime.now()
        if (
            self._automation_test_run_at is not None
            and now >= self._automation_test_run_at
            and not self._automation_running
        ):
            self._automation_test_run_at = None
            self._run_scheduled_automation("teste manual", notify_user=True)
            return

        if (
            self._automation_enabled
            and self._automation_next_run is not None
            and now >= self._automation_next_run
            and not self._automation_running
        ):
            self._automation_next_run = self._compute_next_automation_run(now + dt.timedelta(seconds=1))
            self._run_scheduled_automation(f"agenda {self._automation_time_text()}", notify_user=False)
            return

        self._refresh_automation_controls_ui()

    def _handle_pdf_button(self) -> None:
        if not self._confirm_discard_edits("importar PDF"):
            return
        self._reset_edit_state()
        from global_vars import list_results
        if list_results:
            self._handle_add_more()
            return
        self._select_pdf_flow()

    def _select_pdf_flow(self) -> None:
        if not self._confirm_discard_edits("importar PDF"):
            return
        self._reset_edit_state()
        path = filedialog.askopenfilename(filetypes=[("Arquivos PDF", "*.pdf")])
        if not path:
            return
        origem = self._infer_origem_from_filename(path)
        if not origem:
            dlg = SourceDialog(self)
            origem = dlg.choice()
        if not origem:
            return
        source_pdf_async(
            self.tree_main,
            self.progress_var,
            self.progress_bar_adapter,
            self.root_adapter,
            self.label_files_var,
            QtButtonAdapter(self.btn_cancel),
            path,
            origem,
        )

    def _handle_add_more(self) -> None:
        if not self._confirm_discard_edits("adicionar PDF"):
            return
        adicionar_pdf(
            self.tree_main,
            self.progress_var,
            self.progress_bar_adapter,
            self.root_adapter,
            self.label_files_var,
        )

    def _load_eh_caixa_reports_with_loading(self, data_br: str) -> tuple[dict | None, dict | None, dict | None, str | None]:
        status_queue: queue.Queue[str] = queue.Queue()
        result: dict = {}
        cancel_loading = threading.Event()

        def push_status(message: str) -> None:
            texto = str(message or "").strip()
            if texto:
                status_queue.put(texto)

        def worker() -> None:
            try:
                relatorio, relatorio_fechamento, relatorio_pix = gerar_relatorios_caixa_eh_zweb(
                    data_br,
                    on_status=push_status,
                    cancel_event=cancel_loading,
                )
                result["relatorio"] = relatorio
                result["relatorio_fechamento"] = relatorio_fechamento
                result["relatorio_pix"] = relatorio_pix
            except Exception as exc:
                if str(exc).strip() == "__cancelled__":
                    result["cancelled"] = True
                else:
                    result["error"] = str(exc)

        dialog = LoadingStatusDialog(self, "Caixa EH", "Acessando Zweb...")
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()

        while thread.is_alive():
            while True:
                try:
                    dialog.set_status(status_queue.get_nowait())
                except queue.Empty:
                    break
            if dialog.was_cancelled() and not cancel_loading.is_set():
                cancel_loading.set()
                dialog.set_status("Cancelando...")
            QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 100)
            thread.join(0.05)
            time.sleep(0.01)

        while True:
            try:
                dialog.set_status(status_queue.get_nowait())
            except queue.Empty:
                break
        dialog.close_gracefully()

        if result.get("cancelled") or dialog.was_cancelled():
            return None, None, None, "__cancelled__"
        if result.get("error"):
            return None, None, None, str(result.get("error")).strip() or "Falha ao gerar os relatorios da EH no Zweb."
        return result.get("relatorio"), result.get("relatorio_fechamento"), result.get("relatorio_pix"), None

    def _load_mva_reports_with_loading(
        self,
        path_davs: str,
        path_orcamentos: str,
        path_cupons: str,
    ) -> tuple[dict | None, dict | None, dict | None, str | None]:
        status_queue: queue.Queue[str] = queue.Queue()
        result: dict = {}

        def push_status(message: str) -> None:
            texto = str(message or "").strip()
            if texto:
                status_queue.put(texto)

        def worker() -> None:
            try:
                push_status("Lendo DAV MVA...")
                result["relatorio_davs"] = analisar_pdf_caixa(path_davs)
                push_status("Lendo Orcamento...")
                result["relatorio_orcamentos"] = analisar_pdf_caixa(path_orcamentos)
                result["relatorio_cupons"] = None
                if path_cupons:
                    push_status("Lendo Fechamento de Caixa...")
                    relatorio_cupons = analisar_pdf_fechamento_caixa_mva_clipp(
                        path_cupons,
                        auto_download_missing=True,
                        on_status=push_status,
                    )
                    avisos_cupons = list(relatorio_cupons.get("avisos_usuario") or [])
                    if relatorio_cupons.get("quantidade_nfce", 0) <= 0:
                        relatorio_cupons = analisar_pdf_resumo_nfce(path_cupons)
                        if avisos_cupons:
                            relatorio_cupons["avisos_usuario"] = list(
                                dict.fromkeys(
                                    list(relatorio_cupons.get("avisos_usuario") or [])
                                    + avisos_cupons
                                )
                            )
                    result["relatorio_cupons"] = relatorio_cupons
            except Exception as exc:
                result["error"] = str(exc)

        dialog = LoadingStatusDialog(self, "Caixa MVA", "Processando arquivos da MVA...")
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        dialog.show()
        dialog.raise_()
        dialog.activateWindow()

        while thread.is_alive():
            while True:
                try:
                    dialog.set_status(status_queue.get_nowait())
                except queue.Empty:
                    break
            QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 100)
            thread.join(0.05)
            time.sleep(0.01)

        while True:
            try:
                dialog.set_status(status_queue.get_nowait())
            except queue.Empty:
                break
        dialog.close_gracefully()

        if result.get("error"):
            return None, None, None, str(result.get("error")).strip() or "Falha ao processar os arquivos da MVA."
        return result.get("relatorio_davs"), result.get("relatorio_orcamentos"), result.get("relatorio_cupons"), None

    def _auto_find_mva_caixa_files(self) -> dict[str, str]:
        terms_by_key = {
            "davs": ["dav mva", "dav"],
            "orcamentos": ["orcamento", "orçamento"],
            "cupons": ["fechamento de caixa"],
        }
        base_dirs: list[str] = []
        for raw in [_active_report_dir()]:
            if raw and os.path.isdir(raw) and raw not in base_dirs:
                base_dirs.append(raw)

        found: dict[str, str] = {}
        for base_dir in base_dirs:
            try:
                pdfs = [
                    os.path.join(base_dir, name)
                    for name in os.listdir(base_dir)
                    if name.lower().endswith(".pdf")
                ]
            except OSError:
                continue
            pdfs.sort(key=lambda item: os.path.getmtime(item), reverse=True)
            for key, terms in terms_by_key.items():
                if key in found:
                    continue
                for path in pdfs:
                    nome = corrigir_texto(os.path.basename(path)).casefold()
                    if any(term in nome for term in terms):
                        found[key] = path
                        break
        return found

    def _generate_mva_reports_for_automation(self) -> tuple[dict | None, dict | None, str | None]:
        auto_files = self._auto_find_mva_caixa_files()
        missing: list[str] = []
        path_davs = auto_files.get("davs", "")
        path_orcamentos = auto_files.get("orcamentos", "")
        path_cupons = auto_files.get("cupons", "")

        if not path_davs:
            missing.append("DAV MVA")
        if not path_orcamentos:
            missing.append("Orcamento")
        if not path_cupons:
            missing.append("Fechamento de Caixa")
        if missing:
            return None, None, f"Arquivos da MVA nao encontrados automaticamente: {', '.join(missing)}."

        relatorio_davs, relatorio_orcamentos, relatorio_cupons, mva_msg = self._load_mva_reports_with_loading(
            path_davs,
            path_orcamentos,
            path_cupons,
        )
        if mva_msg:
            return None, None, mva_msg

        davs_ok, davs_msg = validar_arquivo_caixa_mva(relatorio_davs, "exportacao_dados_mva")
        if not davs_ok:
            return None, None, davs_msg

        orc_ok, orc_msg = validar_arquivo_caixa_mva(relatorio_orcamentos, "orcamentos_mva")
        if not orc_ok:
            return None, None, orc_msg

        if relatorio_davs.get("periodo") and relatorio_orcamentos.get("periodo"):
            if relatorio_davs.get("periodo") != relatorio_orcamentos.get("periodo"):
                return None, None, (
                    "A Exportacao de dados e o relatorio de Orcamento da MVA precisam ser do mesmo periodo."
                )

        relatorio = combinar_relatorios_caixa_mva([relatorio_davs, relatorio_orcamentos])
        if relatorio.get("pedidos_total", 0) <= 0:
            return None, None, "Nenhum pedido foi encontrado nos arquivos da MVA."

        caixa_ok, caixa_msg = validar_relatorio_pedidos_importados(
            relatorio,
            modelo_esperado="MVA",
        )
        if not caixa_ok:
            return None, None, caixa_msg

        resumo_ok, resumo_msg = validar_relatorio_resumo_nfce(
            relatorio_cupons,
            modelo_esperado="MVA",
        )
        if not resumo_ok:
            return None, None, resumo_msg
        if relatorio_cupons.get("quantidade_nfce", 0) <= 0:
            return None, None, "Nenhum cupom foi encontrado no Fechamento de Caixa da MVA."

        periodo_ok, periodo_msg = validar_periodo_relatorios_caixa(
            relatorio,
            relatorio_cupons,
            titulo_secundario="relatorio de Cupons",
        )
        if not periodo_ok:
            return None, None, periodo_msg

        fechamento = comparar_caixa_resumo_nfce(relatorio, relatorio_cupons)
        return relatorio, fechamento, None

    def _generate_eh_reports_for_automation(
        self,
        data_br: str,
    ) -> tuple[dict | None, dict | None, dict | None, list[str], str | None]:
        relatorio, relatorio_nfce, relatorio_pix, eh_msg = self._load_eh_caixa_reports_with_loading(data_br)
        if eh_msg == "__cancelled__":
            return None, None, None, [], "A automacao da EH foi cancelada."
        if eh_msg:
            return None, None, None, [], eh_msg

        caixa_ok, caixa_msg = validar_relatorio_pedidos_importados(
            relatorio,
            modelo_esperado="EH",
        )
        if not caixa_ok:
            return None, None, None, [], caixa_msg
        if relatorio.get("pedidos_total", 0) <= 0:
            return None, None, None, [], "Nenhum pedido foi encontrado no caixa da EH."

        fechamento = None
        avisos_usuario: list[str] = []

        if relatorio_nfce:
            resumo_ok, resumo_msg = validar_relatorio_resumo_nfce(
                relatorio_nfce,
                modelo_esperado="EH",
            )
            if not resumo_ok:
                return None, None, None, [], resumo_msg
            if relatorio_nfce.get("quantidade_nfce", 0) <= 0:
                return None, None, None, [], "Nenhuma NFC-e foi encontrada no Fechamento de caixa do Zweb."

            periodo_ok, periodo_msg = validar_periodo_relatorios_caixa(
                relatorio,
                relatorio_nfce,
                titulo_secundario="Fechamento de caixa",
            )
            if not periodo_ok:
                return None, None, None, [], periodo_msg

            fechamento = comparar_caixa_resumo_nfce(relatorio, relatorio_nfce)
            avisos_usuario = list(
                dict.fromkeys(
                    list(relatorio_nfce.get("avisos_usuario") or [])
                    + list((fechamento or {}).get("avisos_usuario") or [])
                    + list((relatorio_pix or {}).get("avisos_usuario") or [])
                )
            )
        else:
            avisos_usuario.append(
                "O Fechamento de caixa do Zweb nao ficou disponivel; a EH sera impressa sem a aba de fechamento."
            )

        periodo_pix = (relatorio_nfce or {}).get("periodo") or relatorio.get("periodo")
        if relatorio_pix is None and periodo_pix:
            relatorio_pix = {
                "arquivo": "",
                "periodo": periodo_pix,
                "quantidade_autorizados": 0,
                "total_autorizado": 0.0,
                "itens_autorizados": [],
                "mensagem": "Nenhum pagamento digital de NFC-e foi encontrado em Financeiro > Movimentacoes para este dia.",
            }
        elif relatorio_pix is None:
            relatorio_pix = {
                "arquivo": "",
                "periodo": None,
                "quantidade_autorizados": 0,
                "total_autorizado": 0.0,
                "itens_autorizados": [],
                "mensagem": "O periodo do caixa nao foi identificado; nao foi possivel consultar os pagamentos digitais do Zweb.",
            }

        return relatorio, fechamento, relatorio_pix, avisos_usuario, None

    def _collect_automation_print_jobs(
        self,
        origem: str,
        relatorio: dict,
        fechamento: dict | None = None,
        relatorio_pix: dict | None = None,
    ) -> list[tuple[str, str]]:
        dialog = CaixaReportDialog(self, relatorio, fechamento, relatorio_pix)
        try:
            return [
                (f"{origem}: {titulo}", html)
                for titulo, html in dialog.build_automation_bundle_jobs()
            ]
        finally:
            dialog.deleteLater()

    def _print_automation_jobs(
        self,
        jobs: list[tuple[str, str]],
    ) -> list[str]:
        printed_titles: list[str] = []
        font_family = self.font().family() or "Lexend"
        for title, html in jobs:
            _render_html_document_to_printer(
                html,
                _resolve_default_printer(),
                font_family,
            )
            printed_titles.append(title)
        return printed_titles

    def _run_scheduled_automation(self, trigger_label: str, notify_user: bool) -> None:
        if self._automation_running:
            return

        self._automation_running = True
        self.btn_automation_power.setEnabled(False)
        self.btn_automation_test.setEnabled(False)
        data_br = self._automation_target_date_br()
        self._automation_last_status = (
            f"Automacao iniciada via {trigger_label} para o dia {data_br}."
        )
        self._refresh_automation_controls_ui()
        QtWidgets.QApplication.processEvents(QtCore.QEventLoop.AllEvents, 100)

        printed_titles: list[str] = []
        print_jobs: list[tuple[str, str]] = []
        warnings: list[str] = []
        errors: list[str] = []

        try:
            relatorio_mva, fechamento_mva, mva_error = self._generate_mva_reports_for_automation()
            if mva_error:
                errors.append(f"MVA: {mva_error}")
            else:
                print_jobs.extend(
                    self._collect_automation_print_jobs("MVA", relatorio_mva, fechamento_mva)
                )

            relatorio_eh, fechamento_eh, relatorio_pix_eh, eh_warnings, eh_error = self._generate_eh_reports_for_automation(data_br)
            if eh_error:
                errors.append(f"EH: {eh_error}")
            else:
                print_jobs.extend(
                    self._collect_automation_print_jobs(
                        "EH",
                        relatorio_eh,
                        fechamento_eh,
                        relatorio_pix_eh,
                    )
                )
                warnings.extend([f"EH: {aviso}" for aviso in eh_warnings if aviso])

            if print_jobs:
                printed_titles.extend(self._print_automation_jobs(print_jobs))
        except Exception as exc:
            errors.append(str(exc))
        finally:
            self._automation_running = False
            self.btn_automation_power.setEnabled(True)
            self.btn_automation_test.setEnabled(True)

        resumo_partes: list[str] = []
        if printed_titles:
            resumo_partes.append(
                f"Impressos enviados: {len(printed_titles)} ({'; '.join(printed_titles)})."
            )
        if warnings:
            resumo_partes.append("Avisos: " + " | ".join(warnings))
        if errors:
            resumo_partes.append("Falhas: " + " | ".join(errors))
        if not resumo_partes:
            resumo_partes.append("Nenhum relatorio foi processado.")

        self._automation_last_status = " ".join(resumo_partes)
        self._refresh_automation_controls_ui()

        if notify_user:
            title = "Teste da automacao"
            message = f"Data alvo: {data_br}\n\n{self._automation_last_status}"
            if errors:
                messagebox.showwarning(title, message)
            else:
                messagebox.showinfo(title, message)

    def _handle_caixa_report(self) -> None:
        cnpj = CaixaCnpjDialog(self).choice()
        if not cnpj:
            return

        if cnpj == "MVA":
            auto_files = self._auto_find_mva_caixa_files()

            path_davs = auto_files.get("davs", "")
            if not path_davs:
                if not InstructionDialog(
                    self,
                    "Caixa MVA - Passo 1 de 3",
                    "Selecione agora o PDF DAV MVA.\n\n"
                    "O app tentou localizar automaticamente um arquivo com nome DAV e não encontrou.",
                ).confirmed():
                    return
                path_davs = filedialog.askopenfilename(
                    filetypes=[("Arquivos PDF", "*.pdf")],
                    title="Caixa MVA - Passo 1 de 3: selecionar DAV MVA",
                )
            if not path_davs:
                return

            path_orcamentos = auto_files.get("orcamentos", "")
            if not path_orcamentos:
                if not InstructionDialog(
                    self,
                    "Caixa MVA - Passo 2 de 3",
                    "Selecione agora o PDF Orcamento.\n\n"
                    "O app tentou localizar automaticamente um arquivo com nome Orcamento e não encontrou.",
                ).confirmed():
                    return
                path_orcamentos = filedialog.askopenfilename(
                    filetypes=[("Arquivos PDF", "*.pdf")],
                    title="Caixa MVA - Passo 2 de 3: selecionar Orcamento",
                )
            if not path_orcamentos:
                messagebox.showwarning(
                    "Arquivo obrigatório",
                    "O relatório de Orcamento da MVA precisa ser selecionado para continuar.",
                )
                return

            path_cupons = auto_files.get("cupons", "")
            if not path_cupons:
                if not InstructionDialog(
                    self,
                    "Caixa MVA - Passo 3 de 3",
                    "Selecione agora o PDF Fechamento de Caixa.\n\n"
                    "O app tentou localizar automaticamente um arquivo com nome Fechamento de Caixa e não encontrou.",
                ).confirmed():
                    return
                path_cupons = filedialog.askopenfilename(
                    filetypes=[("Arquivos PDF", "*.pdf")],
                    title="Caixa MVA - Passo 3 de 3: selecionar Fechamento de Caixa",
                )
            if not path_cupons:
                continuar_sem_cupons = messagebox.askyesno(
                    "Fechamento de Caixa não selecionado",
                    "O Fechamento de Caixa não foi selecionado.\n\n"
                    "Deseja continuar somente com a análise dos DAVs da MVA?",
                )
                if not continuar_sem_cupons:
                    return

            relatorio_davs, relatorio_orcamentos, relatorio_cupons, mva_msg = self._load_mva_reports_with_loading(
                path_davs,
                path_orcamentos,
                path_cupons,
            )
            if mva_msg:
                messagebox.showerror("Erro", f"Erro ao analisar PDF de Caixa:\n{mva_msg}")
                return
            fechamento = None

            davs_ok, davs_msg = validar_arquivo_caixa_mva(
                relatorio_davs,
                "exportacao_dados_mva",
            )
            if not davs_ok:
                messagebox.showwarning("Arquivo inválido", davs_msg)
                return

            orc_ok, orc_msg = validar_arquivo_caixa_mva(
                relatorio_orcamentos,
                "orcamentos_mva",
            )
            if not orc_ok:
                messagebox.showwarning("Arquivo inválido", orc_msg)
                return

            if relatorio_davs.get("periodo") and relatorio_orcamentos.get("periodo"):
                if relatorio_davs.get("periodo") != relatorio_orcamentos.get("periodo"):
                    messagebox.showwarning(
                        "Período inválido",
                        "A Exportação de dados e o relatório de Orçamentos precisam ser do mesmo período.",
                    )
                    return

            relatorio = combinar_relatorios_caixa_mva(
                [relatorio_davs, relatorio_orcamentos]
            )

            if relatorio.get("pedidos_total", 0) <= 0:
                messagebox.showwarning("Aviso", "Nenhum pedido foi encontrado neste PDF.")
                return

            caixa_ok, caixa_msg = validar_relatorio_pedidos_importados(
                relatorio,
                modelo_esperado="MVA",
            )
            if not caixa_ok:
                messagebox.showwarning("Arquivo inválido", caixa_msg)
                return

            if path_cupons and relatorio_cupons:
                resumo_ok, resumo_msg = validar_relatorio_resumo_nfce(
                    relatorio_cupons,
                    modelo_esperado="MVA",
                )
                if not resumo_ok:
                    messagebox.showwarning("Arquivo inválido", resumo_msg)
                    return
                if relatorio_cupons.get("quantidade_nfce", 0) <= 0:
                    messagebox.showwarning("Aviso", "Nenhum cupom foi encontrado no relatorio informado.")
                    return
                periodo_ok, periodo_msg = validar_periodo_relatorios_caixa(
                    relatorio,
                    relatorio_cupons,
                    titulo_secundario="relatório de Cupons",
                )
                if not periodo_ok:
                    messagebox.showwarning("Período inválido", periodo_msg)
                    return
                fechamento = comparar_caixa_resumo_nfce(relatorio, relatorio_cupons)

            avisos_usuario = list(
                dict.fromkeys(list((relatorio_cupons or {}).get("avisos_usuario") or []))
            )
            if avisos_usuario:
                messagebox.showwarning("Aviso", "\n\n".join(avisos_usuario))

            CaixaReportDialog(self, relatorio, fechamento).exec()
            return

        data_caixa = CaixaDateDialog(
            self,
            "Caixa EH - Data",
            "Selecione o dia do relatório da EH.",
        ).selected_date()
        if not data_caixa:
            return

        relatorio, relatorio_nfce, relatorio_pix, eh_msg = self._load_eh_caixa_reports_with_loading(data_caixa)
        if eh_msg == "__cancelled__":
            return

        if eh_msg:
            messagebox.showerror("Caixa EH", eh_msg)
            return

        fechamento = None

        caixa_ok, caixa_msg = validar_relatorio_pedidos_importados(
            relatorio,
            modelo_esperado="EH",
        )
        if not caixa_ok:
            messagebox.showwarning("Arquivo inválido", caixa_msg)
            return

        if relatorio.get("pedidos_total", 0) <= 0:
            messagebox.showwarning("Aviso", "Nenhum pedido foi encontrado neste PDF.")
            return

        if relatorio_nfce:
            resumo_ok, resumo_msg = validar_relatorio_resumo_nfce(
                relatorio_nfce,
                modelo_esperado="EH",
            )
            if not resumo_ok:
                messagebox.showwarning(
                    "Arquivo inválido",
                    "O Fechamento de caixa do Zweb não trouxe um total válido para comparação.",
                )
                return
            if relatorio_nfce.get("quantidade_nfce", 0) <= 0:
                messagebox.showwarning("Aviso", "Nenhuma NFC-e foi encontrada no Fechamento de caixa do Zweb.")
                return
            periodo_ok, periodo_msg = validar_periodo_relatorios_caixa(
                relatorio,
                relatorio_nfce,
                titulo_secundario="Fechamento de caixa",
            )
            if not periodo_ok:
                messagebox.showwarning("Período inválido", periodo_msg)
                return
            fechamento = comparar_caixa_resumo_nfce(relatorio, relatorio_nfce)
            avisos_usuario = list(
                dict.fromkeys(
                    list(relatorio_nfce.get("avisos_usuario") or [])
                    + list((fechamento or {}).get("avisos_usuario") or [])
                    + list((relatorio_pix or {}).get("avisos_usuario") or [])
                )
            )
            if avisos_usuario:
                messagebox.showwarning("Aviso", "\n\n".join(avisos_usuario))

        periodo_pix = (relatorio_nfce or {}).get("periodo") or relatorio.get("periodo")
        if relatorio_pix is None and periodo_pix:
            relatorio_pix = {
                "arquivo": "",
                "periodo": periodo_pix,
                "quantidade_autorizados": 0,
                "total_autorizado": 0.0,
                "itens_autorizados": [],
                "mensagem": "Nenhum pagamento digital de NFC-e foi encontrado em Financeiro > Movimentações para este dia.",
            }
        elif relatorio_pix is None:
            relatorio_pix = {
                "arquivo": "",
                "periodo": None,
                "quantidade_autorizados": 0,
                "total_autorizado": 0.0,
                "itens_autorizados": [],
                "mensagem": "O período do caixa não foi identificado; não foi possível consultar os pagamentos digitais do Zweb.",
            }

        CaixaReportDialog(self, relatorio, fechamento, relatorio_pix).exec()

    def _build_graphs_view(self) -> QtWidgets.QWidget:
        container = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(container)
        layout.setSpacing(10)

        controls = QtWidgets.QHBoxLayout()
        label_source = QtWidgets.QLabel("Fonte:")
        self.graph_source = QtWidgets.QComboBox()
        self.graph_source.addItems(["MVA", "EH", "Todos", "Vendas totais"])
        label_metric = QtWidgets.QLabel("Coluna:")
        self.graph_metric = QtWidgets.QComboBox()
        label_order = QtWidgets.QLabel("Ordem:")
        self.graph_order = QtWidgets.QComboBox()
        self.graph_order.addItems(["Desc", "Asc"])

        controls.addWidget(label_source)
        controls.addWidget(self.graph_source)
        controls.addSpacing(10)
        controls.addWidget(label_metric)
        controls.addWidget(self.graph_metric)
        controls.addSpacing(10)
        controls.addWidget(label_order)
        controls.addWidget(self.graph_order)
        controls.addStretch()

        layout.addLayout(controls)

        self.graph_chart = BarChartWidget()
        self.graph_scroll = QtWidgets.QScrollArea()
        self.graph_scroll.setWidgetResizable(True)
        self.graph_scroll.setWidget(self.graph_chart)
        self.graph_scroll.setSizePolicy(QtWidgets.QSizePolicy.Expanding, QtWidgets.QSizePolicy.Expanding)
        layout.addWidget(self.graph_scroll)

        self.graph_source.currentIndexChanged.connect(self._update_graph_metric_options)
        self.graph_metric.currentIndexChanged.connect(self._refresh_graphs_data)
        self.graph_order.currentIndexChanged.connect(self._refresh_graphs_data)

        self._update_graph_metric_options()
        return container

    def _update_graph_metric_options(self) -> None:
        source = self.graph_source.currentText()
        self.graph_metric.blockSignals(True)
        self.graph_metric.clear()
        self.graph_metric.addItems(list(self.cols_main[1:]))
        self.graph_metric.blockSignals(False)
        self._refresh_graphs_data()

    def _refresh_graphs_data(self) -> None:
        source = self.graph_source.currentText()
        metric = self.graph_metric.currentText()
        if not metric:
            self.graph_chart.set_data([], QtGui.QColor("#59C734"))
            return

        data = []
        show_decimals = metric == "Total Vendas"
        result = self._collect_results_by_source(source, metric)
        if result.get("dual"):
            data = result["data"]
            reverse = self.graph_order.currentText() == "Desc"
            data.sort(key=lambda item: item[1] + item[2], reverse=reverse)
            self.graph_chart.set_data_dual(
                data,
                colors=(QtGui.QColor("#FF8A00"), QtGui.QColor("#2F6BFF")),
                show_decimals=show_decimals,
            )
        else:
            data = result["data"]
            color = result["color"]
            gradient = result["gradient"]
            reverse = self.graph_order.currentText() == "Desc"
            data.sort(key=lambda item: item[1], reverse=reverse)
            self.graph_chart.set_data(data, color, show_decimals=show_decimals, gradient_colors=gradient)

    def _collect_table_data(self, tree: QtTreeAdapter, columns, metric: str):
        try:
            metric_index = columns.index(metric)
        except ValueError:
            return []
        out = []
        for item in tree.get_children():
            values = tree.item(item)["values"]
            if not values or len(values) <= metric_index:
                continue
            label = values[0]
            value = values[metric_index]
            out.append((label, float(parse_number(value))))
        return out

    def _collect_results_by_source(self, source: str, metric: str):
        try:
            from global_vars import results_by_source
        except Exception:
            results_by_source = {"MVA": [], "EH": []}

        def merge_results(items):
            merged = {}
            for _, res in items:
                if not isinstance(res, dict):
                    continue
                if res.get("__cancelled__") or res.get("__empty__") or res.get("__error__"):
                    continue
                for vendedor, dados in res.items():
                    if vendedor not in merged:
                        merged[vendedor] = {
                            "atendidos": 0,
                            "devolucoes": 0,
                            "total_clientes": 0,
                            "total_vendas": 0.0,
                        }
                    merged[vendedor]["atendidos"] += dados.get("atendidos", 0)
                    merged[vendedor]["devolucoes"] += dados.get("devolucoes", 0)
                    merged[vendedor]["total_vendas"] += parse_number(dados.get("total_vendas", 0))

            for dados in merged.values():
                dados["total_clientes"] = dados["atendidos"] - dados["devolucoes"]
            return merged

        gradient = None
        if source == "MVA":
            merged = merge_results(results_by_source.get("MVA", []))
            color = QtGui.QColor("#FF8A00")
        elif source == "EH":
            merged = merge_results(results_by_source.get("EH", []))
            color = QtGui.QColor("#2F6BFF")
        elif source == "Vendas totais":
            data = self._collect_table_data(self.tree_main, self.cols_main, metric)
            color = QtGui.QColor("#FF8A00")
            gradient = (QtGui.QColor("#FF8A00"), QtGui.QColor("#2F6BFF"))
            return {"data": data, "color": color, "gradient": gradient, "dual": False}
        else:
            merged_mva = merge_results(results_by_source.get("MVA", []))
            merged_eh = merge_results(results_by_source.get("EH", []))
            metric_key = {
                "Atendidos": "atendidos",
                "Devoluções": "devolucoes",
                "Total Final": "total_clientes",
                "Total Vendas": "total_vendas",
            }.get(metric)
            if not metric_key:
                return {"data": [], "color": QtGui.QColor("#FF8A00"), "gradient": None, "dual": True}
            vendors = sorted(set(merged_mva.keys()) | set(merged_eh.keys()))
            data = []
            for v in vendors:
                v1 = float(merged_mva.get(v, {}).get(metric_key, 0))
                v2 = float(merged_eh.get(v, {}).get(metric_key, 0))
                data.append((v, v1, v2))
            return {"data": data, "color": QtGui.QColor("#FF8A00"), "gradient": None, "dual": True}

        metric_key = {
            "Atendidos": "atendidos",
            "Devoluções": "devolucoes",
            "Total Final": "total_clientes",
            "Total Vendas": "total_vendas",
        }.get(metric)

        if not metric_key:
            return {"data": [], "color": color, "gradient": gradient, "dual": False}

        data = [(v, float(d.get(metric_key, 0))) for v, d in merged.items()]
        return {"data": data, "color": color, "gradient": gradient, "dual": False}

    def _toggle_graphs_view(self) -> None:
        if not self._showing_graphs:
            if not self._confirm_discard_edits("abrir gráficos"):
                return
            self._reset_edit_state()
            self._showing_graphs = True
            self.table_main.setVisible(False)
            self.online_container.setVisible(False)
            self.graphs_view.setVisible(True)
            self._refresh_graphs_data()
            self.btn_graphs.setText("Tabelas")
            return

        self._showing_graphs = False
        self.graphs_view.setVisible(False)
        self.table_main.setVisible(True)
        self.online_container.setVisible(True)
        self.btn_graphs.setText("Gráficos")

    def _handle_load_planilhas(self) -> None:
        if not self._confirm_discard_edits("carregar planilhas"):
            return
        use_online_bar = self._is_main_progress_active()
        if use_online_bar:
            self.progress_var_online.set(0)
            self.progress_bar_online.setVisible(True)
            progress_var = self.progress_var_online
            progress_bar = self.progress_bar_online_adapter
        else:
            self.progress_bar_online.setVisible(False)
            progress_var = self.progress_var
            progress_bar = self.progress_bar_adapter
        carregar_planilhas_duplas_async(
            self.tree_mva,
            self.tree_eh,
            progress_var,
            progress_bar,
            self.root_adapter,
        )

    def _handle_clear_tables(self) -> None:
        if not self._confirm_discard_edits("limpar tabelas"):
            return
        limpar_tabelas_duplas(
            self.tree_main,
            self.tree_mva,
            self.tree_eh,
            self.label_files_var,
            self.progress_var,
        )

    def _handle_merge_tables(self) -> None:
        if not self._confirm_discard_edits("mesclar tabelas"):
            return
        mesclar_tabelas_duplas(
            self.tree_main,
            self.progress_var,
            self.progress_bar_adapter,
            self.root_adapter,
            self.label_files_var,
            self.tree_mva,
            self.tree_eh,
        )
    def _export_dialog(self) -> None:
        dlg = QtWidgets.QDialog(self)
        dlg.setWindowTitle("Exportar")
        dlg.setFixedSize(250, 150)
        layout = QtWidgets.QVBoxLayout(dlg)
        label = QtWidgets.QLabel("Escolha o formato para exportar:")
        label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(label)
        btn_excel = QtWidgets.QPushButton("Excel")
        btn_pdf = QtWidgets.QPushButton("PDF")
        btn_excel.setStyleSheet("text-align:center;")
        btn_pdf.setStyleSheet("text-align:center;")
        layout.addWidget(btn_excel)
        layout.addWidget(btn_pdf)

        btn_excel.clicked.connect(lambda: self._export_excel(dlg))
        btn_pdf.clicked.connect(lambda: self._export_pdf(dlg))
        dlg.exec()

    def _infer_origem_from_filename(self, path: str) -> str | None:
        name = os.path.basename(path).lower()
        tokens = re.findall(r"[a-z0-9]+", name)
        joined = " ".join(tokens)

        if re.search(r"\bmva\b", joined):
            return "MVA"
        if re.search(r"\beh\b", joined) or "horizonte" in joined:
            return "EH"

        def is_similar(value: str, target: str) -> bool:
            if value == target:
                return True
            ratio = difflib.SequenceMatcher(None, value, target).ratio()
            return ratio >= 0.75

        for token in tokens:
            if is_similar(token, "mva"):
                return "MVA"
            if is_similar(token, "eh"):
                return "EH"
            if is_similar(token, "horizonte"):
                return "EH"

        if is_similar(joined.replace(" ", ""), "mva"):
            return "MVA"
        if is_similar(joined.replace(" ", ""), "horizonte"):
            return "EH"
        return None

    def _toggle_table_edit(self) -> None:
        if not self._edit_mode:
            if not self._has_table_data():
                messagebox.showwarning("Aviso", "Importe um PDF antes de editar a tabela.")
                return
            self._edit_mode = True
            self._table_dirty = False
            self.table_main.setEditTriggers(
                QtWidgets.QAbstractItemView.DoubleClicked
                | QtWidgets.QAbstractItemView.SelectedClicked
                | QtWidgets.QAbstractItemView.EditKeyPressed
            )
            self.btn_edit_table.setText("Salvar Tabela")
            return

        if not self._save_table_pdf():
            return

        self._edit_mode = False
        self._table_dirty = False
        self.table_main.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.btn_edit_table.setText("Editar tabela")

    def _on_table_item_changed(self, _item: QtWidgets.QTableWidgetItem) -> None:
        if self._edit_mode:
            self._table_dirty = True

    def _export_excel(self, dlg: QtWidgets.QDialog) -> None:
        try:
            _excel_export(self.tree_main)
            dlg.accept()
        except Exception as exc:
            messagebox.showerror("Erro", f"Erro ao exportar Excel: {exc}")

    def _export_pdf(self, dlg: QtWidgets.QDialog) -> None:
        try:
            if _pdf_export(self.tree_main):
                dlg.accept()
        except Exception as exc:
            messagebox.showerror("Erro", f"Erro ao exportar PDF: {exc}")

    def _save_table_pdf(self) -> bool:
        try:
            return _pdf_export(self.tree_main)
        except Exception as exc:
            messagebox.showerror("Erro", f"Erro ao salvar tabela: {exc}")
            return False

    def _open_feedback(self) -> None:
        dlg = FeedbackDialog(self, self.tree_main)
        dlg.exec()

    def _setup_import_neon(self) -> None:
        self._neon_timer = QtCore.QTimer(self)
        self._neon_timer.setInterval(25)
        self._neon_timer.timeout.connect(self._update_import_neon)
        self._neon_timer.start()
        self._update_import_neon()

    def _setup_graphs_refresh_timer(self) -> None:
        self._graphs_timer = QtCore.QTimer(self)
        self._graphs_timer.setInterval(500)
        self._graphs_timer.timeout.connect(self._refresh_graphs_if_visible)
        self._graphs_timer.start()

    def _refresh_graphs_if_visible(self) -> None:
        if self._showing_graphs:
            self._refresh_graphs_data()

    def _setup_progress_visibility_timer(self) -> None:
        self._progress_timer = QtCore.QTimer(self)
        self._progress_timer.setInterval(250)
        self._progress_timer.timeout.connect(self._update_progress_visibility)
        self._progress_timer.start()

    def _update_progress_visibility(self) -> None:
        if self.progress_bar_online.isVisible() and not self._is_main_progress_active():
            if self.progress_var_online.get() in (0, 100):
                self.progress_bar_online.setVisible(False)

    def _is_main_progress_active(self) -> bool:
        try:
            value = int(self.progress_var.get() or 0)
        except Exception:
            value = 0
        return 0 < value < 100 or self.btn_cancel.isEnabled()

    def _update_import_neon(self) -> None:
        if self._are_tables_empty():
            self._neon_on = True
            self._neon_hue = (self._neon_hue + 4) % 360
            color_a = QtGui.QColor.fromHsv(self._neon_hue, 180, 200)
            color_b = QtGui.QColor.fromHsv((self._neon_hue + 140) % 360, 180, 200)
            glow = QtGui.QColor.fromHsv(self._neon_hue, 220, 255)
            self.btn_select_pdf.setStyleSheet(
                "QPushButton{"
                "background:qlineargradient(x1:0,y1:0,x2:1,y2:0,"
                f"stop:0 {color_a.name()}, stop:1 {color_b.name()});"
                f"border:1px solid {glow.name()};"
                "border-radius:6px;padding:6px 10px;text-align:center;"
                "}"
            )
            self.btn_select_pdf.update()
            return

        if self._neon_on:
            self._neon_on = False
            self.btn_select_pdf.setStyleSheet("")
            self.btn_select_pdf.update()

    def _are_tables_empty(self) -> bool:
        return (
            not self.tree_main.get_children()
            and not self.tree_mva.get_children()
            and not self.tree_eh.get_children()
        )

    def _has_table_data(self) -> bool:
        return bool(self.tree_main.get_children())

    def _reset_edit_state(self) -> None:
        self._edit_mode = False
        self._table_dirty = False
        self.table_main.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.btn_edit_table.setText("Editar tabela")

    def _confirm_discard_edits(self, acao: str) -> bool:
        if not (self._edit_mode or self._table_dirty):
            return True
        dlg = QtWidgets.QMessageBox(self)
        dlg.setWindowTitle("Alterações não salvas")
        dlg.setText(
            f"Existem alterações não salvas. Se você {acao}, elas serão perdidas.\n"
            "Deseja salvar antes de continuar?"
        )
        dlg.setIcon(QtWidgets.QMessageBox.Warning)
        btn_save = dlg.addButton("Salvar", QtWidgets.QMessageBox.AcceptRole)
        btn_discard = dlg.addButton("Descartar", QtWidgets.QMessageBox.DestructiveRole)
        btn_cancel = dlg.addButton("Cancelar", QtWidgets.QMessageBox.RejectRole)
        dlg.exec()
        clicked = dlg.clickedButton()
        if clicked == btn_save:
            if self._save_table_pdf():
                self._reset_edit_state()
                return True
            return False
        if clicked == btn_discard:
            self._reset_edit_state()
            return True
        return False


    def closeEvent(self, event: QtGui.QCloseEvent) -> None:
        if self._edit_mode or self._table_dirty:
            dlg = QtWidgets.QMessageBox(self)
            dlg.setWindowTitle("Sair sem salvar?")
            dlg.setText(
                "A tabela esta em modo de edição. Alterações não serão salvas.\n"
                "Deseja salvar antes de sair?"
            )
            dlg.setIcon(QtWidgets.QMessageBox.Warning)
            btn_save = dlg.addButton("Salvar", QtWidgets.QMessageBox.AcceptRole)
            btn_discard = dlg.addButton("Descartar", QtWidgets.QMessageBox.DestructiveRole)
            btn_cancel = dlg.addButton("Cancelar", QtWidgets.QMessageBox.RejectRole)
            dlg.exec()
            clicked = dlg.clickedButton()
            if clicked == btn_save:
                if self._save_table_pdf():
                    self._edit_mode = False
                    self._table_dirty = False
                    self.table_main.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
                    self._set_online_tables_editable(False)
                    self.btn_edit_table.setText("Editar tabela")
                    event.accept()
                    return
                event.ignore()
                return
            if clicked == btn_discard:
                event.accept()
                return
            if clicked == btn_cancel:
                event.ignore()
                return
        super().closeEvent(event)

    def _sort_table(self, tree: QtTreeAdapter, cols, idx: int) -> None:
        col = cols[idx]
        key = (id(tree), col)
        reverse = self._sort_state.get(key, False)
        ordenar_coluna(tree, col, reverse)
        self._sort_state[key] = not reverse


def run_app() -> None:
    app = QtWidgets.QApplication.instance() or QtWidgets.QApplication(sys.argv)
    app.aboutToQuit.connect(cleanup_generated_auto_reports)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
