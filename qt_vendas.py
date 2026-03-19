from __future__ import annotations

import sys
import os
import re
import difflib
from typing import List, Optional

from PySide6 import QtCore, QtGui, QtWidgets

from utils import (
    source_pdf_async,
    adicionar_pdf,
    analisar_pdf_caixa,
    analisar_pdf_resumo_nfce,
    comparar_caixa_resumo_nfce,
    validar_periodo_relatorios_caixa,
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
    criar_etiquetas,
    salvar_feedback_db,
    carregar_feedbacks_db,
    excluir_ultimo_feedback,
    atualizar_ultimo_feedback,
    listar_vendedores_db,
    set_ui_refs,
)
from ui_dialogs import messagebox, filedialog, set_parent
from qt_adapters import (
    QtRootAdapter,
    QtVar,
    QtProgressBarAdapter,
    QtButtonAdapter,
    QtTreeAdapter,
)


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
        self.setWindowTitle(title)
        self.setModal(True)
        self.resize(430, 170)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setSpacing(12)

        label = QtWidgets.QLabel(message)
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
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle("Relatorio de Caixa")
        self.resize(920, 700)

        layout = QtWidgets.QVBoxLayout(self)
        layout.setSpacing(12)

        title = QtWidgets.QLabel("Relatorio de Caixa")
        title_font = title.font()
        title_font.setBold(True)
        title_font.setPointSize(13)
        title.setFont(title_font)
        title.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(title)

        tabs = QtWidgets.QTabWidget()
        tabs.addTab(self._build_davs_tab(relatorio_caixa), "DAVs Importados")
        if fechamento:
            tabs.addTab(self._build_fechamento_tab(fechamento), "Fechamento Caixa")
        layout.addWidget(tabs, 1)

        btn_close = QtWidgets.QPushButton("Fechar")
        btn_close.setStyleSheet("text-align:center;")
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close, alignment=QtCore.Qt.AlignHCenter)

    def _build_davs_tab(self, relatorio: dict) -> QtWidgets.QWidget:
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setSpacing(12)

        summary_items = (
            ("Arquivo", relatorio.get("arquivo") or "-"),
            ("Periodo", relatorio.get("periodo") or "Nao identificado"),
            ("Pedidos totais", str(relatorio.get("pedidos_total", 0))),
            ("Pedidos Caixa", str(relatorio.get("pedidos_caixa", 0))),
            ("Fora do balcão", str(relatorio.get("pedidos_excluidos_cliente", 0))),
            ("NF-e excluidas", str(relatorio.get("pedidos_excluidos_documento", 0))),
            ("Pedidos excluidos", str(relatorio.get("pedidos_excluidos", 0))),
            ("Total do documento", f"R$ {format_number_br(relatorio.get('total_documento', 0.0))}"),
            ("Total excluido", f"R$ {format_number_br(relatorio.get('total_excluido', 0.0))}"),
            ("Total Caixa", f"R$ {format_number_br(relatorio.get('total_caixa', 0.0))}"),
        )
        layout.addWidget(self._build_summary_frame(summary_items, {"Total Caixa"}))

        actions = QtWidgets.QHBoxLayout()
        actions.addStretch()
        actions.addWidget(
            self._create_export_button(
                "Imprimir PDF",
                lambda: self._export_davs_pdf(relatorio),
            )
        )
        actions.addStretch()
        layout.addLayout(actions)

        section_label = QtWidgets.QLabel("Pedidos excluidos do calculo")
        section_label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(section_label)

        table = QtWidgets.QTableWidget()
        table.setColumnCount(4)
        table.setHorizontalHeaderLabels(("Pedido", "Cliente", "Documento", "Valor"))
        table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        table.setAlternatingRowColors(True)
        table.verticalHeader().setVisible(False)
        table.setSizePolicy(QtWidgets.QSizePolicy.Fixed, QtWidgets.QSizePolicy.Expanding)
        table.setMinimumSize(0, 0)
        table.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAlwaysOff)
        self._configure_resizable_table(table, [90, 280, 190, 120])

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
            empty_item = QtWidgets.QTableWidgetItem("Nenhum pedido excluido encontrado.")
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
        widget = QtWidgets.QWidget()
        layout = QtWidgets.QVBoxLayout(widget)
        layout.setSpacing(12)

        subtitle = QtWidgets.QLabel(
            "Compara o total de caixa dos DAVs importados com o Resumo NFC-e e aponta as NFC-e faltantes."
        )
        subtitle.setWordWrap(True)
        subtitle.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(subtitle)

        summary_items = (
            ("Arquivo DAVs", fechamento.get("arquivo_caixa") or "-"),
            ("Arquivo Resumo", fechamento.get("arquivo_resumo") or "-"),
            ("Periodo", fechamento.get("periodo") or "Nao identificado"),
            ("Total DAVs importados", f"R$ {format_number_br(fechamento.get('total_caixa', 0.0))}"),
            ("Total Resumo NFC-e", f"R$ {format_number_br(fechamento.get('total_resumo_nfce', 0.0))}"),
            ("NFC-e faltantes", str(fechamento.get("nfces_faltantes_count", 0))),
            ("Valor das faltantes", f"R$ {format_number_br(fechamento.get('valor_faltantes', 0.0))}"),
            ("Status", fechamento.get("status", "-")),
        )
        layout.addWidget(
            self._build_summary_frame(
                summary_items,
                {"Valor das faltantes", "Status"},
                fechamento=fechamento,
            )
        )

        actions = QtWidgets.QHBoxLayout()
        actions.addStretch()
        actions.addWidget(
            self._create_export_button(
                "Imprimir PDF",
                lambda: self._export_fechamento_pdf(fechamento),
            )
        )
        actions.addStretch()
        layout.addLayout(actions)

        section_label = QtWidgets.QLabel("NFC-e faltantes")
        section_label.setAlignment(QtCore.Qt.AlignCenter)
        layout.addWidget(section_label)

        table = QtWidgets.QTableWidget()
        table.setColumnCount(2)
        table.setHorizontalHeaderLabels(("Numero", "Valor"))
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
            empty_item = QtWidgets.QTableWidgetItem("Nenhuma NFC-e faltante encontrada.")
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

    def _build_summary_frame(
        self,
        items,
        highlighted_labels: set[str] | None = None,
        fechamento: dict | None = None,
    ) -> QtWidgets.QFrame:
        highlighted_labels = highlighted_labels or set()
        frame = QtWidgets.QFrame()
        layout = QtWidgets.QGridLayout(frame)
        layout.setHorizontalSpacing(16)
        layout.setVerticalSpacing(8)

        for row, (label_text, value_text) in enumerate(items):
            label = QtWidgets.QLabel(f"{label_text}:")
            value = QtWidgets.QLabel(value_text)
            label.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignVCenter)
            value.setAlignment(QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)

            if label_text in highlighted_labels:
                value_font = value.font()
                value_font.setBold(True)
                value_font.setPointSize(value_font.pointSize() + 1)
                value.setFont(value_font)

            if label_text == "Total Caixa":
                value.setStyleSheet("color:#59C734;")
            if label_text in {"Valor das faltantes", "Status"}:
                cor = "#59C734"
                if fechamento and fechamento.get("status") != "Confere":
                    cor = "#FF4D4F"
                value.setStyleSheet(f"color:{cor};")

            layout.addWidget(label, row, 0)
            layout.addWidget(value, row, 1)

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

    def _export_davs_pdf(self, relatorio: dict) -> None:
        summary_items = (
            ("Arquivo", relatorio.get("arquivo") or "-"),
            ("Periodo", relatorio.get("periodo") or "Nao identificado"),
            ("Pedidos totais", str(relatorio.get("pedidos_total", 0))),
            ("Pedidos Caixa", str(relatorio.get("pedidos_caixa", 0))),
            ("Fora do balcao", str(relatorio.get("pedidos_excluidos_cliente", 0))),
            ("NF-e excluidas", str(relatorio.get("pedidos_excluidos_documento", 0))),
            ("Pedidos excluidos", str(relatorio.get("pedidos_excluidos", 0))),
            ("Total do documento", f"R$ {format_number_br(relatorio.get('total_documento', 0.0))}"),
            ("Total excluido", f"R$ {format_number_br(relatorio.get('total_excluido', 0.0))}"),
            ("Total Caixa", f"R$ {format_number_br(relatorio.get('total_caixa', 0.0))}"),
        )
        rows = [
            (
                self._display_numero(item.get("pedido", "")),
                item.get("cliente", ""),
                item.get("documento", ""),
                f"R$ {format_number_br(item.get('valor', 0.0))}",
            )
            for item in relatorio.get("itens_excluidos", [])
        ]
        self._export_report_pdf(
            title="Relatorio de Caixa - DAVs Importados",
            default_name="relatorio_caixa_davs.pdf",
            summary_items=summary_items,
            headers=("Pedido", "Cliente", "Documento", "Valor"),
            rows=rows,
            empty_message="Nenhum pedido excluido encontrado.",
        )

    def _export_fechamento_pdf(self, fechamento: dict) -> None:
        summary_items = (
            ("Arquivo DAVs", fechamento.get("arquivo_caixa") or "-"),
            ("Arquivo Resumo", fechamento.get("arquivo_resumo") or "-"),
            ("Periodo", fechamento.get("periodo") or "Nao identificado"),
            ("Total DAVs importados", f"R$ {format_number_br(fechamento.get('total_caixa', 0.0))}"),
            ("Total Resumo NFC-e", f"R$ {format_number_br(fechamento.get('total_resumo_nfce', 0.0))}"),
            ("NFC-e faltantes", str(fechamento.get("nfces_faltantes_count", 0))),
            ("Valor das faltantes", f"R$ {format_number_br(fechamento.get('valor_faltantes', 0.0))}"),
            ("Status", fechamento.get("status", "-")),
        )
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
            title="Relatorio de Caixa - Fechamento",
            default_name="relatorio_caixa_fechamento.pdf",
            summary_items=summary_items,
            headers=("Numero", "Valor"),
            rows=rows,
            empty_message="Nenhuma divergencia encontrada.",
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

        def new_page():
            nonlocal y
            pdf.showPage()
            y = height - 40
            pdf.setFont("Helvetica", 10)

        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawString(40, y, title)
        y -= 28

        pdf.setFont("Helvetica", 10)
        for label, value in summary_items:
            text = f"{label}: {value}"
            for part in self._split_pdf_text(text, 95):
                if y < 45:
                    new_page()
                pdf.drawString(40, y, part)
                y -= 14
        y -= 10

        if y < 60:
            new_page()

        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(40, y, " | ".join(headers))
        y -= 16
        pdf.setFont("Helvetica", 9)

        if not rows:
            pdf.drawString(40, y, empty_message)
        else:
            for row in rows:
                line = " | ".join(str(value) for value in row)
                for part in self._split_pdf_text(line, 120):
                    if y < 45:
                        new_page()
                    pdf.drawString(40, y, part)
                    y -= 12

        pdf.save()
        messagebox.showinfo("Exportado", f"PDF salvo em:\n{path}")

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

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, titulo)
    y -= 30

    c.setFont("Helvetica-Bold", 10)
    for i, col in enumerate(cols):
        c.drawString(50 + i * 150, y, col)
    y -= 20

    c.setFont("Helvetica", 9)
    for row in rows:
        for i, val in enumerate(row):
            c.drawString(50 + i * 150, y, str(val))
        y -= 18
        if y < 40:
            c.showPage()
            y = altura - 40
            c.setFont("Helvetica", 9)

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

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, f"Feedbacks - {vendedor}")
    y -= 30
    c.setFont("Helvetica", 10)

    feedbacks_por_vendedor = {}
    for fb in feedbacks:
        nome = fb.get("vendedor", "Desconhecido")
        feedbacks_por_vendedor.setdefault(nome, []).append(fb)

    for vendedor_nome, lista in feedbacks_por_vendedor.items():
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
        right_panel.addWidget(self.btn_cancel, alignment=QtCore.Qt.AlignHCenter)

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

    def _handle_caixa_report(self) -> None:
        if not InstructionDialog(
            self,
            "Caixa - Passo 1 de 2",
            "Selecione agora o PDF de pedidos importados.\n\n"
            "No passo seguinte o sistema vai pedir o Resumo NFC-e para fazer o fechamento do caixa.",
        ).confirmed():
            return
        path_caixa = filedialog.askopenfilename(
            filetypes=[("Arquivos PDF", "*.pdf")],
            title="Caixa - Passo 1 de 2: selecionar relatorio de pedidos importados",
        )
        if not path_caixa:
            return

        if not InstructionDialog(
            self,
            "Caixa - Passo 2 de 2",
            "Agora selecione o PDF Resumo NFC-e.\n\n"
            "Ele sera usado para comparar com os DAVs importados e apontar NFC-e faltantes.",
        ).confirmed():
            return
        path_resumo = filedialog.askopenfilename(
            filetypes=[("Arquivos PDF", "*.pdf")],
            title="Caixa - Passo 2 de 2: selecionar resumo NFC-e",
        )
        if not path_resumo:
            continuar_sem_resumo = messagebox.askyesno(
                "Resumo NFC-e nao selecionado",
                "O Resumo NFC-e nao foi selecionado.\n\n"
                "Deseja continuar somente com a analise dos DAVs importados?",
            )
            if not continuar_sem_resumo:
                return

        QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.WaitCursor)
        try:
            relatorio = analisar_pdf_caixa(path_caixa)
            fechamento = None
            relatorio_nfce = None
            if path_resumo:
                relatorio_nfce = analisar_pdf_resumo_nfce(path_resumo)
        except Exception as exc:
            messagebox.showerror("Erro", f"Erro ao analisar PDF de Caixa:\n{exc}")
            return
        finally:
            QtWidgets.QApplication.restoreOverrideCursor()

        if relatorio.get("pedidos_total", 0) <= 0:
            messagebox.showwarning("Aviso", "Nenhum pedido foi encontrado neste PDF.")
            return

        caixa_ok, caixa_msg = validar_relatorio_pedidos_importados(relatorio)
        if not caixa_ok:
            messagebox.showwarning("Arquivo invalido", caixa_msg)
            return

        if path_resumo and relatorio_nfce and relatorio_nfce.get("quantidade_nfce", 0) <= 0:
            messagebox.showwarning("Aviso", "Nenhuma NFC-e foi encontrada no Resumo NFC-e.")
            return

        if path_resumo and relatorio_nfce:
            resumo_ok, resumo_msg = validar_relatorio_resumo_nfce(relatorio_nfce)
            if not resumo_ok:
                messagebox.showwarning("Arquivo invalido", resumo_msg)
                return
            periodo_ok, periodo_msg = validar_periodo_relatorios_caixa(relatorio, relatorio_nfce)
            if not periodo_ok:
                messagebox.showwarning("Periodo invalido", periodo_msg)
                return
            fechamento = comparar_caixa_resumo_nfce(relatorio, relatorio_nfce)

        CaixaReportDialog(self, relatorio, fechamento).exec()

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
        dlg.setWindowTitle("Alteracoes nao salvas")
        dlg.setText(
            f"Existem alteracoes nao salvas. Se voce {acao}, elas serao perdidas.\n"
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
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
