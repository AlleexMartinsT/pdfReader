from __future__ import annotations

from typing import Iterable, Tuple, Optional

from PySide6 import QtWidgets, QtCore, QtGui

try:
    from ftfy import fix_text as _ftfy_fix_text
except Exception:
    _ftfy_fix_text = None

_TEXT_FALLBACK_REPLACEMENTS = {
    "Relat?rio": "Relatório",
    "relat?rio": "relatório",
    "N?o": "Não",
    "n?o": "não",
    "m?quina": "máquina",
    "M?quina": "Máquina",
    "Per?odo": "Período",
    "Pend?ncias": "Pendências",
    "Transa??o": "Transação",
    "Transa??es": "Transações",
    "transa??o": "transação",
    "transa??es": "transações",
    "Cart?o": "Cartão",
    "cart?o": "cartão",
    "Cr?dito": "Crédito",
    "cr?dito": "crédito",
    "D?bito": "Débito",
    "d?bito": "débito",
    "Eletr?nica": "Eletrônica",
    "eletr?nica": "eletrônica",
    "Impress?o": "Impressão",
    "impress?o": "impressão",
    "Confirma??o": "Confirmação",
    "conclu?da": "concluída",
    "selec??o": "seleção",
}


def _normalize_text(value: object) -> str:
    text = "" if value is None else str(value)
    if _ftfy_fix_text is not None:
        try:
            text = _ftfy_fix_text(text)
        except Exception:
            pass
    if any(marker in text for marker in ("\u00c3", "\u00c2", "\u00e2", "\ud83d")):
        for _ in range(2):
            try:
                candidate = text.encode("latin1").decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                break
            if candidate == text:
                break
            text = candidate
            if _ftfy_fix_text is not None:
                try:
                    text = _ftfy_fix_text(text)
                except Exception:
                    pass
    for origem, destino in _TEXT_FALLBACK_REPLACEMENTS.items():
        text = text.replace(origem, destino)
    return text

_PARENT: Optional[QtWidgets.QWidget] = None


def set_parent(widget: Optional[QtWidgets.QWidget]) -> None:
    global _PARENT
    _PARENT = widget


def _ensure_app() -> QtWidgets.QApplication:
    app = QtWidgets.QApplication.instance()
    if app is None:
        app = QtWidgets.QApplication([])
    return app


def _popup_font() -> QtGui.QFont:
    app = _ensure_app()
    base_font = QtGui.QFont(app.font())
    if base_font.pointSizeF() <= 0:
        base_font.setPointSize(10)
    else:
        base_font.setPointSizeF(10.0)
    return base_font


def _center_message_box_buttons(box: QtWidgets.QMessageBox) -> None:
    button_box = box.findChild(QtWidgets.QDialogButtonBox)
    if button_box is not None:
        button_box.setCenterButtons(True)
    for button in box.buttons():
        try:
            button.setMinimumWidth(110)
        except Exception:
            pass


def _exec_message_box(
    *,
    title: str,
    message: str,
    icon: QtWidgets.QMessageBox.Icon,
    buttons: Iterable[tuple[str, QtWidgets.QMessageBox.ButtonRole]],
    informative_text: str | None = None,
) -> Optional[str]:
    _ensure_app()
    box = QtWidgets.QMessageBox(_PARENT)
    box.setFont(_popup_font())
    box.setWindowTitle(_normalize_text(title))
    box.setText(_normalize_text(message))
    if informative_text:
        box.setInformativeText(_normalize_text(informative_text))
    box.setIcon(icon)
    for text, role in buttons:
        box.addButton(_normalize_text(text), role)
    _center_message_box_buttons(box)
    box.exec()
    clicked = box.clickedButton()
    if clicked is None:
        return None
    try:
        return _normalize_text(clicked.text())
    except RuntimeError:
        return None


def _to_filter(filetypes: Optional[Iterable[Tuple[str, str]]]) -> str:
    if not filetypes:
        return "All Files (*)"
    parts = []
    for label, pattern in filetypes:
        parts.append(f"{_normalize_text(label)} ({pattern})")
    return ";;".join(parts)


class _MessageBox:
    def showinfo(self, title: str, message: str) -> None:
        _exec_message_box(
            title=title,
            message=message,
            icon=QtWidgets.QMessageBox.Information,
            buttons=[("OK", QtWidgets.QMessageBox.AcceptRole)],
        )

    def showwarning(self, title: str, message: str) -> None:
        _exec_message_box(
            title=title,
            message=message,
            icon=QtWidgets.QMessageBox.Warning,
            buttons=[("OK", QtWidgets.QMessageBox.AcceptRole)],
        )

    def showerror(self, title: str, message: str) -> None:
        _exec_message_box(
            title=title,
            message=message,
            icon=QtWidgets.QMessageBox.Critical,
            buttons=[("OK", QtWidgets.QMessageBox.AcceptRole)],
        )

    def askyesno(self, title: str, message: str) -> bool:
        clicked = _exec_message_box(
            title=title,
            message=message,
            icon=QtWidgets.QMessageBox.Question,
            buttons=[
                ("Sim", QtWidgets.QMessageBox.YesRole),
                ("Não", QtWidgets.QMessageBox.NoRole),
            ],
        )
        if clicked is None:
            return False
        return clicked == "Sim"


class _FileDialog:
    def askopenfilename(self, filetypes=None, title: Optional[str] = None) -> str:
        _ensure_app()
        filename, _ = QtWidgets.QFileDialog.getOpenFileName(
            _PARENT,
            _normalize_text(title or "Abrir"),
            "",
            _to_filter(filetypes),
        )
        return filename

    def asksaveasfilename(
        self,
        defaultextension: Optional[str] = None,
        filetypes=None,
        title: Optional[str] = None,
    ) -> str:
        _ensure_app()
        filename, _ = QtWidgets.QFileDialog.getSaveFileName(
            _PARENT,
            _normalize_text(title or "Salvar"),
            "",
            _to_filter(filetypes),
        )
        if filename and defaultextension and not filename.endswith(defaultextension):
            filename += defaultextension
        return filename


messagebox = _MessageBox()
filedialog = _FileDialog()
