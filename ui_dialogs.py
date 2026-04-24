from __future__ import annotations

from typing import Iterable, Tuple, Optional

from PySide6 import QtWidgets, QtCore

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


def _to_filter(filetypes: Optional[Iterable[Tuple[str, str]]]) -> str:
    if not filetypes:
        return "All Files (*)"
    parts = []
    for label, pattern in filetypes:
        parts.append(f"{_normalize_text(label)} ({pattern})")
    return ";;".join(parts)


class _MessageBox:
    def showinfo(self, title: str, message: str) -> None:
        _ensure_app()
        QtWidgets.QMessageBox.information(_PARENT, _normalize_text(title), _normalize_text(message))

    def showwarning(self, title: str, message: str) -> None:
        _ensure_app()
        QtWidgets.QMessageBox.warning(_PARENT, _normalize_text(title), _normalize_text(message))

    def showerror(self, title: str, message: str) -> None:
        _ensure_app()
        QtWidgets.QMessageBox.critical(_PARENT, _normalize_text(title), _normalize_text(message))

    def askyesno(self, title: str, message: str) -> bool:
        _ensure_app()
        reply = QtWidgets.QMessageBox.question(
            _PARENT,
            _normalize_text(title),
            _normalize_text(message),
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )
        return reply == QtWidgets.QMessageBox.Yes


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
