from __future__ import annotations

from typing import Iterable, Tuple, Optional

from PySide6 import QtWidgets, QtCore

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
        parts.append(f"{label} ({pattern})")
    return ";;".join(parts)


class _MessageBox:
    def showinfo(self, title: str, message: str) -> None:
        _ensure_app()
        QtWidgets.QMessageBox.information(_PARENT, title, message)

    def showwarning(self, title: str, message: str) -> None:
        _ensure_app()
        QtWidgets.QMessageBox.warning(_PARENT, title, message)

    def showerror(self, title: str, message: str) -> None:
        _ensure_app()
        QtWidgets.QMessageBox.critical(_PARENT, title, message)

    def askyesno(self, title: str, message: str) -> bool:
        _ensure_app()
        reply = QtWidgets.QMessageBox.question(
            _PARENT,
            title,
            message,
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
        )
        return reply == QtWidgets.QMessageBox.Yes


class _FileDialog:
    def askopenfilename(self, filetypes=None, title: Optional[str] = None) -> str:
        _ensure_app()
        filename, _ = QtWidgets.QFileDialog.getOpenFileName(
            _PARENT,
            title or "Abrir",
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
            title or "Salvar",
            "",
            _to_filter(filetypes),
        )
        if filename and defaultextension and not filename.endswith(defaultextension):
            filename += defaultextension
        return filename


messagebox = _MessageBox()
filedialog = _FileDialog()
