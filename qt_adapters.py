from __future__ import annotations

from typing import Callable, Iterable, List, Optional

from PySide6 import QtCore, QtGui, QtWidgets


class _QtAfterBridge(QtCore.QObject):
    schedule = QtCore.Signal(int, object)

    def __init__(self, parent: QtCore.QObject) -> None:
        super().__init__(parent)
        self.schedule.connect(self._schedule_callback, QtCore.Qt.QueuedConnection)

    @QtCore.Slot(int, object)
    def _schedule_callback(self, ms: int, callback: object) -> None:
        if not callable(callback):
            return
        QtCore.QTimer.singleShot(ms, callback)


class QtRootAdapter:
    def __init__(self, widget: QtWidgets.QWidget) -> None:
        self._widget = widget
        self._bridge = _QtAfterBridge(widget)

    def after(self, ms: int, callback: Callable[[], None]) -> None:
        self._bridge.schedule.emit(ms, callback)


class QtVar:
    def __init__(self, setter: Callable[[object], None], value: Optional[object] = None) -> None:
        self._setter = setter
        self._value = None
        if value is not None:
            self.set(value)

    def set(self, value: object) -> None:
        self._value = value
        self._setter(value)

    def get(self) -> object:
        return self._value


class QtProgressBarAdapter:
    def __init__(self, bar: QtWidgets.QProgressBar) -> None:
        self._bar = bar
        self._mode = "determinate"
        self._bar.setRange(0, 100)

    def update_idletasks(self) -> None:
        # Qt updates in the event loop; nothing to do here.
        pass

    def config(self, mode: Optional[str] = None) -> None:
        if mode:
            self._mode = mode
        if self._mode == "indeterminate":
            self._bar.setRange(0, 0)
        else:
            self._bar.setRange(0, 100)

    def start(self, _interval: int = 10) -> None:
        self.config(mode="indeterminate")

    def stop(self) -> None:
        if self._mode == "indeterminate":
            self._mode = "determinate"
            self._bar.setRange(0, 100)

    def __getitem__(self, key: str) -> object:
        if key == "mode":
            return self._mode
        raise KeyError(key)


class QtButtonAdapter:
    def __init__(self, button: QtWidgets.QPushButton) -> None:
        self._button = button

    def configure(self, **kwargs) -> None:
        state = kwargs.get("state")
        if state is not None:
            self._button.setEnabled(state in ("normal", "enabled"))


class QtTreeAdapter:
    def __init__(self, table: QtWidgets.QTableWidget, columns: Iterable[str]) -> None:
        self._table = table
        self._columns = list(columns)
        self._table.setColumnCount(len(self._columns))
        self._table.setHorizontalHeaderLabels(self._columns)
        self._next_id = 0
        self._table.horizontalHeader().setDefaultAlignment(QtCore.Qt.AlignCenter)

    def get_children(self) -> List[int]:
        ids = []
        for row in range(self._table.rowCount()):
            item = self._table.item(row, 0)
            if item is None:
                continue
            row_id = item.data(QtCore.Qt.UserRole)
            if row_id is not None:
                ids.append(int(row_id))
        return ids

    def delete(self, item_id: int) -> None:
        row = self._find_row(item_id)
        if row is not None:
            self._table.removeRow(row)

    def insert(self, _parent: str, _index: str, values: Iterable[object]):
        item_id = self._next_id
        self._next_id += 1
        self._insert_row(self._table.rowCount(), values, item_id)
        return item_id

    def item(self, item_id: int) -> dict:
        row = self._find_row(item_id)
        values = []
        if row is None:
            return {"values": values}
        for col in range(self._table.columnCount()):
            cell = self._table.item(row, col)
            values.append(cell.text() if cell else "")
        return {"values": values}

    def set(self, item_id: int, col: str) -> str:
        if col not in self._columns:
            return ""
        row = self._find_row(item_id)
        if row is None:
            return ""
        col_idx = self._columns.index(col)
        cell = self._table.item(row, col_idx)
        return cell.text() if cell else ""

    def move(self, item_id: int, _parent: str, index: int) -> None:
        row = self._find_row(item_id)
        if row is None or index < 0:
            return
        values = self.item(item_id)["values"]
        self._table.removeRow(row)
        if index > self._table.rowCount():
            index = self._table.rowCount()
        self._insert_row(index, values, item_id)

    def heading(self, col: str, **kwargs):
        if col not in self._columns:
            return {}
        if kwargs:
            # Tk accepts commands here; Qt handles clicks externally.
            return {}
        return {"text": col}

    def scroll_to_top(self) -> None:
        self._table.clearSelection()
        self._table.scrollToTop()
        bar = self._table.verticalScrollBar()
        if bar is not None:
            bar.setValue(bar.minimum())

    def __getitem__(self, key: str):
        if key == "columns":
            return tuple(self._columns)
        raise KeyError(key)

    def _find_row(self, item_id: int) -> Optional[int]:
        for row in range(self._table.rowCount()):
            item = self._table.item(row, 0)
            if item is None:
                continue
            row_id = item.data(QtCore.Qt.UserRole)
            if row_id == item_id:
                return row
        return None

    def _insert_row(self, row: int, values: Iterable[object], item_id: int) -> None:
        self._table.insertRow(row)
        for col, val in enumerate(values):
            item = QtWidgets.QTableWidgetItem(str(val))
            item.setTextAlignment(QtCore.Qt.AlignCenter)
            if col == 0:
                item.setData(QtCore.Qt.UserRole, item_id)
            self._table.setItem(row, col, item)
