# PDF Reader (Customer Sales Report)

Desktop app to import PDF reports and online spreadsheets, consolidate sales by seller, safely edit local data, and visualize indicators in charts. Built for internal use with two data sources (MVA and EH) and PDF exports.

## Overview

The app reads PDFs and online spreadsheets, detects the source (MVA/EH), organizes data into separate tables, and lets you:
- Import local PDFs.
- Load the online spreadsheet.
- Merge results when both sources are available.
- Edit the local table (with a save-to-new-PDF flow).
- Export spreadsheets and feedbacks to PDF.
- View charts (MVA, EH, All, Total Sales).

## Main features

- PDF import with source detection (MVA/EH), including tolerance for similar names.
- Integrated online spreadsheet, always read-only tables.
- Controlled merge of local and online sources.
- Local table editing with a dedicated button and unsaved-change warnings.
- PDF export for spreadsheets and feedbacks.
- Charts by metrics: Attended, Returns, Final Total, Total Sales.
- Progress bar(s) with cancel.
- Responsive UI, adjustable columns, rounded borders.

## Requirements

- Windows 10/11
- Python 3.13 (recommended)
- Dependencies in `requirements.txt`

## Setup (dev)

```powershell
python -m venv .venv64
.\.venv64\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

## How to use

1) Click Import to select local PDFs.
2) Use Online spreadsheet to load remote data.
3) Merge Spreadsheets only enables when both sources are available.
4) Click Edit to unlock the local table; on save, a new PDF is generated.
5) Export saves spreadsheets or feedbacks.
6) Charts toggles to the indicators screen.

## Build (Windows)

```powershell
.\.venv64\Scripts\python.exe .\build.py
```

The build generates:
- `dist/Relatorio de Clientes/` (onedir)
- `dist/RelatorioClientes-<version>.zip` (release asset)

## Updates

The app checks GitHub releases and downloads the latest ZIP. After download, it extracts to `%LOCALAPPDATA%\RelatorioClientes` and launches the new version.

## Notes

- Online tables are always read-only.
- Editing applies only to the local table.
- The system identifies MVA/EH by filename and asks for confirmation when needed.

## License

Internal use.
