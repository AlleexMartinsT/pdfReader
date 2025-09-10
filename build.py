from library import *

gerar_versionfile()

hiddenImport = "--hidden-import openpyxl --hidden-import gspread"
dataImport = '--add-data "credenciais.json;." --add-data "mapping.json;."'

os.system(
    f'python -m PyInstaller {hiddenImport} {dataImport} --onefile --noconsole '
    '--icon=icone.ico --name "Relatorio de Clientes" --version-file version.txt main.py'
)