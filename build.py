import os
from library import gerar_versionfile

gerar_versionfile()

hidden_imports = [
    "--hidden-import openpyxl",
    "--hidden-import gspread",
    "--hidden-import customtkinter"
]

data_files = [
    '--add-data "credenciais.json;."',
    '--add-data "mapping.json;."',
    '--add-data "basedTheme.json;."',
    '--add-data "C:/Users/vendas/AppData/Local/Packages/PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0/LocalCache/local-packages/Python313/site-packages/customtkinter;customtkinter/."'
]

hidden_imports_str = " ".join(hidden_imports)
data_files_str = " ".join(data_files)

cmd = f'python -m PyInstaller {hidden_imports_str} {data_files_str} --onefile --noconfirm  --noconsole --icon=icone.ico --name "Relatorio de Clientes" --version-file version.txt main.py'

os.system(cmd)