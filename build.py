from library import *

gerar_versionfile()

<<<<<<< HEAD
hiddenImport = "--hidden-import openpyxl --hidden-import gspread"
dataImport = '--add-data "credenciais.json;." --add-data "mapping.json;."'

if not os.path.exists("credenciais.json"):
    print("❌ ERRO: credenciais.json não encontrado na raiz do projeto!")
    sys.exit(1)

if not os.path.exists("mapping.json"):
    print("❌ ERRO: mapping.json não encontrado na raiz do projeto!")
    sys.exit(1)

os.system(
    f'python -m PyInstaller {hiddenImport} {dataImport} --onefile --noconsole '
    '--icon=icone.ico --name "Relatorio de Clientes" --version-file version.txt main.py'
)
=======
os.system('python -m PyInstaller --onefile --noconsole --icon=icone.ico --name "Relatorio de Clientes" --version-file version.txt --add-data "mapping.json;." main.py')
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
