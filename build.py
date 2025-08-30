
from library import *

gerar_versionfile()

os.system('python -m PyInstaller --onefile --noconsole --icon=icone.ico --name "Relatorio de Clientes" --version-file version.txt main.py')