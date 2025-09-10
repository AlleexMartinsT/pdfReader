from utils import *
from library import *
from globalVar import *
from tk import *

# Desativa logs chatos
logging.getLogger("pdfplumber").setLevel(logging.ERROR)
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("PyPDF2").setLevel(logging.ERROR)

try:
    root.iconbitmap("icone.ico")
except Exception as e:
    print("Não consegui carregar ícone:", e)
        
if getattr(sys, 'frozen', False):  # rodando como exe
    base_path = sys._MEIPASS
else:  # rodando como script
    base_path = os.path.dirname(os.path.abspath(__file__))

theme_path = os.path.join(base_path, "basedTheme.json")
customtkinter.set_default_color_theme(theme_path)