from tk import *
from utils import *
from library import *
from globalVar import *

# Desativa logs chatos
logging.getLogger("pdfplumber").setLevel(logging.ERROR)
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("PyPDF2").setLevel(logging.ERROR)

try:
    root.iconbitmap("icone.ico")
except Exception as e:
    print("Não consegui carregar ícone:", e)
<<<<<<< HEAD
        
=======
        
check_for_updates()
root.mainloop()
>>>>>>> cf7f8728b28fa478141cdeabb5748912ba2d612b
