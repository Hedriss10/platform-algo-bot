# src/utils/recapthav1.py

from PIL import Image
import pytesseract

# Carrega a imagem do CAPTCHA
imagem = Image.open('captcha.png')

# Tenta extrair o texto
texto = pytesseract.image_to_string(imagem)

print(f"Texto extraído: {texto}")



class RecapthaV1:
    def __init__(self):
        pass