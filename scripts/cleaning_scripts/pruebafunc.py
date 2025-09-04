import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as pg
import typing
import sys
import logging
import unicodedata
import re
from typing import Optional, List


def normalize_text(value: str, col: str = None, id: int = None, allowed_chars: Optional[List[str]] = None) -> str:
    """
    Normalización de texto:
    - Convierte a minúsculas.
    - Elimina acentos.
    - Elimina puntuación y símbolos, permitiendo solo letras, números, espacios
      y cualquier carácter especificado en allowed_chars.
    - Colapsa espacios múltiples a uno solo.
    """
    if not isinstance(value, str):
        return value

    value = value.upper()
    #eliminacion de acentos
    value = ''.join(
        c for c in unicodedata.normalize('NFD', value)
        if unicodedata.category(c) != 'Mn'
    )

    # patron dinamico del regex
    #defecto: letras, números y espacios
    base_pattern = "A-Z0-9\s"
    if allowed_chars:
        # Escapar caracteres especiales para regex
        extra = "".join(re.escape(char) for char in allowed_chars)
        base_pattern += extra

    # Eliminar todo lo que no esté en el patrón permitido
    value = re.sub(fr"[^{base_pattern}]", "", value)

    value = value.strip()
    value = re.sub(r"\s+", " ", value)

    return value

def clean_cln_14(value: typing.Any, col: str = None, id: int = None) -> typing.Any:
    if not isinstance(value, (str, int)):
        if value is None:
            return None
        else:
            raise TypeError(
                f"El valor de la columna [{col}], es: [{value, type(value)}], "
                f"del procedimiento con id = {id}, no puede ser procesado como string"
            )
    
    # Si es número, conviértelo en string para poder aplicar el regex
    if isinstance(value, (int, float)):
        value = str(value)
    
    # Normaliza el texto si es string --------------------------
    value = normalize_text(value, allowed_chars=['-'])


    # Verifica si contiene al menos un número
    if isinstance(value, str) and re.match(r"^(?=.*\d).+$", value):
        return value
    elif len(value) < 3:
        blacklist = ["NA","N","NO","SN"]
        if value in blacklist:
            return None
        else:
            return value
    else: 
        return None
print(clean_cln_14("S/N"))






