import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as pg
import typing
import sys
import logging
import unicodedata
import re
from typing import Optional, List
import os




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
def clean_blacklist_process(value: str, filename: str , id=None, col=None) -> typing.Any:
    path = "rejected_list/"
    blacklist_path = os.path.join(path, filename)
    if not isinstance(value, str):
        if value is None:
            return value
        else:
            raise TypeError(
                f"El valor de la columna [{col}], es: [{value, type(value)}], "
                f"del procedimiento con id = {id}, no puede ser procesado como string"
            )
        
    value = normalize_text(value)
    with open(blacklist_path, "r", encoding="UTF-8") as file:
        
        for line in file:
            line_norm = normalize_text(line.strip())
            if line_norm == value:
                return None
        return value

def clean_whitelist_process(value: str, table: str, table_column: Optional[str], id=None, col=None):

    global new_engine
    query = f"SELECT nombre FROM {table}"
    dataset = pd.read_sql(query, new_engine)
    if table_column:
        list = dataset[table_column].tolist()
    else:
        list = dataset["nombre"].tolist() #columna por default
    
    norm_list = [normalize_text(item) for item in list]
    norm_value = normalize_text(value)
    
    for element in norm_list:
        if norm_value in element:
            return element
    return None

    
def clean_cln_25(value: typing.Any, col: str = None, id: int = None) -> typing.Any:
    if not isinstance(value, str):
        if value is None:
            return None
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
            
    value = clean_blacklist_process(value, "adj_domicilios_blacklist.txt")
    if not value: return value
    if value == "MX":
        return "MEXICO"
    value = clean_whitelist_process(value,"paises",id, col)
    return value

new_engine = create_engine('postgresql+psycopg2://postgres:lazar@192.168.100.40:5432/PNT_cleaning_test')
print(clean_cln_25("mx"))





