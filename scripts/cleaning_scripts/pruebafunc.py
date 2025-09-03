import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as pg
import typing
import sys
import logging
import unicodedata
import re


def normalize_text(value: str)-> str:
    """
    Normalization
    -Convert to lowercase
    -Removes accents
    -Removes additional spaces
    -Just leaves basic alphanumeric characters
    """
    if not isinstance(value, str):
        return value
    
    value = value.lower()# convert to lowercase
    #
    value = ''.join( #Remove accents
        c for c in unicodedata.normalize('NFD', value)
        if unicodedata.category(c) != 'Mn'
    )
    
    value = value.strip()#Removes spaces at the beggining to the end
    value = re.sub(r'\s+',' ',value)# Replace multiple spaces to just one
    return value

def clean_cln_13(value: str, col: str = None, id: int = None) -> str:
    if not isinstance(value, (str,int)):
        if value is None:
            return None
        else:
            raise TypeError(
                f"El valor de la columna [{col}], es: [{value, type(value)}], "
                f"del procedimiento con id = {id}, no puede ser procesado como string"
            )
        
    if isinstance(value, int):
        return value

    if isinstance(value, str) and len(value) == 1 and not re.match(r'^[A-Z0-9]$', value):
        return None
    
    value = value.upper()
    value = value.replace('Ñ', '__ENYE__') #este metodo reemplaza la Ñ por _ENYE__ para una depuración limpia sin problemas.

    if len(value) <= 1 and value != 'Ñ':
        return value
    # Eliminar acentos
    value = ''.join(
        c for c in unicodedata.normalize('NFD', value)
        if unicodedata.category(c) != 'Mn'
    )
    # restaurar la Ñ
    value = value.replace('__ENYE__', 'Ñ')#este metodo restaura la ñ depués de la depuración.
    # elimina puntuacion (Unicode category P)
    value = ''.join(
        c for c in value
        if unicodedata.category(c)[0] != 'P'

    )

    # Reemplazar múltiples espacios por uno solo
    value = re.sub(r'\s+', ' ', value).strip()
    
    return value






