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
            if line_norm in value:
                return None
        return value

cleaned_value = clean_blacklist_process("NIETO","adj_primer_apellido_adjudicado.txt")