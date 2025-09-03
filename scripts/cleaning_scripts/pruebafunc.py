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


def clean_cln_11(value: str, id=None, col=None) -> typing.Any:
    if not isinstance(value, str):
        return None
    value = value.strip().upper()
    rfc_regex_grouped = (
        r'([A-ZÑ&]{3,4}'          # prefijo
        r'\d{2}'                   # año
        r'(?:0[1-9]|1[0-2])'       # mes (no captura)
        r'(?:0[1-9]|[12]\d|3[01])' # día (no captura)
        r'[A-Z0-9]{2}[0-9A-Z])'    # homoclave (3)
    )
    rfc_full = re.compile(r'^' + rfc_regex_grouped + r'$')

    if rfc_full.match(value):
        return value
   
    rfcs = re.findall(rfc_regex_grouped, value) # Si hay varios RFCs dentro del texto, extrae todos
    if rfcs:
        # Evitar duplicados
        seen, out = set(), []
        for r in rfcs:
            if r not in seen:
                seen.add(r)
                out.append(r)
        cleaned_rfc = ", ".join(out)
        return cleaned_rfc

    return None


print(clean_cln_11("ZASARF660511T64        JATL661125D17"))                 # -> 'ROJR611222AEA'


