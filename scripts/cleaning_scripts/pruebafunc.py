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
from rapidfuzz import fuzz, process


def normalize_text(value: str,col: str = None,id: int = None,allowed_chars: Optional[List[str]] = None,replace_with_space: Optional[List[str]] = None) -> str:
    """
    Normalización de texto:
    - Convierte a mayúsculas.
    - Elimina acentos.
    - Elimina puntuación y símbolos, permitiendo solo letras, números, espacios
      y cualquier carácter especificado en allowed_chars.
    - Colapsa espacios múltiples a uno solo.
    - Si replace_with_space está definido, esos caracteres se reemplazan por espacio en vez de eliminarse.
    """
    if not isinstance(value, str):
        return value

    # Convertir a mayúsculas
    value = value.upper()

    # Eliminación de acentos
    value = ''.join(
        c for c in unicodedata.normalize('NFD', value)
        if unicodedata.category(c) != 'Mn'
    )

    # Reemplazar ciertos caracteres por espacio (ejemplo: ".")
    if replace_with_space:
        for ch in replace_with_space:
            value = value.replace(ch, " ")

    # Construir patrón dinámico
    base_pattern = "A-Z0-9\s"
    if allowed_chars:
        extra = "".join(re.escape(char) for char in allowed_chars)
        base_pattern += extra

    # Eliminar todo lo que no esté en el patrón permitido
    value = re.sub(fr"[^{base_pattern}]", "", value)

    # Quitar espacios extras
    value = value.strip()
    value = re.sub(r"\s+", " ", value)

    return value


def clean_blacklist_process(value: str, filename: str , id=None, col=None, threshold=90) -> typing.Any:
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

    value_norm = normalize_text(value).strip()

    with open(blacklist_path, "r", encoding="UTF-8") as file:
        for line in file:
            line_norm = normalize_text(line.strip())
            
            # ignorar entradas muy cortas
            if len(line_norm) < 3:
                continue

            # comparacion exacta o fuzzy completa
            score = fuzz.ratio(value_norm, line_norm)
            if score >= threshold:
                print(f"El valor '{value}' coincide con la blacklist '{line.strip()}' (score {score})")
                return None

    return value

    

def clean_nombre(value: typing.Any,type: str ,id: int = None, col: str = None) -> typing.Any:
        
    def company_remove(value: str, threshold: int = 90) -> str:
            """
            Verifica si el valor contiene algún sufijo de empresa y retorna None si lo encuentra.
            """
            if not isinstance(value, str) or not value.strip():
                return value  # No procesable, retorna tal cual

            # Lista de sufijos
            suffixes = [
                "S.A. de C.V.",
                "S. de R.L. de C.V.",
                "S.C.",
                "A.C.",
                "S.A.",
                "S. de R.L.",
                "SAB DE CV",
            ]

            # Normalizar valor (mayúsculas, puntos por espacios, quitar extras)
            norm_value = value.upper().replace(".", " ").strip()
            norm_value = re.sub(r"\s+", " ", norm_value)

            # Normalizar sufijos de la misma manera
            norm_suffixes = [re.sub(r"\s+", " ", s.upper().replace(".", " ").strip()) for s in suffixes]

            # Revisar si termina con algún sufijo
            for suffix in norm_suffixes:
                # fuzzy match al final de la cadena
                if fuzz.partial_ratio(norm_value, suffix) >= threshold or norm_value.endswith(suffix):
                    return None

            return value
        
    if not isinstance(value, str):
        if value is None:
            return None
        else:
            raise TypeError(
                f"El valor de la columna [{col}], es: [{value, type(value)}], "
                f"del procedimiento con id = {id}, no puede ser procesado como string"
            )

    # eliminar valores que son solo números
    if value.isdigit():
        return None
    try:
        float(value)
        return None
    except ValueError:
        pass

    # normalizar para procesar prefijos
    norm_value = normalize_text(value=value, replace_with_space=["."])

    # aplicar blacklist
    blacklist_value = clean_blacklist_process(value=value, filename="adj_domicilios_blacklist.txt")
    if not blacklist_value:
        print("ff")
        return None

    company_value = company_remove(value)
    if not company_value:
        return None
    
    # lista de abreviaturas
    profesiones = [
        "Lic.", "Ing.", "Dr.", "Doc.", "Mtro.", "Ma.", "Arq.", "C.P.", "Q.F.B.", "M.C.",
        "M.I.", "M.D.", "M.V.Z.", "Psic.", "Abog.", "T.S.", "Econ.", "Adm.", "C.D.", "Enf.", "C",
        "CIVIL"  # si quieres considerar "CIVIL" como sub-prefijo
    ]
    norm_prof = [normalize_text(abrv, replace_with_space=["."]) for abrv in profesiones]

    # eliminar prefijos iterativamente mientras coincida
    tokens = norm_value.split()
    while tokens and any(fuzz.ratio(tokens[0], abrv) >= 90 for abrv in norm_prof):
        tokens.pop(0)

    # reconstruir el nombre final
    final_name = " ".join(tokens).strip()
    
    tokens = final_name.split()

    if len(tokens) <= 5:
        return final_name
    else: return "--"+final_name

 
            
print(clean_nombre(value="Kareen Giselle", type=2))

    



