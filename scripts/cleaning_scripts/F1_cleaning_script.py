import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as pg
import typing
from typing import Optional, List
import sys
import logging
import unicodedata
import re
import os
from rapidfuzz import fuzz, process

table = 'procedimientos_adj'
query_block=[]

rejected_col=["id","id_procedimiento"]
logging.basicConfig(filename='cleaning_errors.log', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


#-------Global normalization functions-------
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

def extract_integer(value): #Extracción de enteros limpios
    if re.fullmatch(r"\d+(\.\d+)?", value):
        return str(int(float(value.split('.')[0])))
    return value

def clean_amount(value: typing.Any, id: int = None, col: str = None) -> typing.Any: #funcion golbal para limpiar montos 

    if not isinstance(value, str):
        if value is None: 
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")

    try:
        if float(value): return float(value)
    except:
        return "NULL"


def clean_nombre(value: typing.Any, type: str, id: int = None, col: str = None) -> typing.Any:

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
            if fuzz.partial_ratio(norm_value, suffix) >= threshold or norm_value.endswith(suffix):
                return "NULL"

        return value

    if not isinstance(value, str):
        if value is None:
            return "NULL"
        else:
            raise TypeError(
                f"El valor de la columna [{col}], es: [{value, type(value)}], "
                f"del procedimiento con id = {id}, no puede ser procesado como string"
            )

    # eliminar valores que son solo números
    if value.isdigit():
        return "NULL"
    try:
        float(value)
        return "NULL"
    except ValueError:
        pass

    # 🔹 Nueva verificación: si contiene cualquier dígito, devolver None
    if any(char.isdigit() for char in value):
        return "NULL"

    # normalizar para procesar prefijos
    norm_value = normalize_text(value=value, replace_with_space=["."])

    # aplicar blacklist
    blacklist_value = clean_blacklist_process(value=value, filename="adj_domicilios_blacklist.txt")
    blacklist_nombre_value = clean_blacklist_process(value=value, filename="adj_nombre_adjudicado.txt")
    if blacklist_nombre_value == "NULL":
        print("ff")
        return "NULL"
    if blacklist_value == "NULL":
        print("ff")
        return "NULL"

    company_value = company_remove(value)
    if company_value == "NULL":
        return "NULL"

    # lista de abreviaturas
    profesiones = [
        "Lic.", "Ing.", "Dr.", "Doc.", "Mtro.", "Ma.", "Arq.", "C.P.", "Q.F.B.", "M.C.",
        "M.I.", "M.D.", "M.V.Z.", "Psic.", "Abog.", "T.S.", "Econ.", "Adm.", "C.D.", "Enf.", "C",
        "CIVIL"
    ]
    norm_prof = [normalize_text(abrv, replace_with_space=["."]) for abrv in profesiones]

    tokens = norm_value.split()
    while tokens and any(fuzz.ratio(tokens[0], abrv) >= 90 for abrv in norm_prof):
        tokens.pop(0)

    final_name = " ".join(tokens).strip()

    tokens = final_name.split()

    if len(tokens) <= 5:
        return final_name
    else:
        return "--" + final_name


#--------------------------------cleaning function throught blacklist process---------------------
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
                return "NULL"

    return value_norm

#--------------------------------cleaning function throught whitelist or catalog process---------------------
def clean_whitelist_process(value, table: str, table_column: str, id=None, col=None, threshold: int = 80):
    global new_engine
    query = f"SELECT {table_column} FROM {table}"
    dataset = pd.read_sql(query, new_engine)
    values = dataset[table_column].dropna().tolist()

    
    # Normalizar listas y valor
    norm_list = [normalize_text(item) for item in values]
    if not value:
        return "NULL"
    norm_value = normalize_text(value)

    # Exact match / subcadena
    for element in norm_list:
        if norm_value in element:
            return element

    # Fuzzy match
    for element in norm_list:
        score = fuzz.partial_ratio(norm_value, element)
        if score >= threshold:
            return element

    return "NULL"

#--------------------------------cleaning function of date columns-------------------------------

def clean_cln_date(value, id=None, col=None)->typing.Any:
    if isinstance(value, str):
        
        date = pd.to_datetime(value, dayfirst=True, errors="coerce")  # convert
        if pd.isna(date):
            return "NULL"
        # Pasar a formato yyyy-mm-dd (string) 
        return date.strftime("%Y-%m-%d")

#--------------------------------cleaning functions per column specific-----------------------------
def clean_cln_1(value, id=None, col=None)-> typing.Any:
    try:
        # Convertir a entero, manejar valores no numéricos
        value = int(value)
    except (ValueError, TypeError):
        logging.error(f"Error al convertir '{value}' a entero en la columna 'ejercicio'")
        return "NULL"
    
    if 2021 <= value <= 2023:
        return value
    else:
        logging.warning(f"Valor '{value}' fuera del rango 2021-2023 en la columna 'ejercicio'")
        return "NULL"
    

def clean_cln_4(value, id=None, col=None) -> typing.Any:

    if not isinstance(value, str):
        if value == None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
    
    value = clean_whitelist_process(value=value, table="tipo_procedimiento", table_column="tipo", id=id, col=col)
    return value


def clean_cln_11(value: str, id=None, col=None) -> typing.Any: #RFC Regex
    if not isinstance(value, str):
        if value == None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")

    value = re.sub(r"\s+", "", value).upper()

    rfc_regex_grouped = (
        r'([A-ZÑ&]{3,4}'          # prefijo
        r'(?:\d{2})'              # año
        r'(?:0[1-9]|1[0-2])'      # mes
        r'(?:0[1-9]|[12]\d|3[01])' # día
        r'[A-Z0-9]{2}[0-9A-Z])'   # homoclave
    )
    rfc_full = re.compile(r'^' + rfc_regex_grouped + r'$')

    invalid_prefixes = {"XXXX"}  
    if value[:4] in invalid_prefixes or value[:3] in invalid_prefixes:
        return "NULL"

    if rfc_full.match(value):
        return value

    rfcs = re.findall(rfc_regex_grouped, value)
    if rfcs:
        seen, out = set(), []
        for r in rfcs:
            if r not in seen:
                seen.add(r)
                out.append(r)
        cleaned_rfc = ", ".join(out)
        return cleaned_rfc

    return "NULL"

def clean_cln_12(value: str, id=None, col=None):
    
    if not isinstance(value, str):
        if value == None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
        
    tipo_vialidad = ["Carretera","Privada","Eje vial","Circunvalación","Brecha","Diagonal","Calle","Corredor","Circuito",
                     "Pasaje","Vereda","Calzada","Viaducto","Prolongación","Boulevard","Peatonal","Retorno","Camino","Callejón",
                     "Cerrada","Ampliación","Continuación","Terracería","Andador","Periférico","Avenida"
                     ]

    norm_tipo_vialidad = [normalize_text(tipo) for tipo in tipo_vialidad]
    if normalize_text(value) in norm_tipo_vialidad:
        return value
    else:
        return "NULL"


def clean_cln_13(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
    if not isinstance(value, (str,int)):
        if value is None:
            return "NULL"
        else:
            raise TypeError(
                f"El valor de la columna [{col}], es: [{value, type(value)}], "
                f"del procedimiento con id = {id}, no puede ser procesado como string"
            )
        
    if isinstance(value, int):
        return value

    if isinstance(value, str) and len(value) == 1 and not re.match(r'^[A-Z0-9]$', value):
        return "NULL"
    
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

def clean_cln_14(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
    if not isinstance(value, (str, int)):
        if value is None:
            return "NULL"
        else:
            raise TypeError(
                f"El valor de la columna [{col}], es: [{value, type(value)}], "
                f"del procedimiento con id = {id}, no puede ser procesado como string"
            )
    
    # Si es numero, hacerlo en string para poder aplicar el regex
    if isinstance(value, (int,float)):
        value = str(value)

    value = normalize_text(value, allowed_chars=['-'])

    if isinstance(value, str) and re.match(r"^(?=.*\d).+$", value):
        return value
    else:
        return "NULL"

def clean_cln_15(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
    if not isinstance(value, (str, int)):
        if value is None:
            return "NULL"
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
    if isinstance(value, str) and re.match(r"^(?=.*\d).+$", value):#Verfifica si contiene un numero para mantener los domicilios largos, para una posterior exyraccion de datos separados
        return value
    elif len(value) < 3:
        blacklist = ["NA","N","NO","SN"] #Hay veces en las que el numero son solamente letras, ejemplo: AB, AC, BA, etc sin embargo aqui se evalua que no pase de 2 caracteres ni que sea igual a esas abreviaciones de la blacklist
        if value in blacklist:
            return "NULL"
        else:
            return value
    else: 
        return "NULL"
    
def clean_cln_16(value: typing.Any, id: int = None, col: str = None): #domicilio_fiscal_nombre_asentamiento

    if not isinstance(value, str):
        if value == None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
        
    tipo_asentamiento = ["Colonia","Ciudad","Fraccionamiento","Parque industrial","Zona industrial","Residencial","Barrio","Pueblo","Sección","Condominio",
                     "Ranchería","Conjunto habitacional","Ejido","Fracción","Ampliación","Ciudad industrial","Paraje","Manzana","Corredor industrial",
                     "Exhacienda","Hacienda","Rancho","Privada","Zona federal","Unidad habitacional","Prolongación","Aeropuerto","Rinconada"
                     ]
    norm_tipo_asentamiento = [normalize_text(tipo) for tipo in tipo_asentamiento]
    if normalize_text(value) in norm_tipo_asentamiento:
        return value
    else:
        return "NULL"
    
def clean_cln_20(value: typing.Any, db_table: Optional[str] = None, id: int = None ,col: str = None):
    if not isinstance(value, str):
        if value is None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
    
    # Primeramente verifica si el valor es un numero, si es un numero, llama a la funcion para normalizarlo a un entero, si no es entero, devuleve el valor normal.
    value = extract_integer(value)

    # En esta capa verifica si el valor normal no está en la blacklist, si está, la fncion retornara un None y será retornado a la funcion principal
    value = clean_blacklist_process(value, "adj_domicilios_blacklist.txt", id, col)
    if not value: # Verifica si es none 
        return "NULL"


    # Si pasa la etapa de la lista negra, verificará si es un nombre de un municipio de querétaro registrado en la tabla de municipios_qro de la base de datos, si lo es, retornará su respectiva clave de municipio
    query = f"SELECT * FROM {db_table}"
    dataset = pd.read_sql(query, new_engine)
    municipios_dic = dataset.set_index("clave")["nombre"].to_dict()
    
    for clave, nombre in municipios_dic.items(): 
        if value == normalize_text(nombre):
            return str(clave)

    # Si no hay coincidencias, devolver tal cual

    return value

def clean_cln_24(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
    if not isinstance(value, str):
        if value is None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
    
    # checar si tiene decimales 
    if re.match(r'^\d+\.\d+$', value):
        cleaned_value = value.replace('.', '')
        if len(cleaned_value) == 5 and cleaned_value.isdigit():
            return cleaned_value
    
    try:
        int(value)
        if len(value) == 5:
            return value
        else:
            return "NULL"
    except:
        return "NULL"

def clean_cln_25(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
    if not isinstance(value, str):
        if value is None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
            
    value = clean_blacklist_process(value, "adj_domicilios_blacklist.txt")
    if value == "NULL": return value
    if value == "MX":
        return "MEXICO"
    value = clean_whitelist_process(value, table="paises",table_column="nombre", id= id, col=col)
    return value

def clean_cln_26(value: typing.Any, id: int = None, col: str = None) -> typing.Any: #domicilio_extranjero_ciudad, domicilio_extranjero_calle, area_solicitante, origen_recursos_publicos,fuente_financiamiento, mecanismos_vigilancia_supervision
    if not isinstance(value, str):
        if value is None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
    try:
        if int(value) or float(value):
            return "NULL"
    except:
        pass
    value = clean_blacklist_process(value=value, filename="adj_domicilios_blacklist.txt", id=id, col=col)

    if value == "NULL":
        return "NULL"
    else:
        return value

def clean_cln_38(value: typing.Any, id: int = None, col: str = None) -> typing.Any: #Tipo de moneda, verifica con listas si hay abreviaciones relaconadas con pesos mexicanos o dolares
    if not isinstance(value, str):
        if value is None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
    value = normalize_text(value)
    if value == "NULL": return "NULL"

    abbrev_files = {
        "MXN": "abreviaciones/mxn.txt",
        "USD": "abreviaciones/usd.txt"
    }

    abbreviations = {}
    for code, path in abbrev_files.items():
        with open(path, "r", encoding="utf-8") as f:
            abbreviations[code] = [normalize_text(line.strip()) for line in f if line.strip()]
    mxn_list = abbreviations["MXN"]
    usd_list = abbreviations["USD"]

    for mxn_ab in mxn_list:
        if mxn_ab == value:
            return "MXN"

    for usd_ab in usd_list:
        if usd_ab == value:
            return "USD"

    return "NULL"
def clean_cln_40(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
    if not isinstance(value, str):
        if value is None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")

    value = normalize_text(value)  # usa librería fuzz para encontrar el porcentaje de parecifo entre las llaves y el valor que se paso de parametro
    transfer_keywords = ["transferencia", "transf", "transfer", "transf"]
    cash_keywords = ["efec","efectivo","contado"]
    keywords = {
        "TRANSFERENCIA BANCARIA": [normalize_text(key) for key in transfer_keywords],
        "EFECTIVO":[normalize_text(key) for key in cash_keywords]
    }
    for new_val, kw in keywords.items():
        for kw_element in kw:
            score = fuzz.partial_ratio(kw_element,value)
            if score >= 80:
                return new_val
    return "NULL"

def clean_cln_46(value: typing.Any, id: int = None, col: str = None) -> typing.Any: #mecanismos_vigilincia_supervision
    if not isinstance(value, str):
        if value is None:
            return "NULL"
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
    try:
        if int(value) or float(value):
            return "NULL"
    except:
        pass
    norm_value = normalize_text(value=value, allowed_chars=["-"])
    value = clean_blacklist_process(value=value, filename="adj_domicilios_blacklist.txt", id=id, col=col)

    if value == "NULL":
        return "NULL"
    else:
        return norm_value

#--------------------------------create query functions-----------------------------

def create_update_query(id_procedimiento: int, cln_value: list, columns: list)->None:
    global table

    set_clauses = []
    for col, val in zip(columns, cln_value):
        # Si es string, se agrega con comillas
        if isinstance(val, str):
            set_clauses.append(f"{col} = '{val}'")
        else:
            set_clauses.append(f"{col} = {val}")
    set_query = ", ".join(set_clauses)

    query = f"UPDATE {table} SET {set_query} WHERE id_procedimiento = {id_procedimiento};"
    query_block.append(query)
    


 

#--------------------------------proccess of cleaning -----------------------------------
def start_cleaning_process(columnas: list, database) -> None:
    global rejected_col
    id_proc = 0
    print("Recorriendo el DataFrame fila por fila:\n")
    for idx in database.index:
        column_list = []
        value_list = []
        fila = database.loc[idx]
        for col in columnas:
            valor = fila[col]
            if col == "id_procedimiento":
                id_proc = valor
            if col == "ejercicio":
                column_list.append(col)
                cleaned_value = clean_cln_1(valor)
                value_list.append(cleaned_value)
            elif col == "fecha_inicio_periodo":
                column_list.append(col)
                cleaned_value = clean_cln_date(valor)
                value_list.append(cleaned_value)
            elif col == "fecha_termino_periodo":
                column_list.append(col)
                cleaned_value = clean_cln_date(valor)
                value_list.append(cleaned_value)
            elif col == "tipo_procedimiento":
                column_list.append(col)
                cleaned_value = clean_cln_4(valor, id_proc, col)
                logging.info(f"tipo_procedimiento: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "fecha_convocatoria_invitacion":#---------------
                column_list.append(col)
                cleaned_value = clean_cln_date(valor)
                value_list.append(cleaned_value)
            elif col == "fecha_junta_aclaraciones":#---------------
                column_list.append(col)
                cleaned_value = clean_cln_date(valor)
                value_list.append(cleaned_value)
            elif col == "nombre_contratista_proveedor":#--
                column_list.append(col)
                cleaned_value = clean_nombre(valor,"adj_nombre_adjudicado.txt" ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "primer_apellido_contratista":
                column_list.append(col)
                cleaned_value =clean_nombre(valor,"adj_primer_apellido_adjudicado.txt" ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)                
            elif col == "segundo_apellido_contratista":
                column_list.append(col)
                cleaned_value = clean_nombre(valor,"adj_segundo_apellido_adjudicado.txt" ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "razon_social_contratista":
                column_list.append(col)
                cleaned_value = clean_blacklist_process(valor,"adj_razon_social_adjudicado.txt" ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "rfc_contratista":
                column_list.append(col)
                cleaned_value = clean_cln_11(valor ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "domicilio_fiscal_tipo_vialidad":
                column_list.append(col)
                cleaned_value = clean_cln_12(valor ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "domicilio_fiscal_nombre_vialidad":
                column_list.append(col)
                cleaned_value = clean_cln_13(valor ,id_proc, col)
                logging.info(f"domicilio_fiscal_nombre_vialidad: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value) 
            elif col == "domicilio_fiscal_numero_exterior":
                column_list.append(col)
                cleaned_value = clean_cln_14(valor ,id_proc, col)
                logging.info(f"domicilio_fiscal_nombre_vialidad: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value) 
            elif col == "domicilio_fiscal_numero_interior":
                column_list.append(col)
                cleaned_value = clean_cln_15(valor ,id_proc, col)
                logging.info(f"domicilio_fiscal_nombre_vialidad: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value) 
            elif col == "domicilio_fiscal_tipo_asentamiento":
                column_list.append(col)
                cleaned_value = clean_cln_16(valor ,id_proc, col)
                logging.info(f"domicilio_fiscal_nombre_vialidad: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value) 
            elif col == "domicilio_fiscal_nombre_asentamiento": 
                column_list.append(col)
                cleaned_value = clean_blacklist_process(valor ,"adj_domicilios_blacklist.txt",id_proc, col)
                logging.info(f"domicilio_fiscal_nombre_vialidad: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value) 
            elif col == "domicilio_fiscal_nombre_localidad": 
                column_list.append(col)
                cleaned_value = clean_blacklist_process(valor ,"adj_domicilios_blacklist.txt",id_proc, col)
                logging.info(f"domicilio_fiscal_nombre_vialidad: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "domicilio_fiscal_clave_municipio":
                column_list.append(col)
                cleaned_value = clean_cln_20(valor,"municipio_qro",id_proc, col) #se le puede mandar como parametro opcional, la tabla de la base de datos
                value_list.append(cleaned_value)
            elif col == "domicilio_fiscal_nombre_municipio":
                column_list.append(col)
                cleaned_value = clean_blacklist_process(valor ,"adj_domicilios_blacklist.txt",id_proc, col)
                value_list.append(cleaned_value)
            elif col == "domicilio_fiscal_clave_entidad_federativa":
                column_list.append(col)
                cleaned_value = clean_cln_20(valor,"entidad_federativa", id_proc, col) #Se utiliza la misma funcion de limpieza que el de clave de minucipio debido a que ocupa la misma lógica
                value_list.append(cleaned_value)
            elif col == "domicilio_fiscal_nombre_entidad_federativa":
                column_list.append(col)
                cleaned_value = clean_whitelist_process(valor, table="entidad_federativa",table_column="nombre", id= id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "domicilio_fiscal_codigo_postal":
                column_list.append(col)
                cleaned_value = clean_cln_24(valor, id_proc, col)
                value_list.append(cleaned_value)
            elif col == "domicilio_extranjero_ciudad":
                column_list.append(col)
                cleaned_value = clean_cln_26(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "domicilio_extranjero_calle":
                column_list.append(col)
                cleaned_value = clean_cln_26(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "domicilio_extranjero_numero":
                column_list.append(col)
                cleaned_value = clean_cln_15(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "area_solicitante":
                column_list.append(col)
                cleaned_value = clean_cln_26(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "area_responsable_ejecucion":
                column_list.append(col)
                cleaned_value = clean_cln_26(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "area_contratante":
                column_list.append(col)
                cleaned_value = clean_cln_26(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "numero_contrato":#---
                column_list.append(col)
                cleaned_value = clean_blacklist_process(value=valor, filename="adj_domicilios_blacklist.txt",id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "fecha_contrato":
                column_list.append(col)
                cleaned_value = clean_cln_date(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "fecha_inicio_vigencia_contrato":
                column_list.append(col)
                cleaned_value = clean_cln_date(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "fecha_termino_vigencia_contrato":
                column_list.append(col)
                cleaned_value = clean_cln_date(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "monto_contrato_sin_impuestos":
                column_list.append(col)
                cleaned_value = clean_amount(value=valor, id=id_proc, col=col)
                value_list.append(valor)
            elif col == "monto_total_contrato_con_impuestos":
                column_list.append(col)
                cleaned_value = clean_amount(value=valor, id=id_proc, col=col)
                value_list.append(valor)     
            elif col == "monto_minimo_con_impuestos":
                column_list.append(col)
                cleaned_value = clean_amount(value=valor, id=id_proc, col=col)
                value_list.append(valor) 
            elif col == "monto_maximo_con_impuestos":
                column_list.append(col)
                cleaned_value = clean_amount(value=valor, id=id_proc, col=col)
                value_list.append(valor) 
            elif col == "tipo_moneda":
                column_list.append(col)
                cleaned_value = clean_cln_38(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "tipo_cambio_referencia":
                column_list.append(col)
                cleaned_value = clean_cln_38(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "forma_pago":
                column_list.append(col)
                cleaned_value = clean_cln_40(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "monto_total_garantias":
                column_list.append(col)
                cleaned_value = clean_amount(value=valor, id=id_proc, col=col)
                value_list.append(valor) 
            elif col == "fecha_inicio_plazo_entrega_ejecucion":
                column_list.append(col)
                cleaned_value = clean_cln_date(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "fecha_termino_plazo_entrega_ejecucion":#-----
                column_list.append(col)
                cleaned_value = clean_cln_date(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "origen_recursos_publicos":
                column_list.append(col)
                cleaned_value = clean_cln_26(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "fuente_financiamiento":
                column_list.append(col)
                cleaned_value = clean_cln_46(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "tipo_fondo_participacion_aportacion":
                column_list.append(col)
                cleaned_value = clean_cln_46(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "mecanismos_vigilincia_supervision":
                column_list.append(col)
                cleaned_value = clean_cln_46(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "area_responsable_informacion":
                column_list.append(col)
                cleaned_value = clean_cln_26(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "fecha_validacion":
                column_list.append(col)
                cleaned_value = clean_cln_date(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "fecha_actualizacion":
                column_list.append(col)
                cleaned_value = clean_cln_date(value=valor, id=id_proc, col=col)
                value_list.append(cleaned_value)



    with open("queries.txt", "w", encoding="utf-8") as file:
        for query in query_block:
            file.write(query + "\n")





new_engine = create_engine('postgresql+psycopg2://postgres:lazar@192.168.100.40:5432/PNT_cleaning_test')

database = pd.read_sql_table(table, new_engine)

columnas = [col for col in database.columns]

start_cleaning_process(columnas, database)