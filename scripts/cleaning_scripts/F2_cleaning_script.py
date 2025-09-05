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

table = 'procedimientos_adj'
query_block=[]

rejected_col=["id","id_procedimiento"]
logging.basicConfig(filename='cleaning_errors.log', level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


#-------Global normalization functions-------
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

def extract_integer(value): #Extracción de enteros limpios
    if re.fullmatch(r"\d+(\.\d+)?", value):
        return str(int(float(value.split('.')[0])))
    return value


#--------------------------------cleaning function throught blacklist process---------------------

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
#--------------------------------cleaning function throught whitelist or catalog process---------------------
def clean_whitelist_process(value, table: str, table_column: Optional[str] = None, id=None, col=None ):


    global new_engine
    query = f"SELECT nombre FROM {table}"
    dataset = pd.read_sql(query, new_engine)
    if table_column:
        list = dataset[table_column].tolist()
    else:
        list = dataset["nombre"].tolist() #columna por default
    
    norm_list = [normalize_text(item) for item in list]
    norm_value = normalize_text(value)
    if not value:
        return None
    
    for element in norm_list:
        if norm_value in element:
            return element
    return None

#--------------------------------cleaning function of date columns-------------------------------

def clean_cln_date(value, id=None, col=None)->typing.Any:
    if isinstance(value, str):
        
        date = pd.to_datetime(value, dayfirst=True, errors="coerce")  # convert
        if pd.isna(date):
            return None
        # Pasar a formato yyyy-mm-dd (string) 
        return date.strftime("%Y-%m-%d")

#--------------------------------cleaning functions per column specific-----------------------------
def clean_cln_1(value, id=None, col=None)-> typing.Any:
    try:
        # Convertir a entero, manejar valores no numéricos
        value = int(value)
    except (ValueError, TypeError):
        logging.error(f"Error al convertir '{value}' a entero en la columna 'ejercicio'")
        return None
    
    if 2021 <= value <= 2023:
        return value
    else:
        logging.warning(f"Valor '{value}' fuera del rango 2021-2023 en la columna 'ejercicio'")
        return None
    

def clean_cln_4(value, id=None, col=None) -> typing.Any:
    rejected_values = ["Otra (especificar)", "Otro (especificar)"]# add other values in the case of more 
    normalized_rejected_values = [normalize_text(v) for v in rejected_values]

    if not isinstance(value, str):
        if value == None:
            return None
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
    
    value = normalize_text(value)
    if value in normalized_rejected_values:
        return "Otro"
    else:
        return value
    

def clean_cln_11(value: str, id=None, col=None) -> typing.Any: #RFC Regex
    if not isinstance(value, str):
        if value == None:
            return None
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
        return None

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

    return None

def clean_cln_12(value: str, id=None, col=None):
    
    if not isinstance(value, str):
        if value == None:
            return None
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
        return None


def clean_cln_13(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
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

def clean_cln_14(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
    if not isinstance(value, (str, int)):
        if value is None:
            return None
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
        return None

def clean_cln_15(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
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
    
def clean_cln_16(value: typing.Any, id: int = None, col: str = None): #domicilio_fiscal_nombre_asentamiento

    if not isinstance(value, str):
        if value == None:
            return None
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
        return None
    
def clean_cln_20(value: typing.Any, db_table: Optional[str] = None, id: int = None ,col: str = None):
    if not isinstance(value, str):
        if value is None:
            return None
        else:
            raise TypeError(f"El valor de la columna [{col}], es: [{value, type(value)}], "
                            f"del procedimiento con id = {id}, no puede ser procesado como string")
    
    # Primeramente verifica si el valor es un numero, si es un numero, llama a la funcion para normalizarlo a un entero, si no es entero, devuleve el valor normal.
    value = extract_integer(value)

    # En esta capa verifica si el valor normal no está en la blacklist, si está, la fncion retornara un None y será retornado a la funcion principal
    value = clean_blacklist_process(value, "adj_domicilios_blacklist.txt", id, col)
    if not value: # Verifica si es none 
        return None


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
            return None
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
            return None
    except:
        return None

def clean_cln_25(value: typing.Any, id: int = None, col: str = None) -> typing.Any:
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
    value = clean_whitelist_process(value, table="paises", id= id, col=col)
    return value
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
            elif col == "nombre_adjudicado":
                column_list.append(col)
                cleaned_value = clean_blacklist_process(valor,"adj_nombre_adjudicado.txt" ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "primer_apellido_adjudicado":
                column_list.append(col)
                cleaned_value = clean_blacklist_process(valor,"adj_primer_apellido_adjudicado.txt" ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)                
            elif col == "segundo_apellido_adjudicado":
                column_list.append(col)
                cleaned_value = clean_blacklist_process(valor,"adj_segundo_apellido_adjudicado.txt" ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "razon_social_adjudicado":
                column_list.append(col)
                cleaned_value = clean_blacklist_process(valor,"adj_razon_social_adjudicado.txt" ,id_proc, col)
                logging.info(f"nombre_adjudicado: Input={valor}, Output={cleaned_value}, ID={id_proc}")
                value_list.append(cleaned_value)
            elif col == "rfc_adjudicado":
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
                cleaned_value = clean_whitelist_process(valor, table="entidad_federativa", id= id_proc, col=col)
                value_list.append(cleaned_value)
            elif col == "domicilio_fiscal_codigo_postal":
                column_list.append(col)
                cleaned_value = clean_cln_24(valor, id_proc, col)
                value_list.append(cleaned_value)
            elif col == "domicilio_extranjero_ciudad":
                column_list.append(col)
                cleaned_value = clean_cln_25(valor, id_proc, col)
        create_update_query(id_proc, value_list, column_list)

    with open("queries.txt", "w", encoding="uft-8") as file:
        for query in query_block:
            file.write(query + "\n")


            #--------inician sentencias if y elif para el proceso de limpieza en las funciones----------
              
            #print(f"  Columna: {col}, Valor: {valor}")
        # Opcional: Imprimir el id_procedimiento de la fila actual
        #print(f"  ID Procedimiento de la fila: {id_proc}")






new_engine = create_engine('postgresql+psycopg2://postgres:lazar@192.168.100.40:5432/PNT_cleaning_test')





database = pd.read_sql_table(table, new_engine)


columnas = [col for col in database.columns]


start_cleaning_process(columnas, database)


