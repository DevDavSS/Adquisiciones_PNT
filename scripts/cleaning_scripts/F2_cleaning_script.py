import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as pg
import typing
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


#-------Global normalization function-------
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
#--------------------------------cleaning function with date columns-------------------------------

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

    invalid_prefixes = {"XXXX", "YYYY", "ZZZZ"}  
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
    if value in tipo_vialidad:
        return value
    else:
        return None
    
        
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
        create_update_query(id_proc, value_list, column_list)

    with open("queries.txt", "w") as file:
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


