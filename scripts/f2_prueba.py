import pandas as pd
from sqlalchemy import create_engine

#configuracion de la base de datos con sqlalchemy
#PostgreSQL: 'postgresql+psycopg2://usuario:contraseña@host:puerto/nombre_db'

new_engine =  create_engine('postgresql+psycopg2://postgres:lazar@localhost:5432/Prueba')

tabla = 'procedimientos_lic_inv'

df = pd.read_sql_table(tabla, new_engine)


columnas_analizar = ["ejercicio","fecha_inicio_periodo","fecha_termino_periodo","tipo_procedimiento","materia_tipo_contratacion",
"caracter_procedimiento","fecha_convocatoria_invitacion","fecha_junta_aclaraciones","nombre_contratista_proveedor",
"primer_apellido_contratista","segundo_apellido_contratista","razon_social_contratista","sexo","rfc_contratista",
"domicilio_fiscal_tipo_vialidad","domicilio_fiscal_nombre_vialidad","domicilio_fiscal_numero_exterior",
"domicilio_fiscal_numero_interior","domicilio_fiscal_tipo_asentamiento","domicilio_fiscal_nombre_asentamiento",
"domicilio_fiscal_clave_localidad","domicilio_fiscal_nombre_localidad","domicilio_fiscal_clave_municipio",
"domicilio_fiscal_nombre_municipio_delegacion","domicilio_fiscal_clave_entidad_federativa","domicilio_fiscal_nombre_entidad_federativa",
"domicilio_fiscal_codigo_postal","domicilio_extranjero_pais","domicilio_extranjero_ciudad","domicilio_extranjero_calle",
"domicilio_extranjero_numero","area_solicitante","area_contratante","area_responsable_ejecucion","fecha_contrato",
"fecha_inicio_vigencia_contrato","fecha_termino_vigencia_contrato","tipo_moneda","tipo_cambio_referencia",
"forma_pago","fecha_inicio_plazo_entrega_ejecucion","fecha_termino_plazo_entrega_ejecucion","fuente_financiamiento",
"tipo_fondo_participacion_aportacion","etapa_obra_publica_servicio","se_realizaron_convenios_modificatorios",
"mecanismos_vigilancia_supervision","area_responsable_informacion","fecha_validacion","fecha_actualizacion"
]

#comprobar si las columnas a analizar existen en la base de datos, si es así, estas se guardaran en un nuevo arreglo 
#de columnas validadas
columnas_validadas = [col for col in columnas_analizar if col in df.columns]

columnas_no_encontradas = []
for col in columnas_analizar:
    if col not in df.columns:
        columnas_no_encontradas.append(col)
        print ("\n columna no encontrada: ", col)


if columnas_no_encontradas:
    print("Hay coumnas de las columnas a analizar que no se encuentran en la base de datos")

if not columnas_validadas:
    print("Ninguna de las columnas fue encontrada en columnas_validadas")
else:
    print("Registro de las columnas validadas: ")
    print(df[columnas_validadas])

    print("\nAnalisis de variaciones de datos en cada columna")
    for columna in columnas_validadas:
        print(f"\nColumna: {columna}")
        print("Numero de valores únicos: ", df[columna].nunique())

        variaciones = df[columna].value_counts()
        print("Varaciones, valores y frecuencias: " )
        print(variaciones)

