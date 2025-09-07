import pandas as pd
from sqlalchemy import create_engine

#configuracion de la base de datos con sqlalchemy
#PostgreSQL: 'postgresql+psycopg2://usuario:contraseña@host:puerto/nombre_db'

new_engine =  create_engine('postgresql+psycopg2://postgres:lazar@l92.168.100.40:5432/PNT_cleaning_test')

tabla = 'procedimientos_adj'

df = pd.read_sql_table(tabla, new_engine)

columnas_analizar = ["ejercicio","fecha_inicio_periodo","fecha_termino_periodo","tipo_procedimiento",
"materia","caracter_procedimiento","nombre_adjudicado","primer_apellido_adjudicado","segundo_apellido_adjudicado",
"razon_social_adjudicado","rfc_adjudicado","domicilio_fiscal_tipo_vialidad","domicilio_fiscal_nombre_vialidad",
"domicilio_fiscal_numero_exterior","domicilio_fiscal_numero_interior","domicilio_fiscal_tipo_asentamiento",
"domicilio_fiscal_nombre_asentamiento","domicilio_fiscal_clave_localidad","domicilio_fiscal_nombre_localidad",
"domicilio_fiscal_clave_municipio","domicilio_fiscal_nombre_municipio_delegacion","domicilio_fiscal_clave_entidad_federativa",
"domicilio_fiscal_nombre_entidad_federativa","domicilio_fiscal_codigo_postal","domicilio_extranjero_pais",
"domicilio_extranjero_ciudad","domicilio_extranjero_calle","domicilio_extranjero_numero","area_solicitante",
"area_responsable_ejecucion","fecha_contrato","fecha_inicio_vigencia_contrato","fecha_termino_vigencia_contrato",
"monto_contrato_sin_impuestos","monto_contrato_con_impuestos","monto_minimo","monto_maximo","tipo_moneda",
"tipo_cambio_referencia","forma_pago","monto_garantias_contragarantias","fecha_inicio_plazo_entrega_ejecucion",
"fecha_termino_plazo_entrega_ejecucion","origen_recursos_publicos","fuente_financiamiento","se_realizaron_convenios_modificatorios",
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

    #analyses = {col: df[col].value_counts() for col in columnas_validadas }
    #forma desglosada:
    """
    analisys = {}
    for col in columnas_validadas:
        analisys[col] = df[col].value_counts()
    """
analyses = {}
for col in columnas_validadas:
    try:
        counts = df[col].value_counts()
        if not counts.empty:  # Solo incluir columnas con datos válidos
            analyses[col] = counts
        else:
            print(f"Advertencia: La columna '{col}' está vacía o no tiene valores válidos.")
    except Exception as e:
        print(f"Error al procesar la columna '{col}': {e}")

# Crear archivo Excel
with pd.ExcelWriter('analisis_variaciones_F2.xlsx', engine='xlsxwriter') as writer:
    # Hoja de Resumen
    summary_data = []
    for col, counts in analyses.items():
        unique_count = len(counts)
        total_freq = counts.sum()
        # Determinar tipo predominante
        dtype = 'Integer' if pd.api.types.is_integer_dtype(df[col]) else \
                'Float' if pd.api.types.is_float_dtype(df[col]) else \
                'Datetime' if pd.api.types.is_datetime64_any_dtype(df[col]) else \
                'String'
        # Observaciones iniciales
        observaciones = (
            f"Valores concentrados en {counts.index[:2].tolist()} con {counts.iloc[:2].sum()/total_freq*100:.1f}%"
            if unique_count > 1 else "Columna constante"
        )
        # Algoritmo sugerido basado en tipo y distribución
        algoritmo_sugerido = (
            'Detección de outliers (Z-score/IQR) y estandarización' if dtype in ['Integer', 'Float'] else
            'Normalización de texto (strip, lower, regex)' if dtype == 'String' else
            'Parsing de fechas (to_datetime, coerce)'
        )
        summary_data.append([col, unique_count, dtype, total_freq, observaciones, algoritmo_sugerido])
    
    # Crear DataFrame de resumen
    summary_df = pd.DataFrame(summary_data, columns=[
        'Columna', 'Valores Únicos', 'Tipo Predominante', 'Frecuencia Total', 'Observaciones', 'Algoritmo Sugerido'
    ])
    summary_df.to_excel(writer, sheet_name='Resumen', index=False)
    
    # Ajustar ancho de columnas en la hoja de Resumen
    worksheet = writer.sheets['Resumen']
    for idx, col in enumerate(summary_df.columns):
        max_len = max(summary_df[col].astype(str).map(len).max(), len(col)) + 2
        worksheet.set_column(idx, idx, max_len if not pd.isna(max_len) else 10)
    
    # Hojas detalladas por columna
    for col, counts in analyses.items():
        # Convertir value_counts a DataFrame
        counts_df = counts.reset_index(name='Frecuencia')
        counts_df.columns = ['Valor', 'Frecuencia']
        # Exportar a hoja (nombre truncado a 31 caracteres por límite de Excel)
        counts_df.to_excel(writer, sheet_name=col[:31], index=False)
        
        # Ajustar ancho de columnas
        worksheet = writer.sheets[col[:31]]
        for idx, col_name in enumerate(counts_df.columns):
            try:
                max_len = max(counts_df[col_name].astype(str).map(len).max(), len(col_name)) + 2
                worksheet.set_column(idx, idx, max_len if not pd.isna(max_len) else 10)
            except ValueError:
                print(f"Advertencia: No se pudo ajustar ancho de columna '{col_name}' en hoja '{col[:31]}'. Usando ancho por defecto.")
                worksheet.set_column(idx, idx, 10)
        
        # Agregar gráfico de barras (solo si hay suficientes datos)
        if len(counts_df) >= 1:  # Requiere al menos 1 fila para el gráfico
            workbook = writer.book
            chart = workbook.add_chart({'type': 'bar'})
            chart.add_series({
                'categories': f'={col[:31]}!$A$2:$A${len(counts_df)+1}',
                'values': f'={col[:31]}!$B$2:$B${len(counts_df)+1}',
                'name': f'Frecuencia de {col}',
                'fill': {'color': '#1f77b4'},
                'line': {'color': '#1f77b4'}
            })
            chart.set_title({'name': f'Valores de {col}'})
            chart.set_x_axis({'name': 'Valor'})
            chart.set_y_axis({'name': 'Frecuencia'})
            worksheet.insert_chart('D2', chart)
        else:
            print(f"Advertencia: No se generó gráfico para la columna '{col}' porque no hay datos suficientes.")


    """for columna in columnas_validadas:
        print(f"\nColumna: {columna}")
        print("Numero de valores únicos: ", df[columna].nunique())

        variaciones = df[columna].value_counts()
        print("Varaciones, valores y frecuencias: " )
        print(variaciones)"""

