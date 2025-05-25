import pandas as pd
import sys
from pathlib import Path
import glob

def combinar_precios(precios):
    """Combina precios diferentes para el mismo día usando guión"""
    precios = [p for p in precios if pd.notna(p)]  # Eliminar NaN
    if not precios:
        return None
    precios = list(set(precios))  # Eliminar duplicados
    if len(precios) == 1:
        return precios[0]
    return '-'.join(sorted(map(str, precios)))

def buscar_producto_en_archivo(archivo_csv, id_producto, df_comercios):
    try:
        print(f"Procesando {archivo_csv}...")
        # Leemos el CSV con el tipo correcto para id_producto
        df = pd.read_csv(archivo_csv, dtype={'id_producto': str}, low_memory=False)
        
        # Si no tiene la columna id_producto, saltamos el archivo
        if 'id_producto' not in df.columns:
            print(f"Archivo {archivo_csv} no tiene la columna id_producto, saltando...")
            return pd.DataFrame()
        
        # Filtramos por el ID específico del producto
        resultado = df[df['id_producto'] == str(id_producto)]
        
        if len(resultado) > 0:
            # Obtenemos todas las columnas que son precios
            columnas_precios = [col for col in df.columns if col.startswith('precio_')]
            
            # Creamos un nuevo dataframe para el resultado
            filas = []
            
            # Para cada fila encontrada
            for _, row in resultado.iterrows():
                id_comercio = row['id_comercio']
                try:
                    # Buscamos el nombre del comercio
                    comercio_info = df_comercios[df_comercios['id_comercio'] == id_comercio].iloc[0]
                    nombre_comercio = comercio_info['comercio_bandera_nombre']
                except:
                    nombre_comercio = f'Comercio_{id_comercio}'
                
                # Creamos un diccionario con los datos base
                fila = {
                    'id_comercio': id_comercio,
                    'id_bandera': row['id_bandera'],
                    'id_sucursal': row['id_sucursal'],
                    'nombre_comercio': nombre_comercio,
                    'provincia': row['sucursales_provincia']
                }
                
                # Agregamos los precios
                for col in columnas_precios:
                    fecha = col.replace('precio_', '')
                    fila[fecha] = row[col]
                
                filas.append(fila)
            
            return pd.DataFrame(filas)
        return pd.DataFrame()
    except Exception as e:
        print(f"Error al procesar {archivo_csv}: {str(e)}")
        return pd.DataFrame()

def ordenar_columnas_fecha(df):
    """Ordena las columnas de fecha en formato YYYYMMDD"""
    # Obtenemos columnas que no son fechas
    columnas_base = ['id_comercio', 'id_bandera', 'id_sucursal', 'nombre_comercio', 'provincia']
    columnas_fecha = [col for col in df.columns if col not in columnas_base]
    
    # Ordenamos las columnas de fecha
    columnas_fecha_ordenadas = sorted(columnas_fecha)
    
    # Retornamos el dataframe con las columnas ordenadas
    return df[columnas_base + columnas_fecha_ordenadas]

def procesar_archivos_2025(carpeta_base, id_producto):
    # Leemos el archivo de comercios
    try:
        df_comercios = pd.read_csv('ids_comercios.csv', sep='|')
    except Exception as e:
        print(f"Error al leer ids_comercios.csv: {str(e)}")
        df_comercios = pd.DataFrame()
    
    # Lista para almacenar todos los resultados
    resultados = []
    
    # Buscamos todos los archivos CSV en la carpeta 2025
    patron_busqueda = str(Path(carpeta_base) / '2025' / '*.csv')
    archivos = glob.glob(patron_busqueda)
    
    # Filtramos archivos mayoristas
    archivos = [archivo for archivo in archivos if 'mayorista' not in archivo.lower()]
    
    # Procesamos cada archivo
    for archivo in archivos:
        df_resultado = buscar_producto_en_archivo(archivo, id_producto, df_comercios)
        if not df_resultado.empty:
            resultados.append(df_resultado)
    
    if not resultados:
        print(f"No se encontró el producto {id_producto} en ningún archivo")
        return
    
    # Combinamos todos los resultados
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Agrupamos por comercio y provincia
    columnas_agrupacion = ['id_comercio', 'id_bandera', 'id_sucursal', 'nombre_comercio', 'provincia']
    columnas_precio = [col for col in df_final.columns if col not in columnas_agrupacion]
    
    # Agregamos los precios, combinando los duplicados
    df_agrupado = df_final.groupby(columnas_agrupacion).agg(
        {col: lambda x: combinar_precios(x) for col in columnas_precio}
    ).reset_index()
    
    # Ordenamos las columnas de fecha
    df_agrupado = ordenar_columnas_fecha(df_agrupado)
    
    # Guardamos el resultado
    nombre_archivo = 'precios_vera_730_2025.csv'
    df_agrupado.to_csv(nombre_archivo, index=False)
    print(f"\nSe guardaron los resultados en {nombre_archivo}")
    print(f"Se encontraron {len(df_agrupado)} registros únicos")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python buscar_producto_anual.py <id_producto>")
        sys.exit(1)
    
    id_producto = sys.argv[1]
    carpeta_base = "/Volumes/SSD_Fermin/TP_ANALITICA/data"
    
    procesar_archivos_2025(carpeta_base, id_producto) 