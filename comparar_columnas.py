import os
import pandas as pd
from collections import defaultdict
from tqdm import tqdm

def obtener_columnas(archivo):
    try:
        # Leer solo la primera fila para obtener las columnas
        return set(pd.read_csv(archivo, nrows=0).columns)
    except Exception as e:
        print(f"\nError leyendo {archivo}: {str(e)}")
        return set()

def agrupar_columnas_similares(columnas):
    """Agrupa columnas que comparten los primeros 10 caracteres, con manejo especial para fechas"""
    grupos = defaultdict(list)
    
    for col in columnas:
        # Verificar si la columna termina en 8 dígitos (formato fecha YYYYMMDD)
        if len(col) > 8 and col[-8:].isdigit():
            # Para columnas que terminan en fecha, usar todo excepto los últimos 8 caracteres
            prefijo = col[:-8]
        else:
            # Para el resto de columnas, usar los primeros 10 caracteres
            prefijo = col[:10]
        grupos[prefijo].append(col)
    
    # Procesar cada grupo
    columnas_procesadas = set()
    for prefijo, cols in grupos.items():
        if len(cols) == 1:
            # Si solo hay una columna con ese prefijo, la dejamos tal cual
            columnas_procesadas.add(cols[0])
        else:
            # Si hay varias, agregamos *** al prefijo
            columnas_procesadas.add(f"{prefijo}***")
    
    return columnas_procesadas

def analizar_columnas_unicas(ruta):
    # Encontrar todos los CSVs
    archivos_csv = []
    for root, _, files in os.walk(ruta):
        for f in files:
            if f.endswith('.csv') and not f.startswith('._'):
                archivos_csv.append(os.path.join(root, f))
    
    if not archivos_csv:
        print("No se encontraron archivos CSV")
        return
    
    print(f"\nAnalizando {len(archivos_csv)} archivos CSV...")
    
    # Set para almacenar todas las columnas únicas encontradas
    todas_las_columnas = set()
    
    # Procesar cada archivo
    for archivo in tqdm(archivos_csv, desc="Leyendo archivos"):
        columnas = obtener_columnas(archivo)
        todas_las_columnas.update(columnas)
    
    # Agrupar columnas similares
    columnas_agrupadas = agrupar_columnas_similares(todas_las_columnas)
    
    # Mostrar resultados
    print("\n=== Columnas únicas encontradas ===")
    print(f"\nSe encontraron {len(columnas_agrupadas)} columnas únicas en total:")
    for i, col in enumerate(sorted(columnas_agrupadas), 1):
        print(f"{i}. {col}")

def main():
    print("Ingresá el path completo a la carpeta data")
    print("Para unidades externas en Mac, usá por ejemplo: /Volumes/SSD_Fermin/TP_ANALITICA/data")
    data_root = input("Path: ").strip()
    
    if not os.path.exists(data_root):
        print(f"Error: El path especificado '{data_root}' no existe.")
        return
    
    if os.path.basename(data_root) != 'data':
        print(f"Error: La carpeta debe llamarse 'data'")
        print(f"Path actual: {data_root}")
        return
    
    analizar_columnas_unicas(data_root)

if __name__ == "__main__":
    main() 