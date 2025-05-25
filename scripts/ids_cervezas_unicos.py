import pandas as pd

def extraer_ids_unicos(ruta_archivo, ruta_salida):
    """
    Extrae los IDs únicos y los nombres de las cervezas desde un archivo CSV.
    En caso de duplicados de ID, mantiene la descripción más larga.
    
    Args:
        ruta_archivo: Ruta del archivo CSV de entrada.
        ruta_salida: Ruta del archivo CSV de salida.
    """
    # Leer el archivo CSV
    df = pd.read_csv(ruta_archivo, sep='|')
    
    # Agregar columna con longitud de descripción
    df['longitud_desc'] = df['productos_descripcion'].str.len()
    
    # Ordenar por ID y longitud de descripción (descendente)
    df_ordenado = df.sort_values(['id_producto', 'longitud_desc'], ascending=[True, False])
    
    # Mantener solo la primera ocurrencia de cada ID (la que tiene la descripción más larga)
    df_unicos = df_ordenado.drop_duplicates(subset=['id_producto'])
    
    # Eliminar columna auxiliar y mantener solo las columnas necesarias
    df_unicos = df_unicos[['id_producto', 'productos_descripcion']]
    
    # Guardar el resultado en un nuevo archivo CSV
    df_unicos.to_csv(ruta_salida, index=False, sep='|')
    print(f"Archivo guardado en: {ruta_salida}")

if __name__ == "__main__":
    RUTA_ARCHIVO = "ids_cervezas.csv"  # Cambia esta ruta según sea necesario
    RUTA_SALIDA = "ids_cervezas_unicos.csv"  # Ruta de salida para el archivo resultante
    
    extraer_ids_unicos(RUTA_ARCHIVO, RUTA_SALIDA)