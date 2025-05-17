import pandas as pd
import sys
from pathlib import Path

def buscar_producto(archivo_csv, id_producto, df_comercios):
    try:
        # Leemos el CSV
        df = pd.read_csv(archivo_csv)
        
        # Filtramos por el ID específico del producto
        resultado = df[df['id_producto'] == int(id_producto)]
        
        if len(resultado) > 0:
            # Obtenemos todas las columnas que son precios
            columnas_precios = [col for col in df.columns if col.startswith('precio_')]
            
            # Creamos un nuevo dataframe para el resultado
            filas = []
            
            # Para cada fila encontrada
            for _, row in resultado.iterrows():
                id_comercio = row['id_comercio']
                # Buscamos el nombre del comercio
                nombre_comercio = df_comercios[df_comercios['id_comercio'] == id_comercio]['comercio_bandera_nombre'].iloc[0] if not df_comercios.empty else 'Desconocido'
                
                # Creamos un diccionario con los datos base
                fila = {
                    'id_comercio': id_comercio,
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

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python buscar_producto.py <archivo_csv> <id_producto>")
        sys.exit(1)
    
    archivo_csv = sys.argv[1]
    id_producto = sys.argv[2]
    
    # Leemos el archivo de comercios
    try:
        df_comercios = pd.read_csv('ids_comercios.csv', sep='|')
    except Exception as e:
        print(f"Error al leer ids_comercios.csv: {str(e)}")
        df_comercios = pd.DataFrame()
    
    # Buscamos el producto
    df_resultado = buscar_producto(archivo_csv, id_producto, df_comercios)
    
    if not df_resultado.empty:
        # Guardamos el resultado
        nombre_archivo = 'precios_vera_730.csv'
        df_resultado.to_csv(nombre_archivo, index=False)
        print(f"\nSe guardaron los resultados en {nombre_archivo}")
        print(f"Se encontraron {len(df_resultado)} registros")
    else:
        print(f"No se encontró el producto {id_producto} en {archivo_csv}") 