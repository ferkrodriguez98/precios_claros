import pandas as pd
import os
from pathlib import Path

def procesar_cervezas():
    # Inicializar un DataFrame para almacenar los resultados
    resultados = pd.DataFrame(columns=['id_comercio', 'id_producto', 'productos_descripcion'])
    
    # Recorrer todos los directorios en data_daily_test
    for root, dirs, files in os.walk('data_daily_test'):
        for file in files:
            if file == 'productos.csv':
                try:
                    # Extraer el id_comercio del path
                    id_comercio = int(root.split('sepa-')[-1].split('_')[0])
                    
                    # Leer el CSV usando | como separador
                    df = pd.read_csv(os.path.join(root, file), 
                                   sep='|',
                                   usecols=['id_producto', 'productos_descripcion'],
                                   dtype={'id_producto': str},  # Leer id_producto como string
                                   low_memory=False)
                    
                    # Filtrar solo productos que contengan "cerveza" (case insensitive)
                    df = df[df['productos_descripcion'].str.contains('cerveza', case=False, na=False)]
                    
                    if not df.empty:
                        # Agregar el id_comercio y concatenar
                        df['id_comercio'] = id_comercio
                        resultados = pd.concat([resultados, df[['id_comercio', 'id_producto', 'productos_descripcion']]])
                
                except Exception as e:
                    continue
    
    # Convertir id_comercio a número y ordenar
    resultados['id_comercio'] = pd.to_numeric(resultados['id_comercio'])
    resultados = resultados.sort_values(['id_comercio', 'id_producto']).drop_duplicates()
    
    # Leer y mergear con datos de comercios
    comercios = pd.read_csv('ids_comercios.csv', sep='|')
    resultados = pd.merge(resultados, comercios, on='id_comercio', how='left')
    
    # Reordenar columnas
    resultados = resultados[['id_comercio', 'comercio_bandera_nombre', 'id_producto', 'productos_descripcion']]
    
    # Guardar resultados
    resultados.to_csv('ids_cervezas.csv', index=False, sep='|')

if __name__ == "__main__":
    procesar_cervezas() 