import pandas as pd
import os
from pathlib import Path

def procesar_comercios():
    # Inicializar un DataFrame para almacenar los resultados
    resultados = pd.DataFrame(columns=['id_comercio', 'comercio_bandera_nombre'])
    errores_encontrados = False
    
    # Recorrer todos los directorios en data_daily_test
    for root, dirs, files in os.walk('data_daily_test'):
        for file in files:
            if file == 'comercio.csv':
                archivo_path = os.path.join(root, file)
                print(f"Procesando: {archivo_path}")
                
                try:
                    # Leer el CSV usando | como separador
                    df = pd.read_csv(archivo_path, 
                                   sep='|')
                    
                    # Filtrar filas que no son datos válidos
                    df = df[df['id_comercio'].astype(str).str.match(r'^\d+$')]
                    
                    # Verificar inconsistencias
                    inconsistencias = df.groupby('id_comercio')['comercio_bandera_nombre'].nunique() > 1
                    if inconsistencias.any():
                        errores_encontrados = True
                        print("\n¡ATENCIÓN! Se encontraron inconsistencias:")
                        for id_comercio in inconsistencias[inconsistencias].index:
                            nombres = df[df['id_comercio'] == id_comercio]['comercio_bandera_nombre'].unique()
                            print(f"id_comercio {id_comercio} tiene múltiples nombres: {nombres}")
                    
                    # Concatenar con resultados existentes
                    resultados = pd.concat([resultados, df[['id_comercio', 'comercio_bandera_nombre']]]).drop_duplicates()
                
                except Exception as e:
                    print(f"Error al procesar {archivo_path}: {str(e)}")
    
    # Convertir id_comercio a número para ordenar correctamente
    resultados['id_comercio'] = pd.to_numeric(resultados['id_comercio'])
    
    # Ordenar resultados por id_comercio y comercio_bandera_nombre
    resultados = resultados.sort_values(['id_comercio', 'comercio_bandera_nombre'])
    
    # Guardar resultados usando | como separador
    resultados = resultados.drop_duplicates()
    resultados.to_csv('comercios_unicos.csv', index=False, sep='|')
    print(f"\nProceso completado. Se encontraron {len(resultados)} comercios únicos.")
    print(f"Resultados guardados en 'comercios_unicos.csv'")
    
    if errores_encontrados:
        print("\n⚠️ ADVERTENCIA: Se encontraron inconsistencias en los datos. Ver detalles arriba.")

if __name__ == "__main__":
    procesar_comercios() 