import pandas as pd
import glob
from pathlib import Path

def combinar_precios(precios):
    """Combina precios diferentes para el mismo día usando guión"""
    precios = [p for p in precios if pd.notna(p)]  # Eliminar NaN
    if not precios:
        return None
    precios = list(set(precios))  # Eliminar duplicados
    if len(precios) == 1:
        return precios[0]
    return '-'.join(sorted(map(str, precios)))

def buscar_precios_cervezas(carpeta_base, ids_cervezas, df_comercios):
    resultados = []

    # Recorrer todos los archivos CSV en la carpeta 2025
    patron_busqueda = str(Path(carpeta_base) / '2025' / '*.csv')
    archivos = glob.glob(patron_busqueda)

    # Filtrar archivos mayoristas
    archivos = [archivo for archivo in archivos if 'mayorista' not in archivo.lower()]
    
    print(f"Encontrados {len(archivos)} archivos CSV de precios para procesar (excluyendo mayoristas)")
    if len(archivos) == 0:
        print("No se encontraron archivos para procesar")
        return

    # Procesar todos los archivos
    for i, archivo in enumerate(archivos, 1):
        print(f"\nProcesando archivo {i}/{len(archivos)}: {archivo}")
        
        try:
            # Leer el archivo CSV
            df = pd.read_csv(archivo, dtype={'id_producto': str}, low_memory=False)
            print(f"Total de filas en el archivo: {len(df)}")

            # Asegurarnos que ambos sean strings
            df['id_producto'] = df['id_producto'].astype(str)
            ids_cervezas['id_producto'] = ids_cervezas['id_producto'].astype(str)

            # Filtrar solo los productos que están en ids_cervezas
            df_filtrado = df[df['id_producto'].isin(ids_cervezas['id_producto'])]
            print(f"Filas encontradas con cervezas: {len(df_filtrado)}")

            if not df_filtrado.empty:
                # Agregar información de comercio y provincia
                for _, row in df_filtrado.iterrows():
                    id_comercio = row['id_comercio']
                    
                    # Buscar nombre del comercio en df_comercios
                    nombre_comercio = 'Desconocido'
                    try:
                        nombre_comercio = df_comercios[df_comercios['id_comercio'] == id_comercio]['comercio_bandera_nombre'].iloc[0]
                    except:
                        pass

                    # Crear un diccionario con los datos
                    fila = {
                        'id_comercio': id_comercio,
                        'id_bandera': row['id_bandera'],
                        'id_sucursal': row['id_sucursal'],
                        'nombre_comercio': nombre_comercio,
                        'provincia': row['sucursales_provincia'] if 'sucursales_provincia' in df.columns else 'Desconocida',
                        'id_producto': row['id_producto'],
                    }

                    # Agregar precios
                    columnas_precios = [col for col in df.columns if col.startswith('precio_')]
                    for col in columnas_precios:
                        fecha = col.replace('precio_', '')
                        fila[fecha] = row[col]

                    resultados.append(fila)

                    print(f"\nArchivo {archivo} procesado correctamente")

        except Exception as e:
            print(f"Error al procesar {archivo}: {str(e)}")
            continue

    # Crear un DataFrame con los resultados
    if resultados:
        df_resultados = pd.DataFrame(resultados)
        print(f"\nResultados totales obtenidos: {len(df_resultados)} filas")

        # Agrupar precios combinando duplicados
        columnas_agrupacion = ['id_comercio', 'id_bandera', 'id_sucursal', 'nombre_comercio', 'provincia', 'id_producto']
        columnas_precio = [col for col in df_resultados.columns if col not in columnas_agrupacion]

        df_agrupado = df_resultados.groupby(columnas_agrupacion).agg(
            {col: combinar_precios for col in columnas_precio}
        ).reset_index()

        # Ordenar columnas de fecha
        columnas_fecha = sorted([col for col in df_agrupado.columns if col not in columnas_agrupacion])
        df_agrupado = df_agrupado[columnas_agrupacion + columnas_fecha]

        # Guardar el resultado en un nuevo archivo CSV
        df_agrupado.to_csv('precios_cervezas_2025.csv', index=False)
        print(f"\nSe guardaron los resultados en precios_cervezas_2025.csv")
        print(f"Dimensiones del archivo final: {df_agrupado.shape}")
    else:
        print("\nNo se encontraron resultados para guardar.")

if __name__ == "__main__":
    carpeta_base = "/Volumes/SSD_Fermin/TP_ANALITICA/data"
    
    # Leer ids_cervezas_unicos.csv
    print("Leyendo archivo ids_cervezas_unicos.csv...")
    ids_cervezas = pd.read_csv('ids_cervezas_unicos.csv', sep='|')
    print(f"Cervezas únicas encontradas: {len(ids_cervezas)}")

    # Leer ids_comercios.csv
    print("\nLeyendo archivo ids_comercios.csv...")
    df_comercios = pd.read_csv('ids_comercios.csv', sep='|')
    print(f"Comercios encontrados: {len(df_comercios)}\n")

    buscar_precios_cervezas(carpeta_base, ids_cervezas, df_comercios)