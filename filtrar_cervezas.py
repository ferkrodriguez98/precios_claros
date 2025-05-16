import pandas as pd
import sys

def filtrar_cervezas(patron):
    # Leer el CSV de cervezas
    df = pd.read_csv('ids_cervezas.csv', sep='|')
    
    # Filtrar por el patrón (case insensitive)
    df_filtrado = df[df['productos_descripcion'].str.contains(patron, case=False, na=False)]
    
    # Guardar resultado
    nombre_archivo = f'ids_{patron.lower()}.csv'
    df_filtrado.to_csv(nombre_archivo, index=False, sep='|')
    return len(df_filtrado)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python filtrar_cervezas.py <patron>")
        sys.exit(1)
        
    patron = sys.argv[1]
    cantidad = filtrar_cervezas(patron)
    print(f"Se encontraron {cantidad} productos que contienen '{patron}'")
    print(f"Resultados guardados en 'ids_{patron.lower()}.csv'") 