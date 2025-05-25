import os
import pandas as pd
import pyreadstat
from tqdm import tqdm
import time

def verificar_archivo(ruta):
    """
    Verifica si el archivo existe y retorna su tamaño
    """
    if not os.path.exists(ruta):
        return False, 0
    
    tamano = os.path.getsize(ruta) / (1024*1024*1024)  # Tamaño en GB
    return True, tamano

def encontrar_archivos_dta(ruta):
    """
    Busca recursivamente todos los archivos .dta en la ruta y sus subcarpetas
    """
    archivos_dta = []
    for root, dirs, files in os.walk(ruta):
        for file in files:
            if file.endswith('.dta') and not file.startswith('._'):
                ruta_completa = os.path.join(root, file)
                existe, tamano = verificar_archivo(ruta_completa)
                if existe:
                    archivos_dta.append((ruta_completa, file, tamano))
    return sorted(archivos_dta, key=lambda x: x[2])  # Ordenar por tamaño

def convertir_dta_a_csv(ruta_origen, ruta_destino):
    """
    Convierte archivos .dta de Stata a CSV
    Args:
        ruta_origen: Ruta donde están los archivos .dta
        ruta_destino: Ruta donde se guardarán los CSV
    """
    # Crear directorio destino específico para archivos convertidos de .dta
    ruta_destino = os.path.join(ruta_destino, "convertidos_dta")
    if not os.path.exists(ruta_destino):
        os.makedirs(ruta_destino)

    # Buscar todos los archivos .dta recursivamente
    archivos_dta = encontrar_archivos_dta(ruta_origen)

    print(f"\nEncontrados {len(archivos_dta)} archivos .dta en total")
    
    # Analizar estado de cada archivo
    archivos_pendientes = []
    archivos_convertidos = []
    
    for ruta_archivo, archivo, tamano_dta in archivos_dta:
        nombre_csv = archivo.replace('.dta', '.csv')
        ruta_csv = os.path.join(ruta_destino, nombre_csv)
        
        if os.path.exists(ruta_csv):
            tamano_csv = os.path.getsize(ruta_csv) / (1024*1024*1024)  # Tamaño en GB
            archivos_convertidos.append((archivo, tamano_csv))
        else:
            archivos_pendientes.append((archivo, ruta_archivo, tamano_dta))

    # Mostrar resumen
    print("\nArchivos ya convertidos:")
    for archivo, tamano in sorted(archivos_convertidos):
        print(f"✓ {archivo} -> {archivo.replace('.dta', '.csv')} ({tamano:.2f} GB)")
    
    print("\nArchivos pendientes por convertir (ordenados por tamaño):")
    for archivo, _, tamano in archivos_pendientes:
        print(f"• {archivo} ({tamano:.2f} GB)")

    if not archivos_pendientes:
        print("\n¡Todos los archivos ya están convertidos!")
        return

    print(f"\nComenzando conversión de {len(archivos_pendientes)} archivos pendientes...\n")

    # Convertir solo los archivos pendientes
    for archivo, ruta_archivo, tamano in archivos_pendientes:
        nombre_csv = archivo.replace('.dta', '.csv')
        ruta_csv = os.path.join(ruta_destino, nombre_csv)

        print(f"\n{'='*80}")
        print(f"Procesando: {archivo} ({tamano:.2f} GB)")
        print(f"Ruta completa: {ruta_archivo}")
        print(f"{'='*80}\n")

        inicio = time.time()
        
        try:
            print(f"[{time.strftime('%H:%M:%S')}] Intentando leer {archivo} con latin1...")
            try:
                df, _ = pyreadstat.read_dta(ruta_archivo, encoding='latin1')
                print(f"[{time.strftime('%H:%M:%S')}] Lectura con latin1 exitosa")
            except Exception as e1:
                print(f"[{time.strftime('%H:%M:%S')}] Error con latin1: {str(e1)}")
                print(f"[{time.strftime('%H:%M:%S')}] Intentando con utf-8...")
                df, _ = pyreadstat.read_dta(ruta_archivo, encoding='utf-8')
                print(f"[{time.strftime('%H:%M:%S')}] Lectura con utf-8 exitosa")
            
            print(f"[{time.strftime('%H:%M:%S')}] Guardando CSV...")
            df.to_csv(ruta_csv, index=False)
            
            fin = time.time()
            tiempo_total = fin - inicio
            print(f"\n✓ Convertido exitosamente: {archivo} -> {nombre_csv}")
            print(f"  Dimensiones: {df.shape[0]} filas x {df.shape[1]} columnas")
            print(f"  Tiempo total: {tiempo_total/60:.1f} minutos")
            
        except Exception as e:
            fin = time.time()
            tiempo_total = fin - inicio
            print(f"\n❌ Error al convertir {archivo} después de {tiempo_total/60:.1f} minutos:")
            print(f"Error: {str(e)}")
            print("\nContinuando con el siguiente archivo...\n")

if __name__ == "__main__":
    # Rutas para los archivos
    RUTA_ORIGEN = "/Volumes/SSD_Fermin/TP_ANALITICA/data"
    RUTA_DESTINO = "/Volumes/SSD_Fermin/TP_ANALITICA/data"
    
    convertir_dta_a_csv(RUTA_ORIGEN, RUTA_DESTINO) 