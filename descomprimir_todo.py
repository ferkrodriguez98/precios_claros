import os
import gzip
import shutil
import zipfile
import subprocess
import sys
import shutil as sh
from tqdm import tqdm
import libarchive

EXT_COMP = ['.gz', '.rar', '.zip']
RATIO_ESTIMADO = 10  # Ratio de compresión estimado para CSVs

def ruta_data(archivo, crudo_root, data_root):
    rel_path = os.path.relpath(archivo, crudo_root)
    return os.path.join(data_root, os.path.dirname(rel_path))

def descomprimir_gz(archivo, destino_dir):
    os.makedirs(destino_dir, exist_ok=True)
    destino = os.path.join(destino_dir, os.path.basename(archivo)[:-3])
    if os.path.exists(destino):
        return
    with gzip.open(archivo, 'rb') as f_in:
        with open(destino, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def descomprimir_zip(archivo, destino_dir):
    os.makedirs(destino_dir, exist_ok=True)
    # Para zip, verificamos si todos los archivos ya existen
    with zipfile.ZipFile(archivo, 'r') as zip_ref:
        archivos = zip_ref.namelist()
        todos_existen = all(os.path.exists(os.path.join(destino_dir, f)) for f in archivos)
        if todos_existen:
            return
        zip_ref.extractall(destino_dir)

def descomprimir_rar(archivo, destino_dir):
    os.makedirs(destino_dir, exist_ok=True)
    try:
        # Para rar, verificamos si todos los archivos ya existen
        with libarchive.file_reader(archivo) as archive:
            archivos = [entry.pathname for entry in archive]
            todos_existen = all(os.path.exists(os.path.join(destino_dir, f)) for f in archivos)
            if todos_existen:
                return

        # Si no existen todos, procedemos con la descompresión
        with libarchive.file_reader(archivo) as archive:
            for entry in archive:
                entry_path = os.path.join(destino_dir, entry.pathname)
                if entry.isdir:
                    os.makedirs(entry_path, exist_ok=True)
                else:
                    os.makedirs(os.path.dirname(entry_path), exist_ok=True)
                    with open(entry_path, 'wb') as f:
                        for block in entry.get_blocks():
                            f.write(block)
    except Exception as e:
        print(f"\nError al descomprimir {archivo}: {str(e)}")
        print("Asegurate de tener instalado el paquete libarchive:")
        print("pip install libarchive")
        raise

def copiar_no_comprimido(archivo, destino_dir):
    os.makedirs(destino_dir, exist_ok=True)
    destino = os.path.join(destino_dir, os.path.basename(archivo))
    if os.path.exists(destino):
        return
    shutil.copy2(archivo, destino)

def procesar_archivo(archivo, crudo_root, data_root):
    # Ignorar archivos de metadatos de macOS
    if os.path.basename(archivo).startswith('._'):
        return
        
    destino_dir = ruta_data(archivo, crudo_root, data_root)
    if archivo.endswith('.gz'):
        descomprimir_gz(archivo, destino_dir)
    elif archivo.endswith('.zip'):
        descomprimir_zip(archivo, destino_dir)
    elif archivo.endswith('.rar'):
        descomprimir_rar(archivo, destino_dir)
    else:
        copiar_no_comprimido(archivo, destino_dir)

def contar_archivos_y_comprimidos(ruta):
    total = 0
    comprimidos = 0
    tam_comprimidos = 0
    for root, dirs, files in os.walk(ruta):
        for nombre in files:
            total += 1
            if any(nombre.endswith(ext) for ext in EXT_COMP):
                comprimidos += 1
                tam_comprimidos += os.path.getsize(os.path.join(root, nombre))
    return total, comprimidos, tam_comprimidos

def espacio_libre(path):
    total, used, free = sh.disk_usage(path)
    return free

def recorrer_y_procesar(ruta, crudo_root, data_root):
    archivos = []
    for root, dirs, files in os.walk(ruta):
        for nombre in files:
            archivos.append(os.path.join(root, nombre))
    for archivo in tqdm(archivos, desc="Procesando archivos"):
        procesar_archivo(archivo, crudo_root, data_root)

def verificar_acceso_escritura(path):
    try:
        test_file = os.path.join(path, '.test_write')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        return True
    except (IOError, OSError):
        return False

def main():
    print("Ingresá el path completo a la carpeta de datos crudos")
    print("Para unidades externas en Mac, usá por ejemplo: /Volumes/SSD_Fermin/TP_ANALITICA/crudo")
    print("IMPORTANTE: La carpeta debe llamarse 'crudo'")
    crudo_root = input("Path: ").strip()

    # Verificar que el path termina en 'crudo'
    if os.path.basename(crudo_root) != 'crudo':
        print(f"Error: El path debe terminar en una carpeta llamada 'crudo'")
        print(f"Path actual: {crudo_root}")
        print("Ejemplo correcto: /Volumes/MI_SSD/carpeta/crudo")
        sys.exit(1)

    # Verificar existencia del path
    if not os.path.exists(crudo_root):
        print(f"Error: El path especificado '{crudo_root}' no existe.")
        print("Asegurate que:")
        print("1. La unidad externa esté correctamente montada en /Volumes/")
        print("2. La carpeta 'crudo' exista en la ubicación especificada")
        sys.exit(1)

    # Verificar permisos de escritura
    if not verificar_acceso_escritura(crudo_root):
        print(f"Error: No tenés permisos de escritura en '{crudo_root}'")
        print("Verificá los permisos de la carpeta y que la unidad no esté bloqueada.")
        sys.exit(1)

    data_root = os.path.join(os.path.dirname(crudo_root), 'data')
    
    # Verificar permisos en la carpeta de destino
    if not verificar_acceso_escritura(os.path.dirname(crudo_root)):
        print(f"Error: No tenés permisos para crear la carpeta 'data' en '{os.path.dirname(crudo_root)}'")
        print("Verificá los permisos de la unidad externa.")
        sys.exit(1)

    print(f"La carpeta de salida 'data' se creará en: {data_root}")
    total, comprimidos, tam_comprimidos = contar_archivos_y_comprimidos(crudo_root)
    free = espacio_libre(os.path.dirname(crudo_root))
    estimado = tam_comprimidos * RATIO_ESTIMADO
    print(f"Se encontraron {total} archivos en total.")
    print(f"De los cuales {comprimidos} son comprimidos (.gz, .rar, .zip).")
    print(f"Tamaño total de archivos comprimidos: {tam_comprimidos/1024/1024/1024:.2f} GB.")
    print(f"Espacio estimado necesario para descomprimir (ratio {RATIO_ESTIMADO}x): hasta {estimado/1024/1024/1024:.2f} GB.")
    print(f"Espacio libre en disco: {free/1024/1024/1024:.2f} GB.")
    resp = input("¿Continuar? (s/n): ")
    if resp.lower() != 's':
        print("Cancelado.")
        sys.exit(0)
    recorrer_y_procesar(crudo_root, crudo_root, data_root)

if __name__ == "__main__":
    main()
