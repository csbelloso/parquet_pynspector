#Importamos todas las librerias que necesitamos
import os
import sys
import importlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Comprobamos que viene la ruta y nombre del fichero a procesar

if len(sys.argv) != 2 :
    print("[ERROR] Uso:\n\tparquet_pynspector.py nombre_fichero.parquet")
    sys.exit(1)

filename = sys.argv[1]

# Lista de bibliotecas que necesitas
libraries = ['pandas', 'pyarrow']

for lib in libraries:
    try:
        importlib.import_module(lib)
    except ImportError:
        print(f"{lib} no está instalada. Por favor, instálala antes de ejecutar este script.")
        sys.exit(1)

def clearscr():

    # borrado de pantalla para Windows
    if os.name == 'nt':
        _ = os.system('cls')

    # borrado de sistemas Linux, MacOS
    else:
        _ = os.system('clear')

def banner():
    clearscr()
    print('[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]')
    print('[][][]                   PARQUET FILE PY-NSPECTOR                     [][][]')
    print('[][][]                           by CSB                               [][][]')
    print('[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]\n')

# Cargamos el archivo Parquet utilizando memory map

def metadatos (filename):  

    with pa.memory_map(filename, 'r') as source:
        tabla_parquet = pq.read_table(source)
        metadata = pq.read_metadata(source)

    tamanio = os.stat(filename).st_size

    #Metadatos del fichero
    print("\nMETADATOS DEL FICHERO:\n")
    print("\tNombre del fichero: ", filename)
    print("\tTamaño en Mb: ",round(tamanio/(1024*1024),2))
    print("\tVersión de PARQUET: ",metadata.format_version)
    print("\tCreado por: ",metadata.created_by[0:26])
    print("\tNúmero de Columnas: ",metadata.num_columns)
    print("\tNúmero de Filas: ",metadata.num_rows,"\n")

    # Obtener el esquema
    esquema = tabla_parquet.schema

    # Imprimir información sobre las columnas
    print("CAMPOS DEL FICHERO PARQUET:\n")
    for campo in esquema:
        print(f"\t{campo.name}, Tipo: {campo.type}")
        
def main(filename):
    banner()
    metadatos(sys.argv[1])
    respuesta = input ("\n\nQuieres exportar el contenido en CSV? [S]/[N]: ")
    match respuesta:
        case "s" | "S":
            exportcsv(filename)
            print("\n\n\tFichero CSV creado. Saliendo.\n")
            exit(0)
        case "n" | "N":
            print ("\n\n\tNo se exporta en CSV. Saliendo.\n")
            exit(0)
        case _:
            main(filename)
            
def exportcsv (filename):
    # Leer los archivos Parquet utilizando memory map
    output_file=filename+"CSV.csv"
    dfs = []

    with pa.memory_map(filename, 'r') as source:
         df = pa.parquet.read_table(source).to_pandas()
         dfs.append(df)

    # Combinar dataframes
    combined_df = pd.concat(dfs, ignore_index=True)

    # Guardar el resultado en un archivo CSV
    combined_df.to_csv(output_file, index=False)
    
main(filename)

