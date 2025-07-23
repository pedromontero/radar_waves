import os
import getradarfiles
import radarhf_waves  # El fichero que contiene la clase Wave y las funciones de BBDD

# --- CONSTANTE GLOBAL CON LAS ESTACIONES ---
# Si necesitas añadir más estaciones en el futuro, solo tienes que cambiar esta línea.
STATIONS = ['SILL', 'PRIO', 'VILA']


def create_station_directories(data_folder):
    """
    Asegura que los directorios locales para cada estación existan.
    Si un directorio no existe, lo crea.
    """
    print("--- Verificando y creando directorios de estaciones ---")
    for station in STATIONS:
        path = os.path.join(data_folder, 'radarhf_tmp', 'wls', station)
        # os.makedirs crea la ruta completa y con exist_ok=True no falla si ya existe
        os.makedirs(path, exist_ok=True)
        print(f"Directorio asegurado: {path}")


def waves2db(data_folder):
    """
    Busca y procesa todos los ficheros .wls para las estaciones definidas.
    """
    for station in STATIONS:
        path = os.path.join(data_folder, 'radarhf_tmp', 'wls', station)

        if not os.path.isdir(path):
            print(f"Directorio no encontrado, se omite el procesamiento para: {path}")
            continue

        print(f"\n--- Buscando ficheros para procesar en: {path} ---")

        try:
            filenames_in_dir = os.listdir(path)
        except FileNotFoundError:
            print(f"Directorio no encontrado, se omite: {path}")
            continue

        for filename in filenames_in_dir:
            if filename.endswith('.wls'):
                radarhf_waves.wave2db(station, path, filename)
            else:
                print(f"Se ignora el fichero '{filename}' porque no es un .wls")


def delete_processed_files(data_folder):
    """
    Busca y borra todos los ficheros .wls que han sido procesados.
    """
    print("\n--- Limpiando ficheros procesados ---")

    for station in STATIONS:
        path = os.path.join(data_folder, 'radarhf_tmp', 'wls', station)

        if not os.path.isdir(path):
            print(f"Directorio no encontrado para limpiar: {path}")
            continue

        for filename in os.listdir(path):
            if filename.endswith('.wls'):
                file_to_delete = os.path.join(path, filename)
                try:
                    os.remove(file_to_delete)
                    print(f"Fichero borrado: {file_to_delete}")
                except OSError as e:
                    print(f"Error al borrar el fichero {file_to_delete}: {e}")


if __name__ == '__main__':
    root = r'../datos/'

    # 1. Asegurar que los directorios locales existen (NUEVO PASO)
    create_station_directories(root)

    # 2. Descargar los ficheros (ahora no fallará por falta de directorios)
    getradarfiles.get_waves_files(STATIONS, root, number_of_last_files=3)

    # 3. Procesar TODOS los ficheros descargados
    waves2db(root)

    # 4. Borrar los ficheros procesados
    delete_processed_files(root)

    print("\nProceso completado.")