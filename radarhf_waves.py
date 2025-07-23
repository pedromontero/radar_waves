import io
from datetime import datetime
import json
from collections import OrderedDict
import psycopg2
import pandas as pd


class Wave:
    """
    Clase para leer y procesar ficheros WLS (waves) de un CODAR SeaSonde.
    Esta versión está refactorizada para ser más robusta, eficiente y fácil de usar.
    """
    # Definimos los tipos de datos como un atributo de clase para mayor claridad
    _DATA_TYPES = {
        'TIME': int, 'MWHT': float, 'MWPD': float, 'WAVB': float, 'WNDB': float,
        'ACNT': int, 'DIST': float, 'RCLL': float, 'WDPT': int, 'MTHD': int,
        'FLAG': int, 'TYRS': int, 'TMON': int, 'TDAY': int, 'THRS': int,
        'TMIN': int, 'TSEC': int, 'PMWH': float, 'LOND': float, 'LATD': float
    }

    def __init__(self, file_wls: str):
        """
        Constructor de la clase.
        """
        self.metadata = {}
        self.data_tables = {}  # Usamos un diccionario: {range_cell_number: DataFrame}
        self.headers = []

        self._process_wls_file(file_wls)

    def _process_wls_file(self, file_wls: str):
        """
        Método principal que coordina la lectura y procesamiento del fichero.
        """
        try:
            with open(file_wls, 'rb') as f:
                content_raw = [line.decode('utf-8', errors='ignore').strip() for line in f.readlines()]
        except FileNotFoundError:
            print(f"ERROR: Fichero no encontrado en la ruta: {file_wls}")
            return  # Termina la inicialización si el fichero no existe

        self._parse_metadata(content_raw)
        self._parse_tables(content_raw)
        self._convert_data_types()

    def _parse_metadata(self, content_raw: list):
        """
        Extrae los metadatos de la cabecera del fichero.
        """
        metadata_lines = []
        for line in content_raw:
            if line.startswith('%Table'):
                break
            if line.startswith('%') and not line.startswith('%%'):
                metadata_lines.append(line.strip('%'))

        for line in metadata_lines:
            if ':' in line:
                parts = line.split(':', 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()
                    if key:
                        self.metadata[key] = value

    def _parse_tables(self, content_raw: list):
        """
        Encuentra y procesa cada bloque de tabla en el fichero.
        Esta versión definitiva lee primero la cabecera global del fichero y luego
        procesa los bloques de datos, haciéndola compatible con ambos formatos.
        """
        # --- PASO 1: Encontrar la cabecera de columnas global para todo el fichero ---
        global_header = []
        for line in content_raw:
            if '%TableColumnTypes:' in line:
                global_header = line.split(':', 1)[1].split()
                break  # Dejamos de buscar una vez encontrada

        if not global_header:
            print(f"AVISO: No se encontró la línea '%TableColumnTypes:' en el fichero. No se puede procesar.")
            return

        # --- PASO 2: Encontrar todos los bloques de datos (%TableStart ... %TableEnd) ---
        table_blocks = []
        current_block = []
        in_block = False
        for line in content_raw:
            if line.startswith('%TableStart'):
                in_block = True
                # Reiniciamos el bloque actual justo después de encontrar la marca de inicio
                current_block = []
                continue # No incluimos la línea %TableStart en el bloque
            if line.startswith('%TableEnd'):
                in_block = False
                table_blocks.append(current_block)
            if in_block:
                current_block.append(line)

        if not table_blocks:
            print("AVISO: No se encontraron bloques de datos (%TableStart/%TableEnd) en el fichero.")
            return

        # --- PASO 3: Iterar sobre cada bloque de datos encontrado ---
        for block in table_blocks:
            range_cell = -1
            data_str = ""

            # Dentro del bloque, buscamos el RangeCell (para Formato A) y las líneas de datos
            for line in block:
                if 'RangeCell:' in line and line.strip().startswith('%'):
                    try:
                        range_cell = int(line.split(':')[1].strip())
                    except (ValueError, IndexError):
                        print(f"AVISO: No se pudo leer el número de RangeCell en la línea: {line}")
                        range_cell = -1
                elif not line.startswith('%'):
                    data_str += line + '\n'

            if not data_str:
                continue

            # Usamos la cabecera global que encontramos en el PASO 1
            df_block = pd.read_csv(io.StringIO(data_str), sep=r'\s+', header=None, names=global_header, na_values='999.00')

            # --- Lógica de decisión para asignar los datos ---
            if range_cell != -1: # Formato A: El RangeCell se especificó dentro del bloque
                self.data_tables[range_cell] = df_block
            elif 'RCLL' in df_block.columns: # Formato B: El RangeCell es una columna en los datos
                df_block.dropna(subset=['RCLL'], inplace=True)
                if df_block.empty:
                    continue
                df_block['RCLL'] = df_block['RCLL'].astype(int)
                for rc_num, group_df in df_block.groupby('RCLL'):
                    self.data_tables[rc_num] = group_df.copy()
            else:
                print("AVISO: El bloque de tabla se omitirá (no contiene '% RangeCell:' ni columna 'RCLL').")

        if self.data_tables:
            first_key = next(iter(self.data_tables))
            self.headers = list(self.data_tables[first_key].columns)

    def _convert_data_types(self):
        """
        Aplica los tipos de datos correctos a todas las columnas conocidas en las tablas.
        """
        for rc_num, table in self.data_tables.items():
            types_to_apply = {col: self._DATA_TYPES[col] for col in table.columns if col in self._DATA_TYPES}
            for col, dtype in types_to_apply.items():
                self.data_tables[rc_num][col] = pd.to_numeric(table[col], errors='coerce').astype(dtype,
                                                                                                  errors='ignore')

    @staticmethod
    def get_time(row: pd.Series) -> datetime:
        """
        Extrae y convierte la fecha y hora de una fila de datos.
        """
        return datetime(
            int(row['TYRS']), int(row['TMON']), int(row['TDAY']),
            int(row['THRS']), int(row['TMIN']), int(row['TSEC'])
        )

    @staticmethod
    def get_wave_values(row: pd.Series) -> tuple[float, float, float]:
        """
        Extrae los valores principales de oleaje de una fila de datos.
        """
        return (
            float(row['MWHT']),
            float(row['MWPD']),
            float(row['WAVB'])
        )


# --- FUNCIONES DE UTILIDAD ---

def convert_into_dictionary(list_of_tuples: list) -> dict:
    """
    Convierte una lista de tuplas en un diccionario de forma más concisa.
    """
    return {key: value for key, value in list_of_tuples}


def read_connection(input_file):
    try:
        with open(input_file, 'r') as f:
            return json.load(f, object_pairs_hook=OrderedDict)
    except FileNotFoundError:
        print(f'File not found: {input_file} ')
        # Evitar 'input' en scripts automáticos; mejor terminar con un error.
        return None


def get_db_connection(db_json):
    database_data = read_connection(db_json)
    if not database_data:
        return None

    connection_string = 'host={0} port={1} dbname={2} user={3} password={4}'.format(
        database_data['host'],
        database_data['port'],
        database_data['dbname'],
        database_data['user'],
        database_data['password']
    )
    try:
        return psycopg2.connect(connection_string)
    except psycopg2.OperationalError as e:
        print(f"PRECAUCIÓN: ERROR AL CONECTAR CON {database_data['host']}\n{e}")
        return None


# --- FUNCIÓN PRINCIPAL DE PROCESAMIENTO ---

def wave2db(site_name, path, file_in):
    db_json_file = r'./pass/svr_database.json'
    full_path_file = f"{path}/{file_in}"

    print(f"Procesando fichero: {full_path_file}")
    wave = Wave(full_path_file)

    connection = get_db_connection(db_json_file)
    if not connection:
        print(f"No se pudo obtener la conexión a la base de datos para el fichero {file_in}. Abortando.")
        return

    try:
        with connection, connection.cursor() as cursor:
            sql = '''SELECT code, pk FROM waves.sites ORDER BY pk ASC'''
            cursor.execute(sql)
            id_sites = convert_into_dictionary(cursor.fetchall())
            id_site = id_sites.get(site_name)

            if id_site is None:
                print(f"ERROR: El sitio '{site_name}' no se encontró en la base de datos.")
                return

            # Iteramos directamente sobre el diccionario .data_tables
            for rcell_number, tabla in wave.data_tables.items():

                # Usamos .iterrows() para una iteración más eficiente y limpia
                for index, row in tabla.iterrows():
                    try:
                        current_date = Wave.get_time(row)
                        height, period, direction = Wave.get_wave_values(row)

                        # Si hay valores NaN (por datos inválidos o filas incompletas), saltamos esa fila
                        if pd.isna(height) or pd.isna(current_date) or pd.isna(period) or pd.isna(direction):
                            continue

                       # print(f'Range = {rcell_number}: date: {current_date} ---> ({height}, {period}, {direction})')

                        date_sql = current_date.strftime('%Y-%m-%d %H:%M:00.00')

                        sql_select = '''SELECT 1 FROM waves.values WHERE datetime = %s AND fk_site = %s AND fk_range = %s'''
                        params_select = (date_sql, id_site, rcell_number)
                        cursor.execute(sql_select, params_select)
                        existe = bool(cursor.fetchone())

                        if not existe:
                            sql_insert = '''INSERT INTO waves.values(fk_site, fk_range, datetime, height, period, direction) 
                                            VALUES(%s, %s, %s, %s, %s, %s)'''
                            params_insert = (id_site, rcell_number, date_sql, height, period, direction)
                            cursor.execute(sql_insert, params_insert)

                    except (ValueError, TypeError, KeyError) as e:
                        print(
                            f"AVISO: Se saltó una fila por datos incorrectos o faltantes en la tabla {rcell_number}. Error: {e}")
                        continue

    except (Exception, psycopg2.Error) as error:
        print(f"Error durante la operación con la base de datos: {error}")

    finally:
        if connection:
            connection.close()


# --- BLOQUE DE EJECUCIÓN PRINCIPAL ---

if __name__ == '__main__':
    # Aquí puedes definir los ficheros que quieres procesar
    files_to_process = {
        'PRIO': ['WVLM_PRIO_2022_02_01_0000.wls', 'WVLM_PRIO_2025_07_01_0000.wls'],
        'SILL': ['WVLM_SILL_2025_06_01_0000.wls', 'WVLM_SILL_2025_07_01_0000.wls']
    }

    base_path = r'../../datos/radarhf_tmp/wls'  # Ajusta esta ruta base

    for station, filenames in files_to_process.items():
        for filename in filenames:
            station_path = f"{base_path}/{station}"
            wave2db(station, station_path, filename)