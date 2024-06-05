import psutil
import logging
import pandas as pd
import os
import sys
import sqlalchemy as sa
from sqlalchemy import text, Engine, Connection, Table
from urllib.parse import quote
import yaml
from pandas import DataFrame
from pandas.io.sql import SQLTable
import time
from datetime import datetime, timedelta
import warnings
from src.telegram_bot import enviar_mensaje
import asyncio
import subprocess

warnings.simplefilter(action='ignore', category=UserWarning)


act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)


log_main = logging.basicConfig(
    level=logging.INFO,
    filename=(os.path.join(proyect_dir, 'log', 'logs_main.log')),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)
log_error = logging.getLogger('error')
log_error.setLevel(logging.INFO)

handler_other = logging.FileHandler(
    os.path.join(proyect_dir, 'log', 'logs_error.log'))
handler_other.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s", datefmt='%d-%b-%y %H:%M:%S')
handler_other.setFormatter(formatter)

log_error.addHandler(handler_other)


with open(os.path.join(proyect_dir, 'config', 'credentials.yml'), 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1 = config['source1']
    except yaml.YAMLError as e:
        log_error.error(str(e), exc_info=True)


with open(os.path.join(proyect_dir, 'config', 'path_files.yml'), 'r') as f:

    try:
        paths = yaml.safe_load(f)
    except yaml.YAMLError as e:
        log_error.error(str(e), exc_info=True)


def cerrar_conexiones_sqlcmd():
    for proceso in psutil.process_iter(['pid', 'name']):
        if proceso.info['name'] == 'sqlcmd':
            proceso.kill()


class Engine_sql:

    def __init__(self, username: str, password: str, host: str, database: str, port: str = 1433) -> None:
        self.user = username
        self.passw = password
        self.host = host
        self.dat = database
        self.port = port

    def get_engine(self) -> Engine:
        return sa.create_engine(f"mssql+pyodbc://{self.user}:{quote(self.passw)}@{self.host}:{self.port}/{self.dat}?driver=ODBC+Driver+17+for+SQL+Server")

    def get_connect(self) -> Connection:
        return self.get_engine().connect()


engine_49 = Engine_sql(**source1)


class Load_raw:

    def __init__(self, path_orig: str, sheet_name: str, table_dest: str, backup: str, sp: str) -> None:
        self.path_orig = path_orig
        self.sheet_name = sheet_name
        self.table_dest = table_dest
        self.backup = os.path.join(proyect_dir, 'shared', backup)
        self.sp = sp
        self.mov = bool
        self.file_path = ''
        self.file_name = ''
        self.time = datetime.now() - timedelta(hours=1)

    def to_sql_replace(self, table: SQLTable, con: Engine | Connection, keys: list[str], data_iter):

        satable: Table = table.table
        ckeys = list(map(lambda s: s.replace(' ', '_'), keys))
        data = [dict(zip(ckeys, row)) for row in data_iter]
        values = ','.join(f':{nm}' for nm in ckeys)
        stmt = f"REPLACE INTO {satable.name} VALUES ({values})"
        con.execute(text(stmt), data)

    def set_file_path(self):

        logging.info(
            f"Ruta absoluta: {str(abs_path := os.path.join(proyect_dir, 'shared', self.path_orig))}")

        logging.info(
            f"Nombre de Archivo: {str(file_name := str(os.listdir(abs_path)[0]))}")

        logging.info(
            f"Archivo cargado en carpeta el: {(time.ctime(os.path.getmtime(os.path.join(abs_path, self.file_name))))}")

        self.file_name = file_name
        self.file_path = os.path.join(abs_path, file_name)

        asyncio.run(enviar_mensaje(f"{self.file_name} \U00002755"))

    def extract(self) -> DataFrame:

        try:
            df = pd.read_excel(self.file_path, header=0,
                               sheet_name=self.sheet_name)
            return df
        except Exception as e:
            log_error.error(str(e), exc_info=True)

            asyncio.run(enviar_mensaje(
                f"\U0000274c error al extraer el archivo{self.file_name}: {str(e)}"))

    def transform(self, df: DataFrame, engine: Connection) -> DataFrame:

        with engine as con:

            try:
                column_names = con.execute(
                    text(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{self.table_dest}'")).fetchall()

                column_names = [row[0] for row in column_names]

                df = df.iloc[:, :len(column_names)-4]
                df = df.dropna(how='all')
                df['archivo_origen'] = self.file_name
                df['fecha_actualizacion'] = (
                    self.time.strftime("%Y-%m-%d %H:%M:%S"))
                print(self.file_name[-15:-5])
                df['fecha_datos'] = pd.to_datetime(
                    self.file_name[-15:-5], yearfirst=True)
                df['Fecha_inserto'] = (self.time.strftime("%Y-%m-%d %H:%M:%S"))

                df = df.rename(columns=dict(zip(df.columns, column_names)))

                return df
            except Exception as e:

                log_error.error(str(e), exc_info=True)
                asyncio.run(enviar_mensaje(
                    f"\U0000274c error al tranformar el archivo{self.file_name}: {str(e)}"))

    def load(self, df: pd.DataFrame, engine: Connection) -> None:

        with engine as con:

            try:

                df.to_sql(self.table_dest, con=con, if_exists='append',
                          index=False)

                logging.info(
                    f'\U0000303d Se cargan {(len_df := len(df))} datos')
                asyncio.run(enviar_mensaje(f"Cargados {len_df} registros"))

                self.sp_exec(**source1)

                if self.mov:

                    subprocess.run(
                        f"sudo mv -f '{self.file_path}' '{self.backup}' ", shell=True)

                    asyncio.run(enviar_mensaje(
                        f"\U00002714 Backup correcto"))
                    asyncio.run(enviar_mensaje(
                        f"_______________________________"))

                else:
                    asyncio.run(enviar_mensaje(
                        f"\U0000274c ejecucion SP fallo"))
                    asyncio.run(enviar_mensaje(
                        f"_______________________________"))

            except Exception as e:

                log_error.error(str(e), exc_info=True)
                asyncio.run(enviar_mensaje(
                    f"\U0000274c error al cargar al servidor: {str(e)}"))

    def sp_exec(self, username: str, password: str, host: str, database: str):
        command = [
            '/opt/mssql-tools/bin/sqlcmd',
            '-S', host,
            '-U', username,
            '-P', password,
            '-d', database,
            '-Q', self.sp
        ]

        try:
            result = subprocess.run(
                command, capture_output=True, text=True, check=True)
            if 'failed' in result.stdout or 'denied' in result.stdout:
                self.mov = False
            else:
                self.mov = True
            asyncio.run(enviar_mensaje(f"{result.stdout}"))
        except subprocess.CalledProcessError as e:
            asyncio.run(enviar_mensaje(
                "\U0000274c Error al ejecutar el SP:", e))

    def exec(self):
        self.set_file_path()
        self.load(self.transform(self.extract(),
                  engine_49.get_connect()), engine_49.get_connect())

    def verify(self):
        if len(os.listdir(os.path.join(proyect_dir, 'shared', self.path_orig))) == 0:
            log_error.info(f"{self.path_orig} sin archivos")
            pass
        else:
            self.exec()
