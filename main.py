#!/usr/bin/python

import sys
import os
from src.utils import *
from src.telegram_bot import enviar_mensaje

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir_src = os.path.join(act_dir, 'src')
sys.path.append(proyect_dir_src)


def init():

    try:

        for item in paths:
            source = paths[item]
            object = Load_raw(**source)
            object.verify()
            cerrar_conexiones_sqlcmd()
    except Exception as e:
        log_error.error(str(e), exc_info=True)
        asyncio.run(enviar_mensaje(
            f"error al ejecutar: {str(e)}"))


if __name__ == "__main__":

    init()
    log_error.info(f"\n\n")
