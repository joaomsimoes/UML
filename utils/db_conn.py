from utils.keys import DB
import logging
from datetime import datetime
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)


def conn_database():
    return create_engine(DB)


def query(prod=None, values=None):
    try:
        engine = conn_database()
        connection = engine.raw_connection()
        cursor = connection.cursor()
        cursor.callproc(prod, values)
        results = list(cursor.fetchall())
        connection.commit()
        connection.close()

        return results

    except Exception as e:
        now = datetime.now()
        logging.exception(now.strftime('%H:%M:%S') + " ; " + str(e) + "\n")
