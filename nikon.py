import time

import db


class FDCPGSQL:
    """ETL PostgreSQL DB
    """
    def get_lastendtime(self, equipment):
        """get apname last insert time
        """
        cursor = db.get_cursor()
        cursor.execute(
            """
            SELECT apname, last_end_time, virtual_recipe
            FROM "lastendtime"
            WHERE TOOLID = %(equipment)s
            AND enabled = "TRUE"
            """,
            {'equipment: self.equipment.upper()'},
        )
        rows = cursor.fetchall()
        return rows


class FDCORACLE:
    """Innoux Oracle DB
    """
    def get_lastendtime(self, table):
        cursor = db.get_cursor()
        cursor.execute(
            """
            SELECT to_date(to_char(max(endtime),'yyyy-mm-dd hh24:mi:ss'),'yyyy-mm-dd hh24:mi:ss') as ora_last_end_time 
            FROM "index_glassout"
            WHERE toolid 
            like 'TLCD__01'
            """,
            {}
        )
        rows = cursor.fetchall()
        return rows

    