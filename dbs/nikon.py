from . import db, db_fdc, db_pg


def dictfetchall(cursor):
    """Return all rows from a cursor as a dict
    """
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


class FdcPGSQL:
    """ETL PostgreSQL DB method
    """

    def get_lastendtime(self, toolid, apname):
        """get apname last insert time
        """
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            SELECT "apname", "last_end_time", "virtual_recipe"
            FROM "lastendtime"
            WHERE "toolid" = %(toolid)s
            AND "enabled" = 'TRUE'
            AND "apname" = %(apname)s
            """,
            {
                'toolid': toolid.upper(),
                'apname': apname
            },
        )
        queryset = dictfetchall(cursor)
        return queryset

    def get_pgclass(self, toolid):
        """get pglass rownum data
        """
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            SELECT COUNT(1)
            FROM "pg_class"
            WHERE 1=1
            AND "relname" = %(toolid_rawdata)s
            """,
            {
                'toolid_rawdata': '{}_rawdata'.format(toolid)
            }
        )
        queryset = dictfetchall(cursor)
        return queryset

    def get_schemacolnames(self, toolid):
        """
        """
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            SELECT "column_name"
            FROM "information_schema.columns"
            WHERE 1=1
            AND "table_name" = %(toolid_rawdata)s
            """,
            {
                'toolid_rawdata': '{}_rawdata'.format(toolid)
            }
        )
        rows = cursor.fetchall()
        return rows

    def get_toolid(self, update_starttime, update_endtime, num="01"):
        """
        """
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            SELECT DISTINCT "toolid"
            FROM "index_glassout" s
            WHERE "toolid" LIKE %(tlcd)s
            AND "endtime" > %(update_starttime)s
            AND "endtime" <= %(update_endtime)s
            AND "operationid" in (
                SELECT "proc_operation"
                FROM "tlcd_nikon_avm_operation_associate_ct"
            )
            AND "productid" LIKE 'TL______'
            AND s.toolid in (
                SELECT upper(substr(relname,1,8))
                FROM "pg_class"
                WHERE 1=1
                AND "relname" LIKE 'tlcd__01_rawdata' 
            )
            """,
            {
                'tlcd': 'TLCD__{}'.format(num),
                'update_starttime': update_starttime,
                'update_endtime': update_endtime
            }
        )
        rows = cursor.fetchall()
        return rows

    def get_nikonrot(self, toolid, update_starttime, update_endtime):
        """
        """
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            SELECT *
            FROM %(table)s
            WHERE tstamp >= %(update_starttime)s
            AND tstamp < %(update_endtime)s
            """,
            {
                'table': '{}_rawdata'.format(toolid),
                'update_starttime': update_starttime,
                'update_endtime': update_endtime
            }
        )

    def delete_tlcd(self, psql_lastendtime, ora_lastendtime, num='01'):
        """default num = 01
        """
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            DELETE 
            FROM "index_glassout"  
            WHERE "toolid" 
            LIKE %(tlcd)s
            AND "endtime" > %(psql_lastendtime)s 
            AND "endtime" <= %(ora_lastendtime)s
            """,
            {
                'tlcd': 'TLCD__{}'.format(num),
                "psql_lastendtime": psql_lastendtime,
                "ora_lastendtime": ora_lastendtime
            }
        )

    def delete_toolid(self, toolid, psql_lastendtime, ora_lastendtime):
        """
        """
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            DELETE
            FROM %(toolid_rawdata)s
            WHERE tstamp > %(psql_lastendtime)s
            AND tstamp <= %(ora_lastendtime)s
            """,
            {
                'toolid_rawdata': '{}_rawdata'.format(toolid),
                'psql_lastendtime': psql_lastendtime,
                'ora_lastendtime': ora_lastendtime
            }
        )

    def save_endtime(self, endtime_data):
        """Insert many rows at a times
        """
        cursor = db_pg.get_cursor()
        cursor.executemany(
            """
            INSERT INTO "index_glassout"
            VALUES %(endtime_data)s
            """,
            {'endtime_data': endtime_data}
        )

    def save_edcdata(self, toolid, edcdata):
        """
        """
        cursor = db_pg.get_cursor()
        cursor.executemany(
            """
            INSERT INTO %(toolid_rawdata)s
            VALUES %(edcdata)s
            """,
            {
                "toolid_rawdata": "{}_rawdata".format(toolid),
                "edcdata": edcdata
            }
        )

    def update_lastendtime(self, toolid, apname, last_endtime):
        """
        """
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            UPDATE "lastendtime"
            SET last_endtime = %(last_endtime)s, update_time = now()
            WHERE apname = %(apname)s
            AND toolid = %(toolid)s
            """,
            {
                "ap_lastendtimetblname": "lastendtime",
                "last_endtime": last_endtime,
                "apname": apname,
                "toolid": toolid
            }
        )

    def refresh_nikonmea(self):
        """
        """
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            REFRESH MATERIALIZED VIEW
            tlcd_nikon_mea_process_summary_mv
            """
        )


class FdcOracle:
    """InnoLux  FDC Oracle DB method
    """

    def get_lastendtime(self):
        """
        """
        cursor = db_fdc.get_cursor()
        cursor.execute(
            """
            SELECT to_date(to_char(max(endtime),'yyyy-mm-dd hh24:mi:ss'),'yyyy-mm-dd hh24:mi:ss') as ora_last_end_time 
            FROM fdc.index_glassout
            WHERE toolid 
            LIKE 'TLCD__01'
            """
        )
        row = cursor.fetchone()
        return row

    def get_endtimedata(self, psql_lastendtime, ora_lastendtime, num='01'):
        """
        """
        cursor = db_fdc.get_cursor()
        cursor.execute(
            """
            SELECT *
            FROM fdc.index_glassout
            WHERE toolid LIKE :tlcd
            AND endtime > :psql_lastendtime
            AND endtime <= :ora_lastendtime
            """,
            {
                'tlcd': 'TLCD__{}'.format(num),
                'psql_lastendtime': psql_lastendtime,
                'ora_lastendtime': ora_lastendtime
            }
        )
        queryset = dictfetchall(cursor)
        return queryset

    def get_edcdata(self, colname, toolid, psql_lastendtime, ora_lastendtime):
        """
        """
        cursor = db_fdc.get_cursor()
        cursor.execute(
            """
            SELECT :colname
            FROM :fdc_rawdata
            WHERE tstamp > :psql_lastendtime
            AND tstamp <= :ora_lastendtime
            """,
            {
                'colname': colname,
                'fdc_rawdata': "fdc.{}_rawdata".format(toolid),
                'psql_lastendtime': psql_lastendtime,
                'ora_lastendtime': ora_lastendtime
            }
        )
        queryset = dictfetchall(cursor)
        return queryset


class EdaOracle:
    """InnoLux EDC Oracle DB method
    """

    def get_measrotdata(self, update_starttime, update_endtime):
        """
        """
        cursor = db.get_cursor()
        cursor.execute(
            """
            SELECT 
            a.step_id, a.glass_id, a.glass_start_time,
            a.update_time, a.product_id, a.lot_id, 
            a.equip_id, b.PARAM_COLLECTION, b.PARAM_NAME,
            b.PARAM_VALUE, b.SITE_NAME
            FROM lcdsys.array_glass_v a, cdsys.array_result_v b 
            WHERE 1=1
            AND a.STEP_ID in ( 'DA60','1360' )
            AND a.UPDATE_TIME >= :update_starttime
            AND a.UPDATE_TIME <= :update_endtime
            AND b.PARAM_NAME in ('TP_X','TP_Y')
            AND b.GLASS_ID = a.GLASS_ID
            AND b.STEP_ID = a.STEP_ID
            AND b.GLASS_START_TIME = a.GLASS_START_TIME
            """,
            {
                'update_starttime': update_starttime,
                'update_endtime': update_endtime
            }
        )
        queryset = dictfetchall(cursor)
        return queryset
