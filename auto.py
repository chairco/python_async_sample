import time

from concurrent import futures

try:
    from . import db
except Exception as e:
    import db


__all__ = [
            'get_edc_glass_history', 'get_edc_data',
            'get_teg_glass_history', 'get_teg_raw_data',
            'get_teg_summary_data'
        ]


class GlassDoesNotExist(ValueError):

    def __init__(self, *, glass_id):
        super().__init__(f'(type={glass_id!r})')


def get_edc_glass_history(glass_id):
    """From array_pds_glass_t table
    type glass_id: list()
    rtype: row(step_id, glass_id, start_time, sub_equip, product_id)
    """
    cursor = db.get_cursor()
    cursor.execute(
        """
        SELECT "GLASS_ID", "STEP_ID", "GLASS_START_TIME" 
        FROM lcdsys.array_pds_glass_t t
        WHERE 1=1
        AND t.glass_id = :glass_id
        ORDER BY glass_start_time
        """,
        {'glass_id': glass_id}
    )
    rows = cursor.fetchall()
    if rows is None:
        raise GlassDoesNotExist(glass_id=glass_id)
    return rows


def get_edc_data(glass_id, step_id, start_time):
    """
    :rtype
    """
    cursor = db.get_cursor()
    cursor.execute(
        """
        SELECT *
        FROM lcdsys.array_pds_glass_summary_v t
        WHERE 1=1 
        AND t.GLASS_ID = :glass_id
        AND t.STEP_ID = :step_id
        AND t.GLASS_START_TIME = :start_time
        """,
        {
            'glass_id': glass_id,
            'step_id': step_id,
            'start_time': start_time
        }
    )
    rows = cursor.fetchall()
    return rows


def get_teg_glass_history(glass_id):
    """
    rtype
    """
    cursor = db.get_cursor()
    cursor.execute(
        """
        SELECT "GLASS_ID", "STEP_ID", "GLASS_START_TIME"
        FROM lcdsys.array_glass_v t
        WHERE 1=1
        AND t.glass_id = :glass_id
        ORDER BY glass_Start_time ASC
        """,
        {'glass_id': glass_id}
    )
    rows = cursor.fetchall()
    return rows


def get_teg_data(glass_id, step_id, start_time):
    """
    rtype
    """
    cursor = db.get_cursor()
    cursor.execute(
        """
        SELECT *
        FROM lcdsys.array_glass_summary_v t
        WHERE 1=1
        AND t.GLASS_ID = :glass_id
        AND t.STEP_ID = :step_id
        AND t.GLASS_START_TIME = :start_time
        """,
        {
            'glass_id': glass_id,
            'step_id': step_id,
            'start_time': start_time
        }
    )
    rows = cursor.fetchall()
    return rows


def get_teg_summary_data(glass_id, step_id, param_name):
    """
    rtype
    """
    cursor = db.get_cursor()
    cursor.execute(
        """
        SELECT *
        FROM lcdsys.array_result_v t
        WHERE 1=1
        AND glass_id = :glass_id
        AND step_id = :step_id
        AND param_name = :param_name
        """,
        {
            'glass_id': glass_id,
            'step_id': step_id,
            'param_name': param_name
        }
    )
    rows = cursor.fetchall()
    return rows
