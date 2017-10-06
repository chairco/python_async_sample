try:
    from . import db
except Exception as e:
    import db


__all__ = ['get_edc_glass_history']


class FDC:
    """glass shop flow database

    Here is get db glass_id factory flow
    """

    def __init__(self, *, glass_id):
        self.glass_id = glass_id

    def get_edc_glass_history(self):
        """From array_pds table
        rtype: list
        """
        cursor = db.get_cursor()
        cursor.execute(
            """
                SELECT * from lcdsys.array_pds_glass_t t WHERE 1=1
                AND t.glass_id = :glass_id
                ORDER BY glass_start_time
                """,
            {'glass_id': self.glass_id}
        )
        rows = cursor.fetchall()
        return rows

    def get_edc_data(self):
        """
        rtype
        """
        pass

    def get_teg_glass_history(self):
        """
        rtype
        """
        pass

    def get_teg_summary_data(self):
        """
        rtype
        """
        pass

    def get_teg_raw_data(self):
        """
        rtype
        """
        pass