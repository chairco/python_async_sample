try:
	from . import db
except Exception as e:
	import db


__all__ = ['get_array_pds']


class FDC:
    """glass shop flow database

    Here is get db glass_id factory flow
    """
    def __init__(self, *, glass_id):
        self.glass_id = glass_id

    def get_array_pds(self):
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


if __name__ == '__main__':
    fdc = FDC(glass_id='TL6AJ0HAV')
    print(fdc.get_array_pds())
