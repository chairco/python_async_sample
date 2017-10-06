
import cx_Oracle
try:
	from . import db
except Exception as e:
	import db


__all__ = ['get_array_pds']


def get_array_pds(glass_id):
	oracleSystemLoginInfo = u'L6EDA_FDC/pl6eda_fdc123@10.59.116.81:1521/L6EDA'
	oracleConn = cx_Oracle.connect(oracleSystemLoginInfo)
	oracleCursor = oracleConn.cursor()
	oracleCursor.execute(
		"""
		SELECT * from lcdsys.array_pds_glass_t t WHERE 1=1
		AND t.glass_id = :glass_id
		ORDER BY glass_start_time
		""",
		{'glass_id': glass_id}
	)
	rows = oracleCursor.fetchall()
	oracleCursor.close()
	oracleConn.close()
	return rows


class FDC:
	"""glass shop flow database
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
	#print(get_array_pds(glass_id='TL6AJ0HAV'))
	fdc = FDC(glass_id='TL6AJ0HAV')
	#fdc.glass_id = 'TL6AJ0HAV'
	print(fdc.get_array_pds())