import psycopg2
import DB_credentials
from typing import Optional, Tuple, List

# DB Schema for now: ID (autoincrement), title < 100 chars, contents < 2000 chars



def create_DB():
	query = """CREATE TABLE notes (
		id SERIAL PRIMARY KEY,
		title VARCHAR(100),
		contents VARCHAR(2000)
	);"""

	try:
		connection = psycopg2.connect(
			host = DB_credentials.host,
			database = DB_credentials.dbname,
			user = DB_credentials.user,
			password = DB_credentials.password
		)
		cursor = connection.cursor()
		print("Connected to database in create_DB().")

		#DB OPERATION HERE
		cursor.execute(query)
		connection.commit()

	except (Exception, psycopg2.Error) as error:
		print("Error in create_DB():", error)

	finally:
		if connection:
			cursor.close()
			connection.close()
			print("Closed database connection in create_DB().")

def create_note(title: str, contents: str) -> Optional[int]:
	query_format = "INSERT INTO notes (title, contents) VALUES (%s, %s);"
	data = (title, contents)


	try:
		connection = psycopg2.connect(
			host = DB_credentials.host,
			database = DB_credentials.dbname,
			user = DB_credentials.user,
			password = DB_credentials.password
		)
		cursor = connection.cursor()
		print("Connected to database in create_note().")

		# DB OPERATIONS HERE
		cursor.execute(query_format, data)

		cursor.execute("SELECT MAX(id) FROM notes;")
		return_id_tuple = cursor.fetchone()
		return_id = return_id_tuple[0]
		assert(type(return_id) == int), 'create_note(): type of return_id is not int'

		connection.commit()

		# finally will still run after this returns
		return return_id

	except (Exception, psycopg2.Error) as error:
		print("Error in create_note():", error)

	finally:
		if connection:
			cursor.close()
			connection.close()
			print("Closed database connection in create_note().")

def read_note(id: int) -> Optional[Tuple[str, str]]:
	query_format = "SELECT (title, contents) FROM notes WHERE id = %s;"
	data = (id,)

	try:
		connection = psycopg2.connect(
			host = DB_credentials.host,
			database = DB_credentials.dbname,
			user = DB_credentials.user,
			password = DB_credentials.password
		)
		cursor = connection.cursor()
		print("Connected to database in read_note().")

		# DB OPERATIONS HERE
		cursor.execute(query_format, data)
		res = cursor.fetchall() # returns list of tuples
		connection.commit()

		if len(res) != 1:
			# no note with given ID
			if len(res) == 0:
				raise Exception("No note with given ID")
			# more than 1 note with given ID
			else:
				raise Exception("Broken primary key constraint")
		
		# finally will still run after this returns
		return res[0]

	except (Exception, psycopg2.Error) as error:
		print("Error in read_note():", error)

	finally:
		if connection:
			cursor.close()
			connection.close()
			print("Closed database connection in read_note().")

def get_all_note_ids() -> List[int]:
	query = "SELECT id FROM notes ORDER BY id DESC;"

	try:
		connection = psycopg2.connect(
			host = DB_credentials.host,
			database = DB_credentials.dbname,
			user = DB_credentials.user,
			password = DB_credentials.password
		)
		cursor = connection.cursor()
		print("Connected to database in get_all_note_ids().")

		# DB OPERATIONS HERE
		cursor.execute(query)
		res = cursor.fetchall() # returns a list of tuples
		connection.commit()

		out = []

		for tup in res:
			out.append(tup[0])

		# finally will still run after this returns
		return out

	except (Exception, psycopg2.Error) as error:
		print("Error in get_all_note_ids():", error)

	finally:
		if connection:
			cursor.close()
			connection.close()
			print("Closed database connection in get_all_note_ids().")

def get_all_notes() -> Optional[Tuple[int, str, str]]:
	query = "SELECT (id, title, contents) FROM notes ORDER BY id DESC;"

	try:
		connection = psycopg2.connect(
			host = DB_credentials.host,
			database = DB_credentials.dbname,
			user = DB_credentials.user,
			password = DB_credentials.password
		)
		cursor = connection.cursor()
		print("Connected to database in get_all_notes().")

		# DB OPERATIONS HERE
		cursor.execute(query)
		res = cursor.fetchall()
		connection.commit()

		# finally will still run after this returns
		return res

	except (Exception, psycopg2.Error) as error:
		print("Error in get_all_notes():", error)

	finally:
		if connection:
			cursor.close()
			connection.close()
			print("Closed database connection in get_all_notes().")

# looks DONE-ish
def update_note(id: int, title: str, contents: str) -> Optional[Tuple[int, str, str]]:
	query_format = "UPDATE notes SET title = %s, contents = %s WHERE id = %s;"
	data = (title, contents, id)

	try:
		connection = psycopg2.connect(
			host = DB_credentials.host,
			database = DB_credentials.dbname,
			user = DB_credentials.user,
			password = DB_credentials.password
		)
		cursor = connection.cursor()
		print("Connected to database in update_note().")

		# DB OPERATIONS HERE
		cursor.execute(query_format, data)

		# verify update
		assert((title, contents) == read_note(id)), "unsuccessful update"

		connection.commit()

		# return new rows
		# finally will still run after this returns
		return (id, title, contents)

	except (Exception, psycopg2.Error) as error:
		print("Error in update_note():", error)

	finally:
		if connection:
			cursor.close()
			connection.close()
			print("Closed database connection in update_note().")

def delete_note(id: int) -> bool:
	query_format = "DELETE FROM notes WHERE id = %s;"
	data = (id,)

	try:
		connection = psycopg2.connect(
			host = DB_credentials.host,
			database = DB_credentials.dbname,
			user = DB_credentials.user,
			password = DB_credentials.password
		)
		cursor = connection.cursor()
		print("Connected to database in delete_note().")

		# DB OPERATIONS HERE
		cursor.execute(query_format, data)

		# verify success
		cursor.execute("SELECT * FROM notes WHERE id = %s", data)
		res = cursor.fetchall()
		assert(len(res) == 0), "unsuccessful deletion"

		connection.commit()

		# finally will still run after this returns
		return True # success

	except (Exception, psycopg2.Error) as error:
		print("Error in delete_note():", error)
		return False # failure

	finally:
		if connection:
			cursor.close()
			connection.close()
			print("Closed database connection in delete_note().")



"""
# TEMPLATE

try:
	connection = psycopg2.connect(
		host = DB_credentials.host,
		database = DB_credentials.dbname,
		user = DB_credentials.user,
		password = DB_credentials.password
	)
	cursor = connection.cursor()
	print("Connected.")

	# DB OPERATIONS HERE

except (Exception, psycopg2.Error) as error:
	print("Error with database in [FUNCTION NAME]:", error)

finally:
	if connection:
		cursor.close()
		connection.close()
		print("Closed database connection.")
"""
