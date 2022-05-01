#!/usr/bin/python

import sys, sqlite3, shutil, os
import collections



class WAMerge(object):


	def __init__(self, parent=None):
		self.db_a = None
		self.db_b = None
		self.db_c = None

	def attachDB(self):

		try:
			conn = sqlite3.connect(self.file_name_a)
			conn.execute("ATTACH ? AS db2", [self.file_name_b])
			conn.execute("ATTACH ? AS db3", [self.file_name_c])
			return (1,conn)
		except Exception as e:
			return (0,0)
		
	def checkTables(self, conn):

		db_a_tables = conn.execute("SELECT name, sql FROM main.sqlite_master WHERE type='table';").fetchall()
		tables_to_merge = ['messages','messages_quotes','message_thumbnails']

		for table_item in db_a_tables:
			current_table = table_item[0].strip()
			current_table_schema = table_item[1]
			# print("-> " + current_table)
			
			# Skip Virtual tables
			if("CREATE VIRTUAL TABLE" in current_table_schema):
				continue

			if(str(current_table).strip() in tables_to_merge):
				tables_to_merge.remove(current_table)

			colnames_a = conn.execute("select * from main.{0}".format(current_table)).description
			colnames_b = conn.execute("select * from db2.{0}".format(current_table)).description
			
			compare = lambda x, y: collections.Counter(x) == collections.Counter(y)
			if(sorted(colnames_a) != sorted(colnames_b)):
				return (0, 0)

		return (1, tables_to_merge)
	
	def merge(self, conn):


		db_a_tables = conn.execute("SELECT name, sql FROM main.sqlite_master WHERE type='table';").fetchall()
		current_table_schema = ""
		sql_headers = ""
		sql_holders = ""
		count = 0
		quote_row_id_map = {}

		for table_item in db_a_tables:
			current_table = str(table_item[0]).strip()
			current_table_schema = table_item[1]

			
			# Skip Virtual tables
			if("CREATE VIRTUAL TABLE" in current_table_schema):
				continue

			if(current_table not in ['messages','messages_quotes','message_thumbnails']):
				continue

			# colnames_a = conn.execute("select * from main.{0}".format(current_table)).description
			# colnames_b = conn.execute("select * from db2.{0}".format(current_table)).description

			headers = current_table_schema.split('(')[1].replace(")","")
			for header in headers.split(','):
				if('AUTOINCREMENT' in header):
					continue
				sql_headers += header.strip().split(' ')[0] + ', '
				sql_holders += '?,'
			sql_headers = sql_headers[:-2]
			sql_holders = sql_holders[:-1]
		
			if(current_table == 'messages'):
				key_id_list = ""
				tmp_key_id_list = conn.execute("SELECT key_id FROM main.messages;").fetchall()
				for item in tmp_key_id_list:
					key_id_list += "'"+str(item[0]) + "', "
				key_id_list = key_id_list[:-2]
				
				rows = conn.execute("SELECT {1} FROM db2.{0} WHERE key_id IN ({2});".format(table_name,sql_headers,key_id_list)).fetchall()
				if(len(rows) >=2 ):
					print("[+] Duplicate records Found : ")
					print(rows)
				else:
					print("[+] No duplicate records Found")
				
				# rows = conn.execute("SELECT {0} FROM db2.{1} WHERE key_remote_jid LIKE '%@s.whatsapp.net' and quoted_row_id=0;".format(sql_headers, current_table)).fetchall()
				# rows_quote = conn.execute("SELECT {0} FROM db2.{1} WHERE key_remote_jid LIKE '%@s.whatsapp.net' and quoted_row_id!=0;".format(sql_headers, current_table)).fetchall()
				
				rows = conn.execute("SELECT {0} FROM db2.{1} WHERE quoted_row_id=0;".format(sql_headers, current_table)).fetchall()
				rows_quote = conn.execute("SELECT {0} FROM db2.{1} WHERE quoted_row_id!=0;".format(sql_headers, current_table)).fetchall()
				

				print("[+] %i records will be affected in %s table" % (len(rows)+len(rows_quote), current_table) )
				for record in rows:
					count += 1
					''' Skip -1 record '''
					if(record[3]==u'-1' or record[3]==-1):
						continue
					''' Skip group creation msg '''
					if(record[3]==u'6' or record[3]==6):
						continue
					''' Skip preview text '''
					if(record[3]==u'5' or record[3]==5):
						continue


					sql = "INSERT INTO db3.{0} ({1}) VALUES({2});".format(current_table,sql_headers,sql_holders)
					conn.execute(sql, record)

				quoted_row_id = ""
				tmp_quoted_row_id = conn.execute("SELECT quoted_row_id FROM main.messages;").fetchall()
				for item in tmp_quoted_row_id:
					if(str(item[0]) == '0' or str(item[0]) == 'None'):continue
					quoted_row_id += str(item[0]) + ", "
				quoted_row_id = quoted_row_id[:-2]

				# rows = conn.execute("SELECT {0} FROM db2.{1} WHERE key_remote_jid LIKE '%@s.whatsapp.net' and quoted_row_id IN ({2});".format(sql_headers, current_table, quoted_row_id)).fetchall()
				rows = conn.execute("SELECT {0} FROM db2.{1} WHERE quoted_row_id IN ({2});".format(sql_headers, current_table, quoted_row_id)).fetchall()
				

				if(len(rows) >=1 ):
					print("[+] Duplicate records Found : %i" % len(rows))
				else:
					print("[+] No duplicate records Found")

				tmp_rows = conn.execute("SELECT quoted_row_id FROM main.{1};".format(sql_headers, current_table)).fetchall()
				print(sorted(tmp_rows)[-1][0])
				quoted_row_id_count = sorted(tmp_rows)[-1][0]

				for record in rows:
					count += 1
					''' Skip -1 record '''
					if(record[3]==u'-1' or record[3]==-1):
						continue
					''' Skip group creation msg '''
					if(record[3]==u'6' or record[3]==6):
						continue
					''' Skip preview text '''
					if(record[3]==u'5' or record[3]==5):
						continue

					quoted_row_id_count += 1
					record = list(record)
					quote_row_id_map[int(record[30])] = quoted_row_id_count
					# bypass or set quote mode. set 0 to bypass
					record[30] = 0
					record = tuple(record)
					
					sql = "INSERT INTO db3.{0} ({1}) VALUES({2});".format(current_table,sql_headers,sql_holders)
					conn.execute(sql, record)


			elif(current_table == 'messages_quotes'):

				quoted_row_id = ""
				tmp_quoted_row_id = conn.execute("SELECT quoted_row_id FROM main.messages;").fetchall()
				for item in tmp_quoted_row_id:
					if(str(item[0]) == '0' or str(item[0]) == 'None'):continue
					quoted_row_id += str(item[0]) + ", "
				quoted_row_id = quoted_row_id[:-2]

				# rows_quote = conn.execute("SELECT {0} FROM db2.{1} WHERE key_remote_jid LIKE '%@s.whatsapp.net' and quoted_row_id IN ({2});".format(sql_headers, current_table, quoted_row_id)).fetchall()
				# rows_quote_non_duplicate = conn.execute("SELECT {0} FROM db2.{1} WHERE key_remote_jid LIKE '%@s.whatsapp.net' and quoted_row_id NOT IN ({2});".format(sql_headers, current_table, quoted_row_id)).fetchall()

				rows_quote = conn.execute("SELECT {0} FROM db2.{1} WHERE quoted_row_id IN ({2});".format(sql_headers, current_table, quoted_row_id)).fetchall()
				rows_quote_non_duplicate = conn.execute("SELECT {0} FROM db2.{1} WHERE quoted_row_id NOT IN ({2});".format(sql_headers, current_table, quoted_row_id)).fetchall()


				for record in rows_quote:
					quote_id = quote_row_id_map[int(record[30])]
					record = list(record)
					# bypass or set quote mode. set 0 to bypass
					record[30] = 0
					record = tuple(record)

					sql = "INSERT INTO db3.{0} ({1}) VALUES({2});".format(current_table,sql_headers,sql_holders)
					conn.execute(sql, record)

				for record in rows_quote_non_duplicate:
					sql = "INSERT INTO db3.{0} ({1}) VALUES({2});".format(current_table,sql_headers,sql_holders)
					conn.execute(sql, record)

			else:
				rows = conn.execute("SELECT {0} FROM db2.{1};".format(sql_headers, current_table)).fetchall()

				for record in rows:
					count += 1
					''' Skip -1 record '''
					if(record[3]==u'-1' or record[3]==-1):
						continue
					''' Skip group creation msg '''
					if(record[3]==u'6' or record[3]==6):
						continue
					''' Skip preview text '''
					if(record[3]==u'5' or record[3]==5):
						continue


					sql = "INSERT INTO db3.{0} ({1}) VALUES({2});".format(current_table,sql_headers,sql_holders)
					conn.execute(sql, record)


			current_table_schema = ""
			sql_headers = ""
			sql_holders = ""

		return 1

	def main(self, out):

		self.file_name_b = "new.db"
		self.file_name_a = "old.db"
		self.file_name_c = out

		if os.path.exists(out):
			os.remove(out)

		shutil.copy(self.file_name_a, self.file_name_c)
		if(not os.access(out, os.W_OK)):
			print("[!] output.db is read-only. Changing permissions")
			os.system('attrib {0} -r -s -h -a -i'.format(self.file_name_c))
		
		result,connection = self.attachDB()
		if(result):
			print("[+] Databases are connected")
		else:
			print("[!] Error connecting Databases")
			Exit()

		result, not_found = self.checkTables(connection)
		if(len(not_found) != 0):
			print("[-] {0} Tables are missing !!!".format(not_found))
		if(result):
			print("[+] Table structures are matching")
		else:
			print("[+] Can not merge. Table structures are not matching")
			Exit()

		self.merge(connection)

		connection.commit()
		connection.close()

		return

if __name__ == '__main__':

	out = "output.db"
	print(sys.version)


	app = WAMerge()
	app.main(out)
