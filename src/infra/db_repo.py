import pyodbc
from pyodbc import OperationalError
import asyncio

class DatabaseManager:
    def __init__(self, server, database, username, password, pool_size=3):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.connection_pool = []  # Pool of connections
        self.pool_size = pool_size  # Maximum number of connections in the pool
        self.active_connections = 0  # Track active connections

        self._initialize_pool()
    
    def _initialize_pool(self):
        while len(self.connection_pool) < self.pool_size:
            conn = self._create_connection()
            if conn:
                self.connection_pool.append(conn)

    def _create_connection(self):
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'
        try:
            connection = pyodbc.connect(conn_str)
            print("New connection created.")
            return connection
        except OperationalError as e:
            print("Error creating new connection:", e)
            return None

    def _get_connection(self):
        if self.connection_pool:
            conn = self.connection_pool.pop()  # Get a connection from the pool
            self.active_connections += 1
            return conn
        else:
            # If no connections are available, try to create a new one
            if self.active_connections < self.pool_size:
                return self._create_connection()
            else:
                print("Max connection limit reached. No available connections.")
                return None

    def _is_valid_connection(self, connection):
        try:
            # A simple query to check if the connection is still valid
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _return_connection(self, connection):
        if self._is_valid_connection(connection):
            self.connection_pool.append(connection)  # Return connection to the pool
        else:
            print("Invalid connection. Closing it.")
            connection.close()  # Close the invalid connection
        self.active_connections -= 1

    async def single_inserts_async(self, query, params):
        conn = await asyncio.to_thread(self._get_connection)
        if not conn:
            return -1

        cursor = conn.cursor()
        try:
            # Perform the insert operation asynchronously in a separate thread
            await asyncio.to_thread(cursor.execute, query, params)
            await asyncio.to_thread(conn.commit)
            return 0
        except Exception as e:
            print("Error in database add:", e)
            return None
        finally:
            await asyncio.to_thread(self._return_connection, conn)

    def single_inserts(self, query, params):
        conn = self._get_connection()
        if not conn:
            return -1

        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
            return 0
        except Exception as e:
            print("Error in database add:", e)
            return None
        finally:
            self._return_connection(conn)

    def multiple_inserts(self, query, params):
        conn = self._get_connection()
        if not conn:
            return -1

        cursor = conn.cursor()
        try:
            cursor.executemany(query, params)
            conn.commit()
            print("Sucessful insertion")
            return 0
        except Exception as e:
            print("Error in database multiple execute add:", e)
            if "Violation of UNIQUE KEY" in str(e):
                print("Error: Duplicate entry detected. Skipping insertion.")
                return 0.
            else:
                print(f"Database error occurred: {e}")
                return -1
        finally:
            self._return_connection(conn)
            
            
    def fetch_records(self, query, params):
        conn = self._get_connection()
        if not conn:
            return []

        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            print("Error fetching records:", e)
            return []
        finally:
            self._return_connection(conn)
    
    def fetch_record(self, query, params):
        conn = self._get_connection()
        if not conn:
            return []

        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            rows = cursor.fetchone()
            return rows
        except Exception as e:
            print("Error fetching records:", e)
            return []
        finally:
            self._return_connection(conn)

    def fetch_multiple_query(self, query, params):
        conn = self._get_connection()
        if not conn:
            return None

        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            count = cursor.nextset().fetchone()[0]
            return rows, count
        except Exception as e:
            print("Error fetching records:", e)
            return None
        finally:
            self._return_connection(conn)
    
    def get_columns_of_table(self, table_name):
        conn = self._get_connection()
        if not conn:
            return None

        cursor = conn.cursor()
        try:
            query = "select column_name, data_type from information_schema.columns where table_name=?"
            cursor.execute(query, (table_name,))
            rows = cursor.fetchall()
            return rows
        except Exception as e:
            print("Error fetching records:", e)
            return None
        finally:
            self._return_connection(conn)