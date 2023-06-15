import time
import sshtunnel
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime


class ClickHouse:
    def __init__(self, host: str, user: str, password: str, port = None, db: str = 'default',
                  **ssh):
        # main connection data
        self.host: str = host
        self.port = port
        self.db: str = db
        self.user: str = user
        self.password: str = password
        self._connection = None

        # ssh tunnel data
        self.ssh_host: tuple = ssh.get('ssh_host') if ssh.get('ssh_host') else None
        self.ssh_username: str = ssh.get('ssh_username') if ssh.get('ssh_username') else None
        self.ssh_pkey: str = ssh.get('ssh_pkey') if ssh.get('ssh_pkey') else None
        self.ssh_remote_host: tuple = ssh.get('ssh_remote_host') if ssh.get('ssh_remote_host') else None
        self._tunnel = None

    def _ssh_tunnel(self):
        """Method that create tunnel to the remote server"""
        tunnel = sshtunnel.SSHTunnelForwarder(ssh_address_or_host=self.ssh_host,
                                              ssh_username=self.ssh_username,
                                              ssh_pkey=self.ssh_pkey,
                                              remote_bind_address=self.ssh_remote_host)
        return tunnel

    def create_connection(self, sqlalchemy_engine: bool = True, ssh: bool = False):
        """Method that create a connection with ClickHouse database"""
        if self._connection:
            self._log(f'connect: WARNING → Connection to database {self.db} is already established')
            return self._connection
        
        if ssh:
            self._tunnel = self._ssh_tunnel()
            self._tunnel.start()
            time.sleep(1)
        
        if sqlalchemy_engine:
            if self._tunnel:
                self.port = self._tunnel.local_bind_port

            conn_str = f'clickhouse+native://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}'
            self._connection = create_engine(conn_str)
        
        self._log(f'connect: SUCCESS → Connection to database "{self.db}" established')
        return self._connection

    def select(self, query: str, return_df: bool = True, ssh: bool = False):
        """Method that extract data from ClickHouse database"""
        try:
            self._log(f'select: START')
            if self._connection is None:
                self._connection = self.create_connection(ssh=ssh)

            if return_df:
                data = pd.read_sql(query, con=self._connection)
                self._log(f'select: SUCCESS → Data from database "{self.db}" extracted')
            
            self._tunnel.stop()
            return data
        except (SQLAlchemyError, Exception) as ex:
            self._log(f'select: ERROR\n{ex}')
            raise ex
        finally:
            self._log(f'select: END')

    def _log(self, message):
        print(f'{datetime.now()} | ClickHouse → {message}')
