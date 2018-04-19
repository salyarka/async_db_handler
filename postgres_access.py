import asyncio
import select

from datetime import datetime
from typing import Union, List

import psycopg2

from psycopg2.extras import DictCursor, DictRow

from exceptions import DBException


class PostgresAccess:
    """Class for access postgres db
    Attention: work with the instances of the class
    should be implemented in with statement. Example of usage:

           with PostgresAccess() as db:
               result = db.execute(...)
    """

    # TODO: add psycopg2.extras.execute_values for inserting multiple rows
    # TODO: add typing to init args
    def __init__(self, connection, loop=None):
        """

        :param connection: connection to Postgres
        :param loop: asyncio loop
        """
        self.__conn = connection
        if loop is None:
            self.__conn.autocommit = True
        else:
            self.wait(connection)
        self.__cursor = self.__conn.cursor(cursor_factory=DictCursor)
        self.__is_transaction = False
        self.__loop = loop
        self.execute = self.execute_async if loop is not None \
            else self.execute_normal
        self.result = None
        self.ev = asyncio.Event()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __cursor_execute(
            self, query: str, params: tuple, method: str
    ) -> Union[None, str]:
        """Method for executing commands for cursor.

        :param query: sql query
        :param params: parameters of query
        :param method: execute method of cursor (execute, mogrify, callproc...)
        :return: different methods returns different results
            (execute - returns None, mogrify - returns query string ...)
        """
        try:
            return getattr(self.__cursor, method)(query, params)
        except psycopg2.Error as e:
            raise DBException(e)

    def __cursor_retrieve_async(
            self, method: Union[None, str]
    ):
        """Method for retrieving data from database,
        used after executing query.

        :param method: method of retrieving (fetch, fetchall ...)
        :return: different methods/attributes returns different results
            (fetchone - returns tuple,
             fetchall - returns list of psycopg2.extras.DictRow's,
             rowcount - this attribute specifies the number of rows
                        that the last query produced)
        """
        print('!!! __cursor_retrieve_async')
        self.wait(self.__conn)
        try:
            if method is None:
                self.result = self.__cursor.rowcount
            else:
                self.result = getattr(self.__cursor, method)()
            print('!!! result', self.result)
            print('!!! method', method)
            self.__loop.remove_reader(self.__conn)
            self.ev.set()
        except psycopg2.Error as e:
            raise DBException(e)

    def __cursor_retrieve(
            self, method: Union[None, str]
    ) -> Union[int, tuple, list]:
        """Method for retrieving data from database,
        used after executing query.

        :param method: method of retrieving (fetch, fetchall ...)
        :return: different methods/attributes returns different results
            (fetchone - returns tuple,
             fetchall - returns list of psycopg2.extras.DictRow's,
             rowcount - this attribute specifies the number of rows
                        that the last query produced)
        """
        try:
            if method is None:
                return self.__cursor.rowcount
            return getattr(self.__cursor, method)()
        except psycopg2.Error as e:
            raise DBException(e)

    async def __execute_async(
            self, query: str, params: Union[None, tuple],
            exec_method: str, retrieve_method: Union[None, str]
    ):
        """Method for executing query.

        :param query: sql query
        :param params: parameters of query
        :param exec_method: method of execution
            (execute, executemany, callproc ...)
        :param retrieve_method: method for retrieving data (fetch, fetchall)
        :return: result of the query
        """
        self.__cursor_execute(query, params, exec_method)
        await self.watch(self.__conn.fileno(), retrieve_method)
        print('!!! after watch')

    def __execute_normal(
            self, query: str, params: Union[None, tuple],
            exec_method: str, retrieve_method: Union[None, str]
    ) -> Union[int, tuple, list]:
        """Method for executing query.

        :param query: sql query
        :param params: parameters of query
        :param exec_method: method of execution
            (execute, executemany, callproc ...)
        :param retrieve_method: method for retrieving data (fetch, fetchall)
        :return: result of the query
        """
        self.__cursor_execute(query, params, exec_method)
        return self.__cursor_retrieve(retrieve_method)

    def __return_autocommit(self) -> None:
        """Method for setting attributes after transaction.
        Called after transaction, in all cases(actions were successful or not).

        :return:
        """
        if self.__is_transaction:
            self.__conn.autocommit = True
            self.__is_transaction = False

    def start_transaction(self) -> None:
        """Method for starting transaction. Example of usage:

           with PostgresAccess() as db:
               try:
                   db.start_transaction()
                   ...
                   db.execute(...)
                   result = db.exec(...)
                   db.close_transaction()
               except SomeException as e:
                   ...

        After this method was called it is necessary
        to set connection attribute autocommit to True and
        self.__is_transaction attribute to False. This attributes are set using
        methods close_transaction and rollback_transaction
        (using return_autocommit method), the first is called in cases where
        all actions within transaction were successful and second
        in cases where something was wrong inside transaction.
        :return:
        """
        if not self.__is_transaction:
            self.__conn.autocommit = False
            self.__is_transaction = True

    def close_transaction(self) -> None:
        """Method for closing transaction.
        Called when all actions were successful.

        :return:
        """
        if self.__is_transaction:
            self.__conn.commit()
            self.__return_autocommit()

    def rollback_transaction(self) -> None:
        """Method for rollback transaction.
        Called when actions were not successful.

        :return:
        """
        if self.__is_transaction:
            self.__conn.rollback()
            self.__return_autocommit()

    async def execute_async(
            self, query: str,
            params: Union[None, tuple]=None,
            result: bool=False
    ):
        """Method for executing query without retrieving result.

        :param query: sql query
        :param params: parameters of query
        :param result: flag to determine whether to return the query result,
            if False method returns number of rows produced by query
        :return: number of rows that query produced
        """
        retrieve_method = 'fetchall' if result else None
        await self.__execute_async(query, params, 'execute', retrieve_method)
        print('!!! after __execute_async')

    def execute_normal(
            self, query: str,
            params: Union[None, tuple]=None,
            result: bool=False
    ) -> Union[List[DictRow], int]:
        """Method for executing query without retrieving result.

        :param query: sql query
        :param params: parameters of query
        :param result: flag to determine whether to return the query result,
            if False method returns number of rows produced by query
        :return: number of rows that query produced
        """
        retrieve_method = 'fetchall' if result else None
        return self.__execute_normal(query, params, 'execute', retrieve_method)

    async def watch(self, fd, retrieve_method):
        print('!!! watch')
        self.__loop.add_reader(
            fd,
            self.__cursor_retrieve_async,
            retrieve_method
        )
        print('!!! before await event')
        await self.ev.wait()
        print('!!! after await event')
        self.ev.clear()

    def wait(self, conn):
        # TODO: redesign
        while 1:
            state = conn.poll()
            if state == psycopg2.extensions.POLL_OK:
                break
            elif state == psycopg2.extensions.POLL_WRITE:
                select.select([], [conn.fileno()], [])
            elif state == psycopg2.extensions.POLL_READ:
                select.select([conn.fileno()], [], [])
            else:
                raise psycopg2.OperationalError("poll() returned %s" % state)
