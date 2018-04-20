import asyncio
import select

from datetime import datetime
from typing import Union, List, Callable

from psycopg2 import Error
from psycopg2.extensions import (
    POLL_OK, POLL_WRITE,
    POLL_READ, connection,
    Notify
)
from psycopg2.extras import DictCursor, DictRow

from exceptions import DBException


# TODO:
#       - ???make db listener as separated class???
#       - ???redesign wait method???
#       - ???in old implementation was autocommit, now need commit???

class AsyncPostgresAccess:
    """Class for access postgres db in async mode
    Attention: work with the instances of the class
    should be implemented in with statement. Example of usage:

           with AsyncPostgresAccess() as db:
               result = await db.execute(...)
    """

    def __init__(self, conn: connection, loop: asyncio.SelectorEventLoop):
        """

        :param conn: connection to Postgres
        :param loop: asyncio loop
        """
        self.__conn = conn
        self.__wait(conn)
        self.__cur = self.__conn.cursor(cursor_factory=DictCursor)
        # self.__is_transaction = False
        self.__loop = loop
        # stores retrieve method of cursor
        self.__retrieve_method = None
        # stores result of the query
        self.__result = None
        # object to indicate that the specified event has occurred
        self.__ev = asyncio.Event()

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
            return getattr(self.__cur, method)(query, params)
        except Error as e:
            raise DBException(e)

    def __cursor_retrieve(self) -> None:
        """Method for retrieving data from database,
        used after executing query as a callback,
        stores result in self.__result attribute

        :return:
        """
        self.__wait(self.__conn)
        try:
            if self.__retrieve_method is None:
                self.__result = self.__cur.rowcount
            else:
                self.__result = getattr(self.__cur, self.__retrieve_method)()
            self.__loop.remove_reader(self.__conn.fileno())
            self.__ev.set()
        except Error as e:
            raise DBException(e)

    async def __execute(
                self, query: str, params: Union[None, tuple],
                exec_method: str
        ) -> None:
        """Method for executing query.

        :param query: sql query
        :param params: parameters of query
        :param exec_method: method of execution
            (execute, executemany, callproc ...)
        :return:
        """
        self.__cursor_execute(query, params, exec_method)
        await self.__do_by_response(self.__cursor_retrieve)

    def __retrieve_notifications(self) -> None:
        """Method used as callback for retrieving notifications from Postgres,
        stores retrieved notifications to self.__result attribute.

        :return:
        """
        self.__loop.remove_reader(self.__conn.fileno())
        self.__conn.poll()
        notifications = []
        while self.__conn.notifies:
            notification = self.__conn.notifies.pop()
            notifications.append(notification)
        self.__result = notifications
        self.__ev.set()

    async def __do_by_response(self, callback: Callable) -> None:
        """Adds callback for read availability event from Postgres connection
        file descriptor.

        :param callback: function that should be called when
            response come from Postgres. Callback must set
            asyncio.Event() object stored in self.__ev attribute when work is
            done, and must remove file descriptor of Postgres connection
            from event loop.
        :return:
        """
        self.__loop.add_reader(self.__conn.fileno(), callback)
        await self.__ev.wait()
        self.__ev.clear()

    @staticmethod
    def __wait(conn):
        while 1:
            state = conn.poll()
            if state == POLL_OK:
                break
            elif state == POLL_WRITE:
                select.select([], [conn.fileno()], [])
            elif state == POLL_READ:
                select.select([conn.fileno()], [], [])
            else:
                raise DBException('poll() returned %s' % state)

    # def __return_autocommit(self) -> None:
    #     """Method for setting attributes after transaction.
    #     Called after transaction, in all cases(actions were successful or not).
    #
    #     :return:
    #     """
    #     if self.__is_transaction:
    #         self.__conn.autocommit = True
    #         self.__is_transaction = False
    #
    # def start_transaction(self) -> None:
    #     """Method for starting transaction. Example of usage:
    #
    #        with PostgresAccess() as db:
    #            try:
    #                db.start_transaction()
    #                ...
    #                db.execute(...)
    #                result = db.exec(...)
    #                db.close_transaction()
    #            except SomeException as e:
    #                ...
    #
    #     After this method was called it is necessary
    #     to set connection attribute autocommit to True and
    #     self.__is_transaction attribute to False. This attributes are set using
    #     methods close_transaction and rollback_transaction
    #     (using return_autocommit method), the first is called in cases where
    #     all actions within transaction were successful and second
    #     in cases where something was wrong inside transaction.
    #     :return:
    #     """
    #     if not self.__is_transaction:
    #         self.__conn.autocommit = False
    #         self.__is_transaction = True
    #
    # def close_transaction(self) -> None:
    #     """Method for closing transaction.
    #     Called when all actions were successful.
    #
    #     :return:
    #     """
    #     if self.__is_transaction:
    #         self.__conn.commit()
    #         self.__return_autocommit()
    #
    # def rollback_transaction(self) -> None:
    #     """Method for rollback transaction.
    #     Called when actions were not successful.
    #
    #     :return:
    #     """
    #     if self.__is_transaction:
    #         self.__conn.rollback()
    #         self.__return_autocommit()

    async def execute(
            self, query: str,
            params: Union[None, tuple]=None,
            result: bool=False
    ) -> Union[List[DictRow], int]:
        """Method for executing query.

        :param query: sql query
        :param params: parameters of query
        :param result: flag to determine whether to return the query result,
            if False method returns number of rows produced by query
        :return: number of rows that query produced if result is None and list
            of DictRow's if result is True
        """
        self.__retrieve_method = 'fetchall' if result else None
        await self.__execute(query, params, 'execute')
        return self.__result

    async def listen(self, chanel) -> None:
        """Method for starting listening chanel.

        :param chanel: chanel to listen
        :return:
        """
        await self.execute('LISTEN %s;' % chanel)

    async def get_notifications(self) -> List[Notify]:
        """Method for receiving notifications from Postgres, adds callback for
        read availability event from Postgres connection file descriptor.

        :return: list of notifications from Postgres
        """
        await self.__do_by_response(self.__retrieve_notifications)
        return self.__result
