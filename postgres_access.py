import asyncio
import select

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
#       - ???redesign wait method???
#       - implement work with transactions

class AsyncPostgresAccess:
    """Class for access postgres db in async mode.
    NOTE: asynchronous connection is always in autocommit mode
    Attention: work with the instances of the class
    should be implemented in with statement. Example of usage:

           with AsyncPostgresAccess() as db:
               result = await db.execute(...)
    """

    def __init__(self, conn: connection, loop: asyncio.SelectorEventLoop):
        """

        :param conn: connection to Postgres in asynchronous mode
        :param loop: asyncio loop
        """
        self.__conn = conn
        self.__wait()
        self.__cur = self.__conn.cursor(cursor_factory=DictCursor)
        self.__loop = loop
        self.__event = asyncio.Event()
        self.__retrieve_method = None
        self.__result = None

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
        self.__wait()
        try:
            if self.__retrieve_method is None:
                self.__result = self.__cur.rowcount
            else:
                self.__result = getattr(self.__cur, self.__retrieve_method)()
            self.__loop.remove_reader(self.__conn.fileno())
            self.__event.set()
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
        self.__event.set()

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
        await self.__event.wait()
        self.__event.clear()

    def __wait(self):
        while 1:
            state = self.__conn.poll()
            if state == POLL_OK:
                break
            elif state == POLL_WRITE:
                select.select([], [self.__conn.fileno()], [])
            elif state == POLL_READ:
                select.select([self.__conn.fileno()], [], [])
            else:
                raise DBException('poll() returned %s' % state)

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
