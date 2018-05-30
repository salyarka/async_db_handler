import asyncio

from typing import Union, List, Callable

from psycopg2 import Error, connect
from psycopg2.extensions import (
    POLL_OK, POLL_WRITE,
    POLL_READ, Notify
)
from psycopg2.extras import DictCursor, DictRow

from exceptions import DBException


# TODO:
#       - implement work with transactions

class AsyncPostgresAccess:
    """Class for access postgres db in async mode.
    NOTE: asynchronous connection is always in autocommit mode
    Attention: work with the instances of the class
    should be implemented in with statement. Creation of object must be done
    with create method. Example of usage:

           postgres_access = await AsyncPostgresAccess.create(uri, loop)
           with postgres_access as db:
               result = await db.execute(...)
    """

    def __init__(self, uri: str, loop: asyncio.SelectorEventLoop):
        """

        :param uri: Postgres address
        :param loop: asyncio loop
        """
        self.__conn = connect(uri, async=True)
        self.__loop = loop
        self.__event = asyncio.Event()
        self.__retrieve_method = None
        self.__result = None
        self.__connected = False
        self.__call = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @classmethod
    async def create(cls, uri, loop):
        """Creates AsyncPostgresAccess instance and wait for connection
        to Postgres.

        :param uri: Postgres address
        :param loop: asyncio loop
        :return: AsyncPostgresAccess instance
        """
        instance = AsyncPostgresAccess(uri, loop)
        await instance.__init()
        return instance

    async def __init(self) -> None:
        """Initialize cursor after connection is established.

        :return:
        """
        self.__call = self.__wait_connection
        # NOTE: Multiple messaging between postgres and client while connecting.
        self.__check()
        await self.__wait_event()
        self.__cur = self.__conn.cursor(cursor_factory=DictCursor)

    async def __wait_event(self) -> None:
        """Waits for events set.

        :return:
        """
        await self.__event.wait()
        self.__event.clear()

    def __wait_connection(self) -> None:
        """Waits for connection to Postgres.

        :return:
        """
        if self.__connected:
            self.__loop.remove_writer(self.__conn.fileno())
            self.__event.set()
        else:
            self.__check()

    def __check(self) -> None:
        """Checks file descriptor state.

        :return:
        """
        state = self.__conn.poll()
        if state == POLL_OK:
            self.__connected = True
            self.__call()
        elif state == POLL_WRITE:
            self.__loop.add_writer(self.__conn.fileno(), self.__call)
        elif state == POLL_READ:
            self.__loop.add_reader(self.__conn.fileno(), self.__call)
        else:
            raise DBException('poll() returned %s' % state)

    async def __execute(
                self, query: str, params: Union[None, tuple],
                exec_method: str
        ) -> None:
        """Starts executing of query.

        :param query: sql query
        :param params: parameters of query
        :param exec_method: method of execution
            (execute, executemany, callproc ...)
        :return:
        """
        self.__cursor_execute(query, params, exec_method)
        await self.__do_by_response(self.__cursor_retrieve)

    def __cursor_execute(
            self, query: str, params: tuple, method: str
    ) -> Union[None, str]:
        """Executes commands for cursor.

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
        """Retrieves data from database,
        used after executing query as a callback,
        stores result in self.__result attribute

        :return:
        """
        try:
            if self.__retrieve_method is None:
                self.__result = self.__cur.rowcount
            else:
                self.__result = getattr(self.__cur, self.__retrieve_method)()
            self.__event.set()
        except Error as e:
            raise DBException(e)

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
        self.__call = callback
        self.__loop.add_reader(self.__conn.fileno(), self.__check)
        await self.__wait_event()

    def __retrieve_notifications(self) -> None:
        """Used as callback for retrieving notifications from Postgres,
        stores retrieved notifications to self.__result attribute.

        :return:
        """
        self.__conn.poll()
        notifications = []
        while self.__conn.notifies:
            notification = self.__conn.notifies.pop()
            notifications.append(notification)
        self.__result = notifications
        self.__event.set()

    async def execute(
            self, query: str,
            params: Union[None, tuple]=None,
            result: bool=False
    ) -> Union[List[DictRow], int]:
        """Executes query.

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
        """Starts listening chanel.

        :param chanel: chanel to listen
        :return:
        """
        await self.execute('LISTEN %s;' % chanel)

    async def get_notifications(self) -> List[Notify]:
        """Receives notifications from Postgres, adds callback for
        read availability event from Postgres connection file descriptor.

        :return: list of notifications from Postgres
        """
        await self.__do_by_response(self.__retrieve_notifications)
        return self.__result
