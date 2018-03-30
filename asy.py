import asyncio
import functools
import signal
import os
import select

from psycopg2 import connect
from datetime import datetime

from postgres_access import PostgresAccess


# TODO: exit programm when catch signal SIGTERM
# go away from select

async def lala():
    try:
        while True:
            print('lala', datetime.now())
            await asyncio.sleep(3)
    except asyncio.CancelledError:
        print('Task lala canceled!')


async def lolo():
    try:
        while True:
            print('lolo', datetime.now())
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        print('Task lolo canceled!')

async def catch_notify():
    try:
        with PostgresAccess(pg_conn) as db:
            db.execute('LISTEN task;')
            while True:
                if select.select([pg_conn], [], [], 5) == ([], [], []):
                    continue
                pg_conn.poll()
                while pg_conn.notifies:
                    notification = pg_conn.notifies.pop()
                    print('got notification', notification)
    except asyncio.CancelledError:
        print('Task catch_notify canceled!')


async def exit():
    # TODO: RuntimeWarning: coroutine 'exit' was never awaited
    tasks = [
        t for t in asyncio.Task.all_tasks()
        if t is not asyncio.tasks.Task.current_task()
    ]
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


loop = asyncio.get_event_loop()

for signame in ('SIGINT', 'SIGTERM'):
    loop.add_signal_handler(
        getattr(signal, signame),
        functools.partial(asyncio.ensure_future, exit())
    )

#asyncio.ensure_future(lala(), loop=loop)
#asyncio.ensure_future(lolo(), loop=loop)
asyncio.ensure_future(catch_notify(), loop=loop)

if __name__ == '__main__':
    pg_uri = os.getenv('PG_URI')
    if pg_uri is None:
        print('Environment variable PG_URI is not defined!!!')
        exit(1)
    pg_conn = connect(pg_uri)
    try:
        loop.run_forever()
    finally:
        loop.close()

