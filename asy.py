import asyncio
import functools
import signal
import os

from psycopg2 import connect

from postgres_access import AsyncPostgresAccess


# TODO:
#       - redesign all variables, ???go away from global???
#       - make wrapper around Event (for using events by workers)


deferred_tasks = []

async def catch_notify():
    try:
        with AsyncPostgresAccess(listener_conn, loop) as db:
            await db.listen('task')
            while True:
                notifications = await db.get_notifications()
                print('!!! notifications', notifications)
                for n in notifications:
                    for ev in evs:
                        if not ev.is_set():
                            ev.data = n
                            ev.set()
                            break
                    else:
                        print('!!! put task to deferred')
                        global deferred_tasks
                        deferred_tasks += notifications
    except asyncio.CancelledError:
        print('Task catch_notify canceled!')

async def worker(ev, conn):
    try:
        while True:
            await ev.wait()
            print('!!! worker receive %s' % (ev.data,))
            with AsyncPostgresAccess(conn, loop) as db:
                await db.execute('SELECT pg_sleep(5);', result=True)
                print('!!! query result', db.result)
            print('!!! worker finish')
            ev.clear()
            try:
                task = deferred_tasks.pop(0)
                print('!!! Worker get deferred task %s' % (task,))
            except IndexError:
                print('!!! deferred tasks are empty')
    except asyncio.CancelledError:
        print('Worker canceled!')


async def stop():
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

evs = [asyncio.Event() for each in range(10)]

for signame in ('SIGINT', 'SIGTERM'):
    loop.add_signal_handler(
        getattr(signal, signame),
        functools.partial(asyncio.ensure_future, stop())
    )


# if __name__ == '__main__':
pg_uri = os.getenv('PG_URI')
if pg_uri is None:
    print('Environment variable PG_URI is not defined!!!')
    exit(1)
# pg_conn = connect(pg_uri)
listener_conn = connect(pg_uri, async=True)
workers_conns = [connect(pg_uri, async=True) for each in range(10)]

for i in range(10):
    asyncio.ensure_future(worker(evs[i], workers_conns[i]), loop=loop)
asyncio.ensure_future(catch_notify(), loop=loop)

try:
    loop.run_forever()
finally:
    loop.close()
