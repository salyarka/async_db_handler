import asyncio
import functools
import signal
import os

from psycopg2 import connect

from postgres_access import PostgresAccess


def get_notification(fd):
    fd.poll()
    while fd.notifies:
        notification = fd.notifies.pop()
        print('got notification', notification)
        for ev in evs:
            if not ev.is_set():
                ev.data = notification
                ev.set()
                break
        else:
            pass
    # TODO: if all workers are busy, wait for free worker

async def watch(fd):
    future = asyncio.Future()
    loop.add_reader(fd, get_notification, fd)
    future.add_done_callback(lambda x: loop.remove_reader(fd))
    await future


async def catch_notify():
    try:
        with PostgresAccess(pg_conn) as db:
            db.execute('LISTEN task;')
            while True:
                await watch(pg_conn)
    except asyncio.CancelledError:
        print('Task catch_notify canceled!')

async def worker(ev):
    try:
        while True:
            await ev.wait()
            print('!!! worker receive')
            ev.clear()
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

for i in range(10):
    asyncio.ensure_future(worker(evs[i]), loop=loop)
asyncio.ensure_future(catch_notify(), loop=loop)

# if __name__ == '__main__':
pg_uri = os.getenv('PG_URI')
if pg_uri is None:
    print('Environment variable PG_URI is not defined!!!')
    exit(1)
pg_conn = connect(pg_uri)
try:
    loop.run_forever()
finally:
    loop.close()
