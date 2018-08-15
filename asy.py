import asyncio
import signal
import os

from postgres_access import AsyncPostgresAccess


async def catch_notify(queue, uri):
    try:
        loop = asyncio.get_event_loop()
        pa = await AsyncPostgresAccess.create(uri, loop)
        with pa as db:
            await db.listen('task')
            while True:
                notifications = await db.get_notifications()
                print('Got notifications from Postgres', notifications)
                for n in notifications:
                    queue.put_nowait(n)
    except asyncio.CancelledError:
        print('Notify receiver stopped.')


async def do_work(queue, uri, number):
    try:
        loop = asyncio.get_event_loop()
        pa = await AsyncPostgresAccess.create(uri, loop)
        with pa as db:
            while True:
                notification = await queue.get()
                print('Worker %s receive %s' % (number, notification))
                # simulating query
                res = await db.execute('SELECT pg_sleep(5);', result=True)
                print('Worker %s finish with result %s' % (number, res))
                queue.task_done()
    except asyncio.CancelledError:
        print('Worker %s stopped.' % number)


async def stop():
    loop = asyncio.get_event_loop()
    tasks = [
        t for t in asyncio.Task.all_tasks()
        if t is not asyncio.tasks.Task.current_task()
    ]
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


if __name__ == '__main__':
    settings = {
        'PG_URI': os.getenv('PG_URI'),
        'WORKERS_NUM': os.getenv('WORKERS_NUM')
    }
    for k, v in settings.items():
        if v is None:
            print('Environment variable %s is not defined!!!' % (k,))
            exit(1)

    try:
        workers_num = int(settings['WORKERS_NUM'])
    except ValueError:
        print('Environment variable WORKERS_NUM must be an integer!!!')
        exit(1)

    event_loop = asyncio.get_event_loop()
    q = asyncio.Queue()

    for signame in ('SIGINT', 'SIGTERM'):
        event_loop.add_signal_handler(
            getattr(signal, signame),
            lambda: asyncio.ensure_future(stop())
        )

    for i in range(1, workers_num + 1):
        asyncio.ensure_future(do_work(q, settings['PG_URI'], i), loop=event_loop)
    asyncio.ensure_future(catch_notify(q, settings['PG_URI']), loop=event_loop)

    try:
        event_loop.run_forever()
    finally:
        event_loop.close()
