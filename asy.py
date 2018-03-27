import asyncio
import functools
import signal

from datetime import datetime


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

asyncio.ensure_future(lala(), loop=loop)
asyncio.ensure_future(lolo(), loop=loop)

try:
    loop.run_forever()
finally:
    loop.close()

