import asyncio
from datetime import datetime


async def lala():
    while True:
        print('lala', datetime.now())
        await asyncio.sleep(3)


async def lolo():
    while True:
        print('lolo', datetime.now())
        await asyncio.sleep(5)

loop = asyncio.get_event_loop()
tasks = (loop.create_task(lala()), loop.create_task(lolo()))
loop.run_until_complete(asyncio.wait(tasks))

