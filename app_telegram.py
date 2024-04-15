import asyncio
from aiogram.filters import Command
@dp.message(Command("start"))
async def start(message: types.Message):
    await message.reply("""Привет! Для агрегации данных отправь JSON с датой начала
                        (dt_from), датой окончания (dt_upto) и типом агрегации (group_type)
                        Пример запроса: {"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00",
                         "group_type": "month"}"""
                        )

