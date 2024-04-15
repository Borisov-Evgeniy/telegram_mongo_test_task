import asyncio
from aiogram.filters import Command

#Параметры подключения MongoDB
MONGODB_URL = "mongodb://localhost:27017"
DB_NAME = "sampleDB"
COLLECTION_NAME = "sample_collection"


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.reply("""Привет! Для агрегации данных отправь JSON с датой начала
                        (dt_from), датой окончания (dt_upto) и типом агрегации (group_type)
                        Пример запроса: {"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00",
                         "group_type": "month"}"""
                        )


async def check_mongodb_connection():
    try:
        client = AsyncIOMotorClient("mongodb://localhost:27017")
        await client.admin.command('ping')
        print("Сервер MongoDB доступен")
    except Exception as e:
        print(f"Ошибка при подключении к MongoDB: {e}")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_mongodb_connection())