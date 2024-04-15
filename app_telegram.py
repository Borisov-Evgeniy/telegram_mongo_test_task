import asyncio
from datetime import datetime
import json
from motor.motor_asyncio import AsyncIOMotorClient
from aiogram import Bot, Dispatcher, types, BaseMiddleware, F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
import logging

# Параметры для подключения MongoDB
MONGODB_URL = "mongodb://localhost:27017"
DB_NAME = "sampleDB"
COLLECTION_NAME = "sample_collection"

# Параметры для телеграм бота
API_TOKEN = 'YOUR API'

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создание клиента MongoDB
client = AsyncIOMotorClient(MONGODB_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Создание бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher()
router = Router()

@dp.message(Command("start"))
async def start(message: types.Message):
    await message.reply("""Привет! Для агрегации данных отправь JSON с датой начала
                        (dt_from), датой окончания (dt_upto) и типом агрегации (group_type)
                        Пример запроса: {"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00",
                         "group_type": "month"}"""
                        )
@dp.message()
async def process_json(message: types.Message):
    try:
        json_data = message.text
        parsed_data = json.loads(json_data)
        dt_from = parsed_data.get('dt_from')
        dt_upto = parsed_data.get('dt_upto')
        group_type = parsed_data.get('group_type')

        # Получение агрегированных данных из MongoDB
        result = await aggregate_salaries(dt_from, dt_upto, group_type)
        await message.reply(result)
    except Exception as e:
        await message.reply(f"Произошла ошибка: {e}")

async def aggregate_salaries(dt_from, dt_upto, group_type):
    # Преобразование дат в формат MongoDB
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    # Определение соответствующего формата времени в зависимости от типа группировки
    time_format = "%Y-%m-%d"
    if group_type == "hour":
        time_format = "%Y-%m-%d %H"
    elif group_type == "day":
        time_format = "%Y-%m-%d"

    # Построение запроса на агрегацию
    pipeline = [
        {
            "$match": {
                "dt": {"$gte": dt_from, "$lte": dt_upto}
            }
        },
        {
            "$group": {
                "_id": {"$dateToString": {"format": time_format, "date": "$dt"}},
                "total_salaries": {"$sum": "$value"}  # Суммируем значения зарплат
            }
        }
    ]

    # Выполнение запроса и сбор результатов
    result = await collection.aggregate(pipeline).to_list(length=None)

    if result:
        # Преобразуем результат в нужный формат
        dataset = [entry["total_salaries"] for entry in result]
        labels = [datetime.strptime(entry["_id"], "%Y-%m-%d %H").strftime("%Y-%m-%dT%H:%M:%S") for entry in result]
        return json.dumps({"dataset": dataset, "labels": labels})
    else:
        return "Нет данных за указанный период"

async def check_mongodb_connection():
    try:
        client = AsyncIOMotorClient("mongodb://localhost:27017")
        await client.admin.command('ping')
        print("Сервер MongoDB доступен")
    except Exception as e:
        print(f"Ошибка при подключении к MongoDB: {e}")

if __name__ == '__main__':
    # Проверяем доступность MongoDB перед запуском бота
    loop = asyncio.get_event_loop()
    loop.run_until_complete(check_mongodb_connection())
    # Запускаем бота
    loop.create_task(dp.start_polling(bot))
    loop.run_forever()