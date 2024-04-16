import asyncio  # библиотека для асинхронного программирования
from datetime import datetime  # класс datetime для работы с датами и временем
import json  # модуль для работы с JSON
from motor.motor_asyncio import AsyncIOMotorClient  # асинхронный клиент MongoDB
from aiogram import Bot, Dispatcher, types, BaseMiddleware, F, Router  # компоненты Aiogram для работы с Telegram
from aiogram.filters import Command  # фильтр для обработки команд
import logging  # модуль для логгирования
from datetime import datetime, timedelta

# Параметры для подключения MongoDB
MONGODB_URL = "mongodb://localhost:27017"
DB_NAME = "sampleDB"
COLLECTION_NAME = "sample_collection"

# Параметры для телеграм бота
API_TOKEN = 'YOUR API'

# Настройка логгирования
logging.basicConfig(level=logging.INFO)  # уровень логгирования
logger = logging.getLogger(__name__)  # объект логгера для данного модуля

# Создание клиента MongoDB
client = AsyncIOMotorClient(MONGODB_URL)  # Создаем асинхронного клиента MongoDB
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Создание бота и диспетчера
bot = Bot(token=API_TOKEN)  # объект бота с указанным токеном
dp = Dispatcher()  # диспетчер для обработки входящих сообщений
router = Router()  # роутер для маршрутизации запросов


@dp.message(Command("start"))
async def start(message: types.Message):
    """
       Обработчик команды /start.

       Отправляет приветственное сообщение и инструкции пользователю.
    """
    await message.reply("""Привет! Для агрегации данных отправь JSON с датой начала
                        (dt_from), датой окончания (dt_upto) и типом агрегации (group_type)
                        Пример запроса: {"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00",
                         "group_type": "month"}"""
                        )


@dp.message()
async def process_json(message: types.Message):
    """
        Функция обработки входящего JSON.

        Распарсивает JSON, извлекает дату начала, дату окончания и тип агрегации.
        Затем вызывает функцию для агрегации данных и отправляет результат пользователю.
    """
    try:
        json_data = message.text  # Получаем текст сообщения
        parsed_data = json.loads(json_data)  # Распарсиваем JSON
        dt_from = parsed_data.get('dt_from')  # Извлекаем дату начала
        dt_upto = parsed_data.get('dt_upto')  # Извлекаем дату окончания
        group_type = parsed_data.get('group_type')  # Извлекаем тип агрегации

        # Получение агрегированных данных из MongoDB
        result = await aggregate_salaries(dt_from, dt_upto, group_type)  # Вызываем функцию для агрегации данных
        await message.reply(result)  # Отправляем результат пользователю
    except Exception as e:
        await message.reply(f"Произошла ошибка: {e}")  # Обрабатываем исключение


async def aggregate_salaries(dt_from, dt_upto, group_type):
    """
       Функция агрегации данных.

       Агрегирует данные из MongoDB в заданный временной интервал и в указанном формате.

       Возвращает: JSON-строку с агрегированными данными.
       """
    # Преобразование дат в формат MongoDB
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    # Определение соответствующего формата времени в зависимости от типа группировки
    if group_type == "hour":
        time_format = "%Y-%m-%d %H"  # Формат для агрегации по часам
        label_format = "%Y-%m-%dT%H:%M:%S"
    elif group_type == "day":
        time_format = "%Y-%m-%d"  # Формат для агрегации по дням
        label_format = "%Y-%m-%dT00:00:00"
    elif group_type == "month":
        time_format = "%Y-%m"  # Формат для агрегации по месяцам
        label_format = "%Y-%m-%dT00:00:00"
    else:
        raise ValueError("Неподдерживаемый тип агрегации")

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

    print("MongoDB pipeline:", pipeline)  # Отладочный вывод запроса

    # Выполнение запроса и сбор результатов
    result = await collection.aggregate(pipeline).to_list(length=None)  # выполнение запроса к базе данных MongoDB

    if result:
        # Создание пустого списка для всех дат в указанном диапазоне
        labels = []
        current_date = dt_from
        while current_date <= dt_upto:
            labels.append(current_date.strftime(label_format)) # добавление форматированной даты в список меток времени
            if group_type == 'day':
                current_date += timedelta(days=1) # увеличение текущей даты на один день
            elif group_type == 'month':
                current_date = (current_date.replace(day=1) + timedelta(days=32)).replace(day=1) # увеличение текущей даты на один месяц
            elif group_type == 'hour':
                current_date += timedelta(hours=1) # увеличение текущей даты на один час

        # Заполнение dataset нулями для отсутствующих данных
        dataset = [0] * len(labels)
        for entry in result:
            entry_date = entry['_id'] # получение даты из результата запроса
            entry_value = entry['total_salaries'] # получение суммы зарплат из результата запроса
            index = labels.index(datetime.strptime(entry_date, time_format).strftime(label_format)) # получение индекса метки времени в списке меток
            dataset[index] = entry_value # присвоение значению зарплаты в соответствующем индексе в dataset

        return json.dumps({"dataset": dataset, "labels": labels}) # возврат результата в формате JSON
    else:
        return "Нет данных за указанный период"


async def check_mongodb_connection():
    """
        Функция проверки соединения с MongoDB.

        Проверяет доступность сервера MongoDB.

        """
    try:
        client = AsyncIOMotorClient("mongodb://localhost:27017")
        await client.admin.command('ping')
        print("Сервер MongoDB доступен")
    except Exception as e:
        print(f"Ошибка при подключении к MongoDB: {e}")


if __name__ == '__main__':
    # Проверяем доступность MongoDB перед запуском бота
    loop = asyncio.get_event_loop()  # цикл событий asyncio
    loop.run_until_complete(check_mongodb_connection())  # проверка доступности MongoDB

    # Запускаем бота
    loop.create_task(dp.start_polling(bot))  # процесс получения обновлений от Telegram
    loop.run_forever()  # цикл обработки событий бота
