import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон
    Получение списка товаров от Ozon.
    Возваращает список товаров
    Args:
        last_id (str): Идентификатор последнего товара для начала выборки.
        client_id (str): Идентификатор клиента для аутентификации API.
        seller_token (str): Токен продавца для доступа к API.

    Returns:
        list: Список товаров магазина. Каждый товар представлен в виде словаря.
    """
    
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон
    Извлечение всех артикулов на товары. Функция отправляет запросы к API для получения списков товаров и собирает артикулы в один список.
    Args:
        client_id (str): Идентификатор клиента для аутентификации API.
        seller_token (str): Токен продавца для доступа к API.

    Returns:
        list: Список артикулов товаров магазина. Каждый артикул представлен
              в виде строки.
    """
    
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров
    Функция отправляет обновленные цены товаров в магазин на Озон, используя указанные идентификатор клиента и токен продавца. 
    Она формирует запрос к API Озон для импорта новых цен и возвращает ответ от сервера.

    Args:
        prices (list): Список словарей с новыми ценами для товаров. Каждый  словарь должен содержать ключи 'offer_id' и 'price'.
        client_id (str): Идентификатор клиента для аутентификации API.
        seller_token (str): Токен продавца для доступа к API.

    Returns:
        dict: Ответ от API в виде словаря, содержащий информацию об успешном обновлении цен или ошибках.
    """
    
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки
    Отправляет обновленные остатки товаров в магазин на озон.
    Формирует запрос к API для импорта новых остатков и возвращает ответ ото сервера.
     Args:
        stocks (list): Список словарей с новыми остатками для товаров. Каждый словарь должен содержать ключи 'offer_id' и 'stock'.
        client_id (str): Идентификатор клиента для аутентификации API.
        seller_token (str): Токен продавца для доступа к API.

    Returns:
        dict: Ответ от API в виде словаря, содержащий информацию об успешном обновлении остатков или ошибках.
    """
    
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio
    Скачивает файл с остатками часов. Возвращает данные о часах 
        Returns:
        list: Список словарей, содержащих информацию о остатках часов.
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену. Пример: 5'990.00 руб. -> 5990
        Удаляет все символы, кроме цифр, из строки, представляющей цену.
    Возвращает строку, содержащую только числовое значение цены.
    
    Args:
        price (str): Строка, представляющая цену "5'990.00 руб.".

    Returns:
        str: Строка, содержащая только числовую часть цены"5990".
    """
        
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов
    Делит список на подсписок. 
    Args:
        lst (list): Список, который необходимо разделить.
        n (int): Количество элементов в каждом подсписке.
        
    Returns: 
        list: Подсписки, содержащие до n элементов из исходного списка.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
