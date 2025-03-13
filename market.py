import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """ Получает список товаров на Яндекс.Маркете
    
    Args:
        page (str): Используется для конкретных страниц результатов.
        campaign_id (str): Идентификатор кампании, из которой нужно получить товары.
        access_token (str): Токен доступа.

    Returns:
        list: Список записей товарных предложений, полученнных от Яндекс.Маркета
    """
    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """ Обновление запасов товаров на Яндекс.Маркете. 
    Args:
        stocks (list): Список товаров, для которых необходимо обновить запасы.
        campaign_id (str): Идентификатор кампании, в которой обновляются запасы.
        access_token (str): Токен доступа для авторизации.

    Returns:
        dict: Ответ от API Яндекс.Маркета, содержащий информацию об обновленных запасах
    """
    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """ Обновление цены товаров на Яндекс.Маркете.
    Отправляются обновления цен для указанных товаров. Передаются в виде списка предложений
    Args:
         prices (list): Список объектов, содержащих информацию о ценах товаров.
        campaign_id (str): Идентификатор кампании, в которой обновляются цены.
        access_token (str): Токен доступа для авторизации.
        
    Returns:
        dict: Ответ от API Яндекс.Маркета, содержащий информацию об обновленных ценах.
    """
    
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """ Получить артикулы товаров Яндекс маркета
    Извлекает список артикулов всех товаров и собирает все артикулы в один список.
    Args:
        campaign_id (str): Идентификатор кампании, для которой необходимо получить товары.
        market_token (str): Токен доступа для авторизации.
    Returns:
        list: Список артикулов  товаров.
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """ Создает список остатков товаров для определнного склада.
    Обрабатываются остатки товаров, проверяется наличие артикулов в списке приедложений и формируется структура данных.
    Args:
        watch_remnants (list): Список словарей с информацией о остатках товаров. Каждый словарь должен содержать ключи "Код" и "Количество".
        offer_ids (list): Список идентификаторов предложений, которые нужно проверить на наличие в остатках.
        warehouse_id (str): Идентификатор склада, для которого создаются остатки.

    Returns:
        list: Список словарей, каждый из которых представляет собой остаток товара с ключами "sku", "warehouseId" и "items".
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """ Создается список цен для товаров.
    Обрабатываются остатки товаров и формируется структура с ценами.

    Args:
        watch_remnants (list): Список словарей с информацией о остатках товаров. Каждый словарь должен содержать ключи "Код" и "Цена".
        offer_ids (list): Список идентификаторов предложений, для которых необходимо получить цены.

    Returns:
        list: Список словарей, каждый из которых представляет собой цену товара с ключами "id" и "price".
    
    """
    
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """ Обновляет цены на товары.
    Создается список цен на основе остатка товаров и обновляет цены по 500.
    
    Args:
        watch_remnants (list): Список остатков товаров.
        campaign_id (str): Идентификатор кампании, для которой обновляются цены.
        market_token (str): Токен доступа к маркетплейсу для аутентификации запросов.

    Returns:
        list: Список обновленных цен для товаров.

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """ Обновляет запасы товаров.
    Создает список запасов на основе остатков товаров и обновляет запасы по 2000. В результате получаются два списка.
    
    Args:
        watch_remnants (list): Список остатков товаров, содержащий информацию о каждом товаре.
        campaign_id (str): Идентификатор кампании, для которой обновляются запасы.
        market_token (str): Токен доступа к маркетплейсу для аутентификации запросов.
        warehouse_id (str): Идентификатор склада, на котором находятся товары.

    Returns:
            list: Список непустых запасов товаров.
            list: Полный список запасов товаров.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
