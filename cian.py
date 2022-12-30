import json
import os
import sys
from datetime import datetime
from json import JSONDecodeError
from math import ceil
from time import sleep

import requests
from fake_useragent import UserAgent
from loguru import logger
from lxml import html as ht
from tqdm import tqdm

from utils import Rent

ATTEMPTS = 3  # Кол-во попыток получения ответа
RESULT_FILENAME = "data.json"  # Название файла результата
TIMEOUT = 20  # Время ожидания ответа

curr_dir = os.path.dirname(os.path.realpath(__file__))
ua = UserAgent()
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(os.path.join(curr_dir, "debug.log"), level="DEBUG", rotation="20 MB", retention=0)


def post_request(url, json_data):
	"""Отправка POST запроса на сервер

	:param url: Ссылка на страницу
	:param dict json_data: Данные запроса
	:return: Ответ сервера
	:rtype: dict
	"""
	headers = {
		"user-agent": ua.random
	}
	for i in range(ATTEMPTS):
		try:
			logger.debug(f"<POST {i} {url}, {json_data}")
			r = requests.post(url, json=json_data, headers=headers, timeout=TIMEOUT)
			if r.status_code == 200:
				return r.json()
		except Exception as e:
			logger.debug(f"<POST {i} failed {url}, {json_data}, {e}")
			sleep(3)
	return None


def get_request(url):
	"""Отправка запроса на сервер

	:param str url: Ссылка
	:return: Ответ сервера или None
	:rtype: str | None
	"""
	headers = {"user-agent": ua.random}
	for i in range(ATTEMPTS):
		try:
			logger.debug(f"<GET {i} {url}")
			r = requests.get(url, headers=headers, timeout=TIMEOUT)
			if r.status_code == 200:
				return r.text
		except Exception as e:
			logger.debug(f"<GET {i} failed {url}, {e}")
			sleep(3)
	return None


def _parse_links(json_data):
	"""Сбор ссылок на странице выдачи объявлений

	:param dict json_data: Данные запроса
	:return: Список ссылок на объявления и номер максимально страницы
	:rtype: tuple[list, str | None]
	"""
	page_data = post_request("https://api.cian.ru/search-offers/v2/search-offers-desktop/", json_data)
	if page_data is None:
		logger.warning("Не удалось получить ссылки")
		logger.debug(f"{json_data}")
		return []
	page_data = page_data["data"]
	offers_cnt = page_data["offerCount"]
	max_page = ceil(offers_cnt / 28)
	data = []
	for item in page_data["offersSerialized"]:
		ad = Rent(platform="cian", link=item["fullUrl"])
		match item["category"]:
			case "flatRent":
				ad.housing_type = "Квартира"
			case "roomRent":
				ad.housing_type = "Комната"
			case "houseRent":
				ad.housing_type = "Дом"
		try:
			ad.total_area = float(item["totalArea"])
		except (ValueError, TypeError):
			pass
		ad.published_at = datetime.fromtimestamp(item["addedTimestamp"]).strftime("%d.%m.%Y")
		try:
			ad.kitchen_area = float(item["kitchenArea"])
		except (ValueError, TypeError):
			pass
		ad.description = item["description"]
		imgs = item["photos"]
		imgs = [img["fullUrl"] for img in imgs]
		ad.photos = [{"link": img} for img in imgs]
		ad.balcony = item["balconiesCount"]
		tmp = item.get("bargainTerms", {})
		ad.price = tmp.get("price")
		ad.commission = tmp.get("agentFee")
		ad.deposit = tmp.get("deposit")
		ad.rooms_count = item["roomsCount"]
		ad.total_floors = item.get("building", {}).get("floorsCount")
		try:
			ad.living_area = float(item["livingArea"])
		except (TypeError, ValueError):
			pass
		if item["isByHomeowner"] is not None and item["isByHomeowner"]:
			ad.is_owner = True
		ad.floor = item["floorNumber"]
		ad.address = item.get("geo", {}).get("userInput")
		data.append(ad)
	return data, max_page


def parse_links():
	"""Сбор ссылок на странице выдачи объявлений

	:return: Список ссылок на объявления
	:rtype: list[str]
	"""
	json_datas = [
		# Квартира
		{
			"jsonQuery": {
				"_type": "flatrent",
				"engine_version": {
					"type": "term",
					"value": 2
				},
				"for_day": {
					"type": "term",
					"value": "0"
				},
				"region": {
					"type": "terms",
					"value": [
						4611
					]
				}
			}
		},
		# Комната
		{
			"jsonQuery": {
				"_type": "flatrent",
				"engine_version": {
					"type": "term",
					"value": 2
				},
				"for_day": {
					"type": "term",
					"value": "0"
				},
				"region": {
					"type": "terms",
					"value": [
						4611
					]
				},
				"room": {
					"type": "terms",
					"value": [
						0
					]
				}
			}
		},
		# Дом, часть дома, таунхаус
		{
			"jsonQuery": {
				"_type": "suburbanrent",
				"engine_version": {
					"type": "term",
					"value": 2
				},
				"for_day": {
					"type": "term",
					"value": "0"
				},
				"object_type": {
					"type": "terms",
					"value": [
						1,
						2,
						4
					]
				},
				"region": {
					"type": "terms",
					"value": [
						4611
					]
				}
			}
		}
	]
	data = []
	for item in json_datas:
		page = 1
		max_page = 1
		while page <= max_page:
			item["jsonQuery"]["page"] = {"type": "term", "value": page}
			_data, max_page = _parse_links(item)
			logger.debug(f"С {item} получено {len(_data)} ссылок")
			data += _data
			page += 1
	return data


def get_additional_data(ad):
	"""Получает доп. информацию

	:param Rent ad: Объявление
	"""
	txt = get_request(ad.link)
	if txt is None:
		logger.warning(f"Не удалось получить данные {ad.link}")
		return
	doc = ht.document_fromstring(txt)
	try:
		xpath = "//li[@data-name='AdditionalFeatureItem']/span[contains(text(), 'Ремонт')]/../span[2]"
		ad.repair = doc.xpath(xpath)[
			0].text
	except (AttributeError, IndexError):
		pass
	try:
		xpath = "//li[@data-name='AdditionalFeatureItem']/span[contains(text(), 'Санузел')]/../span[2]"
		ad.bathroom = doc.xpath(xpath)[0].text
	except (AttributeError, IndexError):
		pass
	tmp = doc.xpath("//li[@data-name='FeatureItem' and contains(text(), 'Мебель в комнатах')]")
	if len(tmp) > 0:
		ad.is_furniture = True
	keys = ("Холодильник", "Стиральная машина", "Телевизор")
	for key in keys:
		if len(doc.xpath(f"//li[@data-name='AmenityItem']/p[contains(text(), '{key}')]")) > 0:
			ad.is_technique = True
			break
	tmp = doc.xpath("//ul[@data-name='Tenants']/li[contains(text(), 'Можно с детьми')]")
	if len(tmp) > 0:
		ad.with_children = True
	tmp = doc.xpath("//ul[@data-name='Tenants']/li[contains(text(), 'Можно с животными')]")
	if len(tmp) > 0:
		ad.with_animals = True
	ad.name = doc.xpath("//h1")[0].text


def save(data):
	"""Сохраняет данные в json файл

	:param list[Rent] data: Список данных объявлений
	"""
	logger.info("Сохраняю данные")
	data = [item.__dict__ for item in data]
	filepath = os.path.join(curr_dir, RESULT_FILENAME)
	logger.debug(f"Сохраняю данные в {filepath}")
	try:
		with open(filepath, "w", encoding="utf-8") as file:
			json.dump(data, file, ensure_ascii=False, indent=4)
	except JSONDecodeError as e:
		logger.warning("Не удалось сохранить данные")
		logger.debug(f"{e}")


def parse():
	"""Функция сбора данных из объявлений"""
	logger.info("Начинаю сбор ссылок на объявления")
	data = []
	try:
		data = parse_links()
		logger.info("Начинаю сбор информации из объявлений")
		for item in tqdm(data):
			try:
				get_additional_data(item)
			except Exception as e:
				logger.warning(f"Не удалось получить данные по объявлению {item.link}")
				logger.debug(f"{e}")
	finally:
		save(data)


if __name__ == '__main__':
	parse()
