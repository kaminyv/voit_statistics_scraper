import logging
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any, Optional, Iterable
from copy import copy

import scrapy
from scrapy.http import HtmlResponse
from voit_scraper.items import VoitScraperItem

logger = logging.getLogger(__name__)


@dataclass
class SearchProperty:
    mun_id: Optional[str] = None
    party_key: Optional[str] = None
    party_id: Optional[int] = None
    party_name: Optional[str] = None
    candidate_list: Optional[list] = None


class VlaanderenkiestBeSpider(scrapy.Spider):
    name = "vlaanderenkiest_be"
    allowed_domains = ["vlaanderenkiest.be"]

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self.start_urls = self._generate_start_urls()

    @staticmethod
    def _build_search_url(search_property: SearchProperty) -> str:
        return (f'https://vlaanderenkiest.be'
                f'/verkiezingen2018/api'
                f'/2018/lv/gemeente'
                f'/{search_property.mun_id}/entiteitLijsten.json')

    @staticmethod
    def _build_candidate_list_url(search_property: SearchProperty) -> str:
        return (f'https://vlaanderenkiest.be/verkiezingen2018'
                f'/api/2018/lv/gemeente'
                f'/{search_property.mun_id}'
                f'/{search_property.party_id}/lijst.json')

    @staticmethod
    def _build_candidate_result_list_url(search_property: SearchProperty) -> str:
        return (f'https://vlaanderenkiest.be/verkiezingen2018/'
                f'api/2018/lv/gemeente'
                f'/{search_property.mun_id}'
                f'/{search_property.party_id}/uitslag.json')

    def _extract_value_by_key(self, dictionary, key: str) -> Optional[dict]:
        if key in dictionary:
            return dictionary[key]

        for value in dictionary.values():
            if isinstance(value, dict):
                result = self._extract_value_by_key(value, key)

                if result is not None:
                    return result

        return None

    def _generate_start_urls(self):
        with open('municipality_ids.txt', 'r') as f:
            for mun_id in [i for i in f.readlines() if i.isdigit()]:
                search_property = SearchProperty(mun_id=mun_id)
                yield self._build_search_url(search_property), search_property

    def start_requests(self) -> Iterable[scrapy.Request]:
        if not self.start_urls and hasattr(self, "start_url"):
            raise AttributeError(
                "Crawling could not start: 'start_urls' not found "
                "or empty (but found 'start_url' attribute instead, "
                "did you miss an 's'?)"
            )
        for url, search_property in self.start_urls:
            yield scrapy.Request(url=url,
                                 meta={'search_property': search_property},
                                 dont_filter=True)

    def parse(self, response: HtmlResponse, **kwargs) -> Optional[Iterable[scrapy.Request]]:
        try:
            json_data = response.json()
        except JSONDecodeError:
            return

        data = self._extract_value_by_key(json_data, 'G')
        if data is None:
            logger.error(f'Value not found for key G')
            return

        for key, value in data.items():
            party_key = key
            party_id = value.get('nr')
            party_name = value.get('nm')
            if not party_id or not party_name:
                continue

            search_property = copy(response.meta.get('search_property'))
            search_property.party_key = party_key
            search_property.party_id = party_id
            search_property.party_name = party_name

            url = self._build_candidate_list_url(search_property)
            yield scrapy.Request(url=url,
                                 meta={'search_property': search_property},
                                 callback=self.parse_candidate_list,
                                 dont_filter=True)

    def parse_candidate_list(self, response: HtmlResponse) -> Optional[scrapy.Request]:
        try:
            json = response.json()
        except JSONDecodeError:
            return

        search_property = response.meta.get('search_property')
        data = self._extract_value_by_key(json, search_property.party_key)
        if data is None:
            logger.error(f'Value not found for key {search_property.party_key}')
            return

        search_property.candidate_list = data
        url = self._build_candidate_result_list_url(search_property)
        return scrapy.Request(url=url,
                              meta={'search_property': search_property},
                              callback=self.parse_candidate_result_list,
                              dont_filter=True)

    def parse_candidate_result_list(self, response: HtmlResponse) -> Optional[Iterable[VoitScraperItem]]:
        try:
            json = response.json()
        except Exception as e:
            return

        data = self._extract_value_by_key(json, 'kd')
        if data is None:
            logger.error(f'Value not found for key kd')
            return

        search_property = response.meta.get('search_property')
        merge_results = {}
        for key, value in search_property.candidate_list.items():
            for d_value in data:
                t = d_value.get(key)
                if t is not None:
                    merge_results[key] = value | t
                    break

        for _, value in merge_results.items():
            active = value.get('vk')
            if '1' != active:
                continue
            item = VoitScraperItem()
            item['party'] = search_property.party_name
            item['ranking'] = value.get('vv')
            item['name'] = value.get('nm')
            item['votes'] = value.get('ns')
            yield item
