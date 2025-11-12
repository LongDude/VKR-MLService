# fetcher.py
import asyncio
import aiohttp
import datetime
import xml.etree.ElementTree as ET
from psycopg2 import connect 
from redis import asyncio as aioredis
import json
from typing import List, Dict, Optional
from dotenv import load_dotenv
from service_lib import ServiceConnectionFactory

if load_dotenv(): print("INFO: loaded dotenv")
ARXIV_API = "http://export.arxiv.org/oai2"

async def fetch_arxiv_newest(hours_window=24, max_results=100, *, hours_offset=0,  days_offset=0, days_window=0):
    end_date = datetime.datetime.now() - datetime.timedelta(hours=hours_offset, days=days_offset)
    start_date = end_date - datetime.timedelta(hours=hours_window, days=days_window)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"verb=ListRecords&metadataPrefix=arXiv&from={start_str}&until={end_str}"
    url = f"{ARXIV_API}?{query}"
    print(f"INFO: request string: {url}")

    all_records=[]
    async with aiohttp.ClientSession() as session:
        while url and (len(all_records) < max_results or max_results <= 0):
            async with session.get(url) as resp:
                text = await resp.text()
                print(f"INFO: got result for time window [{start_str}, {end_str}]. Size: {text.__sizeof__()} bytes")
                
                records = parse_records_from_xml(text)

                print(f"INFO: extracted {len(records)} records")
                all_records.extend(records)

                resumption_token = extract_resumption_token(text)
                if resumption_token:
                    print(f"INFO: Обнаружен токен продолжения ({resumption_token})")
                    if len(all_records) >= max_results > 0:
                        print(f"INFO: скипаем повторный запрос (переполнение очереди)")
                        break
                    url = f"{ARXIV_API}?verb=ListRecords&resumptionToken={resumption_token}"
                else:
                    break
    
    print(f"INFO: получена очередь записей: {len(all_records)}/{max_results} записей")
    return all_records[:max_results] if max_results > 0 else all_records


def parse_records_from_xml(xml_text: str) -> List[Dict[str, str]]:
    records = []
    try:
        root = ET.fromstring(xml_text)
        
        # Определяем namespace OAI-PMH
        ns = {
            'oai': 'http://www.openarchives.org/OAI/2.0/',
            'arxiv': 'http://arxiv.org/OAI/arXiv/'
        }
        
        # Находим все элементы Record
        record_elements = root.findall('.//oai:record', ns)
        
        for record_elem in record_elements:
            record_data = {}
            
            # Извлекаем идентификатор
            header = record_elem.find('oai:header', ns)
            if header is not None:
                identifier_elem = header.find('oai:identifier', ns)
                if identifier_elem is not None:
                    oai_id = identifier_elem.text
                    if oai_id.startswith("oai:"):  # type: ignore
                        record_data['id'] = oai_id[4:] # type: ignore
                    else:
                        record_data['id'] = oai_id
                
                # Дата публикации
                datestamp_elem = header.find('oai:datestamp', ns)
                if datestamp_elem is not None:
                    record_data['submitted_date'] = datestamp_elem.text
            
            # Извлекаем метаданные arXiv
            metadata = record_elem.find('oai:metadata', ns)
            if metadata is not None:
                arxiv_data = metadata.find('arxiv:arXiv', ns)
                if arxiv_data is not None:
                    # Основные поля
                    title_elem = arxiv_data.find('arxiv:title', ns)
                    if title_elem is not None:
                        record_data['title'] = title_elem.text
                    
                    authors_elems = arxiv_data.findall('arxiv:authors/arxiv:author', ns)
                    authors = []
                    for author_elem in authors_elems:
                        keyname_elem = author_elem.find('arxiv:keyname', ns)
                        forenames_elem = author_elem.find('arxiv:forenames', ns)

                        keyname = keyname_elem.text.strip() if keyname_elem is not None and keyname_elem.text else None
                        forenames = forenames_elem.text.strip() if forenames_elem is not None and forenames_elem.text else None

                        if keyname or forenames:
                            authors.append({
                                "keyname": keyname or "",
                                "forenames": forenames or ""
                            })

                    if authors:
                        record_data['authors'] = authors
                
                    abstract_elem = arxiv_data.find('arxiv:abstract', ns)
                    if abstract_elem is not None:
                        record_data['abstract'] = abstract_elem.text
                    
                    categories_elem = arxiv_data.find('arxiv:categories', ns)
                    if categories_elem is not None:
                        record_data['categories'] = categories_elem.text
                    
                    # DOI (если есть)
                    doi_elem = arxiv_data.find('arxiv:doi', ns)
                    if doi_elem is not None:
                        record_data['doi'] = doi_elem.text
                    
                    # Версия
                    version_elem = arxiv_data.find('arxiv:version', ns)
                    if version_elem is not None:
                        record_data['version'] = version_elem.text
            
            if record_data:  # Добавляем только если есть данные
                records.append(record_data)
                
    except ET.ParseError as e:
        print(f"ERR: Ошибка парсинга XML: {e}")
    except Exception as e:
        print(f"ERR: Ошибка при обработке XML: {e}")
    
    # print(f"VERBOSE: {records}")
    return records

def extract_resumption_token(xml_text: str) -> Optional[str]:
    """
    Извлекает resumptionToken из XML ответа OAI.
    
    Args:
        xml_text: XML строка ответа от OAI API
        
    Returns:
        Resumption token или None, если его нет
    """
    try:
        root = ET.fromstring(xml_text)
        ns = {'oai': 'http://www.openarchives.org/OAI/2.0/'}
        
        # Ищем resumptionToken
        resumption_token_elem = root.find('.//oai:resumptionToken', ns)
        
        if resumption_token_elem is not None and resumption_token_elem.text:
            return resumption_token_elem.text
        else:
            return None
            
    except ET.ParseError as e:
        print(f"ERR: Ошибка парсинга XML при извлечении resumptionToken: {e}")
        return None
    except Exception as e:
        print(f"ERR: Ошибка при извлечении resumptionToken: {e}")
        return None

async def deduplication(entries):
    conn = ServiceConnectionFactory.createDatabaseConnection()
    r = ServiceConnectionFactory.getRedisClient()
    dedup_entries = []

    for e in entries:
        # check redis for cached requests
        already = r.sismember("seen_arxiv_ids", e["id"]) # type: ignore
        if already: continue

        try:
            id_key, id_val = e["id"].split(":")
            with conn.cursor() as cursor:
                cursor.execute("SELECT add_unique_publication(%s, %s, %s, %s, %s)",
                               (e["title"], e["abstract"], e["submitted_date"], id_key, id_val))
                new_pub_id = cursor.fetchone()[0]
                if new_pub_id != 0:
                    print(f"VERB: Добавлена запись {e['id']} с новым id: {new_pub_id} ")
                    e["db_id"] = new_pub_id
                    if e["authors"]:
                        for author in e["authors"]:
                            cursor.execute("SELECT add_author_publication(%s, %s, %s, %s)",
                                           (author["keyname"], author["forenames"], "Не указано", new_pub_id))
                            print(f"VERB: Добавлен автор {author['keyname']} {author['forenames']} для публикации {new_pub_id}")
                    dedup_entries.append(e)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(f"ERR: Ошибка добавления записи: {e} ")
    
    print(f"INFO: Количество записей после дедупликации: {len(dedup_entries)}/{len(entries)}")
    conn.close()
    return dedup_entries


def push_to_redis(dedupl_entries):
    r = ServiceConnectionFactory.getRedisClient()
    for e in dedupl_entries:
        # make a deterministic fingerprint for dedup in DB
        payload = json.dumps(e, ensure_ascii=False)
        r.xadd("metadata_queue", {"data": payload})
        r.sadd("seen_arxiv_ids", e["id"]) # type: ignore
    print(f"INFO: Сохранил {len(dedupl_entries)} в Redis")

async def main():
    newest_entries = await fetch_arxiv_newest(max_results=500, hours_window=0, days_window=1)
    # Filter by published within last 24h:
    deduplicated_entries = await deduplication(newest_entries)
    push_to_redis(deduplicated_entries)

if __name__ == "__main__":
    asyncio.run(main())
