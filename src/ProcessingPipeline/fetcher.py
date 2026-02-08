
"""ArXiv fetcher that ingests metadata into Redis queues."""

import asyncio
import datetime
import json
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv

from service_lib import ServiceConnectionFactory

ARXIV_API = "http://export.arxiv.org/oai2"
ARXIV_PDF_BASE = "https://arxiv.org/pdf"
ARXIV_SOURCE_BASE = "https://arxiv.org/e-print"


class ArxivFetcher:
    """High-level orchestrator for reading ArXiv OAI feed and pushing into Redis."""

    def __init__(
        self,
        metadata_queue: str = "metadata_queue",
        seen_ids_set: str = "seen_arxiv_ids",
        hours_window: int = 24,
        max_results: int = 100,
    ) -> None:
        if load_dotenv("./core/.env"):
            print("INFO: loaded dotenv")

        self.metadata_queue = metadata_queue
        self.seen_ids_set = seen_ids_set
        self.hours_window = hours_window
        self.max_results = max_results
        self.redis_client = ServiceConnectionFactory.getRedisClient()

    async def fetch_newest(
        self,
        *,
        hours_window: Optional[int] = None,
        max_results: Optional[int] = None,
        hours_offset: int = 0,
        days_offset: int = 0,
        days_window: int = 0,
    ) -> List[Dict[str, Any]]:
        hours_window = hours_window if hours_window is not None else self.hours_window
        max_results = max_results if max_results is not None else self.max_results

        end_date = datetime.datetime.now() - datetime.timedelta(hours=hours_offset, days=days_offset)
        start_date = end_date - datetime.timedelta(hours=hours_window, days=days_window)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        query = f"verb=ListRecords&metadataPrefix=arXiv&from={start_str}&until={end_str}"
        url = f"{ARXIV_API}?{query}"
        print(f"INFO: request string: {url}")

        records: List[Dict[str, Any]] = []
        async with aiohttp.ClientSession() as session:
            while url and (len(records) < max_results or max_results <= 0):
                async with session.get(url) as resp:
                    text = await resp.text()
                    print(f"INFO: got result for window [{start_str}, {end_str}] ({len(text)} bytes)")

                    batch = self.parse_records_from_xml(text)
                    for record in batch:
                        self._add_download_preferences(record)
                    records.extend(batch)

                    token = self.extract_resumption_token(text)
                    if not token:
                        break
                    if len(records) >= max_results > 0:
                        print("INFO: record limit reached before resumption; stopping pagination")
                        break
                    url = f"{ARXIV_API}?verb=ListRecords&resumptionToken={token}"

        print(f"INFO: collected {len(records)}/{max_results} records from ArXiv")
        return records[:max_results] if max_results > 0 else records

    @staticmethod
    def parse_records_from_xml(xml_text: str) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        try:
            root = ET.fromstring(xml_text)
            ns = {
                "oai": "http://www.openarchives.org/OAI/2.0/",
                "arxiv": "http://arxiv.org/OAI/arXiv/",
            }

            for record_elem in root.findall(".//oai:record", ns):
                record_data: Dict[str, Any] = {}

                header = record_elem.find("oai:header", ns)
                if header is not None:
                    identifier_elem = header.find("oai:identifier", ns)
                    if identifier_elem is not None and identifier_elem.text:
                        oai_id = identifier_elem.text
                        record_data["id"] = oai_id[4:] if oai_id.startswith("oai:") else oai_id

                    datestamp_elem = header.find("oai:datestamp", ns)
                    if datestamp_elem is not None:
                        record_data["submitted_date"] = datestamp_elem.text

                metadata = record_elem.find("oai:metadata", ns)
                if metadata is not None:
                    arxiv_data = metadata.find("arxiv:arXiv", ns)
                    if arxiv_data is not None:
                        title_elem = arxiv_data.find("arxiv:title", ns)
                        if title_elem is not None:
                            record_data["title"] = title_elem.text

                        authors: List[Dict[str, str]] = []
                        for author_elem in arxiv_data.findall("arxiv:authors/arxiv:author", ns):
                            keyname_elem = author_elem.find("arxiv:keyname", ns)
                            forenames_elem = author_elem.find("arxiv:forenames", ns)
                            keyname = (keyname_elem.text or "").strip() if keyname_elem is not None else ""
                            forenames = (forenames_elem.text or "").strip() if forenames_elem is not None else ""
                            if keyname or forenames:
                                authors.append({"keyname": keyname, "forenames": forenames})
                        if authors:
                            record_data["authors"] = authors

                        abstract_elem = arxiv_data.find("arxiv:abstract", ns)
                        if abstract_elem is not None:
                            record_data["abstract"] = abstract_elem.text

                        categories_elem = arxiv_data.find("arxiv:categories", ns)
                        if categories_elem is not None:
                            record_data["categories"] = categories_elem.text

                        doi_elem = arxiv_data.find("arxiv:doi", ns)
                        if doi_elem is not None:
                            record_data["doi"] = doi_elem.text

                        version_elem = arxiv_data.find("arxiv:version", ns)
                        if version_elem is not None:
                            record_data["version"] = version_elem.text

                if record_data:
                    records.append(record_data)

        except ET.ParseError as exc:
            print(f"ERR: XML parsing error: {exc}")
        except Exception as exc:
            print(f"ERR: unexpected error while parsing XML: {exc}")

        return records

    @staticmethod
    def extract_resumption_token(xml_text: str) -> Optional[str]:
        try:
            root = ET.fromstring(xml_text)
            ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
            token_elem = root.find(".//oai:resumptionToken", ns)
            if token_elem is not None and token_elem.text:
                return token_elem.text
            return None
        except ET.ParseError as exc:
            print(f"ERR: XML parsing error when extracting resumption token: {exc}")
            return None
        except Exception as exc:
            print(f"ERR: unexpected error when extracting resumption token: {exc}")
            return None

    def _add_download_preferences(self, record: Dict[str, Any]) -> None:
        identifier = record.get("id") or ""
        clean_id = identifier.split(":", 1)[-1] if identifier else ""
        if not clean_id:
            return

        pdf_url = f"{ARXIV_PDF_BASE}/{clean_id}.pdf"
        source_url = f"{ARXIV_SOURCE_BASE}/{clean_id}"

        record["download_preferences"] = {
            "source": {
                "url": source_url,
                "description": "LaTeX source tarball (preferred)",
            },
            "pdf": {
                "url": pdf_url,
                "description": "Rendered PDF",
            },
        }
        record["preferred_format"] = "source"

    async def deduplicate(self, entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        conn = ServiceConnectionFactory.createDatabaseConnection()
        dedup_entries: List[Dict[str, Any]] = []

        for entry in entries:
            identifier = entry.get("id")
            if not identifier:
                continue

            already_seen = self.redis_client.sismember(self.seen_ids_set, identifier)  # type: ignore[attr-defined]
            if already_seen:
                continue

            try:
                id_key, id_val = identifier.split(":")
            except ValueError:
                print(f"WARN: unexpected identifier format {identifier}")
                continue

            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT add_unique_publication(%s, %s, %s, %s, %s)",
                        (entry.get("title"), entry.get("abstract"), entry.get("submitted_date"), id_key, id_val),
                    )
                    new_pub_id = cursor.fetchone()[0]
                    if new_pub_id != 0:
                        entry["db_id"] = new_pub_id
                        authors = entry.get("authors") or []
                        for author in authors:
                            cursor.execute(
                                "SELECT add_author_publication(%s, %s, %s, %s)",
                                (author.get("keyname"), author.get("forenames"), "Not specified", new_pub_id),
                            )
                        dedup_entries.append(entry)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                print(f"ERR: failed to add publication {identifier}: {exc}")

        conn.close()
        print(f"INFO: deduplicated {len(dedup_entries)}/{len(entries)} entries")
        return dedup_entries

    def push_to_redis(self, entries: List[Dict[str, Any]]) -> None:
        for entry in entries:
            payload = json.dumps(entry, ensure_ascii=False)
            self.redis_client.xadd(self.metadata_queue, {"data": payload})
            identifier = entry.get("id")
            if identifier:
                self.redis_client.sadd(self.seen_ids_set, identifier)
        print(f"INFO: pushed {len(entries)} entries to Redis stream {self.metadata_queue}")

    async def run(self) -> None:
        newest_entries = await self.fetch_newest(max_results=self.max_results, hours_window=self.hours_window)
        deduplicated_entries = await self.deduplicate(newest_entries)
        self.push_to_redis(deduplicated_entries)


def ensure_event_loop():
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


async def main() -> None:
    fetcher = ArxivFetcher(max_results=500, hours_window=96)
    await fetcher.run()


if __name__ == "__main__":
    ensure_event_loop()
    asyncio.run(main())
