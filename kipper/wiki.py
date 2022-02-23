import re
import json
from typing import Any, List, Dict, Optional
from pathlib import Path

import requests

from bs4 import BeautifulSoup
from bs4.element import Tag

BASE_URL: str = "https://wiki.apache.org/confluence"
CONTENT_URL: str = BASE_URL + "/rest/api/content"

KIP_PATTERN: re.Pattern = re.compile("KIP-(?P<kip>\d+)", re.IGNORECASE)


def get_kip_main_page_info() -> Dict[str, Any]:
    """Gets the details of the main KIP page"""

    kip_request: requests.Response = requests.get(
        CONTENT_URL,
        params={
            "type": "page",
            "spaceKey": "KAFKA",
            "title": "Kafka Improvement Proposals",
        },
    )

    kip_request.raise_for_status()

    results: List[Dict[str, Any]] = kip_request.json()["results"]

    if len(results) == 0:
        raise RuntimeError("No results found for KIP main page")

    if len(results) > 1:
        raise RuntimeError(f"More than 1 main page found: {results}")

    return results[0]


def get_kip_main_page_body(kip_main_info: Dict[str, Any]) -> str:
    """Gets the RAW HTML body of the KIP main page"""

    kip_body_request: requests.Response = requests.get(
        CONTENT_URL + "/" + kip_main_info["id"], params={"expand": "body.view"}
    )

    kip_body_request.raise_for_status()

    return kip_body_request.json()["body"]["view"]["value"]


def get_kip_child_links(
    kip_main_info: Dict[str, Any],
    chunk: int = 100,
    update: bool = True,
    overwrite_cache: bool = False,
    cache_filepath: str = "kip_url_cache.json",
) -> Dict[int, str]:
    """Gets the URL of all child pages of the KIP main page that relate
    to a KIP. This takes a long time so will cache its results in a json file."""

    if update and overwrite_cache:
        raise ValueError("Either update or overwrite_cache can be true but not both")

    cache_file_path: Path = Path(cache_filepath)
    if cache_file_path.exists() and not overwrite_cache:
        print(f"Loading KIP Wiki information from cache file: {cache_file_path}")
        with open(cache_file_path, "r", encoding="utf8") as cache_file:
            output: Dict[int, str] = {
                int(k): v for k, v in json.load(cache_file).items()
            }
        if not update:
            return output
    else:
        output = {}

    if cache_file_path.exists() and update:
        print("Updating KIP Wiki information with new KIPs")
    else:
        print("Downloading KIP Wiki information for all KIPS")

    kip_child_info_request: requests.Response = requests.get(
        BASE_URL + kip_main_info["_expandable"]["children"]
    )

    kip_child_info_request.raise_for_status()

    first_kip_child_request: requests.Response = requests.get(
        BASE_URL + kip_child_info_request.json()["_expandable"]["page"],
        params={"limit": chunk},
    )

    first_kip_child_request.raise_for_status()

    response_json = first_kip_child_request.json()
    more_results: bool = True

    while more_results:

        for child in response_json["results"]:
            kip_match: Optional[re.Match] = re.search(KIP_PATTERN, child["title"])
            if kip_match:
                kip_id: int = int(kip_match.groupdict()["kip"])
                if kip_id not in output:
                    print(f"Processing KIP {kip_id}")
                    child_url: str = child["_links"]["self"]
                    child_url_response: requests.Response = requests.get(child_url)
                    child_url_response.raise_for_status()
                    output[kip_id] = (
                        BASE_URL + child_url_response.json()["_links"]["webui"]
                    )

        if "next" in response_json["_links"]:
            kip_child_response: requests.Response = requests.get(
                BASE_URL + response_json["_links"]["next"]
            )
            kip_child_response.raise_for_status()
            response_json = kip_child_response.json()
            more_results = True
        else:
            more_results = False

    with open(cache_file_path, "w", encoding="utf8") as cache_file:
        json.dump(output, cache_file)

    return output


def get_kip_tables(kip_main_info: Dict[str, Any]) -> Dict[str, Tag]:
    """Gets the tables from the KIP main page.

    Returns
    -------
    dict
        A dict mapping from the table name [adopted, discussion,
        discarded, recordings] to the Table element."""

    body: str = get_kip_main_page_body(kip_main_info)
    parsed_body: BeautifulSoup = BeautifulSoup(body, "html.parser")

    tables: List[Tag] = [table for table in parsed_body.find_all("table")]

    kip_tables: Dict[str, Tag] = {
        "adopted": tables[0],
        "discussion": tables[1],
        "discarded": tables[2],
        "recordings": tables[3],
    }

    return kip_tables


def process_discussion_table(
    discussion_table: Tag, kip_child_urls: Dict[int, str]
) -> Dict[int, Dict[str, str]]:
    """Process the KIPs under discussion table"""

    # Skip the first row as that is the header
    discussion_rows: List[Tag] = discussion_table.find_all("tr")[1:]

    output: Dict[int, Dict[str, str]] = {}

    for row in discussion_rows:
        kip_dict: Dict[str, str] = {}
        columns: List[Tag] = row.find_all("td")

        kip_text: str = columns[0].a.text
        kip_match: Optional[re.Match] = re.search(KIP_PATTERN, kip_text)

        if kip_match:
            kip_id: int = int(kip_match.groupdict()["kip"])
            kip_dict["text"] = kip_text
            kip_dict["comment"] = columns[1].text
            try:
                kip_dict["url"] = kip_child_urls[kip_id]
            except KeyError:
                kip_dict["url"] = columns[0].a.get("href")
            output[kip_id] = kip_dict
        else:
            continue

    return output
