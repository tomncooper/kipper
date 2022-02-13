import re
from typing import Any, List, Dict, Optional

import requests

from bs4 import BeautifulSoup
from bs4.element import Tag

BASE_URL: str = "https://wiki.apache.org/confluence/rest/api/content"

KIP_PATTERN: re.Pattern = re.compile("KIP-(?P<kip>\d+)", re.IGNORECASE)


def get_kip_main_page_info() -> Dict[str, Any]:
    """Gets the details of the main KIP page"""

    kip_request: requests.Response = requests.get(
        BASE_URL,
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


def get_kip_main_page_body() -> str:
    """Gets the RAW HTML body of the KIP main page"""

    kip_main_info: Dict[str, Any] = get_kip_main_page_info()

    kip_body_request: requests.Response = requests.get(
        BASE_URL + "/" + kip_main_info["id"], params={"expand": "body.view"}
    )

    kip_body_request.raise_for_status()

    return kip_body_request.json()["body"]["view"]["value"]


def get_kip_tables() -> Dict[str, Tag]:
    """Gets the tables from the KIP main page.

    Returns
    -------
    dict
        A dict mapping from the table name [adopted, discussion,
        discarded, recordings] to the Table element."""

    body: str = get_kip_main_page_body()
    parsed_body: BeautifulSoup = BeautifulSoup(body, "html.parser")

    tables: List[Tag] = [table for table in parsed_body.find_all("table")]

    kip_tables: Dict[str, Tag] = {
        "adopted": tables[0],
        "discussion": tables[1],
        "discarded": tables[2],
        "recordings": tables[3],
    }

    return kip_tables


def process_discussion_table(discussion_table: Tag) -> Dict[int, Dict[str, str]]:
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
            kip_dict["url"] = columns[0].a.get("href")
            # TODO: Get the content id from the child pages of the main KIP wiki page
            output[kip_id] = kip_dict
        else:
            continue

    return output
