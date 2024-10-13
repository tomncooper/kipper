import re
import json
from typing import Any, List, Dict, Optional, Union, cast
from pathlib import Path

import requests

from bs4 import BeautifulSoup
from bs4.element import Tag

BASE_URL: str = "https://wiki.apache.org/confluence"
CONTENT_URL: str = BASE_URL + "/rest/api/content"
WIKI_DATE_FORMAT: str = "%Y-%m-%dT%H:%M:%S.000Z"

KIP_PATTERN: re.Pattern = re.compile(r"KIP-(?P<kip>\d+)", re.IGNORECASE)


def get_kip_main_page_info(timeout: int = 30) -> Dict[str, Any]:
    """Gets the details of the main KIP page"""

    kip_request: requests.Response = requests.get(
        CONTENT_URL,
        params={
            "type": "page",
            "spaceKey": "KAFKA",
            "title": "Kafka Improvement Proposals",
        },
        timeout=timeout,
    )

    kip_request.raise_for_status()

    results: List[Dict[str, Any]] = kip_request.json()["results"]

    if len(results) == 0:
        raise RuntimeError("No results found for KIP main page")

    if len(results) > 1:
        raise RuntimeError(f"More than 1 main page found: {results}")

    return results[0]


def get_kip_main_page_body(kip_main_info: Dict[str, Any], timeout: int = 30) -> str:
    """Gets the RAW HTML body of the KIP main page"""

    kip_body_request: requests.Response = requests.get(
        CONTENT_URL + "/" + kip_main_info["id"],
        params={"expand": "body.view"},
        timeout=timeout,
    )

    kip_body_request.raise_for_status()

    return kip_body_request.json()["body"]["view"]["value"]


ACCEPTED_TERMS: List[str] = [
    "accepted",
    "approved",
    "adopted",
    "adopt",
    "implemented",
    "committed",
    "completed",
    "merged",
    "released",
    "accept",
    "vote passed",
]
UNDER_DISCUSSION_TERMS: List[str] = [
    "discussion",
    "discuss",
    "discusion",
    "voting",
    "under vote",
    "draft",
    "wip",
    "under review",
]
NOT_ACCEPTED_TERMS: List[str] = [
    "rejected",
    "discarded",
    "superseded",
    "subsumed",
    "withdrawn",
    "cancelled",
    "abandoned",
    "replaced",
    "moved to",
]
ACCEPTED: str = "accepted"
UNDER_DISCUSSION: str = "under discussion"
NOT_ACCEPTED: str = "not accepted"
UNKNOWN: str = "unknown"


def get_current_state(html: str) -> Optional[str]:
    """Discerns the state of the kip from the supplied current state html paragraph"""

    if any(option in html.lower() for option in ACCEPTED_TERMS):
        return ACCEPTED

    if any(option in html.lower() for option in UNDER_DISCUSSION_TERMS):
        return UNDER_DISCUSSION

    if any(option in html.lower() for option in NOT_ACCEPTED_TERMS):
        return NOT_ACCEPTED

    return None


def enrich_kip_info(body_html: str, kip_dict: Dict[str, Union[str, int]]) -> None:
    """Parses the body of the KIP wiki page pointed to by the 'content_url'
    key in the supplied dictionary. It will add the derived data to the
    supplied dict."""

    parsed_body: BeautifulSoup = BeautifulSoup(body_html, "html.parser")

    state_processed: bool = False
    jira_processed: bool = False

    for para in parsed_body.find_all("p"):

        if not state_processed and "current state" in para.text.lower():
            state: Optional[str] = get_current_state(para.text)
            if state:
                kip_dict["state"] = state
            else:
                print(f"Could not discern KIP state from {para}")
                kip_dict["state"] = UNKNOWN

            state_processed = True

        elif not jira_processed and "jira" in para.text.lower():
            link: Tag = para.find("a")
            if link:
                href: Optional[str] = link.get("href")
            else:
                href = None

            if href:
                kip_dict["jira"] = href
            else:
                print(f"Could not discern JIRA link from {para}")
                kip_dict["jira"] = UNKNOWN

            jira_processed = True

    if not state_processed:
        kip_dict["state"] = UNKNOWN

    if not jira_processed:
        kip_dict["jira"] = UNKNOWN


def process_child_kip(kip_id: int, child: dict):
    """Process and enrich the KIP child page dictionary"""

    print(f"Processing KIP {kip_id} wiki page")
    child_dict: Dict[str, Union[int, str]] = {}
    child_dict["kip_id"] = kip_id
    child_dict["title"] = child["title"]
    child_dict["web_url"] = BASE_URL + child["_links"]["webui"]
    child_dict["content_url"] = child["_links"]["self"]
    child_dict["created_on"] = child["history"]["createdDate"]
    child_dict["created_by"] = child["history"]["createdBy"]["displayName"]
    child_dict["last_modified_on"] = child["history"]["lastUpdated"]["when"]
    child_dict["last_modified_by"] = child["history"]["lastUpdated"]["by"][
        "displayName"
    ]
    enrich_kip_info(child["body"]["view"]["value"], child_dict)

    return child_dict


def get_kip_information(
    kip_main_info: Dict[str, Any],
    chunk: int = 100,
    update: bool = False,
    overwrite_cache: bool = False,
    cache_filepath: str = "kip_wiki_cache.json",
    timeout: int = 30,
) -> Dict[int, Dict[str, Union[int, str]]]:
    """Gets the details of all child pages of the KIP main page that relate
    to a KIP. This takes a long time so will cache its results in a json file."""

    if update and overwrite_cache:
        update = False

    cache_file_path: Path = Path(cache_filepath)
    if cache_file_path.exists() and not overwrite_cache:
        print(f"Loading KIP Wiki information from cache file: {cache_file_path}")
        with open(cache_file_path, "r", encoding="utf8") as cache_file:
            output: Dict[int, Dict[str, Union[int, str]]] = {
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
        BASE_URL + kip_main_info["_expandable"]["children"],
        timeout=timeout,
    )

    kip_child_info_request.raise_for_status()

    first_kip_child_request: requests.Response = requests.get(
        BASE_URL + kip_child_info_request.json()["_expandable"]["page"],
        params={
            "limit": chunk,
            "expand": "history.lastUpdated,body.view",
        },
        timeout=timeout,
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
                    output[kip_id] = process_child_kip(kip_id, child)
                # TODO: Add check of last modified versus the stored one to indicate an update is needed.

        if "next" in response_json["_links"]:
            kip_child_response: requests.Response = requests.get(
                BASE_URL + response_json["_links"]["next"],
                timeout=timeout,
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

    body_html: str = get_kip_main_page_body(kip_main_info)
    parsed_body: BeautifulSoup = BeautifulSoup(body_html, "html.parser")

    tables: List[Tag] = list(parsed_body.find_all("table"))

    kip_tables: Dict[str, Tag] = {
        "adopted": tables[0],
        "discussion": tables[1],
        "discarded": tables[2],
        "recordings": tables[3],
    }

    return kip_tables


def process_discussion_table(
    discussion_table: Tag, kip_child_urls: Dict[int, Dict[str, Union[str, int]]]
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
                kip_dict["url"] = cast(str, kip_child_urls[kip_id]["web_url"])
            except KeyError:
                kip_dict["url"] = columns[0].a.get("href")
            output[kip_id] = kip_dict
        else:
            continue

    return output
