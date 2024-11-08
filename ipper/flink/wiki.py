import re

from typing import Any, Optional, Union

from bs4 import BeautifulSoup
from bs4.element import Tag

from ipper.common.constants import IPState, UNKNOWN_STR
from ipper.common.wiki import (
    APACHE_CONFLUENCE_BASE_URL,
    get_wiki_page_info,
    child_page_generator,
)

KIP_PATTERN: re.Pattern = re.compile(r"FLIP-(?P<flip>\d+)", re.IGNORECASE)


def get_flip_main_page_info(timeout: int = 30) -> dict[str, Any]:
    """Gets the details of the main KIP page"""

    return get_wiki_page_info(
        space_key="FLINK",
        page_title="Flink Improvement Proposals",
        timeout=timeout,
    )


def get_current_state(para):
    return None


def enrich_flip_info(body_html: str, flip_dict: dict[str, Union[str, int]]) -> None:
    """Parses the body of the FLIP wiki page pointed to by the 'content_url'
    key in the supplied dictionary. It will add the derived data to the
    supplied dict."""

    parsed_body: BeautifulSoup = BeautifulSoup(body_html, "html.parser")

    state_processed: bool = False
    jira_processed: bool = False

    for para in parsed_body.find_all("p"):

        if not state_processed and "current state" in para.text.lower():
            state: Optional[str] = get_current_state(para.text)
            if state:
                flip_dict["state"] = state
            else:
                print(f"Could not discern KIP state from {para}")
                flip_dict["state"] = IPState.UNKNOWN

            state_processed = True

        elif not jira_processed and "jira" in para.text.lower():
            link: Tag = para.find("a")
            if link:
                href: Optional[str] = link.get("href")
            else:
                href = None

            if href:
                flip_dict["jira"] = href
            else:
                print(f"Could not discern JIRA link from {para}")
                flip_dict["jira"] = UNKNOWN_STR

            jira_processed = True

    if not state_processed:
        flip_dict["state"] = IPState.UNKNOWN

    if not jira_processed:
        flip_dict["jira"] = UNKNOWN_STR


def process_child_kip(flip_id: int, child: dict):
    """Process and enrich the KIP child page dictionary"""

    print(f"Processing FLIP {flip_id} wiki page")
    child_dict: dict[str, Union[int, str]] = {}
    child_dict["flip_id"] = flip_id
    child_dict["title"] = child["title"]
    child_dict["web_url"] = APACHE_CONFLUENCE_BASE_URL + child["_links"]["webui"]
    child_dict["content_url"] = child["_links"]["self"]
    child_dict["created_on"] = child["history"]["createdDate"]
    child_dict["created_by"] = child["history"]["createdBy"]["displayName"]
    child_dict["last_modified_on"] = child["history"]["lastUpdated"]["when"]
    child_dict["last_modified_by"] = child["history"]["lastUpdated"]["by"][
        "displayName"
    ]
    #enrich_flip_info(child["body"]["view"]["value"], child_dict)

    return child_dict


def get_flip_information(
    flip_main_info,
    chunk: int = 100,
    timeout: int = 30
):

    output = {}

    for child in child_page_generator(flip_main_info, chunk, timeout):
        flip_match: Optional[re.Match] = re.search(KIP_PATTERN, child["title"])
        if flip_match:
            flip_id: int = int(flip_match.groupdict()["flip"])
            if flip_id not in output:
                output[flip_id] = process_child_kip(flip_id, child)

    return output