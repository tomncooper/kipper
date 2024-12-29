import re

from typing import Any, Optional, cast, Union

from bs4 import BeautifulSoup
from bs4.element import Tag

from ipper.common.constants import IPState, UNKNOWN_STR, NOT_SET_STR
from ipper.common.wiki import (
    APACHE_CONFLUENCE_BASE_URL,
    get_wiki_page_info,
    child_page_generator,
)

KIP_PATTERN: re.Pattern = re.compile(r"FLIP-(?P<flip>\d+)", re.IGNORECASE)
RELEASE_NUMBER_PATTERN: re.Pattern = re.compile(r"\d+\.?\d*")

TEMPLATE_BOILER_PLATE_PREFIX = "here (<-"

DISCUSSION_THREAD_KEY = "discussion_thread"
VOTE_THREAD_KEY = "vote_thread"
JIRA_ID_KEY = "jira_id"
JIRA_LINK_KEY = "jira_link"
RELEASE_KEY = "release"


def get_flip_main_page_info(timeout: int = 30) -> dict[str, Any]:
    """Gets the details of the main KIP page"""

    return get_wiki_page_info(
        space_key="FLINK",
        page_title="Flink Improvement Proposals",
        timeout=timeout,
    )


def _find_Jira_key_and_link(row_data: Tag) -> tuple[Optional[str], Optional[str]]:

    jira_div = row_data.find("div", {"class": "content-wrapper"})

    if jira_div:
        jira_span = cast(Tag, jira_div).find(
            "span",
            {"class": "jira-issue conf-macro output-block"},
        )
    else:
        return None, None

    if jira_span:
        if cast(Tag, jira_span).has_attr("data-jira-key"):
            jira_id_values: Optional[Union[list[str], str]] = cast(Tag, jira_span).get(
                "data-jira-key"
            )
            if isinstance(jira_id_values, list):
                jira_id = jira_id_values[0]
            else:
                jira_id = cast(str, jira_id_values)
        else:
            jira_id = None

        link = jira_span.find("a")
        if link and cast(Tag, link).has_attr("href"):
            jira_link_values: Optional[Union[list[str], str]] = cast(Tag, link).get("href")
            if isinstance(jira_link_values, list):
                jira_link = jira_link_values[0]
            else:
                jira_link = cast(str, jira_link_values)
        else:
            jira_link = None

        return jira_id, jira_link

    return None, None


def _add_row_data(header: str, row_data: Tag, flip_dict: dict[str, Union[str, int]]) -> None:

    if "discussion" in header:
        if TEMPLATE_BOILER_PLATE_PREFIX in row_data.text:
            flip_dict[DISCUSSION_THREAD_KEY] = NOT_SET_STR
            return

        link = row_data.find("a")
        if link and cast(Tag, link).has_attr("href"):
            flip_dict[DISCUSSION_THREAD_KEY] = cast(Tag, link).get("href")
        else:
            flip_dict[DISCUSSION_THREAD_KEY] = NOT_SET_STR

        return

    if "vote" in header:
        if TEMPLATE_BOILER_PLATE_PREFIX in row_data.text:
            flip_dict[VOTE_THREAD_KEY] = NOT_SET_STR
            return

        link = row_data.find("a")
        if link and cast(Tag, link).has_attr("href"):
            flip_dict[VOTE_THREAD_KEY] = link.get("href")
        else:
            flip_dict[VOTE_THREAD_KEY] = NOT_SET_STR

        return

    if "jira" in header:
        if TEMPLATE_BOILER_PLATE_PREFIX in row_data.text:
            flip_dict[JIRA_ID_KEY] = NOT_SET_STR
            flip_dict[JIRA_LINK_KEY] = NOT_SET_STR
            return

        jira_id, jira_link = _find_Jira_key_and_link(row_data)

        if jira_id:
            flip_dict[JIRA_ID_KEY] = jira_id
        else:
            flip_dict[JIRA_ID_KEY] = NOT_SET_STR

        if jira_link:
            flip_dict[JIRA_LINK_KEY] = jira_link
        else:
            flip_dict[JIRA_LINK_KEY] = NOT_SET_STR

        return

    if "release" in header:

        result = RELEASE_NUMBER_PATTERN.search(row_data.text)
        if result:
            release_number = result.group()
            if "operator" in row_data.text:
                release_number = "operator " + release_number
            flip_dict[RELEASE_KEY] = release_number
        else:
            flip_dict[RELEASE_KEY] = NOT_SET_STR


def _determine_state(flip_dict) -> None:

    if flip_dict[RELEASE_KEY] != UNKNOWN_STR and flip_dict[RELEASE_KEY] != NOT_SET_STR:
        flip_dict["state"] = IPState.RELEASED
        return

    # TODO: figure out the rest of the state algorithm

def _enrich_flip_info(flip_id: int, body_html: str, flip_dict: dict[str, Union[str, int]]) -> None:
    """Parses the body of the FLIP wiki page pointed to by the 'content_url'
    key in the supplied dictionary. It will add the derived data to the
    supplied dict.

    Search process:
        1. Find the first table in the body (some flips don't have a table and will be ignored)
        2. Identify if there is a Discussion Thread, Vote Thread, JIRA or Release entry. 
           Add the details to the flip_dict.
        3. If there is a release, set the status as RELEASED.
        4. 
    """

    parsed_body: BeautifulSoup = BeautifulSoup(body_html, "html.parser")    

    tables = parsed_body.find_all("table")

    # Setup the status entries to default unknown
    flip_dict[DISCUSSION_THREAD_KEY] = UNKNOWN_STR
    flip_dict[VOTE_THREAD_KEY] = UNKNOWN_STR
    flip_dict[RELEASE_KEY] = UNKNOWN_STR
    flip_dict["state"] = IPState.UNKNOWN

    if not tables:
        print(
            f"WARNING: no summary table in FLIP-{flip_id}. " +
            f"This FLIP state will be set to {UNKNOWN_STR}."
        )
        return

    # We assume that the first table on the page is the summary table
    summary_table = tables[0]
    summary_rows = summary_table.findAll("tr")
    if not summary_rows:
        print(
            f"WARNING: no information in summary table in FLIP-{flip_id}. " +
            f"This FLIP state will be set to {UNKNOWN_STR}."
        )
        return

    for row in summary_rows:
        header_tag = row.find("th")
        if header_tag:
            header = header_tag.text.lower()
        else:
            # We have no idea what this row is
            continue

        row_data = row.find('td')
        if row_data:
            _add_row_data(header, row_data, flip_dict)

    _determine_state(flip_dict)


def process_child_kip(flip_id: int, child: dict):
    """Process and enrich the KIP child page dictionary"""

    print(f"Processing FLIP {flip_id} wiki page")
    child_dict: dict[str, Union[int, str]] = {}
    child_dict["id"] = flip_id
    child_dict["title"] = child["title"]
    child_dict["web_url"] = APACHE_CONFLUENCE_BASE_URL + child["_links"]["webui"]
    child_dict["content_url"] = child["_links"]["self"]
    child_dict["created_on"] = child["history"]["createdDate"]
    child_dict["created_by"] = child["history"]["createdBy"]["displayName"]
    child_dict["last_modified_on"] = child["history"]["lastUpdated"]["when"]
    child_dict["last_modified_by"] = child["history"]["lastUpdated"]["by"][
        "displayName"
    ]
    _enrich_flip_info(flip_id, child["body"]["view"]["value"], child_dict)

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
            else:
                print(f"WARNING: FLIP-{flip_id} has been seen more than once in the child pages")

    return output