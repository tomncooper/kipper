import re
import datetime as dt

from typing import List, Dict, Union, Optional
from enum import Enum

from pandas import DataFrame, Timestamp, Timedelta, to_datetime
from jinja2 import Template, Environment, FileSystemLoader

from kipper.mailing_list import get_most_recent_mention_by_type
from kipper.wiki import (
    get_kip_child_links,
    get_kip_main_page_info,
    get_kip_tables,
    process_discussion_table,
)

KIP_SPLITTER: re.Pattern = re.compile(r"KIP-\d+\W?[:-]?\W?", re.IGNORECASE)


class KIPStatus(Enum):
    """Enum representing the possible values of a KIP's status"""

    GREEN = ("green", Timedelta(weeks=4))
    YELLOW = ("yellow", Timedelta(weeks=12))
    RED = ("red", Timedelta(days=365))
    BLACK = ("black", Timedelta.max)

    def __init__(self, text: str, duration: Timedelta) -> None:
        super().__init__()
        self.text = text
        self.duration = duration


def calculate_status(last_mention: Timestamp) -> KIPStatus:
    """Calculates the appropriate KIPStatus instance based on the time
    difference between now and the last mention."""

    now: Timestamp = to_datetime(dt.datetime.utcnow(), utc=True)
    diff: Timedelta = now - last_mention

    if diff <= KIPStatus.GREEN.duration:
        return KIPStatus.GREEN

    if diff <= KIPStatus.YELLOW.duration:
        return KIPStatus.YELLOW

    if diff <= KIPStatus.RED.duration:
        return KIPStatus.RED

    return KIPStatus.BLACK


def clean_description(description: str):
    """Cleans the kips description of the KIP-XXX string"""

    kip_match: Optional[re.Match] = re.match(KIP_SPLITTER, description)
    if kip_match:
        return description[kip_match.span()[1] :].strip()

    return description


def create_vote_dict(kip_mentions: DataFrame) -> Dict[int, Dict[str, List[str]]]:
    """Creates a dictionary mapping from KIP ID to a dict mapping
    from vote type to list of those who voted that way"""

    vote_dict: Dict[int, Dict[str, List[str]]] = {}
    kip_id: int
    kip_votes: DataFrame
    for kip_id, kip_votes in kip_mentions[~kip_mentions["vote"].isna()][
        ["kip", "from", "vote"]
    ].groupby("kip"):
        kip_dict = {}
        for vote in ["+1", "0", "-1"]:
            kip_dict[f"{vote}"] = list(
                set(
                    [
                        name.replace('"', "")
                        for name in kip_votes[kip_votes["vote"] == vote]["from"]
                    ]
                )
            )
        vote_dict[kip_id] = kip_dict

    return vote_dict


def create_status_dict(
    kip_mentions: DataFrame,
) -> List[Dict[str, Union[int, str, KIPStatus, List[str]]]]:
    """Calculate a status for each KIP based on how recently it was mentioned in an
    email subject"""

    recent_mentions: DataFrame = get_most_recent_mention_by_type(kip_mentions)

    subject_mentions: DataFrame = recent_mentions["subject"].dropna()

    kip_main_page_info = get_kip_main_page_info()

    discussion_table: Dict[int, Dict[str, str]] = process_discussion_table(
        get_kip_tables(kip_main_page_info)["discussion"],
        get_kip_child_links(kip_main_page_info),
    )

    vote_dict: Dict[int, Dict[str, List[str]]] = create_vote_dict(kip_mentions)

    output: List[Dict[str, Union[int, str, KIPStatus, List[str]]]] = []
    for kip_id in sorted(discussion_table.keys(), reverse=True):
        kip_data: Dict[str, str] = discussion_table[kip_id]
        status_entry: Dict[str, Union[int, str, KIPStatus, List[str]]] = {}
        status_entry["id"] = kip_id
        status_entry["text"] = clean_description(kip_data["text"])
        status_entry["url"] = kip_data["url"]
        if kip_id in subject_mentions:
            status_entry["status"] = calculate_status(subject_mentions[kip_id])
        else:
            status_entry["status"] = KIPStatus.BLACK
        for vote in ["+1", "0", "-1"]:
            if kip_id in vote_dict:
                status_entry[vote] = vote_dict[kip_id][vote]
            else:
                status_entry[vote] = []

        output.append(status_entry)

    return output


def render_standalone_status_page(
    kip_mentions: DataFrame, output_filename: str
) -> None:
    """Renders the KIPs under discussion table with a status entry based on
    how recently the KIP was mentioned in an email subject line."""

    kip_status: List[
        Dict[str, Union[int, str, KIPStatus, List[str]]]
    ] = create_status_dict(kip_mentions)

    template: Template = Environment(loader=FileSystemLoader("templates")).get_template(
        "index.html.jinja"
    )

    output: str = template.render(
        kip_status=kip_status,
        kip_status_enum=KIPStatus,
        date=dt.datetime.now(dt.timezone.utc).strftime("%Y/%m/%d %H:%M:%S %Z"),
    )

    with open(output_filename, "w", encoding="utf8") as out_file:
        out_file.write(output)
