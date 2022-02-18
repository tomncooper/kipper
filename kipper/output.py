import re
import datetime as dt

from typing import List, Dict, Union, Optional
from enum import Enum

from pandas import DataFrame, Timestamp, Timedelta, to_datetime
from jinja2 import Template, Environment, BaseLoader

from kipper.mailing_list import get_most_recent_mention_by_type
from kipper.wiki import get_kip_tables, process_discussion_table

KIP_SPLITTER: re.Pattern = re.compile(r"KIP-\d+\W?[:-]?\W?", re.IGNORECASE)

STATUS_TABLE_TEMPLATE: str = """
<table>
    <tr>
        <th>KIP</th>
        <th>Description</th>
        <th>Status</th>
    </tr>
    {% for kip in kip_status %}
    <tr>
        <td><a href={{ kip['url'] }}>{{ kip['id'] }}</a></td>
        <td>{{ kip['text'] }}
        <td style="background-color:{{ kip['status'].text }};"></td>
    </tr> 
    {% endfor %}
</table>
"""

STATUS_KEY_TEMPLATE = """
<table>
    <tr>
        <th>Status</th>
        <th>Mentioned within the last N days</th>
    </tr>
    {% for status in kip_status_enum %}
    <tr>
        <td style="background-color:{{ status.text }};"></td>
        <td>{{ status.duration.days }}</td>
    </tr>
    {% endfor %}
</table>
"""

STANDALONE_STATUS_TEMPLATE: str = f"""
<!DOCTYPE html>
    <head>
        <title>KIP Status</title>
        <style type="text/css">
            table, th, td {{
                border: 1px solid black;
                border-collapse: collapse;
            }}
        </style>
    </head>
    <body>
        <h1>KIPs Under Discussion</h1>
        {STATUS_TABLE_TEMPLATE}
        <h2>Status Key</h2>
        {STATUS_KEY_TEMPLATE}
    </body>
</html>
"""


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


def create_status_dict(
    kip_mentions: DataFrame,
) -> List[Dict[str, Union[int, str, KIPStatus]]]:
    """Calculate a status for each KIP based on how recently it was mentioned in an
    email subject"""

    recent_mentions: DataFrame = get_most_recent_mention_by_type(kip_mentions)

    subject_mentions: DataFrame = recent_mentions["subject"].dropna()

    discussion_table: Dict[int, Dict[str, str]] = process_discussion_table(
        get_kip_tables()["discussion"]
    )

    output: List[Dict[str, Union[int, str, KIPStatus]]] = []
    for kip_id, kip_data in discussion_table.items():
        status_entry: Dict[str, Union[int, str, KIPStatus]] = {}
        status_entry["id"] = kip_id
        status_entry["text"] = clean_description(kip_data["text"])
        status_entry["url"] = kip_data["url"]
        if kip_id in subject_mentions:
            status_entry["status"] = calculate_status(subject_mentions[kip_id])
        else:
            status_entry["status"] = KIPStatus.BLACK

        output.append(status_entry)

    return output


def render_standalone_status_page(
    kip_mentions: DataFrame, output_filename: str
) -> None:
    """Renders the KIPs under discussion table with a status entry based on
    how recently the KIP was mentioned in an email subject line."""

    kip_status: List[Dict[str, Union[int, str, KIPStatus]]] = create_status_dict(
        kip_mentions
    )

    template: Template = Environment(loader=BaseLoader()).from_string(
        STANDALONE_STATUS_TEMPLATE
    )

    output: str = template.render(kip_status=kip_status, kip_status_enum=KIPStatus)

    with open(output_filename, "w", encoding="utf8") as out_file:
        out_file.write(output)
