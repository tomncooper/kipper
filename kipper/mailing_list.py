import os
import re
import datetime as dt

from typing import Dict, List, Tuple, Optional, Union
from pathlib import Path
from enum import Enum

from mailbox import mbox
from email.message import Message
from pandas import DataFrame, concat, to_datetime

import requests


KIP_PATTERN: re.Pattern = re.compile(r"KIP-(?P<kip>\d+)", re.IGNORECASE)
BASE_URL: str = "https://lists.apache.org/api/mbox.lua"
DOMAIN: str = "kafka.apache.org"
MAIL_DATE_FORMAT = "%a, %d %b %Y %H:%M:%S %z"
MAIL_DATE_FORMAT_ZONE = "%a, %d %b %Y %H:%M:%S %z (%Z)"
KIP_MENTION_COLUMNS = [
    "kip",
    "mention_type",
    "message_id",
    "mbox_year",
    "mbox_month",
    "timestamp",
    "from",
]


class KIPMentionType(Enum):
    """Enum class representing the possible types of KIP mention"""

    SUBJECT = "subject"
    VOTE = "vote"
    DISCUSS = "discuss"
    BODY = "body"


def kmt_from_str(mention_type: str) -> KIPMentionType:
    """Finds the KIPMentionType enum value which matches the supplied string.
    Raises a ValueError if the supplied string doesn't match a mention type."""

    for option in KIPMentionType:
        if mention_type == option.value:
            return option

    raise ValueError(f"{mention_type} is not a valid KIPMentionType")


def get_monthly_mbox_file(
    mailing_list: str,
    year: int,
    month: int,
    overwrite: bool = False,
    output_directory: Optional[str] = None,
) -> Path:
    """Downloads the specified mbox archive file from the specified mailing list"""

    filename = f"{mailing_list}_{DOMAIN.replace('.','_')}-{year}-{month}.mbox"

    filepath: Path
    if not output_directory:
        filepath = Path(filename)
    else:
        filepath = Path(output_directory, filename)

    if filepath.exists():
        if not overwrite:
            print(
                f"Mbox file {filepath} already exists. Skipping download (set overwrite to True to re-download)."
            )
            return filepath
        else:
            print(f"Overwritting existing mbox file: {filepath}")

    options: Dict[str, str] = {
        "list": mailing_list,
        "domain": DOMAIN,
        "d": f"{year}-{month}",
    }

    with requests.get(BASE_URL, params=options, stream=True) as response:
        response.raise_for_status()
        with open(filepath, "wb") as mbox_file:
            for chunk in response.iter_content(chunk_size=8192):
                mbox_file.write(chunk)

    return filepath


def generate_month_list(now: dt.datetime, then: dt.datetime) -> List[Tuple[int, int]]:
    """Generates a list of year-month strings spanning from then to now"""

    month_list: List[Tuple[int, int]] = []

    year: int = then.year
    month: int = then.month
    finished: bool = False

    while not finished:
        month_list.append((year, month))
        month = (month + 1) % 13
        if month == 0:
            year += 1
            month = 1
        if month > now.month and year >= now.year:
            finished = True

    return month_list


def get_multiple_mbox(
    mailing_list: str,
    days_back: int,
    output_directory: Optional[str] = None,
    overwrite: bool = False,
) -> List[Path]:
    """Gets all monthly mbox archives from the specified mailing list over the specified number of days into the past"""

    if not output_directory:
        output_directory = mailing_list

    output_dir: Path = Path(output_directory)

    if not output_dir.exists():
        os.mkdir(output_dir)

    now: dt.datetime = dt.datetime.utcnow()
    then: dt.datetime = now - dt.timedelta(days=days_back)

    month_list: List[Tuple[int, int]] = generate_month_list(now, then)

    filepaths: List[Path] = []
    for year, month in month_list:
        print(f"Downloading {mailing_list} archive for {month}/{year}")
        filepath = get_monthly_mbox_file(
            mailing_list,
            year,
            month,
            output_directory=output_directory,
            overwrite=overwrite,
        )
        filepaths.append(filepath)

    return filepaths


def parse_message_timestamp(date_str) -> Optional[dt.datetime]:
    """Parses the message timestamp string and converts to a python datetime object.
    If the string cannot be parsed then None is returned."""

    timestamp: Optional[dt.datetime] = None

    try:
        timestamp = dt.datetime.strptime(date_str, MAIL_DATE_FORMAT)
    except ValueError:
        pass
    else:
        return timestamp

    try:
        timestamp = dt.datetime.strptime(date_str, MAIL_DATE_FORMAT_ZONE)
    except ValueError:
        pass
    else:
        return timestamp

    # If neither the main format or one with TZ string work, try stripping
    # the TZ String as one last hail Mary.
    try:
        timestamp = dt.datetime.strptime(date_str.split(" (")[0], MAIL_DATE_FORMAT)
    except ValueError:
        print(f"Could not parse timestamp: {date_str}")

    return timestamp


def extract_message_payload(msg: Message) -> str:
    """Extract email message string from the supplied message instance. If multiple messages
    are extracted or none then a ValueError is raised."""

    valid_payloads: List[str] = []

    for message in msg.walk():

        payload: str = message.get_payload()

        if (
            ("<html>" in payload)
            or ("</html>" in payload)
            or ("<div>" in payload)
            or ("</div>" in payload)
        ):
            # Sometimes the message will contain an additional html copy of the
            # main message
            continue

        if " " not in payload:
            # If the message doesn't contain a single space the it is probably
            # a public key.
            continue

        valid_payloads.append(payload)

    if len(valid_payloads) == 1:
        return valid_payloads[0]

    if len(valid_payloads) > 1:
        err_msg: str = f"Warning: more than 1 message ({len(valid_payloads)}) in the message payload: {valid_payloads}"
        print(err_msg)
        raise ValueError(err_msg)

    msg_dump: List[str] = [item.get_payload() for item in msg.walk()]
    raise ValueError(f"Message does not contain a valid payload: {msg_dump}")


def process_mbox_archive(filepath: Path) -> DataFrame:
    """Process the supplied mbox archive, harvest the KIP data and
    create a DataFrame containing each mention"""

    mail_box: mbox = mbox(filepath)

    year_month: List[str] = filepath.name.split(".")[0].split("-")
    mbox_year: int = int(year_month[-2])
    mbox_month: int = int(year_month[-1])

    data: List[List[Union[str, int, dt.datetime]]] = []

    for key, msg in mail_box.items():

        # TODO: Add debug logging
        # print(f"Processing message: {key}")

        # TODO: Could there be multiple KIPs mentioned in a subject?
        subject_kip_match: Optional[re.Match] = re.search(KIP_PATTERN, msg["subject"])

        timestamp: Optional[dt.datetime] = parse_message_timestamp(msg["Date"])
        if not timestamp:
            print(f"Could not parse timestamp for message {key}")
            continue

        if subject_kip_match:
            subject_kip_id: int = int(subject_kip_match.groupdict()["kip"])
            data.append(
                [
                    subject_kip_id,
                    KIPMentionType.SUBJECT.value,
                    key,
                    mbox_year,
                    mbox_month,
                    timestamp,
                    msg["from"],
                ]
            )

            if "VOTE" in msg["subject"]:
                data.append(
                    [
                        subject_kip_id,
                        KIPMentionType.VOTE.value,
                        key,
                        mbox_year,
                        mbox_month,
                        timestamp,
                        msg["from"],
                    ]
                )
            elif "DISCUSS" in msg["subject"]:
                data.append(
                    [
                        subject_kip_id,
                        KIPMentionType.DISCUSS.value,
                        key,
                        mbox_year,
                        mbox_month,
                        timestamp,
                        msg["from"],
                    ]
                )

        try:
            payload: str = extract_message_payload(msg)
        except ValueError:
            continue

        try:
            body_matches: List[str] = re.findall(KIP_PATTERN, payload)
        except TypeError:
            print(f"Unable to parse payload of type {type(payload)}")
            continue

        if body_matches:
            for body_kip_str in body_matches:
                body_kip_id: int = int(body_kip_str)
                data.append(
                    [
                        body_kip_id,
                        KIPMentionType.BODY.value,
                        key,
                        mbox_year,
                        mbox_month,
                        timestamp,
                        msg["from"],
                    ]
                )

    output = DataFrame(data, columns=KIP_MENTION_COLUMNS)
    output["timestamp"] = to_datetime(output["timestamp"], utc=True)

    return output.drop_duplicates()


def process_all_mbox_in_directory(dir_path: Path) -> DataFrame:
    """Process all the mbox files in the given directory and harvest all KIP mentions."""

    if not dir_path.is_dir():
        raise ValueError(f"The supplied path ({dir_path}) is not a directory.")

    output: DataFrame = DataFrame(columns=KIP_MENTION_COLUMNS)

    for element in dir_path.iterdir():
        if element.is_file():
            if "mbox" in element.name:
                print(f"Processing file: {element.name}")
                file_data: DataFrame = process_mbox_archive(element)
                output = concat((output, file_data), ignore_index=True)

            else:
                print(f"Skipping non-mbox file: {element.name}")

    return output


def get_most_recent_mentions(kip_mentions: DataFrame) -> DataFrame:
    """Gets the most recent mentions, for each metion type, for each kip from
    the supplied mentions dataframe"""

    output = []

    for _, kip_mention_data in kip_mentions.groupby(["kip", "mention_type"]):
        output.append(
            kip_mention_data[
                kip_mention_data["timestamp"] == kip_mention_data["timestamp"].max()
            ]
        )

    return concat(output, ignore_index=True)


def get_most_recent_mention_by_type(kip_mentions: DataFrame) -> DataFrame:
    """Gets a dataframe indexed by KIP number with the most recent mention of each mention type."""

    most_recent_kip_mentions: DataFrame = get_most_recent_mentions(kip_mentions)

    most_recent: DataFrame = most_recent_kip_mentions.pivot_table(
        index="kip", columns="mention_type", values="timestamp"
    )
    most_recent["overall"] = most_recent.max(axis=1, skipna=True, numeric_only=False)

    return most_recent
