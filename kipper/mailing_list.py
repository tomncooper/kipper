import os
import re
import datetime as dt

from typing import Dict, List, Tuple, Optional, Union, cast
from pathlib import Path
from dataclasses import dataclass

from mailbox import mbox
from email.message import Message

import requests


KIP_PATTERN: re.Pattern = re.compile("KIP-(?P<kip>\d+)", re.IGNORECASE)
BASE_URL: str = "https://lists.apache.org/api/mbox.lua"
DOMAIN: str = "kafka.apache.org"
MAIL_DATE_FORMAT = "%a, %d %b %Y %H:%M:%S %z"
MAIL_DATE_FORMAT_ZONE = "%a, %d %b %Y %H:%M:%S %z (%Z)"


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


@dataclass
class KIPMention:
    """Represents a single mention of a KIP"""

    kip: int
    message_id: int
    mbox_year: int
    mbox_month: int
    timestamp: dt.datetime

    def __lt__(self, other) -> bool:

        if isinstance(other, KIPMention):
            if self.timestamp < other.timestamp:
                return True
            else:
                return False
        else:
            raise ValueError(f"Cannot compare KIPMention with {type(other)}")

    def __gt__(self, other) -> bool:

        if isinstance(other, KIPMention):
            if self.timestamp > other.timestamp:
                return True
            else:
                return False
        else:
            raise ValueError(f"Cannot compare KIPMention with {type(other)}")


@dataclass
class KIPData:
    """Represents all mentions of a KIP"""

    kip_id: int
    vote_mention: Optional[KIPMention] = None
    discuss_mention: Optional[KIPMention] = None
    subject_mention: Optional[KIPMention] = None
    body_mention: Optional[KIPMention] = None

    def add_vote_mention(self, mention: KIPMention) -> None:
        """Add a new vote mention but only if it was more recent
        than the current one."""
        if self.vote_mention:
            if mention > self.vote_mention:
                self.vote_mention = mention
        else:
            self.vote_mention = mention

    def add_discuss_mention(self, mention: KIPMention) -> None:
        """Add a new dicussion mention but only if it was more recent
        than the current one."""
        if self.discuss_mention:
            if mention > self.discuss_mention:
                self.discuss_mention = mention
        else:
            self.discuss_mention = mention

    def add_subject_mention(self, mention: KIPMention) -> None:
        """Add a new subject mention but only if it was more recent
        than the current one."""
        if self.subject_mention:
            if mention > self.subject_mention:
                self.subject_mention = mention
        else:
            self.subject_mention = mention

    def add_body_mention(self, mention: KIPMention) -> None:
        """Add a new body mention but only if it was more recent
        than the current one."""
        if self.body_mention:
            if mention > self.body_mention:
                self.body_mention = mention
        else:
            self.body_mention = mention


def get_kip_data(kip_dict: Dict[int, KIPData], kip_id: int):
    """Returns the KIPData instance for the requested KIP ID. If the KIP
    doesn't have an entry an empty KIPData instances is created for the KIP,
    added to the Dict and returned."""

    if kip_id in kip_dict:
        return kip_dict[kip_id]

    kip_data: KIPData = KIPData(kip_id)
    kip_dict[kip_id] = kip_data
    return kip_data


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


def process_mbox_archive(
    filepath: Path, output: Optional[Dict[int, KIPData]] = None
) -> Dict[int, KIPData]:
    """Process the supplied mbox archive, harvest the KIP data and
    add it to the supplied dictionary (or create a new one if none is
    supplied)."""

    if not output:
        output = {}

    mail_box: mbox = mbox(filepath)

    year_month: List[str] = filepath.name.split(".")[0].split("-")
    mbox_year: int = int(year_month[-2])
    mbox_month: int = int(year_month[-1])

    key: int
    msg: Message
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
            subject_kip_data: KIPData = get_kip_data(output, subject_kip_id)
            subject_mention: KIPMention = KIPMention(
                subject_kip_id, key, mbox_year, mbox_month, timestamp
            )
            subject_kip_data.add_subject_mention(subject_mention)

            if "VOTE" in msg["subject"]:
                vote_mention: KIPMention = KIPMention(
                    subject_kip_id, key, mbox_year, mbox_month, timestamp
                )
                subject_kip_data.add_vote_mention(vote_mention)

            if "DISCUSS" in msg["subject"]:
                discuss_mention: KIPMention = KIPMention(
                    subject_kip_id, key, mbox_year, mbox_month, timestamp
                )
                subject_kip_data.add_discuss_mention(discuss_mention)

        # For some reason the payload of the message can be a nested list of messages?
        temp_payload: Union[str, list] = msg.get_payload()
        while not isinstance(temp_payload, str):
            if isinstance(temp_payload, list):
                if len(temp_payload) > 1:
                    # TODO: Deal with these multiple messages
                    print(
                        f"Warning: more than 1 message ({len(temp_payload)}) in the message payload"
                    )
                temp_payload = temp_payload[0]
            elif isinstance(temp_payload, Message):
                temp_payload = temp_payload.get_payload()
            else:
                print(f"What even is this: {type(temp_payload)}")

        payload: str = temp_payload

        try:
            body_matches: List[str] = re.findall(KIP_PATTERN, payload)
        except TypeError:
            print(f"Unable to parse payload of type {type(payload)}")

        if body_matches:
            for body_kip_str in body_matches:
                body_kip_id: int = int(body_kip_str)
                body_kip_data: KIPData = get_kip_data(output, body_kip_id)
                body_mention: KIPMention = KIPMention(
                    body_kip_id, key, mbox_year, mbox_month, timestamp
                )
                body_kip_data.add_body_mention(body_mention)

    return output


def merge_kip_data(one: KIPData, two: KIPData) -> None:
    """Merges two KIP Data instances together returning a single
    KIPData with the most recent KIPMentions from the two."""

    if one.kip_id != two.kip_id:
        raise ValueError(
            "KIPData instances were not for the same KIP ID and cannot be merged"
        )

    one.add_subject_mention(cast(KIPMention, two.subject_mention))
    one.add_vote_mention(cast(KIPMention, two.vote_mention))
    one.add_discuss_mention(cast(KIPMention, two.discuss_mention))
    one.add_body_mention(cast(KIPMention, two.body_mention))


def merge_kip_data_dicts(
    main_dict: Dict[int, KIPData], add_dict: Dict[int, KIPData]
) -> None:
    """Merges the supplied add_dict into the main_dict. Where main dict
    already has an entry for a KIP in the add)_dict two dict, the most
    recent KIPMentions from each are used to produce a new KIPData instance
    in the main dict."""

    for kip_id, kip_data in add_dict.items():
        if kip_id in main_dict:
            main_data = main_dict[kip_id]
            merge_kip_data(main_data, kip_data)
            main_dict[kip_id] = main_data
        else:
            main_dict[kip_id] = kip_data


def process_all_mbox_in_directory(dir_path: Path) -> Dict[int, KIPData]:
    """Process all the mbox files in the given directory and harvest KIP information"""

    if not dir_path.is_dir():
        raise ValueError(f"The supplied path ({dir_path}) is not a directory.")

    output: Dict[int, KIPData] = {}

    for element in dir_path.iterdir():
        if element.is_file():
            if "mbox" in element.name:
                print(f"Processing file: {element.name}")
                file_data: Dict[int, KIPData] = process_mbox_archive(element, output)
                merge_kip_data_dicts(output, file_data)
            else:
                print(f"Skipping non-mbox file: {element.name}")

    return output
