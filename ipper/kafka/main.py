from argparse import ArgumentParser, Namespace
from pathlib import Path
from typing import List

from pandas import DataFrame

from ipper.kafka.mailing_list import (
    get_multiple_mbox,
    load_mbox_cache_file,
    process_all_mbox_in_directory,
    CACHE_DIR,
    process_mbox_files,
)
from ipper.kafka.output import render_standalone_status_page
from ipper.kafka.wiki import get_kip_information, get_kip_main_page_info


def setup_kafka_parser(top_level_parser: ArgumentParser) -> None:
    """Add the kafka subcommands to the supplied top level subparser"""

    top_level_subparsers = top_level_parser.add_subparsers()

    kafka_parser: ArgumentParser = top_level_subparsers.add_parser("kafka")
    kafka_parser.set_defaults(func=lambda _: print(kafka_parser.format_help()))

    main_subparser = kafka_parser.add_subparsers(
        title="kafka subcommands",
        dest="kafka_subcommand",
    )
    setup_init_command(main_subparser)
    setup_update_command(main_subparser)
    setup_mail_command(main_subparser)
    setup_wiki_command(main_subparser)
    setup_output_command(main_subparser)


def setup_init_command(main_subparser):
    """Setup the initialization command"""

    init_parser = main_subparser.add_parser(
        "init", help="Command for initializing all data caches"
    )

    init_parser.add_argument(
        "-d",
        "--days",
        required=False,
        type=int,
        default=365,
        help="The number of days back in time over which to download mail archives. "
        + "Archives are by month so a full month of data will be downloaded "
        + "even if only 1 day is covered by the requested range.",
    )

    init_parser.add_argument(
        "-od", "--output_dir",
        required=False,
        help="Directory to save mailing list archives too.",
    )
    
    init_parser.add_argument(
        "-c",
        "--chunk",
        required=False,
        type=int,
        default=100,
        help="The number of KIP pages to fetch at once.",
    )

    init_parser.set_defaults(func=run_init_cmd)


def setup_update_command(main_subparser) -> None:
    """Setup the 'update' command parser"""

    update_parser = main_subparser.add_parser(
        "update",
        help="Command for updating the cached data from the KIP Wiki and Mail Archives",
    )

    update_parser.set_defaults(func=run_update_cmd)


def setup_mail_command(main_subparser) -> None:
    """Setup the top level mail command line option."""

    mail_parser = main_subparser.add_parser(
        "mail", help="Command for performing mailing list related commands"
    )
    mail_parser.set_defaults(func=mail_parser.print_help)

    mail_subparser = mail_parser.add_subparsers(dest="mail_subcommand")

    download_subparser = mail_subparser.add_parser(
        "download", help="Command for downloading mailing list archives."
    )

    download_subparser.add_argument(
        "mailing_list",
        choices=["dev", "user", "jira", "commits"],
        help="The mailing list to download archives from.",
    )

    download_subparser.add_argument(
        "-d",
        "--days",
        required=False,
        type=int,
        default=365,
        help="The number of days back in time over which to download archives. "
        + "Archives are by month so a full month of data will be downloaded "
        + "even if only 1 day is covered by the requested range.",
    )

    download_subparser.add_argument(
        "-od", "--output_dir",
        required=False,
        help="Directory to save mailing list archives too.",
    )

    download_subparser.add_argument(
        "-ow",
        "--overwrite",
        required=False,
        action="store_true",
        help="Replace existing mail archives.",
    )

    download_subparser.set_defaults(func=setup_mail_download)

    process_subparser = mail_subparser.add_parser(
        "process", help="Command for processing mailing list archives."
    )

    process_subparser.add_argument(
        "directory", help="The directory containing the mbox files to be processed."
    )

    process_subparser.add_argument(
        "-owc",
        "--overwrite_cache",
        required=False,
        action="store_true",
        help="Reprocess the mbox files and overwrite their cache files.",
    )

    process_subparser.set_defaults(func=process_mail_archives)


def setup_wiki_command(main_subparser):
    """Setup the top level wiki command line option."""

    wiki_parser = main_subparser.add_parser(
        "wiki", help="Command for performing wiki related commands"
    )
    wiki_subparser = wiki_parser.add_subparsers(dest="wiki_subcommand")

    wiki_download_subparser = wiki_subparser.add_parser(
        "download", help="Command for downloading and caching KIP wiki information."
    )

    wiki_download_subparser.add_argument(
        "-c",
        "--chunk",
        required=False,
        type=int,
        default=100,
        help="The number of KIP pages to fetch at once.",
    )

    wiki_download_subparser.add_argument(
        "-ow",
        "--overwrite",
        required=False,
        action="store_true",
        help="Redownload all KIP wiki information.",
    )

    wiki_download_subparser.add_argument(
        "-u",
        "--update",
        required=False,
        action="store_true",
        help=(
            "Update KIP wiki information. " +
            "This will add any newly added KIPs to the existing cache."
        ),
    )

    wiki_download_subparser.set_defaults(func=setup_wiki_download)


def setup_output_command(main_subparser):
    """Setup the top level output command line option."""

    output_parser = main_subparser.add_parser(
        "output", help="Command for performing output related commands"
    )
    output_parser.set_defaults(func=output_parser.print_help)

    output_subparser = output_parser.add_subparsers(dest="output_subcommand")

    standalone_subparser = output_subparser.add_parser(
        "standalone",
        help="Command for rendering a standalone html file of the kp table.",
    )

    standalone_subparser.add_argument(
        "kip_mentions_file", help="The path to the processed kip mentions csv."
    )

    standalone_subparser.add_argument(
        "output_file", help="The path to the output html file"
    )

    standalone_subparser.set_defaults(func=run_output_standalone_cmd)


def setup_mail_download(args: Namespace) -> List[Path]:
    """Run the mail archive download command"""

    if "output_dir" not in args:
        out_dir = None
    else:
        out_dir = args.output_dir

    files: List[Path] = get_multiple_mbox(
        args.mailing_list,
        args.days,
        output_directory=out_dir,
        overwrite=args.overwrite,
    )

    return files


def process_mail_archives(args: Namespace) -> None:
    """Run the mail archive processing command"""

    out_dir: Path = Path(args.directory)
    kip_mentions: DataFrame = process_all_mbox_in_directory(
        out_dir, overwrite_cache=args.overwrite_cache
    )
    output_file: Path = out_dir.joinpath("kip_mentions.csv")
    kip_mentions.to_csv(output_file, index=False)
    print(f"Saved KIP mentions to {output_file}")


def setup_wiki_download(args: Namespace) -> None:
    """Run the KIP wiki information download"""

    kip_main_info = get_kip_main_page_info()
    get_kip_information(
        kip_main_info, chunk=args.chunk, update=args.update, overwrite_cache=args.overwrite
    )


def run_init_cmd(args: Namespace) -> None:
    print("Initializing all data caches")
    print("Downloading KIP Wiki Information")
    args.update = False
    args.overwrite = True
    setup_wiki_download(args)
    print("Downloading Developer Mailing List Archives")
    args.mailing_list = "dev"
    setup_mail_download(args)
    args.overwrite_cache = True
    args.directory = "dev"
    process_mail_archives(args)


def run_update_cmd(args: Namespace) -> None:
    print("Updating all data caches")
    print("Updating KIP Wiki Information")
    args.update = True
    args.overwrite = False
    setup_wiki_download(args)
    print("Updating Developer Mailing List Archives")
    # Re-download the most recent months archive
    args.days = 1
    args.overwrite = True
    args.mailing_list = "dev"
    updated_files: List[Path] = setup_mail_download(args)
    # Reprocess just the newly downloaded mail file
    cache_dir: Path = Path("dev").joinpath(CACHE_DIR)
    process_mbox_files(updated_files, cache_dir, overwrite_cache=True)
    # Overwrite the kip mentions cache by process all the old mbox cache files
    # and the newly overwritten one(s)
    args.directory = "dev"
    args.overwrite_cache = False
    process_mail_archives(args)


def run_output_standalone_cmd(args: Namespace) -> None:
    cache_file = Path(args.kip_mentions_file)
    kip_mentions: DataFrame = load_mbox_cache_file(cache_file)
    render_standalone_status_page(kip_mentions, args.output_file)


if __name__ == "__main__":

    PARSER: ArgumentParser = ArgumentParser("Kafka Improvement Proposal Enrichment Program")
    setup_kafka_parser(PARSER)
    ARGS: Namespace = PARSER.parse_args()
    ARGS.func(ARGS)
