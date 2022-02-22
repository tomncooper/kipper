from argparse import ArgumentParser, Namespace
from pathlib import Path

from pandas import DataFrame, read_csv

from kipper.mailing_list import get_multiple_mbox, process_all_mbox_in_directory
from kipper.output import render_standalone_status_page


def create_parser() -> ArgumentParser:
    """Creates the Argument Parser instance for the command line interface."""

    parser: ArgumentParser = ArgumentParser("KIPper: The KIP Enrichment Program")

    main_subparser = parser.add_subparsers(title="subcommands", dest="subcommand")
    setup_mail_command(main_subparser)
    setup_wiki_command(main_subparser)
    setup_output_command(main_subparser)

    return parser


def setup_mail_command(main_subparser):
    """Setup the top level mail command line option."""

    mail_parser = main_subparser.add_parser(
        "mail", help="Command for performing mailing list related commands"
    )
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
        "-od" "--output_dir",
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


def setup_wiki_command(main_subparser):
    """Setup the top level wiki command line option."""

    wiki_parser = main_subparser.add_parser(
        "wiki", help="Command for performing wiki related commands"
    )
    wiki_subparser = wiki_parser.add_subparsers(dest="wiki_subcommand")


def setup_output_command(main_subparser):
    """Setup the top level output command line option."""

    output_parser = main_subparser.add_parser(
        "output", help="Command for performing output related commands"
    )
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


def run():
    """KIPper Main Method"""

    parser: ArgumentParser = create_parser()
    args: Namespace = parser.parse_args()

    if args.subcommand == "mail":
        if args.mail_subcommand == "download":
            if "output_dir" not in args:
                out_dir = None
            else:
                out_dir = args.output_dir
            get_multiple_mbox(
                args.mailing_list,
                args.days,
                output_directory=out_dir,
                overwrite=args.overwrite,
            )
        elif args.mail_subcommand == "process":
            out_dir: Path = Path(args.directory)
            kip_mentions: DataFrame = process_all_mbox_in_directory(
                out_dir, overwrite_cache=args.overwrite_cache
            )
            output_file: Path = out_dir.joinpath("kip_mentions.csv")
            kip_mentions.to_csv(output_file, index=False)
            print(f"Saved KIP mentions to {output_file}")
    if args.subcommand == "output":
        if args.output_subcommand == "standalone":
            kip_mentions: DataFrame = read_csv(
                args.kip_mentions_file,
                converters={"vote": lambda x: str(x) if x else None},
                parse_dates=["timestamp"],
            )
            render_standalone_status_page(kip_mentions, args.output_file)


if __name__ == "__main__":

    run()
