from argparse import ArgumentParser, Namespace

from kipper.mailing_list import get_multiple_mbox


def create_parser() -> ArgumentParser:
    """Creates the Argument Parser instance for the command line interface."""

    parser: ArgumentParser = ArgumentParser("KIPper: The KIP Enrichment Program")

    main_subparser = parser.add_subparsers(title="subcommands", dest="subcommand")
    setup_mail_command(main_subparser)
    setup_wiki_command(main_subparser)

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


def setup_wiki_command(main_subparser):
    """Setup the top level wiki command line option."""

    wiki_parser = main_subparser.add_parser(
        "wiki", help="Command for performing wiki related commands"
    )
    wiki_subparser = wiki_parser.add_subparsers(dest="wiki_subcommand")


if __name__ == "__main__":

    parser: ArgumentParser = create_parser()
    args: Namespace = parser.parse_args()

    if args.subcommand == "mail":
        if args.mail_subcommand == "download":
            if "output_dir" not in args:
                out_dir = None
            else:
                out_dir = args.output_dir
            mbox_paths = get_multiple_mbox(
                args.mailing_list,
                args.days,
                output_directory=out_dir,
                overwrite=args.overwrite,
            )
