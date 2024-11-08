from argparse import ArgumentParser, Namespace

from ipper.kafka.main import setup_kafka_parser


def setup_top_level_parser() -> ArgumentParser:

    top_level_parser = ArgumentParser(
        "ipper",
        description="Ipper - The Improvement Proposal Enrichment program",
    )
    top_level_parser.set_defaults(func=lambda _: print(top_level_parser.format_help()))

    setup_kafka_parser(top_level_parser)

    return top_level_parser


if __name__ == "__main__":

    PARSER = setup_top_level_parser()
    ARGS: Namespace = PARSER.parse_args()
    ARGS.func(ARGS)
