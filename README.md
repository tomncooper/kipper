# KIPper: The Kafka Improvement Proposal Enrichment Program

[https://tomncooper.github.io/kipper/](https://tomncooper.github.io/kipper/)

This repo holds a collection of scripts for making a more enriched version of the 
Apache Kafka Improvement Proposal (KIP) [summary page](https://cwiki.apache.org/confluence/display/kafka/kafka+improvement+proposals).

## Installation

This project uses [`poetry`](https://python-poetry.org/) to manage dependencies. To install the necessary libraries run:

```bash
$ poetry install 
```

## Downloading and processing KIP data

To download the Apache Kafka `dev` mailing list for the last year (longer periods can be set via the `--days` option), process the archives and download the KIP Wiki information from the confluence site; run the `init` command:

```bash
$ poetry run kipper init --days 365
```

To update only the most recent month and add any new KIPs which have been posted since the last update run:

```bash
$ poetry run kipper update
```

## Creating the standalone site

To create the standalone site html run the command below where the first argument is the kip mentions cache file produced by the step above and the second is the html output filepath:

```bash
$ poetry run kipper output standalone dev/kip_mentions.csv index.html
```

