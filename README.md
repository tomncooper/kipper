# KIPper: The Kafka Improvement Proposal Enrichment Program

This repo holds a collection of scripts for making a more enriched version of the 
Apache Kafka Improvement Proposal (KIP) [summary page](https://cwiki.apache.org/confluence/display/kafka/kafka+improvement+proposals).

## Installation

This project uses [`poetry`](https://python-poetry.org/) to manage dependencies. To install the necessary libraries run:

```bash
$ poetry install 
```

## Creating the site

To download the Apache Kafka `dev` mailing list for the last year (longer periods can be set via the `--days` option), process the archives and download the KIP Wiki information from the confluence site; run the `init` command:

```bash
$ poetry run kipper init --days 365
```

To create the standalone site html run the command below where the first argument is the kip mentions cache file produced by the step above and the second if the html output filepath:

```bash
$ poetry run kipper output standalone dev/kip_mentions.csv index.html
```