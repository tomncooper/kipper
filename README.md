# KIPper: The Kafka Improvement Proposal Enrichment Program

This repo holds a collection of scripts for making a more enriched version of the 
Apache Kafka Improvement Proposal (KIP) [summary page](https://cwiki.apache.org/confluence/display/kafka/kafka+improvement+proposals).

## Installation

This project uses [`poetry`](https://python-poetry.org/) to manage dependencies. To install the necessary libraries run:

```bash
$ poetry install 
```

## Creating the site

To download the Apache Kafka `dev` mailing list archive for the last year run:

```bash
$ poetry run kipper mail download dev --days 365
```

To process the mail archives and create summary csv files run:

```bash
$ poetry run kipper mail process 
```

To create the standalone site html run:

```bash
$ poetry run kipper output standalone dev/kip_mentions.csv dev/standalone.html
```

This will use the csv created in the processing step to create the output html.
