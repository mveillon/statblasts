# Statblasts

Repository to hold SQL queries used to analyze Retrosheet data for Effectively Wild Statblast-esque questions about historical MLB data.

All data downloaded from [Retrosheet](https://www.retrosheet.org/).

The code can be downloaded and ran locally using the instructions below.

## Installation

Clone the repository to your machine using `git clone https://github.com/mveillon/statblasts`. 

If you have not installed Python onto your computer, you will need to do that. This repository has been tested using version 3.12.7. Other versions of Python are not guaranteed to work.

To install the needed Python dependencies, run `python3 -m pip install -r requirements.txt` from the root directory of the repository.

## Input data

The raw, unformatted data should be present in a folder called `data` at the root level. 

All seven pre-parsed csv files, downloaded from [here](http://web.archive.org/web/20250104174619/https://www.retrosheet.org/downloads/othercsvs.html), should be at the top level of `data`.

The `data` folder should also have folders called `build` and `publish`, which is where output tables should go.

## Running SQL

To run a SQL file, simply run `python3 main.py run -f <sql-path>` from the command line.

Additionally, you can pass a `-s` (start year) and a `-e` (end year) parameter to limit how much data is processed. These parameters can be accessed with the {{ start }} and {{ end }} template variables within the SQL files.

You can also run all the files in `src/dml/build` by running `python3 main.py build`.
