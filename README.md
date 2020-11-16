[![Build Status](https://travis-ci.com/daigotanaka/athena_manager.svg?branch=master)](https://travis-ci.com/daigotanaka/athena_manager)

# athena_manager

This is a "lab" stage project with limited documentatioin and support. For other open-source projects by Anelen, please see https://anelen.co/open-source.html

## What is it

This command was created to periodically run Athena queries and dump the
result to S3 so that the downstream ELT process (e.g. StitchData's S3 integration
, singer.io tap-s3-csv) load the result to the destination.

- List (partitioned) tables
- Repair each table
- List views
- Run select on each view and dump the CSV to the specified S3 bucket

## Original repository

- https://github.com/daigotanaka/athena-manager

---

Copyright &copy; 2020 Anelen Co., LLC
