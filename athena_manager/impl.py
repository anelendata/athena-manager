import datetime, json, logging, os, sys

from .aws import athena
from . import utils


logger = logging.getLogger(__name__)


def submit_query(config, query=None, wait=None, dry_run=None, **kwargs):
    if dry_run is None:
        dry_run = config.get("dry_run", False)
    if not query:
        query = config["query"]
    if wait is None:
        wait = config.get("wait", False)

    output_location = os.path.join(config["output_bucket"],
                                   config.get("default_output_path", "Unsaved"))

    if dry_run:
        logger.info("Dry run (output location %s): %s" %
                    (output_location, query))
        return

    logger.debug("Executing query (wait=%s, output_location=%s): %s" %
                 (wait, output_location, query))

    workgroup = config.get("workgroup", "primary")

    if wait:
        response = athena.run_query_and_wait(config["database"], query,
                                             workgroup,
                                             output_location)
    else:
        response = athena.run_query(config["database"], query,
                                    workgroup,
                                    output_location)

    return response


def send_query_from_file(config, query_file=None, **kwargs):
    if not query_file:
        query_file = config["query_file"]
    query = athena.read_query_from_file(query_file)
    return submit_query(config, query=query, **kwargs)


def get_views(config, dry_run=None, **kwargs):
    if dry_run is None:
        dry_run = config.get("dry_run", False)
    if dry_run:
        return config.get("include_views", [])

    query = 'SHOW VIEWS IN ' + config["database"]
    logger.debug('Executing query: %s' % (query))

    response = submit_query(config, query, wait=True)
    logger.debug(response)


    query_execution_id = response["QueryExecution"]["QueryExecutionId"]
    views = athena.fetch_result(query_execution_id)
    return [v[0] for v in views]


def get_tables(config, dry_run=None, **kwargs):
    if dry_run is None:
        dry_run = config.get("dry_run", False)
    if dry_run:
        return config.get("tables", [])

    views = get_views(config, **kwargs)
    query = "SHOW TABLES IN " + config["database"]
    logger.debug("Executing query: %s" % (query))

    response = submit_query(config, query, wait=True)
    logger.debug(response)

    query_execution_id = response["QueryExecution"]["QueryExecutionId"]
    tables_and_views = athena.fetch_result(query_execution_id)
    tables = [t[0] for t in tables_and_views if t[0] not in views]

    return tables


def refresh_table(config, table=None, dry_run=None, **kwargs):
    """
    Run repair table query on the partitioned table on Athena
    """
    if not table:
        table = config["table"]

    query = "MSCK REPAIR TABLE " + table

    if dry_run is None:
        dry_run = config.get("dry_run", False)
    if dry_run:
        logger.info("Dry run: %s" % query)
        return

    response = submit_query(config, query, dry_run=dry_run, wait=True)
    logger.debug(response)

    if dry_run:
        return

    status = response["QueryExecution"]["Status"]["State"]
    if status != "SUCCEEDED":
        raise Exception(
            "repair table query failed for %s (Status %s, response: %s)" %
            (table, status, str(response)))


def query_view(config, view=None, path=None, **kwargs):
    """
    Submit a query to select a view
    """
    if not view:
        raise Exception("view is not set")

    if not path:
        path = config.get("default_output_path", "Unsaved")

    start_offset = datetime.timedelta(days=config.get("start_offset_days", -1))
    end_offset = datetime.timedelta(days=config.get("end_offset_days", 1))
    data = {
        "start_at": config.get("start_at"),
        "end_at": config.get("end_at")
    }
    start_at, end_at = utils.get_time_window(data, date_trunc="day",
                                           start_offset=start_offset,
                                           end_offset=end_offset,
                                           iso_str=True)

    code_dir, _ = os.path.split(__file__)
    query = athena.read_query_from_file(
        os.path.join(code_dir,
                     "sql_template/select_view_time_window_template.sql"))

    params  = dict()
    params.update(config)
    params["start_at"] = start_at
    params["end_at"] = end_at
    params["view"] = view
    query = query.format(**params)

    config_copy = dict()
    config_copy.update(config)
    config_copy["query"] = query
    config_copy["default_output_path"] = path

    return submit_query(config_copy)


def refresh_tables(config, **kwargs):
    tables = get_tables(config, **kwargs)

    for table in tables:
        logger.info("Repairing table: " + table)
        refresh_table(config, table)


def query_views(config, **kwargs):
    """Submit queries to select all the views in the database
    """
    views = get_views(config, **kwargs)
    for view in views:
        logger.info("Querying view: " + view)
        query_view(config, view=view, path=view)


def refresh_and_query_all(config, **kwargs):
    refresh_tables(config, **kwargs)
    query_views(config, **kwargs)


def default(config, **kwargs):
    refresh_and_query_all(config, **kwargs)
