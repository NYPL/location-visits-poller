_REDSHIFT_HOURS_QUERY = """
    SELECT location_id, weekday, regular_open, regular_close
    FROM {}
    WHERE is_current;"""

_REDSHIFT_CLOSURES_QUERY = """
    SELECT location_id, closure_date
    FROM {closures_table}
    WHERE closure_date >= '{start_date}' AND is_full_day;"""

_REDSHIFT_FOUND_SITES_QUERY = """
    SELECT shoppertrak_site_id, increment_start::DATE AS visits_date
    FROM {table}
    WHERE visits_date >= '{start_date}' AND visits_date < '{end_date}'
    GROUP BY shoppertrak_site_id, visits_date;"""

_REDSHIFT_CREATE_TABLE_QUERY = """
    CREATE TEMPORARY TABLE #recoverable_site_dates AS
    SELECT shoppertrak_site_id, increment_start::DATE AS increment_date
    FROM {table}
    WHERE NOT is_healthy_data
        AND is_fresh
        AND increment_start >= '{start_date}'
        AND increment_start < '{end_date}'
    GROUP BY shoppertrak_site_id, increment_date;"""

_REDSHIFT_KNOWN_QUERY = """
    SELECT #recoverable_site_dates.shoppertrak_site_id, orbit, increment_start,
        id, is_healthy_data, enters, exits
    FROM #recoverable_site_dates LEFT JOIN {table}
        ON #recoverable_site_dates.shoppertrak_site_id = {table}.shoppertrak_site_id
        AND #recoverable_site_dates.increment_date = {table}.increment_start::DATE
    WHERE is_fresh;"""

_REDSHIFT_UPDATE_QUERY = """
    UPDATE {table} SET is_fresh = False
    WHERE id IN ({ids});"""

REDSHIFT_DROP_QUERY = "DROP TABLE #recoverable_site_dates;"

REDSHIFT_RECOVERABLE_QUERY = """
    SELECT *
    FROM #recoverable_site_dates
    ORDER BY increment_date, shoppertrak_site_id;"""


def build_redshift_hours_query(hours_table):
    return _REDSHIFT_HOURS_QUERY.format(hours_table)


def build_redshift_closures_query(closures_table, start_date):
    return _REDSHIFT_CLOSURES_QUERY.format(
        closures_table=closures_table, start_date=start_date
    )


def build_redshift_found_sites_query(table, start_date, end_date):
    return _REDSHIFT_FOUND_SITES_QUERY.format(
        table=table, start_date=start_date, end_date=end_date
    )


def build_redshift_create_table_query(table, start_date, end_date):
    return _REDSHIFT_CREATE_TABLE_QUERY.format(
        table=table, start_date=start_date, end_date=end_date
    )


def build_redshift_known_query(table):
    return _REDSHIFT_KNOWN_QUERY.format(table=table)


def build_redshift_update_query(table, ids):
    return _REDSHIFT_UPDATE_QUERY.format(table=table, ids=ids)
