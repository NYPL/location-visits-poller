_REDSHIFT_CREATE_TABLE_QUERY = """
    CREATE TEMPORARY TABLE #recoverable_site_dates AS
    WITH recovered_data_timestamps AS (
        SELECT shoppertrak_site_id, orbit, increment_start
        FROM {table}
        WHERE is_recovery_data
            AND increment_start >= '{start_date}'
            AND increment_start < '{end_date}'
    ), recoverable_data_timestamps AS (
        SELECT shoppertrak_site_id, orbit, increment_start
        FROM {table}
        WHERE NOT is_healthy_orbit
            AND increment_start >= '{start_date}'
            AND increment_start < '{end_date}'
        EXCEPT
        SELECT * FROM recovered_data_timestamps
    )
    SELECT shoppertrak_site_id, increment_start::DATE AS increment_date
    FROM recoverable_data_timestamps
    GROUP BY shoppertrak_site_id, increment_date;"""

_REDSHIFT_KNOWN_QUERY = """
    SELECT #recoverable_site_dates.shoppertrak_site_id, orbit, increment_start, enters,
        exits
    FROM #recoverable_site_dates LEFT JOIN {table}
        ON #recoverable_site_dates.shoppertrak_site_id = {table}.shoppertrak_site_id
        AND #recoverable_site_dates.increment_date = {table}.increment_start::DATE
    WHERE is_healthy_orbit
    ORDER BY poll_date;"""

REDSHIFT_DROP_QUERY = "DROP TABLE #recoverable_site_dates;"

REDSHIFT_RECOVERABLE_QUERY = """
    SELECT *
    FROM #recoverable_site_dates
    ORDER BY increment_date, shoppertrak_site_id;"""


def build_redshift_create_table_query(table, start_date, end_date):
    return _REDSHIFT_CREATE_TABLE_QUERY.format(
        table=table, start_date=start_date, end_date=end_date
    )


def build_redshift_known_query(table):
    return _REDSHIFT_KNOWN_QUERY.format(table=table)
