## 2025-03-11 -- v1.1.3
### Added
- Added 'BAD_POLL_DATES' environment variable, which is a list of date strings. If data is erroring in those days, we do not log an error

## 2025-03-11 -- v1.1.3
### Fixed
- Do not attempt to recover healthy data by re-querying ShopperTrak when the initial unhealthy data occurred during a full-day closure (extended or otherwise)

## 2025-02-04 -- v1.1.2
### Fixed
- Do not attempt to recover healthy data by re-querying ShopperTrak when the initial unhealthy data occurred during an extended closure

## 2024-12-30 -- v1.1.1
### Fixed
- For days that are missing entire sites, send any recovered data to Kinesis even if it's unhealthy
- For days that are missing entire sites, re-query the API even if the site is temporarily closed that day

## 2024-12-23 -- v1.1.0
### Added
- Re-query ShopperTrak API for sites missing from a previous API response. This is distinct from re-querying for sites that appear in the API response but have unhealthy data (which is already done).

## 2024-12-19 -- v1.0.6
### Fixed
- URL quote special characters in site IDs to escape them

## 2024-11-21 -- v1.0.5
### Fixed
- Catch non-fatal XML errors and continue requesting. Only throw an error and stop when the API limit has been exceeded, when the request itself has failed, or when the ShopperTrak server cannot be reached even after retrying.

## 2024-11-14 -- v1.0.4
### Fixed
- When site ID is not found (error code "E101"), skip it without throwing an error

## 2024-10-10 -- v1.0.3
### Fixed
- Retry when server is down (new ShopperTrak error code "E000")

## 2024-07-10 -- v1.0.2
### Fixed
- Fix location hours query to retrieve each location's current hours rather than its first hours

## 2024-05-14 -- v1.0.1
### Fixed
- Do not throw an error if the ShopperTrak API rate limit is hit

## 2024-04-24 -- v1.0.0
### Added
- Perform recovery queries on past thirty days of missing data

## 2024-04-04 -- v0.0.1
### Added
- Initial commit without any recovery queries