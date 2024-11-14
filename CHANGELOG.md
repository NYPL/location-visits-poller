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