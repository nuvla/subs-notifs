# Changelog

## Unreleased

### Changed

## [0.0.10] - 2023-06-23

### Changed

- Change default loglevel to INFO.
  
### Added

- Allow setting loglevel from env vars.

## [0.0.9] - 2023-03-06

### Changed

- Provide units for network metrics.

## [0.0.8] - 2023-02-08

### Changed

- Fix: translate Rx/Tx value to Gb from Bytes for notification.

## [0.0.7] - 2023-02-03

### Changed

- Use nuvlabox-status document timestamp in NE on/offline notifications.

## [0.0.6] - 2023-01-31

### Changed

- When matching Rx/Tx, use bytes instead of gigabytes.

### Changed

## [0.0.5] - 2023-01-30

### Changed

- Empty subscription config filter on tags acts as wildcard.

### Changed

## [0.0.4] - 2023-01-26

### Changed

- Fix: temporary fix to avoid resets of the above threshold flag in a corner case.

### Changed

## [0.0.3] - 2023-01-16

### Changed

- Refactor subscription matching.
- Enforce stronger requirement on missing required attr of subscription
  configuration.

## [0.0.2] - 2023-01-11

### Changed

- Fix: ensure ram/disk/load notification contains value.

## [0.0.1] - 2023-01-06

### Changed

- Initial version
