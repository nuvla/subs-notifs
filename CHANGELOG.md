# Changelog

## Unreleased

### Add

- Expose internal business metrics for Prometheus scraping. 

## [0.0.19] - 2024-02-19

### Add

- Added support for test notification that triggers notification for situations
  when user wants to test the notification mechanism.

### Change

- Fix: in online notification account for NuvlaEdge versions before heartbeat 
  was introduced.

## [0.0.18] - 2024-01-29

### Change

- New microservice to monitor deleted entities (subscription, NuvlaEdge) 
  and clean up the associated Rx/Tx metrics. 

## [0.0.17] - 2024-01-17

### Change

- Improve detection of possible double notifications for the NuvlaEdge online event.

## [0.0.16] - 2024-01-15

### Change

- Workaround to avoid sending double notifications for the NuvlaEdge online event.
 
## [0.0.15] - 2023-12-22

### Change

- Fix: K8s app must be taken into account for app published notifications.

## [0.0.14] - 2023-12-16

### Change

- Improve metrics matching if they are present but None.
- Improve timestamp generation in NE notification.

## [0.0.13] - 2023-12-13

### Add

- Added Notifications to App Bouquet, Deployments, and Deployment Groups on 
  publication of App or App Bouquet.

### Changed

## [0.0.12] - 2023-06-26

### Changed

- Fix: use correct module name in logger init for 'subscription' module.


## [0.0.11] - 2023-06-25

### Added

- Allow setting same log level for all components form env vars.

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
