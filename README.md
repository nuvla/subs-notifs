# Matchmaker of subscriptions to telemetry, events (and alike) and notifications generation micro-service

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg?style=for-the-badge)](https://github.com/nuvla/subs-notifs/graphs/commit-activity)
[![GitHub issues](https://img.shields.io/github/issues/nuvla/subs-notifs?style=for-the-badge&logo=github&logoColor=white)](https://github.com/nuvla/subs-notifs/issues)
[![Docker pulls](https://img.shields.io/docker/pulls/nuvla/subs-notifs?style=for-the-badge&logo=Docker&logoColor=white)](https://cloud.docker.com/u/nuvla/repository/docker/nuvla/subs-notifs)
[![Docker image size](https://img.shields.io/docker/image-size/nuvla/subs-notifs/master?logo=docker&logoColor=white&style=for-the-badge)](https://cloud.docker.com/u/nuvla/repository/docker/nuvla/subs-notifs)

![CI Build](https://github.com/nuvla/subs-notifs/actions/workflows/main.yml/badge.svg)
![CI Release](https://github.com/nuvla/subs-notifs/actions/workflows/release.yml/badge.svg)

**This repository contains the source code for the micro-service for matchmaking
the subscriptions to the telemetry, events and alike and generating notifications.**

This microservice is a component of the Nuvla service.

---

**NOTE:** this microservice is part of a loosely coupled architecture, thus when
deployed by itself, it might not provide all of its functionalities. Please
refer to https://github.com/nuvla/deployment for a fully functional deployment

---

## Running tests and validation

### Unitests

Use the same Python version as defined in Dockerfile. Create a virtual 
environment with the correct Python veresion and run tests in it.

On the example of `pyenv` and Python `3.8.12`.

```shell
pyenv virtualenv 3.8.12 nuvla-subs-notifs-py3.8.12
pyenv activate nuvla-subs-notifs-py3.8.12
```

Run tests in `code/` folder.

```shell
cd code/
export PYTHONPATH=$(pwd):$PYTHONPATH
pip install -r requirements.tests.txt
pytest tests/ --junitxml=test-report.xml -v
```

### Validation on a development instance

To validate the time window based resets of the data
use https://github.com/wolfcw/libfaketime. When the `libfaketime` library is
installed and is in the LD path, all the time related syscalls from your app
will be reporting the time you define.

Below is the generic example of the installation and usage.

**Installation example**

```shell
apk add git make gcc musl-dev
cd /
git clone https://github.com/wolfcw/libfaketime.git
cd /libfaketime/src
make install
```

**Usage example**

```shell
$ date
Wed Jan  4 15:53:47 UTC 2023
$ export LD_PRELOAD=/usr/local/lib/faketime/libfaketime.so.1
$ export FAKETIME="+15d"
$ date
Thu Jan 19 15:53:52 UTC 2023
$
```

This is the option to set the time dynamically on already running application in
a container (for example).

```shell
$ echo '@2023-02-15 00:00:00' > /etc/faketimerc
```

**Image for validation**

Use `Dockerfile.faketime` file to build an image for validation. Start the image
as usual. And when needed, exec into the container and create `/etc/faketimerc`
with the required date. Update `/etc/faketimerc` to set new dates as needed.

```shell
$ docker exec -it -u0 <container-id> bash 
container ~ $ echo '@2023-03-25 01:00:00' > /etc/faketimerc
```

## Building 

**If you're developing and testing locally in your own machine**, simply
run `docker build .`

**If you're developing in a non-main branch**, please push your changes to the
respective branch, and wait for Travis CI to finish the automated build. You'll
find your Docker image in the [nuvladev](https://hub.docker.com/u/nuvladev)
organization in Docker hub, names as _nuvladev/subs-notifs:\<branch\>_.

## Deployment

The service will only work if deployed along with other Nuvla services.
See [Nuvla](https://github.com/nuvla/deployment) deployment for details.

### Prerequisites

- *Docker (version 18 or higher)*
- *Docker Compose (version 1.23.2 or higher)*

### Environment variables

|Env. variable | Description |
| ------------------------ | -------------------------------------------------------------------- |
| KAFKA_BOOTSTRAP_SERVERS | Comma separated list of Kafka endpoints (eg. kafka1:9092,kafka2:9092) |
| ES_HOSTS | Comma separated list of Elasticsearch endpoints (eg. es1:9200,es2:9200) |
| `XYZ`_LOGLEVEL | Per-component loglevel. E.g.: DB_LOGLEVEL, MATCHER_LOGLEVEL. |


## Contributing

This is an open-source project, so all community contributions are more than
welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md)

## Copyright

Copyright &copy; 2023, SixSq SA
