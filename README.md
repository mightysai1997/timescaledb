|Linux/macOS|Windows|Coverity|Code Coverage|
|:---:|:---:|:---:|:---:|
|[![Build Status](https://travis-ci.org/timescale/timescaledb.svg?branch=master)](https://travis-ci.org/timescale/timescaledb)|[![Windows build status](https://ci.appveyor.com/api/projects/status/15sqkl900t04hywu/branch/master?svg=true)](https://ci.appveyor.com/project/timescale/timescaledb/branch/master)|[![Coverity Scan Build Status](https://scan.coverity.com/projects/timescale-timescaledb/badge.svg)](https://scan.coverity.com/projects/timescale-timescaledb)|[![Code Coverage](https://codecov.io/gh/timescale/timescaledb/branch/master/graphs/badge.svg?branch=master)](https://codecov.io/gh/timescale/timescaledb)


## TimescaleDB

TimescaleDB is an open-source database designed to make SQL scalable for
time-series data. It is engineered up from PostgreSQL, providing automatic
partitioning across time and space (partitioning key), as well as full
SQL support.

TimescaleDB is packaged as a PostgreSQL extension.  All code is licensed 
under the Apache-2 open-source license, with the exception of source code 
under the `\tsl` subdirectory, which is licensed under 
the [Timescale License (TSL)](https://github.com/timescale/timescaledb/blob/master/tsl/LICENSE-TIMESCALE).  
For clarity, all code files reference licensing in their header.  [Contributors welcome.](https://github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)

Once TSL-licensed code is released, users will be able to build either a 
pure Apache-2 licensed binary (using only Apache-2 source code) or 
a TSL-Licensed binary (that includes both Apache-2 and TSL source code).

Below is an introduction to TimescaleDB. For more information, please check out these other resources:
- [Developer Documentation](https://docs.timescale.com/)
- [Slack Channel](https://slack-login.timescale.com)
- [Support Email](mailto:support@timescale.com)

(Before building from source, see instructions below.)

### Using TimescaleDB

TimescaleDB scales PostgreSQL for time-series data via automatic
partitioning across time and space (partitioning key), yet retains
the standard PostgreSQL interface.

In other words, TimescaleDB exposes what look like regular tables, but
are actually only an
abstraction (or a virtual view) of many individual tables comprising the
actual data. This single-table view, which we call a
[hypertable](https://docs.timescale.com/latest/introduction/architecture#hypertables),
is comprised of many chunks, which are created by partitioning
the hypertable's data in either one or two dimensions: by a time
interval, and by an (optional) "partition key" such as
device id, location, user id, etc. ([Architecture discussion](https://docs.timescale.com/latest/introduction/architecture))

Virtually all user interactions with TimescaleDB are with
hypertables. Creating tables and indexes, altering tables, inserting
data, selecting data, etc., can (and should) all be executed on the
hypertable.

From the perspective of both use and management, TimescaleDB just
looks and feels like PostgreSQL, and can be managed and queried as
such.

#### Before you start

PostgreSQL's out-of-the-box settings are typically too conservative for modern
servers and TimescaleDB. You should make sure your `postgresql.conf`
settings are tuned, either by using [timescaledb-tune](https://github.com/timescale/timescaledb-tune) or doing it manually.

#### Creating a hypertable

```sql
-- Do not forget to create timescaledb extension
CREATE EXTENSION timescaledb;

-- We start by creating a regular SQL table
CREATE TABLE conditions (
  time        TIMESTAMPTZ       NOT NULL,
  location    TEXT              NOT NULL,
  temperature DOUBLE PRECISION  NULL,
  humidity    DOUBLE PRECISION  NULL
);

-- Then we convert it into a hypertable that is partitioned by time
SELECT create_hypertable('conditions', 'time');
```

- [Quick start: Creating hypertables](https://docs.timescale.com/latest/getting-started/creating-hypertables)
- [Reference examples](https://docs.timescale.com/latest/using-timescaledb/schema-management)

#### Inserting and querying data

Inserting data into the hypertable is done via normal SQL commands:

```sql
INSERT INTO conditions(time, location, temperature, humidity)
  VALUES (NOW(), 'office', 70.0, 50.0);

SELECT * FROM conditions ORDER BY time DESC LIMIT 100;

SELECT time_bucket('15 minutes', time) AS fifteen_min,
    location, COUNT(*),
    MAX(temperature) AS max_temp,
    MAX(humidity) AS max_hum
  FROM conditions
  WHERE time > NOW() - interval '3 hours'
  GROUP BY fifteen_min, location
  ORDER BY fifteen_min DESC, max_temp DESC;
```

In addition, TimescaleDB includes additional functions for time-series
analysis that are not present in vanilla PostgreSQL. (For example, the `time_bucket` function above.)

- [Quick start: Basic operations](https://docs.timescale.com/latest/getting-started/basic-operations)
- [Reference examples](https://docs.timescale.com/latest/using-timescaledb/writing-data)
- [TimescaleDB API](https://docs.timescale.com/latest/api)

### Installation

TimescaleDB is available pre-packaged for several platforms:

- Linux:
    - [RedHat / CentOS](https://docs.timescale.com/getting-started/installation/rhel-centos/installation-yum)
    - [Ubuntu](https://docs.timescale.com/getting-started/installation/ubuntu/installation-apt-ubuntu)
    - [Debian](https://docs.timescale.com/getting-started/installation/debian/installation-apt-debian)
- [Docker](https://docs.timescale.com/getting-started/installation/docker/installation-docker)
- [MacOS (Homebrew)](https://docs.timescale.com/getting-started/installation/macos/installation-homebrew)
- [Windows](https://docs.timescale.com/getting-started/installation/windows/installation-windows)

We recommend following our detailed [installation instructions](https://docs.timescale.com/latest/getting-started/installation).

#### Building from source (Unix-based systems)

If you are building from source for **non-development purposes**
(i.e., you want to run TimescaleDB, not submit a patch), you should
**always use a release-tagged commit and not build from `master`**.
See the Releases tab for the latest release.

**Prerequisites**:

- A standard PostgreSQL 9.6 or 10 installation with development
environment (header files) (e.g., `postgresql-server-dev-9.6 `package
for Linux, Postgres.app for MacOS)
- C compiler (e.g., gcc or clang)
- [CMake](https://cmake.org/) version 3.4 or greater

```bash
git clone git@github.com:timescale/timescaledb.git
cd timescaledb
# Find the latest release and checkout, e.g. for 1.0.0:
git checkout 1.0.0
# Bootstrap the build system
./bootstrap
# To build the extension
cd build && make
# To install
make install
```

Note, if you have multiple versions of PostgreSQL installed you can specify the path to `pg_config` that should be used by using `./bootstrap -DPG_CONFIG=/path/to/pg_config`.

Please see our [additional configuration instructions](https://docs.timescale.com/latest/getting-started/installation#update-postgresql-conf).

#### Building from source (Windows)

If you are building from source for **non-development purposes**
(i.e., you want to run TimescaleDB, not submit a patch), you should
**always use a release-tagged commit and not build from `master`**.
See the Releases tab for the latest release.

**Prerequisites**:

- A standard [PostgreSQL 9.6 or 10 64-bit installation](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads#windows)
- OpenSSL for Windows
- Microsoft Visual Studio 2017 with CMake and Git components
- OR Visual Studio 2015/2016 with [CMake](https://cmake.org/) version 3.4 or greater and Git
- Make sure all relevant binaries are in your PATH: `pg_config` and `cmake`

If using Visual Studio 2017 with the CMake and Git components, you
should be able to simply clone the repo and open the folder in
Visual Studio which will take care of the rest.

If you are using an earlier version of Visual Studio, then it can
be built in the following way:
```bash
git clone git@github.com:timescale/timescaledb.git
cd timescaledb

# Find the latest release and checkout, e.g. for 1.0.0:
git checkout 1.0.0
# Bootstrap the build system
bootstrap.bat
# To build the extension from command line
cmake --build ./build --config Release
# To install
cmake --build ./build --config Release --target install

# Alternatively, build in Visual Studio via its built-in support for
# CMake or by opening the generated build/timescaledb.sln solution file.
```

### Useful tools

- [timescaledb-tune](https://github.com/timescale/timescaledb-tune): Helps
set your PostgreSQL configuration settings based on your system's resources.
- [timescaledb-parallel-copy](https://github.com/timescale/timescaledb-parallel-copy): Parallelize your initial bulk loading by using PostgreSQL's
`COPY` across multiple workers.

### Additional documentation

- [Why use TimescaleDB?](https://docs.timescale.com/latest/introduction)
- [Migrating from PostgreSQL](https://docs.timescale.com/latest/getting-started/setup/migrate-from-postgresql)
- [Writing data](https://docs.timescale.com/latest/using-timescaledb/writing-data)
- [Querying and data analytics](https://docs.timescale.com/latest/using-timescaledb/reading-data)
- [Tutorials and sample data](https://docs.timescale.com/latest/tutorials)

### Support

- [Slack Channel](https://slack.timescale.com)
- [Github Issues](https://github.com/timescale/timescaledb/issues)
- [Support Email](mailto:support@timescale.com)

### Contributing

- [Contributor instructions](https://github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
- [Code style guide](https://github.com/timescale/timescaledb/blob/master/docs/StyleGuide.md)
