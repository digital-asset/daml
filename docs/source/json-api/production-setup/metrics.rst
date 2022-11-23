.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Metrics
#######

Enable and Configure Reporting
******************************


To enable metrics and configure reporting, you can use the below config block in application config:

.. code-block:: none

    metrics {
      //Start a metrics reporter. Must be one of "console", "csv:///PATH", "graphite://HOST[:PORT][/METRIC_PREFIX]", or "prometheus://HOST[:PORT]".
      reporter = "console"
      //Set metric reporting interval , examples : 1s, 30s, 1m, 1h
      reporting-interval = 30s
    }

or the two following CLI options (deprecated):

- ``--metrics-reporter``: passing a legal value will enable reporting; the accepted values
  are as follows:

  - ``console``: prints captured metrics on the standard output

  - ``csv://</path/to/metrics.csv>``: saves the captured metrics in CSV format at the specified location

  - ``graphite://<server_host>[:<server_port>]``: sends captured metrics to a Graphite server. If the port
    is omitted, the default value ``2003`` will be used.

  - ``prometheus://<server_host>[:<server_port>]``: renders captured metrics
    on a http endpoint in accordance with the prometheus protocol. If the port
    is omitted, the default value ``55001`` will be used. The metrics will be
    available under the address ``http://<server_host>:<server_port>/metrics``.

- ``--metrics-reporting-interval``: allows the user to set the interval at which metrics are pre-aggregated on the *HTTP JSON API* and sent to
  the reporter. The formats accepted are based
  on the ISO 8601 duration format ``PnDTnHnMn.nS`` with days considered to be exactly 24 hours.
  The default interval is 10 seconds.

Types of Metrics
****************

This is a list of type of metrics with all data points recorded for each.
Use this as a reference when reading the list of metrics.

Counter
=======

Number of occurrences of some event.

Meter
=====

A meter tracks the number of times a given event occurred (throughput). The following data
points are kept and reported by any meter.

- ``<metric.qualified.name>.count``: number of registered data points overall
- ``<metric.qualified.name>.m1_rate``: number of registered data points per minute
- ``<metric.qualified.name>.m5_rate``: number of registered data points every 5 minutes
- ``<metric.qualified.name>.m15_rate``: number of registered data points every 15 minutes
- ``<metric.qualified.name>.mean_rate``: mean number of registered data points

Timers
======

A timer records the time necessary to execute a given operation (in fractional milliseconds).

Common HTTP Metrics
*******************
If a Prometheus metrics reporter is configured, we also expose the below metrics for all HTTP endpoints
(i.e., helping you to measure `the four golden signals <https://sre.google/sre-book/monitoring-distributed-systems/#xref_monitoring_golden-signals>`__).
These metrics have the following common labels attached:

- **http_verb**: HTTP verb used for a given call (e.g. ``GET`` or ``PUT``)

- **host**: fully qualified hostname of the HTTP endpoint (e.g. ``example.com``)

- **path**: path of the HTTP endpoint (e.g. ``/parties/create``)

- **service**: Daml service's name (``json-api`` for the HTTP JSON API Service)

daml.http.requests.duration.seconds
===================================
- **Description**: Records the durations of serving HTTP requests.
- **Type**: Histogram

daml.http.requests.total
========================
- **Description**: Counts number of HTTP requests completed.
- **Labels**:

  - **http_status**: returned `HTTP status code <https://en.wikipedia.org/wiki/List_of_HTTP_status_codes>`_ for the call

- **Type**: Counter

daml.http.websocket.messages.received.total
===========================================
- **Description**: Counts number of WebSocket messages received.
- **Type**: Counter

daml.http.websocket.messages.sent.total
=======================================
- **Description**: Counts number of WebSocket messages sent.
- **Type**: Counter

daml.http.requests.payload.bytes
================================
- **Description**: Records payload sizes of HTTP requests received.
- **Type**: Histogram

daml.http.responses.payload.bytes
=================================
- **Description**: Records payload sizes of HTTP responses sent.
- **Type**: Histogram

daml.http.websocket.messages.received.bytes
===========================================
- **Description**: Records payload sizes of WebSocket messages received.
- **Type**: Histogram

daml.http.websocket.messages.sent.bytes
=======================================
- **Description**: Records payload sizes of WebSocket messages sent.
- **Type**: Histogram


Metrics Reference
*****************

The following is a list of selected metrics that can be particularly
important to track.

``daml.http_json_api.command_submission_timing``
================================================

A timer. Measures latency (in milliseconds) for processing of a command submission request.

``daml.http_json_api.query_all_timing``
=======================================

A timer. Measures latency (in milliseconds) for processing of a query GET request.

``daml.http_json_api.query_matching_timing``
============================================

A timer. Measures latency (in milliseconds) for processing of a query POST request.

``daml.http_json_api.fetch_timing``
===================================

A timer. Measures latency (in milliseconds) for processing of a fetch request.

``daml.http_json_api.get_party_timing``
=======================================

A timer. Measures latency (in milliseconds) for processing of a get party/parties request.

``daml.http_json_api.allocate_party_timing``
============================================

A timer. Measures latency (in milliseconds) for processing of a party management request.

``daml.http_json_api.download_package_timing``
==============================================

A timer. Measures latency (in milliseconds) for processing of a package download request.

``daml.http_json_api.upload_package_timing``
============================================

A timer. Measures latency (in milliseconds) for processing of a package upload request.

``daml.http_json_api.incoming_json_parsing_and_validation_timing``
==================================================================

A timer. Measures latency (in milliseconds) for parsing and decoding of an incoming json payload

``daml.http_json_api.response_creation_timing``
===============================================

A timer. Measures latency (in milliseconds) for construction of the response json payload.

``daml.http_json_api.db_find_by_contract_key_timing``
=====================================================

A timer. Measures latency (in milliseconds) of the find by contract key database operation.

``daml.http_json_api.db_find_by_contract_id_timing``
====================================================

A timer. Measures latency (in milliseconds) of the find by contract id database operation.

``daml.http_json_api.command_submission_ledger_timing``
=======================================================

A timer. Measures latency (in milliseconds) for processing the command submission requests on the ledger.

``daml.http_json_api.http_request_throughput``
==============================================

A meter. Number of http requests

``daml.http_json_api.websocket_request_count``
==============================================

A Counter. Count of active websocket connections

``daml.http_json_api.command_submission_throughput``
====================================================

A meter. Number of command submissions

``daml.http_json_api.upload_packages_throughput``
=================================================

A meter. Number of package uploads

``daml.http_json_api.allocation_party_throughput``
==================================================

A meter. Number of party allocations
