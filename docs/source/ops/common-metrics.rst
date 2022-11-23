.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Common Metrics
##############

The below sections contain the list of common metrics we expose for Daml services supporting a Prometheus metrics reporter.
For the metric types referenced below, see the `relevant Prometheus documentation <https://prometheus.io/docs/tutorials/understanding_metric_types/>`_.

HTTP Metrics
************
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


