.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Common Metrics
##############

The below sections contain the list of common metrics we expose for Daml services supporting a Prometheus metrics reporter.
These may help you to measure `the four golden signals <https://sre.google/sre-book/monitoring-distributed-systems/#xref_monitoring_golden-signals>`_.

For the metric types referenced below, see the `relevant Prometheus documentation <https://prometheus.io/docs/tutorials/understanding_metric_types/>`_.

gRPC Metrics
************
We expose the below metrics for all gRPC endpoints.
These metrics have the following common labels attached:

- **grpc_service_name**:
    fully qualified name of the gRPC service (e.g. ``com.daml.ledger.api.v1.ActiveContractsService``)

- **grpc_method_name**:
    name of the gRPC method (e.g. ``GetActiveContracts``)

- **grpc_client_type**:
    type of client connection (``unary`` or ``streaming``)

- **grpc_server_type**:
    type of server connection (``unary`` or ``streaming``)

- **service**:
    Canton service's name (e.g. ``participant``, ``sequencer``, etc.)

.. latency

daml_grpc_server_duration_seconds
=================================
- **Description**: Distribution of the durations of serving gRPC requests.
- **Type**: Histogram

.. traffic

daml_grpc_server_messages_sent_total
====================================
- **Description**: Total number of gRPC messages sent (on either type of connection).
- **Type**: Counter

daml_grpc_server_messages_received_total
========================================
- **Description**: Total number of gRPC messages received (on either type of connection).
- **Type**: Counter

daml_grpc_server_started_total
==============================
- **Description**: Total number of started gRPC requests (on either type of connection).
- **Type**: Counter

.. errors

daml_grpc_server_handled_total
==============================
- **Description**: Total number of handled gRPC requests.
- **Labels**:

  - **grpc_code**: returned `gRPC status code <https://grpc.github.io/grpc/core/md_doc_statuscodes.html>`_ for the call (``OK``, ``CANCELLED``, ``INVALID_ARGUMENT``, etc.)

- **Type**: Counter

.. saturation

daml_grpc_server_messages_sent_bytes
====================================
- **Description**: Distribution of payload sizes in gRPC messages sent (both unary and streaming).
- **Type**: Histogram

daml_grpc_server_messages_received_bytes
========================================
- **Description**: Distribution of payload sizes in gRPC messages received (both unary and streaming).
- **Type**: Histogram

HTTP Metrics
************
We expose the below metrics for all HTTP endpoints.
These metrics have the following common labels attached:

- **http_verb**:
    HTTP verb used for a given call (e.g. ``GET`` or ``PUT``)

- **host**:
    fully qualified hostname of the HTTP endpoint (e.g. ``example.com``)

- **path**:
    path of the HTTP endpoint (e.g. ``/parties/create``)

- **service**:
    Daml service's name (``json-api`` for the HTTP JSON API Service)

daml_http_requests_duration_seconds
===================================
- **Description**: Distribution of the durations of serving HTTP requests.
- **Type**: Histogram

daml_http_requests_total
========================
- **Description**: Total number of HTTP requests completed.
- **Labels**:

  - **http_status**: returned `HTTP status code <https://en.wikipedia.org/wiki/List_of_HTTP_status_codes>`_ for the call

- **Type**: Counter

daml_http_websocket_messages_received_total
===========================================
- **Description**: Total number of WebSocket messages received.
- **Type**: Counter

daml_http_websocket_messages_sent_total
=======================================
- **Description**: Total number of WebSocket messages sent.
- **Type**: Counter

daml_http_requests_payload_bytes
================================
- **Description**: Distribution of payload sizes in HTTP requests received.
- **Type**: Histogram

daml_http_responses_payload_bytes
=================================
- **Description**: Distribution of payload sizes in HTTP responses sent.
- **Type**: Histogram

daml_http_websocket_messages_received_bytes
===========================================
- **Description**: Distribution of payload sizes in WebSocket messages received.
- **Type**: Histogram

daml_http_websocket_messages_sent_bytes
=======================================
- **Description**: Distribution of payload sizes in WebSocket messages sent.
- **Type**: Histogram
