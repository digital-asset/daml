.. Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _ops-connect-content_index:

Log aggregation on Kubernetes
=============================

If configured so, every daml service can output it's logs in JSON format.
This makes it easy to analyze these with search engines like `ElasticSearch <https://www.elastic.co/elasticsearch/>`_ in combination with `Kibana <https://www.elastic.co/kibana/>`_.

To collect all logs for analyzing them e.g. in your `ElasticSearch` instance, one easy way to achieve this is using `Filebeat <https://www.elastic.co/beats/filebeat>`_.
It regulary collects the logs of your containers and ingests them into `Elasticsearch`, `Logstash <https://www.elastic.co/logstash/>`_ or `Kafka <https://kafka.apache.org/>`_ etc. and the best is, it doesn't require any additional configuration for the daml services.

You can find external documentation on, how to setup `ElasticSearch` with `Filebeat` and `Kibana` for analyzing logs on your Kubernetes cluster `here <https://www.deepnetwork.com/blog/2020/01/27/ELK-stack-filebeat-k8s-deployment.html>`_.

Daml Metrics options
====================

Most daml services support the option to export metrics for application insight.
Currently we support exporting to these with the corresponding cli option:

- `Prometheus <https://prometheus.io>`_ via ``--metrics-reporter "prometheus://HOST[:PORT]"``

- `Graphite <https://graphiteapp.org>`_ via ``--metrics-reporter "graphite://HOST[:PORT][/METRIC_PREFIX]"``

- console output (logs) via ``--metrics-reporter console``
