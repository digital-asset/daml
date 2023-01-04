.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Logging
#######

The *HTTP JSON API* server uses the industry-standard logback for logging. You can
read more about it in the `Logback documentation <http://logback.qos.ch/>`__.

The logging infrastructure leverages structured logging as implemented by the
`Logstash Logback Encoder <https://github.com/logstash/logstash-logback-encoder/blob/logstash-logback-encoder-6.3/README.md>`__.

Logged events should carry information about the request being served by the
*HTTP JSON API* server. This includes the details of the commands being submitted, the endpoints
being hit, and the response received – highlighting details of failures if any.
When using a traditional logging target (e.g. standard output
or rotating files) this information will be part of the log description.
Using a logging target compatible with the Logstash Logback Encoder allows one to have rich
logs that come with structured information about the event being logged.

The default log encoder used is the plaintext one for traditional logging targets.
