.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _json-api-perf:

HTTP JSON API Service Performance
#################################

How We Measured It
******************
TODO

Start Sandbox without Persistence
=================================

.. code-block:: shell

    $ daml sandbox-classic --ledgerid MyLedger ./.daml/dist/quickstart-0.0.1.dar

Start Sandbox with Persistence
==============================

.. code-block:: shell

    $ daml sandbox-classic --ledgerid MyLedger \
      --sql-backend-jdbcurl="jdbc:postgresql://localhost:5432/sandbox?user=postgres&password=<...>" \
      ./.daml/dist/quickstart-0.0.1.dar


Start JSON API without Query Store Index
========================================

.. code-block:: shell

    $ daml json-api --ledger-host=localhost --ledger-port=6865 --http-port=7575 \
      --package-reload-interval 5h --allow-insecure-tokens

Start JSON API with Query Store Index
=====================================

.. code-block:: shell

    $ echo TODO

Command Submission
******************

See https://github.com/digital-asset/daml/issues/6673

Create Command
==============

- Create IOU contract from :ref:`IOU Quickstart Tutorial <quickstart>`
- Endpoint: ``/v1/create``

Sandbox Persistence Disabled
----------------------------

- Sandbox Persistence enabled: No
- JSON API Query Store Index enabled: No

.. code-block:: text

    ================================================================================
    ---- Global Information --------------------------------------------------------
    > request count                                       1000 (OK=1000   KO=0     )
    > min response time                                      3 (OK=3      KO=-     )
    > max response time                                     22 (OK=22     KO=-     )
    > mean response time                                     4 (OK=4      KO=-     )
    > std deviation                                          1 (OK=1      KO=-     )
    > response time 50th percentile                          4 (OK=4      KO=-     )
    > response time 75th percentile                          4 (OK=4      KO=-     )
    > response time 95th percentile                          4 (OK=4      KO=-     )
    > response time 99th percentile                          5 (OK=5      KO=-     )
    > mean requests/sec                                    200 (OK=200    KO=-     )
    ---- Response Time Distribution ------------------------------------------------
    > t < 800 ms                                          1000 (100%)
    > 800 ms < t < 1200 ms                                   0 (  0%)
    > t > 1200 ms                                            0 (  0%)
    > failed                                                 0 (  0%)
    ================================================================================

Sandbox Persistence Enabled
---------------------------

- Sandbox Persistence enabled: Yes
- JSON API Query Store Index enabled: No

.. code-block:: text

    ================================================================================
    ---- Global Information --------------------------------------------------------
    > request count                                       1000 (OK=1000   KO=0     )
    > min response time                                      9 (OK=9      KO=-     )
    > max response time                                     53 (OK=53     KO=-     )
    > mean response time                                    16 (OK=16     KO=-     )
    > std deviation                                          8 (OK=8      KO=-     )
    > response time 50th percentile                         11 (OK=11     KO=-     )
    > response time 75th percentile                         20 (OK=20     KO=-     )
    > response time 95th percentile                         31 (OK=31     KO=-     )
    > response time 99th percentile                         33 (OK=33     KO=-     )
    > mean requests/sec                                 58.824 (OK=58.824 KO=-     )
    ---- Response Time Distribution ------------------------------------------------
    > t < 800 ms                                          1000 (100%)
    > 800 ms < t < 1200 ms                                   0 (  0%)
    > t > 1200 ms                                            0 (  0%)
    > failed                                                 0 (  0%)
    ================================================================================

Exercise Command
================

- Exercise IOU transfer
- Endpoint: ``/v1/exercise``

Sandbox Persistence Disabled
----------------------------

- Sandbox Persistence enabled: No
- JSON API Query Store Index enabled: No

.. code-block:: text

    ================================================================================
    ---- Global Information --------------------------------------------------------
    > request count                                       2000 (OK=2000   KO=0     )
    > min response time                                      3 (OK=3      KO=-     )
    > max response time                                    152 (OK=152    KO=-     )
    > mean response time                                     5 (OK=5      KO=-     )
    > std deviation                                          4 (OK=4      KO=-     )
    > response time 50th percentile                          4 (OK=4      KO=-     )
    > response time 75th percentile                          5 (OK=5      KO=-     )
    > response time 95th percentile                          6 (OK=6      KO=-     )
    > response time 99th percentile                          9 (OK=9      KO=-     )
    > mean requests/sec                                 71.429 (OK=71.429 KO=-     )
    ---- Response Time Distribution ------------------------------------------------
    > t < 800 ms                                          2000 (100%)
    > 800 ms < t < 1200 ms                                   0 (  0%)
    > t > 1200 ms                                            0 (  0%)
    > failed                                                 0 (  0%)
    ================================================================================

Sandbox Persistence Enabled
---------------------------

- Sandbox Persistence enabled: Yes
- JSON API Query Store Index enabled: No

.. code-block:: text

    ================================================================================
    ---- Global Information --------------------------------------------------------
    > request count                                       2000 (OK=2000   KO=0     )
    > min response time                                      9 (OK=9      KO=-     )
    > max response time                                    196 (OK=196    KO=-     )
    > mean response time                                    18 (OK=18     KO=-     )
    > std deviation                                         10 (OK=10     KO=-     )
    > response time 50th percentile                         13 (OK=13     KO=-     )
    > response time 75th percentile                         21 (OK=21     KO=-     )
    > response time 95th percentile                         37 (OK=37     KO=-     )
    > response time 99th percentile                         39 (OK=39     KO=-     )
    > mean requests/sec                                 25.974 (OK=25.974 KO=-     )
    ---- Response Time Distribution ------------------------------------------------
    > t < 800 ms                                          2000 (100%)
    > 800 ms < t < 1200 ms                                   0 (  0%)
    > t > 1200 ms                                            0 (  0%)
    > failed                                                 0 (  0%)
    ================================================================================

Create and Exercise Command
===========================

- Create IOU contract and Exercise transfer in the same transaction
- Endpoint: ``/v1/create-and-exercise``

Sandbox Persistence Disabled
----------------------------

- Sandbox Persistence enabled: No
- JSON API Query Store Index enabled: No

.. code-block:: text

    ================================================================================
    ---- Global Information --------------------------------------------------------
    > request count                                       1000 (OK=1000   KO=0     )
    > min response time                                      3 (OK=3      KO=-     )
    > max response time                                     28 (OK=28     KO=-     )
    > mean response time                                     4 (OK=4      KO=-     )
    > std deviation                                          1 (OK=1      KO=-     )
    > response time 50th percentile                          4 (OK=4      KO=-     )
    > response time 75th percentile                          4 (OK=4      KO=-     )
    > response time 95th percentile                          5 (OK=5      KO=-     )
    > response time 99th percentile                          5 (OK=5      KO=-     )
    > mean requests/sec                                    200 (OK=200    KO=-     )
    ---- Response Time Distribution ------------------------------------------------
    > t < 800 ms                                          1000 (100%)
    > 800 ms < t < 1200 ms                                   0 (  0%)
    > t > 1200 ms                                            0 (  0%)
    > failed                                                 0 (  0%)
    ================================================================================


Sandbox Persistence Enabled
---------------------------

- Sandbox Persistence enabled: Yes
- JSON API Query Store Index enabled: No

.. code-block:: text

    ================================================================================
    ---- Global Information --------------------------------------------------------
    > request count                                       1000 (OK=1000   KO=0     )
    > min response time                                      9 (OK=9      KO=-     )
    > max response time                                    181 (OK=181    KO=-     )
    > mean response time                                    16 (OK=16     KO=-     )
    > std deviation                                          8 (OK=8      KO=-     )
    > response time 50th percentile                         13 (OK=13     KO=-     )
    > response time 75th percentile                         16 (OK=16     KO=-     )
    > response time 95th percentile                         32 (OK=32     KO=-     )
    > response time 99th percentile                         34 (OK=34     KO=-     )
    > mean requests/sec                                 58.824 (OK=58.824 KO=-     )
    ---- Response Time Distribution ------------------------------------------------
    > t < 800 ms                                          1000 (100%)
    > 800 ms < t < 1200 ms                                   0 (  0%)
    > t > 1200 ms                                            0 (  0%)
    > failed                                                 0 (  0%)
    ================================================================================

