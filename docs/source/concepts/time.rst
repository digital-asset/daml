.. Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _time:

Time on Daml Ledgers
####################

The Daml language contains a function :ref:`getTime <daml-ref-gettime>` which returns the “current time”.
However, the notion of time comes with a lot of problems in a distributed setting.

This document describes the detailed semantics of time on Daml ledgers,
centered around the two timestamps assigned to each transaction:
the *ledger time* ``lt_TX`` and the *record time* ``rt_TX``.


.. _ledger_time:

Ledger Time
***********

The *ledger time* ``lt_TX`` is a property of a transaction.
It is a timestamp that defines the value of all :ref:`getTime <daml-ref-gettime>` calls in the given transaction,
and has microsecond resolution.
The ledger time is assigned by the submitting participant as part of the Daml command interpretation.


.. _record-time:

Record Time
***********

The *record time* ``rt_TX`` is another property of a transaction.
It is timestamp with microsecond resolution,
and is assigned by the ledger when the transaction is recorded on the ledger.

The record time should be an intuitive representation of "real time",
but the Daml ledger model does not prescribe how exactly the record time is assigned.
Each ledger implementation might use a different way of representing time in a distributed setting -
for details, contact your ledger operator.


.. _time_guarantees:

Guarantees
**********

The ledger time of a valid transaction ``TX`` must fulfill the following rules:

#. **Causal monotonicity**: for any action (create, exercise, fetch, lookup) in ``TX``
   on a contract ``C``, ``lt_TX >= lt_C``,
   where ``lt_C`` is the ledger time of the transaction that created ``C``.

#. **Bounded skew**: ``rt_TX - skew_min <= lt_TX <= rt_TX + skew_max``,
   where ``skew_min`` and ``skew_max`` are parameters defined by the ledger.

Apart from that, no other guarantees are given on the ledger time.
In particular, neither the ledger time nor the record time need to be monotonically increasing.

Time has therefore to be considered slightly fuzzy in Daml, with the fuzziness depending on the skew parameters.
Daml applications should not interpret the value returned by :ref:`getTime <daml-ref-gettime>` as a precise timestamp.


.. _ledger-time-model:

Ledger Time Model
*****************

The *ledger time model* is the set of parameters used in the assignment and validation of ledger time.
It consists of the following:

#. ``skew_min`` and ``skew_max``, the bounds on the difference between ``lt_TX`` and ``rt_TX``.

#. ``transaction_latency``, the average duration from the time a transaction is submitted from a participant to the ledger
   until the transaction is recorded.
   This value is used by the participant to account for latency when submitting transactions to the ledger:
   transactions are submitted slightly ahead of their ledger time, with the intention that they arrive at ``lt_TX == rt_TX``.

The ledger time model is part of the ledger configuration and can be changed by ledger operators through the
:ref:`SetTimeModel <com.daml.ledger.api.v1.admin.SetTimeModelRequest>` config management API.


.. _assigning-ledger-time:

Assign Ledger Time
******************

The ledger time is assigned automatically by the participant.
In most cases, Daml applications will not need to worry about ledger time and record time at all.

For reference, this section describes the details of how the ledger time is currently assigned.
The algorithm is not part of the definition of time in Daml, and may change in the future.

#. When submitting commands over the ledger API,
   users can optionally specify a ``min_ledger_time_rel`` or ``min_ledger_time_abs`` argument.
   This defines a lower bound for the ledger time in relative and absolute terms, respectively.

#. The ledger time is set to the highest of the following values:

   #. ``max(lt_C_1, ..., lt_C_n)``, the maximum ledger time of all contracts used by the given transaction
   #. ``t_p``, the local time on the participant
   #. ``t_p + min_ledger_time_rel``, if ``min_ledger_time_rel`` is given
   #. ``min_ledger_time_abs``, if ``min_ledger_time_abs`` is given

#. Since the set of commands used by a given transaction can depend on the chosen time,
   the above process might need to be repeated until a suitable ledger time is found.

#. If no suitable ledger time is found after 3 iterations, the submission is rejected.
   This can happen if there is contention around a contract,
   or if the transaction uses a very fine-grained control flow based on time.

#. At this point, the ledger time may lie in the future (e.g., if a large value for ``min_ledger_time_rel`` was given).
   The participant waits until ``lt_TX - transaction_latency`` before it submits the transaction to the ledger - 
   the intention is that the transaction is recorded at ``lt_TX == rt_TX``.

Use the parameters ``min_ledger_time_rel`` and ``min_ledger_time_abs`` if you expect that
command interpretation will take a considerate amount of time, such that by
the time the resulting transaction is submitted to the ledger, its assigned ledger time is not valid anymore.
Note that these parameters can only make sure that the transaction arrives roughly at ``rt_TX`` at the ledger.
If a subsequent validation on the ledger takes longer than ``skew_max``,
the transaction will still be rejected and you'll have to ask your ledger operator to increase the ``skew_max`` time model parameter.
