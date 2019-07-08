.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

============================
 The DA Platform Time Model
============================

*Maintainers*: Ognjen Maric

*Last Updated*: 2018-01-24

*Status*: Draft.

Time plays a dual role in the DA platform:

  1. the DAML-encoded business processes may rely on time. For
     instance, settlement occurs two days after a trade, options have
     expiry dates etc.

  2. the platform provides no guarantees that a submitted transaction
     will be processed; in particular, transactions can be lost. Thus,
     the clients must occasionally resubmit transactions. This can
     lead to inadvertent duplication of transactions. To prevent this,
     the clients require *finality*: the existence of a time bound
     after which a submitted transaction is either confirmed to be
     processed or it is known that it will not be processed at
     all..

This document provides a higher-level rationale for the current design
of the time model in the platform, taking both of the above roles into
the account. We will start from an unrealistic ideal model, and then
gradually introduce real-world concerns. To keep the descriptions
simple, in the sections before the last one we allow ourselves some
artistic freedom in describing the current platform, conflating
clients and participants, ignoring the delays on the read side, and
ignoring the split of the committer into the commit coordinator and
the sequencer. We repent in the last section.

The Ideal Model
===============

In this model:

  1. there exists a global notion of time and the clocks of all
     components are perfectly synchronized to the global time, and

  2. there are no computation or processing delays.

In this ideal case, it appears natural for the clients to submit their
transactions timestamped with the current time ``t[glob]``. To align
with the current (Apollo) platform, we call this timestamp the *ledger
effective time*, ``LET``. To rule out requests from malicious clients
that might attempt "time-travel", the committer checks (in addition to
validity checks of the transaction against the current ledger) that

.. _ideal check:

::

   LET = t[glob]


The transaction results (acceptance/rejection) are immediately
published on the event stream, and the clients achieve finality.


Adding Clock Skews
==================

Let us now consider clock skews (ignoring delays). The local clock of
each system component can now deviate from the true global time. We
assume (still unrealistically!) an upper limit ``Δt'`` on this skew;
two local clocks are then off by at most ``Δt = 2·Δt'``. The system
operation stays the same as above, with one exception; to account for
skews, it would be sensible for the committer to relax the `ideal check`_
to:

.. _`clock skew check`:

::

   LET ∈ [t[cmtr] - Δt, t[cmtr] + Δt]

where ``t[cmtr]`` denotes the committer's local time.
However, this relaxation is problematic, as the order given by the
committer-produced sequence of transactions and the order induced by
timestamps need not match anymore. This is particularly problematic if
we have two transactions with a DAML-induced causal ordering (``tx2``
comes after ``tx1`` if ``tx2`` consumes outputs of ``tx1``). This
DAML-causal ordering will necessarily be consistent with the committer
ordering, but not with the timestamp one. [#causaltsexample]_ The
committer must thus check the consistency of DAML-causal with
timestamp ordering explicitly. The DA platform now performs this
check. The committer's active contract store stores a LET timestamp
for each created contract. At every exercise node, the referenced
contract is checked to ensure LET monotonicity.

..
   .. todo:: do we want to provide a more well-defined link to the ideal
      implementation? I.e., how does a run of this new system map into
      the ideal system? This appears to be a non-local criterion
      (i.e. not a forward/backward simulation) similar to consistency
      definitions in the literature (e.g. linearizability, sequential
      consistency). The setup is a bit peculiar, since for
      e.g. linearizability we require the existence of some linearization
      compatible with the observed events; in Apollo, a linearization is
      already given by the committer.


Are there other alternatives that avoid this issue? There are at least
two ways to make the timestamp and committer orders coincide:

  1. have the committer choose the ``LET``. However, a LET different
     from the submitted one could invalidate a DAML transaction.  A
     potential solution is to have the parties submit DAML expressions
     instead of their evaluations ("eval traces"), together with a
     time window in which the evaluation is acceptable for the party.
     However, this is unlikely to work well with the current committer
     design, where the commit coordinator evaluates the expression
     first, and the sequencer then updates the ACS, as the evaluation
     could also change between the time of evaluation and time of
     sequencer.

  2. introduce delayed processing in the committer. The transaction's
     ``LET`` would be checked at arrival using the `clock skew check`_, but would
     not be processed immediately. The committer would remove the
     transaction tx from the head of a LET-prioritized queue as soon
     as :math:`t[cmtr] > LET[tx] + {\Delta}t`. This would increase the
     processing latency. In this model with no delays the throughput
     is unaffected. In the real system, the throughput might be
     affected if the distribution of transactions' LETs is not
     uniform.

Processing and Communication Delays
===================================

Let us now add processing and communication delays on the path from
the client to the committer. [#delaydirection]_ We will again be
unrealistic and assume an upper bound ``δ`` on these delays. To
simplify, we will also ignore clock skews. In this case, a transaction
sent at time ``t`` could reach the committer at any point in time
between ``t`` and ``t + δ``. So the committer relaxes the `ideal check`_
to:

.. _`delays check`:

::

   LET ∈ [t[glob] - δ, t[glob]]


Note that this relaxation can cause a disagreement between the
timestamp and DAML-causal orderings just like in the case of only
clock skews.


Double Trouble: Delays and Skews
================================

First, let us consider a system with both clock skews ``Δt`` and
delays ``δ``. The committer check here must be the most permissive
combination of the `clock skew check`_ and `delays check`_, which is:

.. _`combined check`:

::

   LET ∈ [t[cmtr] - Δt - δ, t[cmtr] + Δt]


Finally, let us consider a realistic model, where the bounds ``Δt``
and ``δ`` are respected most of the time, but not
always. [#partialsync]_ In particular, ``δ`` can be infinite, modeling
a lost message.

Since the bounds are respected most of the time, the `combined check`_ allows
the acceptance of (contention-free) honest clients' transactions most
of the time. However, when the bounds are violated, even messages from
honest clients can be rejected. Furthermore, in the model where bounds
were always respected, the system satisfied a *wait-free bounded*
progress condition: every client is certain to make progress within a
known, bounded amount of time (since the committer's answer would
arrive within ``δ``). With possibly infinite delays, a client can be
delayed for an infinite amount of time, and the system is no longer
wait-free bounded. In fact, it is not even *wait-free*, which is a
weaker condition requiring the client to progress within some finite,
but not necessarily bounded time. [#lockfree]_

To recover this progress property, we can introduce timeouts. The
client specifies a time-to-live (TTL) for any submitted transaction;
the committer must reject transactions that arrive after ``LET + TTL`` has
expired. The API is effectively changed; instead of an
"accepted/rejected" response for a transaction, we introduce a
"timed-out" response. [#apitimeout]_

There is, however, another obstacle: the client must detect that the
``LET + TTL`` has expired *at the committer*. If the ``Δt`` bound
were always respected, and if there was no delay on the committer ->
client path, the check would simply be ``t[client] > LET + TTL + Δt``.  But
``Δt`` is *not* always respected. So we apply a different, two-piece
solution:

1. we must ensure that the order of messages on the committer ->
   client path coincides with committer's ordering. That is, the
   committer must always send the messages in increasing order of
   their processing time at the committer, and the committer -> client
   channel must be FIFO.

2. the committer must occasionally announce the timestamps of messages
   on the committer -> client channel.

Then, once the client observes a message from the committer whose
timestamp exceeds a transaction's ``LET + TTL``, and no earlier notification
about the transaction outcome has been received, the transaction has
definitely timed out.

If the link from the committer to the client is cut off forever, this
scheme also fails to provide wait-freedom. However, under the
assumption that the communication is possible infinitely often (but
not necessarily constantly; e.g. all odd-numbered messages pass, and
all even-numbered fail), we obtain wait-freedom.

Lastly, to ensure that transactions do not time out when the bounds
are respected, the client should choose a TTL such that

.. _`ttl check`:

::

   TTL ≈ δ + Δt


Implementation in Apollo
========================

Finally, we describe more precisely how the previous description
relates to the current platform. The committer is divided into a
commit coordinator and a sequencer. The client actually consists of
the actual client application and the participant node. Then:

1. the delay ``δ`` encompasses the delays between the Ledger API and
   the commit coordinator (including the command interpretation and
   communication), as well as the pre-commit validation at the commit
   coordinator.

2. the transactions do not specify a TTL; instead, they specify an
   "end-of-life" time called MRT as

   ``MRT = LET + TTL``.

   MRT stands for "maximum record time". This name is confusing, as we
   discuss shortly.

3. the commit coordinator enforces something similar to the `ttl check`_
   (``CommitCoordinatorImpl.java/verifyMaxRecordTime``). More precisely,
   the check is:

   .. _mrt:

   ::

      MRT - LET ∈ [minTTL, maxTTL]


   for some constants ``minTTL`` and ``maxTTL``. We have:

   ``TTL = MRT - LET`` implying ``TTL ≥ minTTL``

   If ``minTTL`` is close to ``δ + Δt``, this gives us the `ttl check`_.
   This check can help misconfigured clients.

4. the sequencer's mempool acceptance rule
   (``TimeConstraintValidator.java/memPoolAcceptanceCheck``) ensures
   the `combined check`_. More precisely, it checks whether:

   ``t[seq] ∈ [LET - Δt, MRT]``

   This immediately gives us the second half of the `combined check`_:

   ``LET ≤ t[seq] + Δt``

   Moreover, since ``MRT ≤ LET + maxTTL``, we also have

   ``LET ≥ t[seq] - maxTTL``

   Assuming that ``maxTTL`` is chosen to model ``δ + Δt``, this ensures
   the `combined check`_.

5. the block record time provides the "occasional timestamp" that we
   required. However, note that a transaction with a given ``MRT`` can
   be published in a block with a record time that is higher than the
   ``MRT``! It is only that *after* a block with a record time ``t`` has
   been observed that all the transactions with ``MRT ≤ t`` that have
   not been observed yet can be safely regarded as lost. The confusion
   is due to the fact that "record time" is used for both blocks and
   individual transactions; we thus recommend that the name "MRT" be
   changed.

6. finally, the correspondence of timestamp and DAML-based orderings
   is ensured by
   ``MemoryProposedChange.java/verifyPredicatesAreNotBeforeCreation``.

   

.. [#progress] Another perspective is to look at the platform as
   providing a distributed (and thus concurrent) version of a virtual
   shared global ledger. With any shared data structure, the *progress
   conditions* for operations are important. We'll look at them later.


.. [#causaltsexample] For a simple example of why the causal <->
   timestamp order disagreement is problematic, consider two DAML
   contracts ``c1`` and ``c2`` such that a party ``P``:

   - has a choice on ``c1``, *after* time ``t``, which yields a
     contract ``c3``

   - has a choice on ``c2`` that can only be performed *before* time
     ``t``, that requires a contract of a form that ``c3`` matches.

   There is then nothing preventing ``P`` from exercising its choice
   on ``c1`` with a ``LET`` of ``t + Δt/2``, and then feeding the
   resulting ``c3`` into the choice of ``c2``, with a ``LET`` of ``t -
   Δt/2`` (recall that we are assuming no delays in this model).

.. [#delaydirection] The delays on in the other direction ("read
   path") do not seem to have an interesting effect, at least not
   with the current solution to the time problem. We thus ignore them.

.. [#partialsync] This is a form of the standard *partial synchrony*
   assumption in the distributed systems literature.

.. [#lockfree] In fact's fact, it is not even *lock-free*, which is a
   weaker condition still (at least one client makes progress). Which
   is to say that clients can get blocked forever.

.. [#apitimeout] The current Ledger API does not provide such a
   response, so it is only a logical construction. It is worth
   considering whether we want to expose it.

