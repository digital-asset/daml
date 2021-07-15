// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.api.health.ReportsHealth
import com.daml.ledger.configuration.LedgerInitialConditions
import com.daml.ledger.offset.Offset

/** An interface for reading the state of a ledger participant.
  *
  * The state of a ledger participant is communicated as a stream of state
  * [[Update]]s. That stream is accessible via [[ReadService!.stateUpdates]].
  * Commonly that stream is processed by a single consumer that keeps track of
  * the current state and creates indexes to satisfy read requests against
  * that state.
  *
  * See [[com.daml.ledger.participant.state.v1]] for further architectural
  * information. See [[Update]] for a description of the state updates
  * communicated by [[ReadService!.stateUpdates]].
  */
trait ReadService extends ReportsHealth {

  /** Retrieve the static initial conditions of the ledger, containing
    * the ledger identifier and the initial the ledger record time.
    *
    * Returns a single element Source since the implementation may need to
    * first establish connectivity to the underlying ledger. The implementer
    * may assume that this method is called only once, or very rarely.
    * Source is being used instead of Future as this is in line with [[stateUpdates]],
    * and is easy to implement from both Java and Scala.
    */
  def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed]

  /** Get the stream of state [[Update]]s starting from the beginning or right
    * after the given [[Offset]]
    *
    * This is where the meat of the implementation effort lies. Please take your time
    * to read carefully through the properties required from correct implementations.
    * These properties fall into two categories:
    *
    * 1. properties about the sequence of [[(Offset, Update)]] tuples
    *    in a stream read from the beginning, and
    * 2. properties relating the streams obtained from two separate calls
    *   to [[ReadService.stateUpdates]].
    *
    * The first class of properties are invariants of a single stream:
    *
    * - *strictly increasing [[Offset]]s*:
    *   for any two consecutive tuples `(o1, u1)` and `(o2, u2)`, `o1` is
    *   strictly smaller than `o2`.
    *
    * - *initialize before transaction acceptance*: before any
    *   [[Update.TransactionAccepted]], there is a [[Update.ConfigurationChanged]] update
    *   and [[Update.PublicPackageUpload]] updates for all packages referenced by
    *   the [[Update.TransactionAccepted]].
    *
    * - *causal monotonicity*: given a [[Update.TransactionAccepted]] with an associated
    *   ledger time `lt_tx`, it holds that `lt_tx >= lt_c` for all `c`, where `c` is a
    * contract used by the transaction and `lt_c` the ledger time of the
    * [[Update.TransactionAccepted]] that created the contract.
    * The ledger time of a transaction is specified in the corresponding [[TransactionMeta]]
    * meta-data.
    * Note that the ledger time of unrelated updates is not necessarily monotonically
    * increasing.
    *
    * - *time skew*: given a [[Update.TransactionAccepted]] with an associated
    * ledger time `lt_tx` and a record time `rt_tx`, it holds that
    * `rt_TX - minSkew <= lt_TX <= rt_TX + maxSkew`, where `minSkew` and `maxSkew`
    * are parameters specified in the ledger [[com.daml.ledger.configuration.LedgerTimeModel]].
    *
    * - *command deduplication*: if there is a [[Update.TransactionAccepted]] with
    * an associated [[SubmitterInfo]] `info1`, then for every later
    * transaction with [[SubmitterInfo]] `info2` that agrees with
    * `info1` on the `submitter` and `commandId` fields and
    * was submitted before `info1.deduplicateUntil`,
    * a transaction may be rejected without a corresponding update being issued.
    * I.e., transactions may be deduplicated on the `(submitter, commandId)` tuple,
    * but only until the time specified in [[SubmitterInfo.deduplicateUntil]].
    *
    *   TODO (SM): we would like to weaken this requirement to allow multiple
    *   [[Update.TransactionAccepted]] updates provided
    *   the transactions are sub-transactions of each other. Thereby enabling
    *   the after-the-fact communication of extra details about a transaction
    *   in case a party is newly hosted at a participant.
    *   See https://github.com/digital-asset/daml/issues/430
    *
    * - *rejection finality*: if there is a [[Update.CommandRejected]] update
    *   with [[SubmitterInfo]] `info`, then there is no later
    *   [[Update.TransactionAccepted]] with the same associated [[SubmitterInfo]]
    *   `info`. Note that in contrast to *command deduplication*
    *   this only holds wrt the full [[SubmitterInfo]], as a resubmission of a
    *   transaction with a higher `deduplicateUntil` must be allowed.
    *
    * The second class of properties relates multiple calls to
    * [[stateUpdates]]s, and thereby provides constraints on which [[Update]]s
    * need to be persisted. Before explaining them in detail we provide
    * intuition.
    *
    * All [[Update]]s other than [[Update.CommandRejected]] must
    * always be persisted by the backends implementing the [[ReadService]].
    * For rejections, the situation is more nuanced, as we want to provide
    * the backends with additional implementation leeway.
    *
    * [[Update.CommandRejected]] messages are advisory messages to submitters of
    * transactions to inform them in a timely fashion that their transaction
    * has been rejected.
    *
    * Given this intuition for the desired mechanism, we advise participant
    * state implementations to aim to always provide timely
    * [[Update.CommandRejected]] messages.
    *
    * Implementations are free to not persist [[Update.CommandRejected]] updates
    * provided their [[Offset]]s are not reused. This is relevant for the case
    * where a consumer rebuilds his view of the state by starting from a fresh
    * call to [[ReadService.stateUpdates]]; e.g., because it or the
    * stream provider crashed.
    *
    * Formally, we capture the expected relation between two calls
    * `s1 = stateUpdates(o1)` and `s2 = stateUpdates(o2)` for `o1 <= o2` as
    * follows.
    *
    * - *unique offsets*: for any update `u1` with offset `uo` in `s1` and any
    *   update `u2` with the same offset `uo` in `se2` it holds that `u1 == u2`.
    *   This means that offsets can never be reused. Together with
    *   *strictly increasing [[Offset]]* this also implies that the order of
    *   elements present in both `s1` and `s2` cannot change.
    *
    * - *persistent updates*: any update other than
    *   [[Update.CommandRejected]] in `s2` must also be present in `s1`.
    *
    * Last but not least, there is an expectation about the relation between streams visible
    * on *separate* participant state implementations connected to the same ledger.
    * The expectation is that two parties hosted on separate participant nodes are in sync
    * on transaction nodes and contracts that they can both see. The more formal definition
    * is based on the notion of projections of transactions
    * (see https://docs.daml.com/concepts/ledger-model/ledger-privacy.html), as follows.
    *
    * Assume that there is
    * - a party `A` hosted at participant `p1`,
    * - a party `B` hosted at participant `p2`, and
    * - an accepted transaction with identifier `tid` evidenced to both participants `p1` and `p2`
    *   in their state update streams after the [[Update.PartyAddedToParticipant]] updates for
    *   `A`, respectively `B`.
    * The projections of `tx1` and `tx2` to the nodes visible to both `A` and `B` is the same.
    *
    * Note that the transaction `tx1` associated to `tid` on `p1` is not required to be the same as
    * the transaction `tx2` associated to `tid` on `p2`, as these two participants do not necessarily
    * host the same parties; and some implementations ensure data segregation on the ledger. Requiring
    * only the projections to sets of parties to be equal leaves just enough leeway for this
    * data segregation.
    */
  def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed]
}
