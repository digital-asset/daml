// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.ContextualizedErrorLogger
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.ledger.participant.state.index.PackageDetails
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{DomainAlias, LfPartyId}
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** An interface for reading the state of a ledger participant.
  * '''Please note that this interface is unstable and may significantly change.'''
  *
  * The state of a ledger participant is communicated as a stream of state
  * [[Update]]s. That stream is accessible via [[ReadService!.stateUpdates]].
  * Commonly that stream is processed by a single consumer that keeps track of
  * the current state and creates indexes to satisfy read requests against
  * that state.
  *
  * See [[com.digitalasset.canton.ledger.participant.state]] for further architectural
  * information. See [[Update]] for a description of the state updates
  * communicated by [[ReadService!.stateUpdates]].
  */
trait ReadService extends ReportsHealth with InternalStateServiceProvider {

  /** Get the stream of state [[Update]]s starting from the beginning or right
    * after the given [[com.digitalasset.canton.data.Offset]]
    *
    * This is where the meat of the implementation effort lies. Please take your time
    * to read carefully through the properties required from correct implementations.
    * These properties fall into two categories:
    *
    * 1. properties about the sequence of ([[com.digitalasset.canton.data.Offset]],[[Update]]) tuples
    *    in a stream read from the beginning, and
    * 2. properties relating the streams obtained from separate calls
    *   to [[stateUpdates]].
    *
    * The first class of properties are invariants of a single stream:
    *
    * - *strictly increasing [[com.digitalasset.canton.data.Offset]]s*:
    *   for any two consecutive tuples `(o1, u1)` and `(o2, u2)`, `o1` is
    *   strictly smaller than `o2`.
    *
    * - *initialize before transaction acceptance*: before any
    *   [[Update.TransactionAccepted]], DARs have been uploaded for all
    *   packages referenced by the [[Update.TransactionAccepted]].
    *
    * - *causal monotonicity*: given a [[Update.TransactionAccepted]] with an associated
    *   ledger time `lt_tx`, it holds that `lt_tx >= lt_c` for all `c`, where `c` is a
    *   contract used by the transaction and `lt_c` the ledger time of the
    *   [[Update.TransactionAccepted]] that created the contract.
    * The ledger time of a transaction is specified in the corresponding [[TransactionMeta]]
    * meta-data.
    * Note that the ledger time of unrelated updates is not necessarily monotonically
    * increasing.
    * The creating transaction need not have a [[Update.TransactionAccepted]] event on this participant
    * if the participant does not host a stakeholder of the contract, e.g., in the case of divulgence.
    *
    * - *time skew*: given a [[Update.TransactionAccepted]] with an associated
    * ledger time `lt_tx` and a record time `rt_tx`, it holds that
    * `rt_TX - minSkew <= lt_TX <= rt_TX + maxSkew`, where `minSkew` and `maxSkew`
    * are parameters specified in the ledger [[com.digitalasset.canton.ledger.configuration.LedgerTimeModel]].
    *
    * - *command deduplication*: Let there be a [[Update.TransactionAccepted]] with [[CompletionInfo]]
    *   or a [[Update.CommandRejected]] with [[CompletionInfo]] at offset `off2`.
    *   If `off2`'s [[CompletionInfo.optDeduplicationPeriod]] is a [[com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationOffset]],
    *   let `off1` be the first offset after the deduplication offset.
    *   If the deduplication period is a [[com.digitalasset.canton.data.DeduplicationPeriod.DeduplicationDuration]],
    *   let `off1` be the first offset whose record time is at most the duration before `off2`'s record time (inclusive).
    *   Then there is no other [[Update.TransactionAccepted]] with [[CompletionInfo]] for the same [[CompletionInfo.changeId]]
    *   between the offsets `off1` and `off2` inclusive.
    *
    *   So if a command submission has resulted in a [[Update.TransactionAccepted]],
    *   other command submissions with the same [[SubmitterInfo.changeId]] must be deduplicated
    *   if the earlier's [[Update.TransactionAccepted]] falls within the latter's [[CompletionInfo.optDeduplicationPeriod]].
    *
    *   Implementations MAY extend the deduplication period from [[SubmitterInfo]] arbitrarily
    *   and reject a command submission as a duplicate even if its deduplication period does not include
    *   the earlier's [[Update.TransactionAccepted]].
    *   A [[Update.CommandRejected]] completion does not trigger deduplication and implementations SHOULD
    *   process such resubmissions normally.
    *
    * - *finality*: If the corresponding [[WriteService]] acknowledges a submitted transaction or rejection
    *   with [[SubmissionResult.Acknowledged]], the [[ReadService]] SHOULD make sure that
    *   it eventually produces a [[Update.TransactionAccepted]] or [[Update.CommandRejected]] with the corresponding [[CompletionInfo]],
    *   even if there are crashes or lost network messages.
    *
    * The second class of properties relates multiple calls to [[stateUpdates]] to each other.
    * The class contains two properties:
    * (1) a property that enables crash-fault tolerant Ledger API server implementations and
    * (2) a property that enables Ledger API server implementations that are synchronized by a backing ledger.
    *
    * For crash-fault-tolerance, we require an implementation of [[stateUpdates]] to support its consumer to
    * resume consumption starting after the last offset up to which the consumer completed processing.
    * Note that this offset can be before the offset of several of the latest delivered [[Update]]s in case the consumer
    * did not complete their processing before crashing.
    *
    * Formally, we require that the above invariants also hold for any sequence of offset-and-update pairs
    *
    *   `us = takeUntilOffset(us_1, o_2) + takeUntilOffset(us_2, o_3) + ... + takeUntilOffset(us_N-1, o_N) + us_N`
    *
    * where `us_i =` [[stateUpdates]](o_i) and `lastOffsetOf(us_i) >= o_i+1`. Here, `us_i` is the sequence
    * of offset-and-update pairs sourced from a call to [[stateUpdates]] and the side-condition formalizes
    * that later calls must start from an offset before or equal to the last offset delivered in the previous call.
    *
    * For synchronization, we require that two parties hosted on separate participant nodes are in sync
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
    *
    * Note further that the offsets of the transactions might not agree, as these offsets are participant-local.
    */
  def stateUpdates(
      beginAfter: Option[Offset]
  )(implicit traceContext: TraceContext): Source[(Offset, Traced[Update]), NotUsed]

  def getConnectedDomains(request: ReadService.ConnectedDomainRequest)(implicit
      traceContext: TraceContext
  ): Future[ReadService.ConnectedDomainResponse] =
    throw new UnsupportedOperationException()

  /** Get the offsets of the incomplete assigned/unassigned events for a set of stakeholders.
    *
    * @param validAt The offset of validity in participant offset terms.
    * @param stakeholders Only offsets are returned which have at least one stakeholder from this set.
    * @return All the offset of assigned/unassigned events which do not have their conterparts visible at
    *         the validAt offset, and only for the reassignments for which this participant is reassigning.
    */
  def incompleteReassignmentOffsets(
      validAt: Offset,
      stakeholders: Set[LfPartyId],
  )(implicit traceContext: TraceContext): Future[Vector[Offset]] = {
    val _ = validAt
    val _ = stakeholders
    val _ = traceContext
    Future.successful(Vector.empty)
  }

  def getPackageMetadataSnapshot(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): PackageMetadata =
    throw new UnsupportedOperationException()

  def listLfPackages()(implicit
      traceContext: TraceContext
  ): Future[Map[PackageId, PackageDetails]] =
    throw new UnsupportedOperationException()

  def getLfArchive(packageId: PackageId)(implicit
      traceContext: TraceContext
  ): Future[Option[Archive]] =
    throw new UnsupportedOperationException()

  def validateDar(
      dar: ByteString,
      darName: String,
  )(implicit
      traceContext: TraceContext
  ): Future[SubmissionResult] =
    throw new UnsupportedOperationException()
}

object ReadService {
  final case class ConnectedDomainRequest(party: LfPartyId)

  final case class ConnectedDomainResponse(
      connectedDomains: Seq[ConnectedDomainResponse.ConnectedDomain]
  )

  object ConnectedDomainResponse {
    final case class ConnectedDomain(
        domainAlias: DomainAlias,
        domainId: DomainId,
        permission: ParticipantPermission,
    )
  }
}
