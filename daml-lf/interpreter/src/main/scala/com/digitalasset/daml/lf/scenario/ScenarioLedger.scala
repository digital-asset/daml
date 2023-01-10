// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import com.daml.lf.data.Ref._
import com.daml.lf.data.Time
import com.daml.lf.ledger._
import com.daml.lf.transaction.{
  BlindingInfo,
  CommittedTransaction,
  GlobalKey,
  Node,
  NodeId,
  SubmittedTransaction,
  Transaction => Tx,
}
import com.daml.lf.value.Value
import Value._

import com.daml.scalautil.Statement.discard

import scala.collection.immutable

/** An in-memory representation of a ledger for scenarios */
object ScenarioLedger {

  final case class TransactionId(index: Int) extends Ordered[TransactionId] {
    def next: TransactionId = TransactionId(index + 1)
    // The resulting LedgerString is at most 11 chars long
    val id: LedgerString = LedgerString.fromLong(index.toLong)
    override def compare(that: TransactionId): Int = index compare that.index
  }

  /** Errors */
  final case class LedgerException(err: Error)
      extends RuntimeException(err.toString, null, true, false)

  sealed abstract class Error extends Product with Serializable
  final case class ErrorLedgerCrash(reason: String) extends Error

  def crash(reason: String) =
    throwLedgerError(ErrorLedgerCrash(reason))

  def throwLedgerError(err: Error) =
    throw LedgerException(err)

  /** A transaction as it is committed to the ledger.
    *
    * NOTE (SM): This should correspond quite closely to a core
    * transaction. I'm purposely calling it differently to facilitate
    * the discussion when comparing this code to legacy-code for
    * building core transactions.
    *
    * @param committer   The committer
    * @param effectiveAt The time at which this transaction is effective.
    * @param roots       The root nodes of the resulting transaction.
    * @param nodes       All nodes that are part of this transaction.
    * @param disclosures Transaction nodes that must be disclosed to
    *                    individual parties to make this transaction
    *                    valid.
    *
    *                    NOTE (SM): I'm explicitly using the term
    *                    'disclosure' here, as it is more neutral than
    *                    divulgence. I think we can also adapt our
    *                    vocabulary such that we call the disclosures
    *                    happening due to post-commit validation
    *                    'implicit disclosures'.
    */
  final case class RichTransaction(
      actAs: Set[Party],
      readAs: Set[Party],
      effectiveAt: Time.Timestamp,
      transactionId: LedgerString,
      transaction: CommittedTransaction,
      blindingInfo: BlindingInfo,
  )

  object RichTransaction {

    /** Translate an EnrichedTransaction to a RichTransaction. EnrichedTransaction's contain local
      * node id's and contain additional information in the most detailed form suitable for different
      * consumers. The RichTransaction is the transaction that we serialize in the sandbox to compare
      * different ledgers. All relative and absolute node id's are translated to absolute node id's of
      * the package format.
      */
    private[lf] def apply(
        actAs: Set[Party],
        readAs: Set[Party],
        effectiveAt: Time.Timestamp,
        transactionId: LedgerString,
        submittedTransaction: SubmittedTransaction,
    ): RichTransaction = {
      val blindingInfo =
        BlindingTransaction.calculateBlindingInfo(submittedTransaction)
      new RichTransaction(
        actAs = actAs,
        readAs = readAs,
        effectiveAt = effectiveAt,
        transactionId = transactionId,
        transaction = Tx.commitTransaction(submittedTransaction),
        blindingInfo = blindingInfo,
      )
    }

  }

  /** Scenario step representing the actions executed in a scenario. */
  sealed abstract class ScenarioStep extends Product with Serializable

  final case class Commit(
      txId: TransactionId,
      richTransaction: RichTransaction,
      optLocation: Option[Location],
  ) extends ScenarioStep

  final case class PassTime(dtMicros: Long) extends ScenarioStep

  final case class AssertMustFail(
      actAs: Set[Party],
      readAs: Set[Party],
      optLocation: Option[Location],
      time: Time.Timestamp,
      txid: TransactionId,
  ) extends ScenarioStep

  final case class Disclosure(
      since: TransactionId,
      explicit: Boolean,
  )

  // ----------------------------------------------------------------
  // Node information
  // ----------------------------------------------------------------

  /** Node information that we cache to support the efficient
    * consumption of the data stored in the ledger.
    *
    * @param node           The node itself. Repeated here to avoid having to
    *                       look it up
    * @param transaction    The transaction that inserted this node.
    * @param effectiveAt    The time at which this node is effective.
    *
    *                       NOTE (SM): we denormalize this for speed, as
    *                       otherwise we'd have to lookup that
    *                       information on the transaction every time we
    *                       need to check for whether a contract is
    *                       active.
    * @param observingSince A mapping from parties that can see this
    *                       node to the transaction in which the node
    *                       became first visible.
    * @param referencedBy   All nodes referencing this node, which are
    *                       either 'NodeExercises' or 'NodeEnsureActive'
    *                       nodes.
    * @param consumedBy     The node consuming this node, provided such a
    *                       node exists. Consumption under a rollback
    *                       is not included here even for contracts created
    *                       under a rollback node.
    */
  final case class LedgerNodeInfo(
      node: Node,
      optLocation: Option[Location],
      transaction: TransactionId,
      effectiveAt: Time.Timestamp,
      disclosures: Map[Party, Disclosure],
      referencedBy: Set[EventId],
      consumedBy: Option[EventId],
  ) {

    /** 'True' if the given 'View' contains the given 'Node'. */
    def visibleIn(view: View): Boolean = view match {
      case OperatorView => true
      case pview: ParticipantView => !pview.readers.intersect(disclosures.keySet).isEmpty
    }

    def addDisclosures(newDisclosures: Map[Party, Disclosure]): LedgerNodeInfo = {
      // NOTE(MH): Earlier disclosures take precedence (`++` is right biased).
      copy(disclosures = newDisclosures ++ disclosures)
    }
  }

  type LedgerNodeInfos = Map[EventId, LedgerNodeInfo]

  /*
   * Result from lookupGlobalContract. We provide detailed information why a lookup
   * could fail in order to construct good error messages.
   */
  sealed abstract class LookupResult extends Product with Serializable

  final case class LookupOk(
      coid: ContractId,
      coinst: VersionedContractInstance,
      stakeholders: Set[Party],
  ) extends LookupResult
  final case class LookupContractNotFound(coid: ContractId) extends LookupResult

  final case class LookupContractNotEffective(
      coid: ContractId,
      templateId: Identifier,
      effectiveAt: Time.Timestamp,
  ) extends LookupResult
  final case class LookupContractNotActive(
      coid: ContractId,
      templateId: Identifier,
      consumedBy: Option[EventId],
  ) extends LookupResult
  final case class LookupContractNotVisible(
      coid: ContractId,
      templateId: Identifier,
      observers: Set[Party],
      stakeholders: Set[Party],
  ) extends LookupResult

  sealed abstract class CommitError extends Product with Serializable
  object CommitError {
    final case class UniqueKeyViolation(
        error: ScenarioLedger.UniqueKeyViolation
    ) extends CommitError
  }

  /** Updates the ledger to reflect that `committer` committed the
    * transaction `tr` resulting from running the
    * update-expression at time `effectiveAt`.
    */
  def commitTransaction(
      actAs: Set[Party],
      readAs: Set[Party],
      effectiveAt: Time.Timestamp,
      optLocation: Option[Location],
      tx: SubmittedTransaction,
      locationInfo: Map[NodeId, Location],
      l: ScenarioLedger,
  ): Either[CommitError, CommitResult] = {
    // transactionId is small enough (< 20 chars), so we do no exceed the 255
    // chars limit when concatenate in EventId#toLedgerString method.
    val transactionId = l.scenarioStepId.id
    val richTr = RichTransaction(actAs, readAs, effectiveAt, transactionId, tx)
    processTransaction(l.scenarioStepId, richTr, locationInfo, l.ledgerData) match {
      case Left(err) => Left(CommitError.UniqueKeyViolation(err))
      case Right(updatedCache) =>
        Right(
          CommitResult(
            l.copy(
              scenarioSteps = l.scenarioSteps + (l.scenarioStepId.index -> Commit(
                l.scenarioStepId,
                richTr,
                optLocation,
              )),
              scenarioStepId = l.scenarioStepId.next,
              ledgerData = updatedCache,
            ),
            l.scenarioStepId,
            richTr,
          )
        )
    }
  }

  /** The initial ledger */
  def initialLedger(t0: Time.Timestamp): ScenarioLedger =
    ScenarioLedger(
      currentTime = t0,
      scenarioStepId = TransactionId(0),
      scenarioSteps = immutable.IntMap.empty,
      ledgerData = LedgerData.empty,
    )

  /** Views onto the ledger */
  sealed abstract class View extends Product with Serializable

  /** The view of the ledger at the operator, i.e., the view containing
    * all transaction nodes.
    */
  case object OperatorView extends View

  /** The view of the ledger at the given party. */
  // Note that we only separate actAs and readAs to get better error
  // messages. The visibility check only needs the union.
  final case class ParticipantView(actAs: Set[Party], readAs: Set[Party]) extends View {
    val readers: Set[Party] = actAs union readAs
  }

  /** Result of committing a transaction is the new ledger,
    * and the enriched transaction.
    */
  final case class CommitResult(
      newLedger: ScenarioLedger,
      transactionId: TransactionId,
      richTransaction: RichTransaction,
  )

  // ----------------------------------------------------------------------------
  // Enriching transactions with disclosure information
  // ----------------------------------------------------------------------------

  def collectCoids(value: VersionedValue): Set[ContractId] =
    collectCoids(value.unversioned)

  /** Collect all contract ids appearing in a value
    */
  def collectCoids(value: Value): Set[ContractId] = {
    val coids = Set.newBuilder[ContractId]
    def collect(v: Value): Unit =
      v match {
        case ValueRecord(tycon @ _, fs) =>
          fs.foreach { case (_, v) =>
            collect(v)
          }
        case ValueVariant(_, _, arg) => collect(arg)
        case _: ValueEnum => ()
        case ValueList(vs) =>
          vs.foreach(collect)
        case ValueContractId(coid) =>
          discard(coids += coid)
        case _: ValueCidlessLeaf => ()
        case ValueOptional(mbV) => mbV.foreach(collect)
        case ValueTextMap(map) => map.values.foreach(collect)
        case ValueGenMap(entries) =>
          entries.foreach { case (k, v) =>
            collect(k)
            collect(v)
          }
      }

    collect(value)
    coids.result()
  }

  // ----------------------------------------------------------------
  // Cache for active contracts and nodes
  // ----------------------------------------------------------------

  object LedgerData {
    lazy val empty = LedgerData(Set.empty, Map.empty, Map.empty, Map.empty)
  }

  /** @param activeContracts The contracts that are active in the
    *                        current state of the ledger.
    * @param nodeInfos       Node information used to efficiently navigate
    *                        the transaction graph
    */
  final case class LedgerData(
      activeContracts: Set[ContractId],
      nodeInfos: LedgerNodeInfos,
      activeKeys: Map[GlobalKey, ContractId],
      coidToNodeId: Map[ContractId, EventId],
  ) {
    def updateLedgerNodeInfo(
        coid: ContractId
    )(f: (LedgerNodeInfo) => LedgerNodeInfo): LedgerData =
      coidToNodeId.get(coid).map(updateLedgerNodeInfo(_)(f)).getOrElse(this)

    def updateLedgerNodeInfo(
        nodeId: EventId
    )(f: (LedgerNodeInfo) => LedgerNodeInfo): LedgerData =
      copy(
        nodeInfos = nodeInfos
          .get(nodeId)
          .map(ni => nodeInfos.updated(nodeId, f(ni)))
          .getOrElse(nodeInfos)
      )

    def createdIn(coid: ContractId, nodeId: EventId): LedgerData =
      copy(coidToNodeId = coidToNodeId + (coid -> nodeId))

  }

  final case class UniqueKeyViolation(gk: GlobalKey)

  /** Functions for updating the ledger with new transactional information.
    *
    * @param trId transaction identity
    * @param richTr (enriched) transaction
    * @param locationInfo location map
    */
  class TransactionProcessor(
      trId: TransactionId,
      richTr: RichTransaction,
      locationInfo: Map[NodeId, Location],
  ) {

    def duplicateKeyCheck(ledgerData: LedgerData): Either[UniqueKeyViolation, Unit] = {
      val inactiveKeys = richTr.transaction.contractKeyInputs
        .fold(error => crash(s"$error: inconsistent transaction"), identity)
        .collect { case (key, _: Tx.KeyInactive) =>
          key
        }

      inactiveKeys.find(ledgerData.activeKeys.contains(_)) match {
        case Some(duplicateKey) =>
          Left(UniqueKeyViolation(duplicateKey))

        case None =>
          Right(())
      }
    }

    def addNewLedgerNodes(historicalLedgerData: LedgerData): LedgerData =
      richTr.transaction.transaction.fold[LedgerData](historicalLedgerData) {
        case (ledgerData, (nodeId, node)) =>
          val eventId = EventId(trId.id, nodeId)
          val newLedgerNodeInfo = LedgerNodeInfo(
            node = node,
            optLocation = locationInfo.get(nodeId),
            transaction = trId,
            effectiveAt = richTr.effectiveAt,
            // Following fields will be updated by additional calls to node processing code
            disclosures = Map.empty,
            referencedBy = Set.empty,
            consumedBy = None,
          )

          ledgerData.copy(nodeInfos = ledgerData.nodeInfos + (eventId -> newLedgerNodeInfo))
      }

    def createdInAndReferenceByUpdates(historicalLedgerData: LedgerData): LedgerData =
      richTr.transaction.transaction.fold[LedgerData](historicalLedgerData) {
        case (ledgerData, (nodeId, createNode: Node.Create)) =>
          ledgerData.createdIn(createNode.coid, EventId(trId.id, nodeId))

        case (ledgerData, (nodeId, exerciseNode: Node.Exercise)) =>
          ledgerData.updateLedgerNodeInfo(exerciseNode.targetCoid)(ledgerNodeInfo =>
            ledgerNodeInfo.copy(referencedBy =
              ledgerNodeInfo.referencedBy + EventId(trId.id, nodeId)
            )
          )

        case (ledgerData, (nodeId, fetchNode: Node.Fetch)) =>
          ledgerData.updateLedgerNodeInfo(fetchNode.coid)(ledgerNodeInfo =>
            ledgerNodeInfo.copy(referencedBy =
              ledgerNodeInfo.referencedBy + EventId(trId.id, nodeId)
            )
          )

        case (ledgerData, (nodeId, lookupNode: Node.LookupByKey)) =>
          lookupNode.result match {
            case None =>
              ledgerData

            case Some(referencedCoid) =>
              ledgerData.updateLedgerNodeInfo(referencedCoid)(ledgerNodeInfo =>
                ledgerNodeInfo.copy(referencedBy =
                  ledgerNodeInfo.referencedBy + EventId(trId.id, nodeId)
                )
              )
          }

        case (ledgerData, (_, _: Node)) =>
          ledgerData
      }

    def consumedByUpdates(ledgerData: LedgerData): LedgerData = {
      var ledgerDataResult = ledgerData

      for ((contractId, nodeId) <- richTr.transaction.transaction.consumedBy) {
        ledgerDataResult = ledgerDataResult.updateLedgerNodeInfo(contractId) { ledgerNodeInfo =>
          ledgerNodeInfo.copy(consumedBy = Some(EventId(trId.id, nodeId)))
        }
      }

      ledgerDataResult
    }

    def activeContractAndKeyUpdates(ledgerData: LedgerData): LedgerData = {
      ledgerData.copy(
        activeContracts =
          ledgerData.activeContracts ++ richTr.transaction.localContracts.keySet -- richTr.transaction.inactiveContracts,
        activeKeys = richTr.transaction.updatedContractKeys.foldLeft(ledgerData.activeKeys) {
          case (activeKeys, (key, Some(cid))) =>
            activeKeys + (key -> cid)

          case (activeKeys, (key, None)) =>
            activeKeys - key
        },
      )
    }

    def disclosureUpdates(ledgerData: LedgerData): LedgerData = {
      // NOTE(MH): Since `addDisclosures` is biased towards existing
      // disclosures, we need to add the "stronger" explicit ones first.
      richTr.blindingInfo.disclosure.foldLeft(ledgerData) { case (cacheP, (nodeId, witnesses)) =>
        cacheP.updateLedgerNodeInfo(EventId(richTr.transactionId, nodeId))(
          _.addDisclosures(witnesses.map(_ -> Disclosure(since = trId, explicit = true)).toMap)
        )
      }
    }

    def divulgenceUpdates(ledgerData: LedgerData): LedgerData = {
      richTr.blindingInfo.divulgence.foldLeft(ledgerData) { case (cacheP, (coid, divulgees)) =>
        cacheP.updateLedgerNodeInfo(ledgerData.coidToNodeId(coid))(
          _.addDisclosures(divulgees.map(_ -> Disclosure(since = trId, explicit = false)).toMap)
        )
      }
    }
  }

  /** Update the ledger (which records information on all historical transactions) with new transaction information.
    *
    * @param trId transaction identity
    * @param richTr (enriched) transaction
    * @param locationInfo location map
    * @param ledgerData ledger recording all historical transaction that have been processed
    * @return updated ledger with new transaction information
    */
  private[this] def processTransaction(
      trId: TransactionId,
      richTr: RichTransaction,
      locationInfo: Map[NodeId, Location],
      ledgerData: LedgerData,
  ): Either[UniqueKeyViolation, LedgerData] = {

    val processor: TransactionProcessor = new TransactionProcessor(trId, richTr, locationInfo)

    for {
      _ <- processor.duplicateKeyCheck(ledgerData)
    } yield {
      // Update ledger data with new transaction node information *before* performing any other updates
      var cachedLedgerData: LedgerData = processor.addNewLedgerNodes(ledgerData)

      // Update ledger data with any new created in and referenced by information
      cachedLedgerData = processor.createdInAndReferenceByUpdates(cachedLedgerData)
      // Update ledger data with any new consumed by information
      cachedLedgerData = processor.consumedByUpdates(cachedLedgerData)
      // Update ledger data with any new active contract information
      cachedLedgerData = processor.activeContractAndKeyUpdates(cachedLedgerData)
      // Update ledger data with any new disclosure information
      cachedLedgerData = processor.disclosureUpdates(cachedLedgerData)
      // Update ledger data with any new divulgence information
      cachedLedgerData = processor.divulgenceUpdates(cachedLedgerData)

      cachedLedgerData
    }
  }
}

// ----------------------------------------------------------------
// The ledger
// ----------------------------------------------------------------

/** @param currentTime        The current time of the ledger. We only use
  *                           that to implement the 'pass'
  *                           scenario-statement and to have a
  *                           ledger-effective-time for executing 'commit'
  *                           scenario statements.
  *
  *                           NOTE (SM): transactions can be added with any
  *                           ledger-effective time, as the code for
  *                           checking whether a contract instance is
  *                           active always nexplicitly checks that the
  *                           ledger-effective time ordering is maintained.
  * @param scenarioStepId The identitity for the next
  *                           transaction to be inserted. These
  *                           identities are allocated consecutively
  *                           from 1 to 'maxBound :: Int'.
  * @param scenarioSteps      Scenario steps that were executed.
  * @param ledgerData              Cache for the ledger.
  */
final case class ScenarioLedger(
    currentTime: Time.Timestamp,
    scenarioStepId: ScenarioLedger.TransactionId,
    scenarioSteps: immutable.IntMap[ScenarioLedger.ScenarioStep],
    ledgerData: ScenarioLedger.LedgerData,
) {

  import ScenarioLedger._

  /** moves the current time of the ledger by the relative time `dt`. */
  def passTime(dtMicros: Long): ScenarioLedger = copy(
    currentTime = currentTime.addMicros(dtMicros),
    scenarioSteps = scenarioSteps + (scenarioStepId.index -> PassTime(dtMicros)),
    scenarioStepId = scenarioStepId.next,
  )

  def insertAssertMustFail(
      actAs: Set[Party],
      readAs: Set[Party],
      optLocation: Option[Location],
  ): ScenarioLedger = {
    val id = scenarioStepId
    val effAt = currentTime
    val newIMS = scenarioSteps + (id.index -> AssertMustFail(actAs, readAs, optLocation, effAt, id))
    copy(
      scenarioSteps = newIMS,
      scenarioStepId = scenarioStepId.next,
    )

  }

  def query(
      view: View,
      effectiveAt: Time.Timestamp,
  ): Seq[LookupOk] = {
    ledgerData.activeContracts.toList
      .map(cid => lookupGlobalContract(view, effectiveAt, cid))
      .collect { case l @ LookupOk(_, _, _) =>
        l
      }
  }

  /** Focusing on a specific view of the ledger, lookup the
    * contract-instance associated to a specific contract-id.
    */
  def lookupGlobalContract(
      view: View,
      effectiveAt: Time.Timestamp,
      coid: ContractId,
  ): LookupResult = {
    ledgerData.coidToNodeId.get(coid).flatMap(ledgerData.nodeInfos.get) match {
      case None => LookupContractNotFound(coid)
      case Some(info) =>
        info.node match {
          case create: Node.Create =>
            if (info.effectiveAt.compareTo(effectiveAt) > 0)
              LookupContractNotEffective(coid, create.templateId, info.effectiveAt)
            else if (!ledgerData.activeContracts.contains(coid))
              LookupContractNotActive(
                coid,
                create.templateId,
                info.consumedBy,
              )
            else if (!info.visibleIn(view))
              LookupContractNotVisible(
                coid,
                create.templateId,
                info.disclosures.keys.toSet,
                create.stakeholders,
              )
            else
              LookupOk(
                coid,
                create.versionedCoinst,
                create.stakeholders,
              )

          case _: Node.Exercise | _: Node.Fetch | _: Node.LookupByKey | _: Node.Rollback =>
            LookupContractNotFound(coid)
        }
    }
  }

  // Given a ledger and the node index of a node in a partial transaction
  // turn it into a event id that can be used in scenario error messages.
  def ptxEventId(nodeIdx: NodeId): EventId =
    EventId(scenarioStepId.id, nodeIdx)
}
