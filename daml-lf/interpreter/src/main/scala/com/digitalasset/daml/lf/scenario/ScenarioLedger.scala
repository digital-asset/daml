// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package scenario

import com.daml.lf.data.Ref._
import com.daml.lf.data.Time
import com.daml.lf.ledger._
import com.daml.lf.transaction.Node._
import com.daml.lf.transaction.{
  BlindingInfo,
  CommittedTransaction,
  GlobalKey,
  NodeId,
  SubmittedTransaction,
  Transaction => Tx,
}
import com.daml.lf.value.Value
import Value._

import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.immutable

/** An in-memory representation of a ledger for scenarios */
object ScenarioLedger {

  case class TransactionId(index: Int) extends Ordered[TransactionId] {
    def next: TransactionId = TransactionId(index + 1)
    // The resulting LedgerString is at most 11 chars long
    val id: LedgerString = LedgerString.fromLong(index.toLong)
    override def compare(that: TransactionId): Int = index compare that.index
  }

  /** Errors */
  case class LedgerException(err: Error) extends RuntimeException(err.toString, null, true, false)

  sealed trait Error
  final case class ErrorLedgerCrash(reason: String) extends Error

  def crash(reason: String) =
    throwLedgerError(ErrorLedgerCrash(reason))

  def throwLedgerError(err: Error) =
    throw LedgerException(err)

  /** The node of the transaction graph. Only differs from the update
    * transaction node in the node identifier, where here the identifier
    * is an eventId.
    */
  type Node = GenNode[NodeId]

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
  sealed trait ScenarioStep

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
    * @param rolledbackBy   The nearest parent rollback node, provided such a
    *                       node exists.
    * @param parent         If the node is part of a sub-transaction, then
    *                       this is the immediate parent, which must be an
    *                       'NodeExercises' node.
    */
  final case class LedgerNodeInfo(
      node: Node,
      optLocation: Option[Location],
      transaction: TransactionId,
      effectiveAt: Time.Timestamp,
      disclosures: Map[Party, Disclosure],
      referencedBy: Set[EventId],
      consumedBy: Option[EventId],
      rolledbackBy: Option[EventId],
      parent: Option[EventId],
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
  sealed trait LookupResult

  final case class LookupOk(
      coid: ContractId,
      coinst: ContractInst[Value.VersionedValue],
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
      consumedBy: EventId,
  ) extends LookupResult
  final case class LookupContractNotVisible(
      coid: ContractId,
      templateId: Identifier,
      observers: Set[Party],
      stakeholders: Set[Party],
  ) extends LookupResult

  sealed trait CommitError
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
  sealed trait View

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

  //----------------------------------------------------------------------------
  // Enriching transactions with disclosure information
  //----------------------------------------------------------------------------

  def collectCoids(value: VersionedValue): Set[ContractId] =
    collectCoids(value.value)

  /** Collect all contract ids appearing in a value
    */
  def collectCoids(value: Value): Set[ContractId] = {
    val coids =
      implicitly[Factory[ContractId, Set[ContractId]]].newBuilder
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
          coids += coid
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
    def nodeInfoByCoid(coid: ContractId): LedgerNodeInfo = nodeInfos(coidToNodeId(coid))

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

    def markAsActive(coid: ContractId): LedgerData =
      copy(activeContracts = activeContracts + coid)

    def markAsInactive(coid: ContractId): LedgerData =
      copy(activeContracts = activeContracts - coid)

    def createdIn(coid: ContractId, nodeId: EventId): LedgerData =
      copy(coidToNodeId = coidToNodeId + (coid -> nodeId))

    def addKey(key: GlobalKey, acoid: ContractId): LedgerData =
      copy(activeKeys = activeKeys + (key -> acoid))

    def removeKey(key: GlobalKey): LedgerData =
      copy(activeKeys = activeKeys - key)

  }

  case class UniqueKeyViolation(gk: GlobalKey)

  private def processTransaction(
      trId: TransactionId,
      richTr: RichTransaction,
      locationInfo: Map[NodeId, Location],
      ledgerData: LedgerData,
  ): Either[UniqueKeyViolation, LedgerData] = {

    final case class RollbackBeginState(
        rollbackId: EventId,
        activeContracts: Set[ContractId],
        activeKeys: Map[GlobalKey, ContractId],
    )

    final case class ProcessingNode(
        mbParentId: Option[NodeId],
        children: List[NodeId],
        // For rollback nodes, we store the previous state here and restore it.
        // For exercise nodes, we don’t need to restore anything.
        prevState: Option[RollbackBeginState],
    )

    @tailrec
    def processNodes(
        mbCache0: Either[UniqueKeyViolation, LedgerData],
        enps: List[ProcessingNode],
    ): Either[UniqueKeyViolation, LedgerData] = {
      mbCache0 match {
        case Left(err) => Left(err)
        case Right(cache0) =>
          enps match {
            case Nil => Right(cache0)
            case ProcessingNode(_, Nil, optPrevState) :: restENPs => {
              val cache1 = optPrevState.fold(cache0) { case prevState =>
                cache0.copy(
                  activeContracts = prevState.activeContracts,
                  activeKeys = prevState.activeKeys,
                )
              }
              processNodes(Right(cache1), restENPs)
            }
            case (processingNode @ ProcessingNode(
                  mbParentId,
                  nodeId :: restOfNodeIds,
                  optPrevState,
                )) :: restENPs =>
              val eventId = EventId(trId.id, nodeId)
              richTr.transaction.nodes.get(nodeId) match {
                case None =>
                  crash(s"processTransaction: non-existent node '$eventId'.")
                case Some(node) =>
                  val newLedgerNodeInfo = LedgerNodeInfo(
                    node = node,
                    optLocation = locationInfo.get(nodeId),
                    transaction = trId,
                    effectiveAt = richTr.effectiveAt,
                    disclosures = Map.empty,
                    referencedBy = Set.empty,
                    consumedBy = None,
                    rolledbackBy = optPrevState.map(_.rollbackId),
                    parent = mbParentId.map(EventId(trId.id, _)),
                  )
                  val newCache =
                    cache0.copy(nodeInfos = cache0.nodeInfos + (eventId -> newLedgerNodeInfo))
                  val idsToProcess = processingNode.copy(children = restOfNodeIds) :: restENPs

                  node match {
                    case rollback: NodeRollback[NodeId] =>
                      val rollbackState =
                        RollbackBeginState(eventId, newCache.activeContracts, newCache.activeKeys)
                      processNodes(
                        Right(newCache),
                        ProcessingNode(
                          Some(nodeId),
                          rollback.children.toList,
                          Some(rollbackState),
                        ) :: idsToProcess,
                      )

                    case nc: NodeCreate =>
                      val newCache1 =
                        newCache
                          .markAsActive(nc.coid)
                          .createdIn(nc.coid, eventId)
                      val mbNewCache2 = nc.key match {
                        case None => Right(newCache1)
                        case Some(keyWithMaintainers) =>
                          val gk = GlobalKey.assertBuild(nc.templateId, keyWithMaintainers.key)
                          newCache1.activeKeys.get(gk) match {
                            case None => Right(newCache1.addKey(gk, nc.coid))
                            case Some(_) => Left(UniqueKeyViolation(gk))
                          }
                      }
                      processNodes(mbNewCache2, idsToProcess)

                    case NodeFetch(referencedCoid, templateId @ _, _, _, _, _, _, _) =>
                      val newCacheP =
                        newCache.updateLedgerNodeInfo(referencedCoid)(info =>
                          info.copy(referencedBy = info.referencedBy + eventId)
                        )

                      processNodes(Right(newCacheP), idsToProcess)

                    case ex: NodeExercises[NodeId] =>
                      val newCache0 =
                        newCache.updateLedgerNodeInfo(ex.targetCoid)(info =>
                          info.copy(
                            referencedBy = info.referencedBy + eventId,
                            consumedBy = optPrevState match {
                              // consuming exercise outside a rollback node
                              case None if ex.consuming => Some(eventId)
                              case _ => info.consumedBy
                            },
                          )
                        )
                      val newCache1 =
                        if (ex.consuming) {
                          val newCache0_1 = newCache0.markAsInactive(ex.targetCoid)
                          val nc = newCache0_1
                            .nodeInfoByCoid(ex.targetCoid)
                            .node
                            .asInstanceOf[NodeCreate]
                          nc.key match {
                            case None => newCache0_1
                            case Some(keyWithMaintainers) =>
                              newCache0_1.removeKey(
                                GlobalKey.assertBuild(ex.templateId, keyWithMaintainers.key)
                              )
                          }
                        } else newCache0

                      processNodes(
                        Right(newCache1),
                        ProcessingNode(Some(nodeId), ex.children.toList, None) :: idsToProcess,
                      )

                    case nlkup: NodeLookupByKey =>
                      nlkup.result match {
                        case None =>
                          processNodes(Right(newCache), idsToProcess)
                        case Some(referencedCoid) =>
                          val newCacheP =
                            newCache.updateLedgerNodeInfo(referencedCoid)(info =>
                              info.copy(referencedBy = info.referencedBy + eventId)
                            )

                          processNodes(Right(newCacheP), idsToProcess)
                      }
                  }
              }
          }

      }
    }

    val mbCacheAfterProcess =
      processNodes(
        Right(ledgerData),
        List(ProcessingNode(None, richTr.transaction.roots.toList, None)),
      )

    mbCacheAfterProcess.map { cacheAfterProcess =>
      // NOTE(MH): Since `addDisclosures` is biased towards existing
      // disclosures, we need to add the "stronger" explicit ones first.
      val cacheWithExplicitDisclosures =
        richTr.blindingInfo.disclosure.foldLeft(cacheAfterProcess) {
          case (cacheP, (nodeId, witnesses)) =>
            cacheP.updateLedgerNodeInfo(EventId(richTr.transactionId, nodeId))(
              _.addDisclosures(witnesses.map(_ -> Disclosure(since = trId, explicit = true)).toMap)
            )
        }
      richTr.blindingInfo.divulgence.foldLeft(cacheWithExplicitDisclosures) {
        case (cacheP, (coid, divulgees)) =>
          cacheP.updateLedgerNodeInfo(cacheAfterProcess.coidToNodeId(coid))(
            _.addDisclosures(divulgees.map(_ -> Disclosure(since = trId, explicit = false)).toMap)
          )
      }
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
case class ScenarioLedger(
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
          case create: NodeCreate =>
            if (info.effectiveAt.compareTo(effectiveAt) > 0)
              LookupContractNotEffective(coid, create.templateId, info.effectiveAt)
            else if (info.consumedBy.nonEmpty)
              LookupContractNotActive(
                coid,
                create.templateId,
                info.consumedBy.getOrElse(crash("IMPOSSIBLE")),
              )
            else if (!info.visibleIn(view))
              LookupContractNotVisible(
                coid,
                create.templateId,
                info.disclosures.keys.toSet,
                create.stakeholders,
              )
            else
              LookupOk(coid, create.versionedCoinst, create.stakeholders)

          case _: NodeExercises[_] | _: NodeFetch | _: NodeLookupByKey | _: NodeRollback[_] =>
            LookupContractNotFound(coid)
        }
    }
  }

  // Given a ledger and the node index of a node in a partial transaction
  // turn it into a event id that can be used in scenario error messages.
  def ptxEventId(nodeIdx: NodeId): EventId =
    EventId(scenarioStepId.id, nodeIdx)
}
