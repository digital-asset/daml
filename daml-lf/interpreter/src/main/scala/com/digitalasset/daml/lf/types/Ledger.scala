// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package types

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.transaction.Transaction
import com.digitalasset.daml.lf.value.Value
import Value._
import com.digitalasset.daml.lf.data.Relation.Relation

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.collection.breakOut
import scala.collection.immutable

/** An in-memory representation of a ledger for scenarios */
object Ledger {

  import Ref.{ContractIdString => _, _}

  type ScenarioNodeId = LedgerString

  private object ScenarioNodeId {
    def apply(commitPrefix: LedgerString, txnid: Transaction.NodeId): ScenarioNodeId =
      txNodeIdToScenarioNodeId(commitPrefix, txnid.index)

  }

  /** This is the function that we use to turn relative contract ids (which are made of
    * transaction node ids) in the still
    * to be committed transaction into absolute contract ids in the ledger.
    */
  //  The prefix should be smaller than 244 chars.
  @inline
  private def txNodeIdToScenarioNodeId(
      commitPrefix: LedgerString,
      txnidx: Int,
  ): ScenarioNodeId =
    LedgerString.assertConcat(commitPrefix, LedgerString.fromInt(txnidx))

  @inline
  private def relativeToScenarioNodeId(
      commitPrefix: LedgerString,
      cid: RelativeContractId,
  ): ScenarioNodeId =
    txNodeIdToScenarioNodeId(commitPrefix, cid.txnid.index)

  @inline
  def relativeToContractIdString(
      commitPrefix: LedgerString,
      cid: RelativeContractId,
  ): LedgerString =
    LedgerString.assertConcat(commitPrefix, LedgerString.fromInt(cid.txnid.index))

  @inline
  private def contractIdToAbsoluteContractId(
      commitPrefix: LedgerString,
      cid: ContractId,
  ): AbsoluteContractId =
    cid match {
      case acoid: AbsoluteContractId => acoid
      case rcoid: RelativeContractId =>
        AbsoluteContractId(relativeToScenarioNodeId(commitPrefix, rcoid))
    }

  def contractIdToAbsoluteContractId(
      transactionId: ScenarioTransactionId,
      cid: ContractId,
  ): AbsoluteContractId = {
    // commitPrefix is small enough (< 20 chars), so we do no exceed the 255
    // chars limit when concatenate in txNodeIdToScenarioNodeId method.
    val commitPrefix = transactionId.makeCommitPrefix
    contractIdToAbsoluteContractId(commitPrefix, cid)
  }

  @inline
  def assertNoContractId(key: VersionedValue[Value.ContractId]): VersionedValue[Nothing] =
    key.ensureNoCid.fold(
      cid => crash(s"Not expecting to find a contract id here, but found '$cid'"),
      identity,
    )

  private val `:` = LedgerString.assertFromString(":")
  private val `#` = LedgerString.assertFromString("#")

  case class ScenarioTransactionId(index: Int) extends Ordered[ScenarioTransactionId] {
    def next: ScenarioTransactionId = ScenarioTransactionId(index + 1)
    // The resulting LedgerString is at most 11 chars long
    val id: LedgerString = LedgerString.fromLong(index.toLong)
    override def compare(that: ScenarioTransactionId): Int = index compare that.index
    // The resulting LedgerString is at most 13 chars long
    def makeCommitPrefix: LedgerString = LedgerString.assertConcat(`#`, id, `:`)
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
    * transaction node * in the node identifier, where here the identifier
    * is global.
    *
    * Note that here the contract ids refer to NodeIds. Or in other
    * words, all AbsoluteContractIds are also NodeIds (but not the
    * reverse, node ids might be exercises)
    */
  type Node = GenNode.WithTxValue[ScenarioNodeId, AbsoluteContractId]

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
      committer: Party,
      effectiveAt: Time.Timestamp,
      roots: ImmArray[ScenarioNodeId],
      nodes: immutable.HashMap[ScenarioNodeId, Node],
      explicitDisclosure: Relation[ScenarioNodeId, Party],
      localImplicitDisclosure: Relation[ScenarioNodeId, Party],
      globalImplicitDisclosure: Relation[AbsoluteContractId, Party],
      failedAuthorizations: FailedAuthorizations,
  ) {
    def disclosures = Relation.union(explicitDisclosure, localImplicitDisclosure)
  }

  final case class EnrichedTransaction(
      // The transaction root nodes.
      roots: ImmArray[Transaction.NodeId],
      // All nodes of this transaction.
      nodes: immutable.HashMap[Transaction.NodeId, Transaction.Node],
      // A relation between a node id and the parties to which this node gets explicitly disclosed.
      explicitDisclosure: Relation[Transaction.NodeId, Party],
      // A relation between a node id and the parties to which this node get implictly disclosed
      // (aka divulgence)
      localImplicitDisclosure: Relation[Transaction.NodeId, Party],
      // A relation between absolute contract id and the parties to which the contract id gets
      // explicitly disclosed.
      globalImplicitDisclosure: Relation[AbsoluteContractId, Party],
      // A map from node ids to authorizations that failed for them.
      failedAuthorizations: FailedAuthorizations,
  )

  /**
    * Translate an EnrichedTransaction to a RichTransaction. EnrichedTransaction's contain relative
    * node id's and contain additional information in the most detailed form suitable for different
    * consumers. The RichTransaction is the transaction that we serialize in the sandbox to compare
    * different ledgers. All relative and absolute node id's are translated to absolute node id's of
    * the package format.
    */
  private def enrichedTransactionToRichTransaction(
      commitPrefix: LedgerString,
      committer: Party,
      effectiveAt: Time.Timestamp,
      enrichedTx: EnrichedTransaction,
  ): RichTransaction = {
    def makeAbs(cid: Value.RelativeContractId) = relativeToContractIdString(commitPrefix, cid)
    RichTransaction(
      committer = committer,
      effectiveAt = effectiveAt,
      roots = enrichedTx.roots.map(ScenarioNodeId(commitPrefix, _)),
      nodes = enrichedTx.nodes.map {
        case (nodeId, node) =>
          ScenarioNodeId(commitPrefix, nodeId) -> node
            .resolveRelCid(makeAbs)
            .mapNodeId(ScenarioNodeId(commitPrefix, _))
      }(breakOut),
      explicitDisclosure = enrichedTx.explicitDisclosure.map {
        case (nodeId, ps) =>
          (ScenarioNodeId(commitPrefix, nodeId), ps)
      },
      localImplicitDisclosure = enrichedTx.localImplicitDisclosure.map {
        case (nodeId, ps) =>
          (ScenarioNodeId(commitPrefix, nodeId), ps)
      },
      globalImplicitDisclosure = enrichedTx.globalImplicitDisclosure,
      failedAuthorizations = enrichedTx.failedAuthorizations,
    )
  }

  /** Scenario step representing the actions executed in a scenario. */
  sealed trait ScenarioStep

  final case class Commit(
      txId: ScenarioTransactionId,
      richTransaction: RichTransaction,
      optLocation: Option[Location],
  ) extends ScenarioStep

  final case class PassTime(dtMicros: Long) extends ScenarioStep

  final case class AssertMustFail(
      actor: Party,
      optLocation: Option[Location],
      time: Time.Timestamp,
      txid: ScenarioTransactionId,
  ) extends ScenarioStep

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
    *                       node exists.
    * @param parent         If the node is part of a sub-transaction, then
    *                       this is the immediate parent, which must be an
    *                       'NodeExercises' node.
    */
  final case class LedgerNodeInfo(
      node: Node,
      transaction: ScenarioTransactionId,
      effectiveAt: Time.Timestamp,
      observingSince: Map[Party, ScenarioTransactionId],
      referencedBy: Set[ScenarioNodeId],
      consumedBy: Option[ScenarioNodeId],
      parent: Option[ScenarioNodeId],
  ) {

    /** 'True' if the given 'View' contains the given 'Node'. */
    def visibleIn(view: View): Boolean = view match {
      case OperatorView => true
      case ParticipantView(party) => observingSince contains party
    }

    def addObservers(witnesses: Map[Party, ScenarioTransactionId]): LedgerNodeInfo = {
      // NOTE(JM): We combine with bias towards entries in `observingSince`.
      copy(observingSince = witnesses ++ observingSince)
    }
  }

  type LedgerNodeInfos = Map[ScenarioNodeId, LedgerNodeInfo]

  /*
   * Result from lookupGlobalContract. We provide detailed information why a lookup
   * could fail in order to construct good error messages.
   */
  sealed trait LookupResult

  final case class LookupOk(
      coid: AbsoluteContractId,
      coinst: ContractInst[Transaction.Value[AbsoluteContractId]],
  ) extends LookupResult
  final case class LookupContractNotFound(coid: AbsoluteContractId) extends LookupResult

  final case class LookupContractNotEffective(
      coid: AbsoluteContractId,
      templateId: Identifier,
      effectiveAt: Time.Timestamp,
  ) extends LookupResult
  final case class LookupContractNotActive(
      coid: AbsoluteContractId,
      templateId: Identifier,
      consumedBy: ScenarioNodeId,
  ) extends LookupResult
  final case class LookupContractNotVisible(
      coid: AbsoluteContractId,
      templateId: Identifier,
      observers: Set[Party],
  ) extends LookupResult

  // ----------------------------------------------------------------
  // The ledger
  // ----------------------------------------------------------------

  /**
    * @param currentTime        The current time of the ledger. We only use
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
  case class Ledger(
      currentTime: Time.Timestamp,
      scenarioStepId: ScenarioTransactionId,
      scenarioSteps: immutable.IntMap[ScenarioStep],
      ledgerData: LedgerData,
  ) {

    /** moves the current time of the ledger by the relative time `dt`. */
    def passTime(dtMicros: Long): Ledger = copy(
      currentTime = currentTime.addMicros(dtMicros),
      scenarioSteps = scenarioSteps + (scenarioStepId.index -> PassTime(dtMicros)),
      scenarioStepId = scenarioStepId.next,
    )

    def insertAssertMustFail(p: Party, optLocation: Option[Location]): Ledger = {
      val id = scenarioStepId
      val effAt = currentTime
      val newIMS = scenarioSteps + (id.index -> AssertMustFail(p, optLocation, effAt, id))
      copy(
        scenarioSteps = newIMS,
        scenarioStepId = scenarioStepId.next,
      )
    }

    /** Focusing on a specific view of the ledger, lookup the
      * contract-instance associated to a specific contract-id.
      */
    def lookupGlobalContract(
        view: View,
        effectiveAt: Time.Timestamp,
        coid: AbsoluteContractId,
    ): LookupResult = {
      ledgerData.coidToNodeId.get(coid).flatMap(ledgerData.nodeInfos.get) match {
        case None => LookupContractNotFound(coid)
        case Some(info) =>
          info.node match {
            case create: NodeCreate.WithTxValue[AbsoluteContractId] =>
              if (info.effectiveAt.compareTo(effectiveAt) > 0)
                LookupContractNotEffective(coid, create.coinst.template, info.effectiveAt)
              else if (info.consumedBy.nonEmpty)
                LookupContractNotActive(
                  coid,
                  create.coinst.template,
                  info.consumedBy.getOrElse(crash("IMPOSSIBLE")),
                )
              else if (!info.visibleIn(view))
                LookupContractNotVisible(
                  coid,
                  create.coinst.template,
                  info.observingSince.keys.toSet,
                )
              else
                LookupOk(coid, create.coinst)

            case _: NodeExercises[_, _, _] | _: NodeFetch[_] | _: NodeLookupByKey[_, _] =>
              LookupContractNotFound(coid)
          }
      }
    }
  }

  sealed trait CommitError
  object CommitError {
    final case class FailedAuthorizations(
        errors: com.digitalasset.daml.lf.types.Ledger.FailedAuthorizations,
    ) extends CommitError
    final case class UniqueKeyViolation(
        error: com.digitalasset.daml.lf.types.Ledger.UniqueKeyViolation,
    ) extends CommitError
  }

  /** Updates the ledger to reflect that `committer` committed the
    * transaction `tr` resulting from running the
    * update-expression at time `effectiveAt`.
    */
  def commitTransaction(
      committer: Party,
      effectiveAt: Time.Timestamp,
      optLocation: Option[Location],
      tr: Transaction.Transaction,
      l: Ledger,
  ): Either[CommitError, CommitResult] = {
    val i = l.scenarioStepId
    // commitPrefix is small enough (< 20 chars), so we do no exceed the 255
    // chars limit when concatenate in txNodeIdToScenarioNodeId method.
    val commitPrefix = i.makeCommitPrefix
    val richTr =
      enrichedTransactionToRichTransaction(
        commitPrefix,
        committer,
        effectiveAt,
        enrichTransaction(Authorize(Set(committer)), tr),
      )
    if (richTr.failedAuthorizations.nonEmpty)
      Left(CommitError.FailedAuthorizations(richTr.failedAuthorizations))
    else {
      processTransaction(i, richTr, l.ledgerData) match {
        case Left(err) => Left(CommitError.UniqueKeyViolation(err))
        case Right(updatedCache) =>
          Right(
            CommitResult(
              l.copy(
                scenarioSteps = l.scenarioSteps + (i.index -> Commit(i, richTr, optLocation)),
                scenarioStepId = l.scenarioStepId.next,
                ledgerData = updatedCache,
              ),
              i,
              richTr,
            ),
          )
      }
    }
  }

  /** The initial ledger */
  def initialLedger(t0: Time.Timestamp): Ledger =
    Ledger(
      currentTime = t0,
      scenarioStepId = ScenarioTransactionId(0),
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
  final case class ParticipantView(party: Party) extends View

  /** Result of committing a transaction is the new ledger,
    * and the enriched transaction.
    */
  final case class CommitResult(
      newLedger: Ledger,
      transactionId: ScenarioTransactionId,
      richTransaction: RichTransaction,
  )

  sealed trait FailedAuthorization

  type FailedAuthorizations = Map[Transaction.NodeId, FailedAuthorization]

  final case class FACreateMissingAuthorization(
      templateId: Identifier,
      optLocation: Option[Location],
      authorizingParties: Set[Party],
      requiredParties: Set[Party],
  ) extends FailedAuthorization

  final case class FAMaintainersNotSubsetOfSignatories(
      templateId: Identifier,
      optLocation: Option[Location],
      signatories: Set[Party],
      maintainers: Set[Party],
  ) extends FailedAuthorization

  final case class FAFetchMissingAuthorization(
      templateId: Identifier,
      optLocation: Option[Location],
      stakeholders: Set[Party],
      authorizingParties: Set[Party],
  ) extends FailedAuthorization

  final case class FAExerciseMissingAuthorization(
      templateId: Identifier,
      choiceId: ChoiceName,
      optLocation: Option[Location],
      authorizingParties: Set[Party],
      requiredParties: Set[Party],
  ) extends FailedAuthorization

  final case class FAActorMismatch(
      templateId: Identifier,
      choiceId: ChoiceName,
      optLocation: Option[Location],
      controllers: Set[Party],
      givenActors: Set[Party],
  ) extends FailedAuthorization

  final case class FANoSignatories(
      templateId: Identifier,
      optLocation: Option[Location],
  ) extends FailedAuthorization

  final case class FANoControllers(
      templateId: Identifier,
      choiceid: ChoiceName,
      optLocation: Option[Location],
  ) extends FailedAuthorization

  final case class FALookupByKeyMissingAuthorization(
      templateId: Identifier,
      optLocation: Option[Location],
      maintainers: Set[Party],
      authorizingParties: Set[Party],
  ) extends FailedAuthorization

  /** State to use during enriching a transaction with disclosure information. */
  final case class EnrichState(
      nodes: Map[ScenarioNodeId, Node],
      disclosures: Relation[Transaction.NodeId, Party],
      localDivulgences: Relation[Transaction.NodeId, Party],
      globalDivulgences: Relation[AbsoluteContractId, Party],
      failedAuthorizations: Map[Transaction.NodeId, FailedAuthorization],
  ) {
    def discloseNode(
        parentWitnesses: Set[Party],
        nid: Transaction.NodeId,
        node: Transaction.Node,
    ): (Set[Party], EnrichState) = {
      val witnesses = parentWitnesses union node.informeesOfNode
      witnesses ->
        copy(
          disclosures = disclosures
            .updated(nid, witnesses union disclosures.getOrElse(nid, Set.empty)),
        )
    }

    def divulgeContracts(witnesses: Set[Party], coids: Set[ContractId]): EnrichState =
      coids.foldLeft(this) {
        case (s, coid) => s.divulgeCoidTo(witnesses, coid)
      }

    def divulgeCoidTo(witnesses: Set[Party], coid: ContractId): EnrichState = {
      def divulgeRelativeCoidTo(ws: Set[Party], rcoid: RelativeContractId): EnrichState = {
        val nid = rcoid.txnid
        copy(
          localDivulgences = localDivulgences
            .updated(nid, ws union localDivulgences.getOrElse(nid, Set.empty)),
        )
      }

      coid match {
        case rcoid: RelativeContractId =>
          divulgeRelativeCoidTo(witnesses, rcoid)
        case acoid: AbsoluteContractId =>
          divulgeAbsoluteCoidTo(witnesses, acoid)
      }
    }

    def divulgeAbsoluteCoidTo(witnesses: Set[Party], acoid: AbsoluteContractId): EnrichState = {
      copy(
        globalDivulgences = globalDivulgences
          .updated(acoid, witnesses union globalDivulgences.getOrElse(acoid, Set.empty)),
      )
    }

    def authorize(
        nodeId: Transaction.NodeId,
        passIf: Boolean,
        failWith: FailedAuthorization,
    ): EnrichState =
      if (passIf ||
        failedAuthorizations.contains(nodeId) /* already failed? keep the first one */
        )
        this
      else
        copy(failedAuthorizations = failedAuthorizations + (nodeId -> failWith))

    def authorizeCreate(
        nodeId: Transaction.NodeId,
        create: NodeCreate.WithTxValue[ContractId],
        signatories: Set[Party],
        authorization: Authorization,
        /** If the create has a key, these are the maintainers */
        mbMaintainers: Option[Set[Party]],
    ): EnrichState =
      authorization.fold(this)(authParties => {
        val auth = this
          .authorize(
            nodeId = nodeId,
            passIf = signatories subsetOf authParties,
            failWith = FACreateMissingAuthorization(
              templateId = create.coinst.template,
              optLocation = create.optLocation,
              authorizingParties = authParties,
              requiredParties = signatories,
            ),
          )
          .authorize(
            nodeId = nodeId,
            passIf = signatories.nonEmpty,
            failWith = FANoSignatories(create.coinst.template, create.optLocation),
          )
        mbMaintainers match {
          case None => auth
          case Some(maintainers) =>
            auth.authorize(
              nodeId = nodeId,
              passIf = maintainers subsetOf signatories,
              failWith = FAMaintainersNotSubsetOfSignatories(
                templateId = create.coinst.template,
                optLocation = create.optLocation,
                signatories = signatories,
                maintainers = maintainers,
              ),
            )
        }
      })

    def authorizeExercise(
        nodeId: Transaction.NodeId,
        ex: NodeExercises.WithTxValue[Transaction.NodeId, ContractId],
        actingParties: Set[Party],
        authorization: Authorization,
        controllers: Set[Party],
    ): EnrichState = {
      // well-authorized by A : actors == controllers(c)
      //                        && actors subsetOf A
      //                        && childrenActions well-authorized by
      //                           (signatories(c) union controllers(c))

      authorization.fold(this)(
        authParties =>
          this
            .authorize(
              nodeId = nodeId,
              passIf = controllers.nonEmpty,
              failWith = FANoControllers(ex.templateId, ex.choiceId, ex.optLocation),
            )
            .authorize(
              nodeId = nodeId,
              passIf = actingParties == controllers,
              failWith = FAActorMismatch(
                templateId = ex.templateId,
                choiceId = ex.choiceId,
                optLocation = ex.optLocation,
                controllers = controllers,
                givenActors = actingParties,
              ),
            )
            .authorize(
              nodeId = nodeId,
              passIf = actingParties subsetOf authParties,
              failWith = FAExerciseMissingAuthorization(
                templateId = ex.templateId,
                choiceId = ex.choiceId,
                optLocation = ex.optLocation,
                authorizingParties = authParties,
                requiredParties = actingParties,
              ),
          ))
    }

    def authorizeFetch(
        nodeId: Transaction.NodeId,
        fetch: NodeFetch[ContractId],
        stakeholders: Set[Party],
        authorization: Authorization,
    ): EnrichState = {
      authorization.fold(this)(
        authParties =>
          this.authorize(
            nodeId = nodeId,
            passIf = stakeholders.intersect(authParties).nonEmpty,
            failWith = FAFetchMissingAuthorization(
              templateId = fetch.templateId,
              optLocation = fetch.optLocation,
              stakeholders = stakeholders,
              authorizingParties = authParties,
            )
        ))
    }

    /*
      If we have `authorizers` and lookup node with maintainers
      `maintainers`, we have three options:

      1. Not authorize at all (always accept the lookup node);

         - Not good because it allows you to guess what keys exist, and thus
           leaks information about what contract ids are active to
           non-stakeholders.

      2. `authorizers ∩ maintainers ≠ ∅`, at least one.

         - This is a stricter condition compared to fetches, because with
           fetches we check that `authorizers ∩ stakeholders ≠ ∅`, and we
           know that `maintainers ⊆ stakeholders`, since `maintainers ⊆
           signatories ⊆ stakeholders`. In other words, you won't be able
           to look up a contract by key if you're an observer but not a
           signatory.

         - However, this is problematic since lookups will induce work for *all*
           maintainers even if only a subset of the maintainers have
           authorized it, violating the tenet that nobody can be forced to
           perform work.

           To make this a bit more concrete, consider the case where a
           negative lookup is the only thing that induces a validation
           request to a maintainer who would have received the transaction
           to validate otherwise.

      3. `authorizers ⊇ maintainers`, all of them.

         - This seems to be the only safe choice for lookups, *but* note
           that for fetches which fail if the key cannot be found we can use
           the same authorization rule we use for fetch, which is much more
           lenient. The way we achieve this is that we have two DAML-LF
           primitives, `fetchByKey` and `lookupByKey`, with the former
           emitting a normal fetch node, and the latter emitting a lookup
           node.

           The reason why we have a full-blown lookup node rather than a
           simple "key does not exist" node is so that the transaction
           structure is stable with what regards wrong results coming from
           the key oracle, which will happen when the user requests a key
           for a contract that is not available locally but is available
           elsewhere.

           From a more high level perspective, we want to make the
           authorization checks orthogonal to DAML-LF interpretation, which
           would not be the case if we added a "key does not exist" node as
           described above.

         - Observation by Andreas: don't we end up in the same situation if
           we have a malicious submitter node that omits the authorization
           check? For example, it could craft transactions which involve
           arbitrary parties which then will have to perform work in
           re-interpreting the transaction.

           Francesco: yes, but there is a key difference: the above scenario
           requires a malicious (or at the very least negligent / defective) *participant*,
           while in this case we are talking about malicious *code* being
           able to induce work. So the "threat model" is quite different.

      To be able to make a statement of non-existence of a key, it's clear
      that we must authorize against the maintainers, and not the
      stakeholders, since there are no observers to speak of.

      On the other hand, when making a positive statement, we can use the
      same authorization rule that we use for fetch -- that is, we check
      that `authorizers ∩ stakeholders ≠ ∅`.
     */
    def authorizeLookupByKey(
        nodeId: Transaction.NodeId,
        lbk: NodeLookupByKey.WithTxValue[ContractId],
        authorization: Authorization,
    ): EnrichState = {
      authorization.fold(this) { authorizers =>
        this.authorize(
          nodeId = nodeId,
          passIf = lbk.key.maintainers subsetOf authorizers,
          failWith = FALookupByKeyMissingAuthorization(
            lbk.templateId,
            lbk.optLocation,
            lbk.key.maintainers,
            authorizers,
          ),
        )
      }
    }
  }

  object EnrichState {
    def empty =
      EnrichState(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)
  }

  sealed trait Authorization {
    def fold[A](ifDontAuthorize: A)(ifAuthorize: Set[Party] => A): A =
      this match {
        case DontAuthorize => ifDontAuthorize
        case Authorize(authorizers) => ifAuthorize(authorizers)
      }

    def map(f: Set[Party] => Set[Party]): Authorization = this match {
      case DontAuthorize => DontAuthorize
      case Authorize(parties) => Authorize(f(parties))
    }
  }

  /** Do not authorize the transaction. If this is passed in, failedAuthorizations is guaranteed to be empty. */
  case object DontAuthorize extends Authorization

  /** Authorize the transaction using the provided parties as initial authorizers for the dynamic authorization. */
  final case class Authorize(authorizers: Set[Party]) extends Authorization

  /** Enrich a transaction with disclosure and authorization information.
    *
    * PRE: The transaction must create contract-instances before
    * consuming them.
    *
    * @param authorization the authorization mode
    * @param tr                     transaction resulting from executing the update
    *                               expression at the given effective time.
    */
  def enrichTransaction(
      authorization: Authorization,
      tr: Transaction.Transaction,
  ): EnrichedTransaction = {

    // Before we traversed through an exercise node the exercise witnesses
    // contain only the initial authorizers.
    val initialParentExerciseWitnesses: Set[Party] =
      authorization match {
        case DontAuthorize => Set.empty
        case Authorize(authorizers) => authorizers
      }

    def enrichNode(
        state: EnrichState,
        parentExerciseWitnesses: Set[Party],
        authorization: Authorization,
        nodeId: Transaction.NodeId,
    ): EnrichState = {
      val node =
        tr.nodes
          .getOrElse(nodeId, crash(s"enrichNode - precondition violated: node $nodeId not present"))
      node match {
        case create: NodeCreate.WithTxValue[ContractId] =>
          // ------------------------------------------------------------------
          // witnesses            : stakeholders union witnesses of parent exercise
          //                        node
          // divulge              : Nothing
          // well-authorized by A : signatories subsetOf A && non-empty signatories
          // ------------------------------------------------------------------
          state
            .authorizeCreate(
              nodeId,
              create,
              signatories = create.signatories,
              authorization = authorization,
              mbMaintainers = create.key.map(_.maintainers),
            )
            .discloseNode(parentExerciseWitnesses, nodeId, create)
            ._2

        case fetch: NodeFetch[ContractId] =>
          // ------------------------------------------------------------------
          // witnesses            : parent exercise witnesses
          // divulge              : referenced contract to witnesses of parent exercise node
          // well-authorized by A : A `intersect` stakeholders(fetched contract id) = non-empty
          // ------------------------------------------------------------------
          state
            .divulgeCoidTo(parentExerciseWitnesses -- fetch.stakeholders, fetch.coid)
            .discloseNode(parentExerciseWitnesses, nodeId, fetch)
            ._2
            .authorizeFetch(
              nodeId,
              fetch,
              stakeholders = fetch.stakeholders,
              authorization = authorization,
            )

        case ex: NodeExercises.WithTxValue[Transaction.NodeId, ContractId] =>
          // ------------------------------------------------------------------
          // witnesses:
          //    | consuming  -> stakeholders(targetId) union witnesses of parent exercise node
          //    | non-consuming ->  signatories(targetId) union actors
          //                        union witnesses of parent exercise
          // divulge: target contract id to parent exercise witnesses.
          // well-authorized by A : actors == controllers(c)
          //                        && actors subsetOf A
          //                        && childrenActions well-authorized by
          //                           (signatories(c) union controllers(c))
          //                        && controllers non-empty
          // ------------------------------------------------------------------

          // Authorize the exercise
          val state0 =
            state.authorizeExercise(
              nodeId,
              ex,
              actingParties = ex.actingParties,
              authorization = authorization,
              controllers = ex.controllers,
            )

          // Then enrich and authorize the children.
          val (witnesses, state1) = state0.discloseNode(parentExerciseWitnesses, nodeId, ex)
          val state2 =
            state1.divulgeCoidTo(parentExerciseWitnesses -- ex.stakeholders, ex.targetCoid)
          ex.children.foldLeft(state2) { (s, childNodeId) =>
            enrichNode(
              s,
              witnesses,
              authorization.map(_ => ex.controllers union ex.signatories),
              childNodeId,
            )
          }

        case nlbk: NodeLookupByKey.WithTxValue[ContractId] =>
          // ------------------------------------------------------------------
          // witnesses: parent exercise witnesses
          //
          // divulge: nothing
          //
          // well-authorized by A: maintainers subsetOf A.
          // ------------------------------------------------------------------
          state
            .authorizeLookupByKey(nodeId, nlbk, authorization)
            .discloseNode(parentExerciseWitnesses, nodeId, nlbk)
            ._2

      }
    }

    val finalState =
      tr.roots.foldLeft(EnrichState.empty) { (s, nodeId) =>
        enrichNode(s, initialParentExerciseWitnesses, authorization, nodeId)
      }

    EnrichedTransaction(
      roots = tr.roots,
      nodes = tr.nodes,
      explicitDisclosure = finalState.disclosures,
      localImplicitDisclosure = finalState.localDivulgences,
      globalImplicitDisclosure = finalState.globalDivulgences,
      failedAuthorizations = finalState.failedAuthorizations,
    )
  }

  //----------------------------------------------------------------------------
  // Enriching transactions with disclosure information
  //----------------------------------------------------------------------------

  def collectCoids(value: VersionedValue[ContractId]): Set[ContractId] =
    collectCoids(value.value)

  /** Collect all contract ids appearing in a value
    */
  def collectCoids(value: Value[ContractId]): Set[ContractId] = {
    val coids =
      implicitly[CanBuildFrom[Nothing, ContractId, Set[ContractId]]].apply()
    def collect(v: Value[ContractId]): Unit =
      v match {
        case ValueRecord(tycon @ _, fs) =>
          fs.foreach {
            case (_, v) => collect(v)
          }
        case ValueStruct(fs) =>
          fs.foreach {
            case (_, v) => collect(v)
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
          entries.foreach {
            case (k, v) =>
              collect(k)
              collect(v)
          }
      }

    collect(value)
    coids.result()
  }

  def makeAbsolute(
      commitPrefix: LedgerString,
      value: VersionedValue[ContractId],
  ): VersionedValue[AbsoluteContractId] = {
    VersionedValue(value.version, makeAbsolute(commitPrefix, value.value))
  }

  /** Convert all relative to absolute contract-ids in the value.
    *
    * TODO(FM) make this tail recursive
    */
  def makeAbsolute(
      commitPrefix: LedgerString,
      value: Value[ContractId],
  ): Value[AbsoluteContractId] = {
    def rewrite(v: Value[ContractId]): Value[AbsoluteContractId] =
      v match {
        case ValueRecord(tycon, fs) =>
          ValueRecord(tycon, fs.map[(Option[Name], Value[AbsoluteContractId])] {
            case (k, v) => (k, rewrite(v))
          })
        case ValueStruct(fs) =>
          ValueStruct(fs.map[(Name, Value[AbsoluteContractId])] {
            case (k, v) => (k, rewrite(v))
          })
        case ValueVariant(tycon, variant, value) =>
          ValueVariant(tycon, variant, rewrite(value))
        case ValueList(vs) => ValueList(vs.map(rewrite))
        case ValueContractId(coid) =>
          val acoid = contractIdToAbsoluteContractId(commitPrefix, coid)
          ValueContractId(acoid)
        case vlit: ValueCidlessLeaf => vlit
        case ValueOptional(mbV) => ValueOptional(mbV.map(rewrite))
        case ValueTextMap(map) => ValueTextMap(map.mapValue(rewrite))
        case ValueGenMap(entries) =>
          ValueGenMap(entries.map { case (k, v) => rewrite(k) -> rewrite(v) })
      }
    rewrite(value)
  }

  /*
  def relativeContractIdToNodeId(commitPrefix: String,
                                 rcoid: RelativeContractId): NodeId =
    NodeId(commitPrefix ++ rcoid.index.toString)

  def contractIdToNodeId(commitPrefix: String, coid: ContractId): NodeId =
    coid match {
      case acoid: AbsoluteContractId => absoluteContractIdToNodeId(acoid)
      case rcoid: RelativeContractId =>
        relativeContractIdToNodeId(commitPrefix, rcoid)
    }

  def contractIdToNodeIdOrTrNodeId(
      coid: ContractId): Either[NodeId, Tr.NodeId] = {
    coid match {
      case AbsoluteContractId(acoid) => Left(acoid)
      case RelativeContractId(rcoid) => Right(rcoid)
    }
  }
   */

  // ----------------------------------------------------------------
  // Cache for active contracts and nodes
  // ----------------------------------------------------------------

  object LedgerData {
    lazy val empty = LedgerData(Set.empty, Map.empty, Map.empty, Map.empty)
  }

  /**
    * @param activeContracts The contracts that are active in the
    *                        current state of the ledger.
    * @param nodeInfos       Node information used to efficiently navigate
    *                        the transaction graph
    */
  final case class LedgerData(
      activeContracts: Set[AbsoluteContractId],
      nodeInfos: LedgerNodeInfos,
      activeKeys: Map[GlobalKey, AbsoluteContractId],
      coidToNodeId: Map[AbsoluteContractId, ScenarioNodeId],
  ) {
    def nodeInfoByCoid(coid: AbsoluteContractId): LedgerNodeInfo = nodeInfos(coidToNodeId(coid))

    def updateLedgerNodeInfo(
        coid: AbsoluteContractId,
    )(f: (LedgerNodeInfo) => LedgerNodeInfo): LedgerData =
      coidToNodeId.get(coid).map(updateLedgerNodeInfo(_)(f)).getOrElse(this)

    def updateLedgerNodeInfo(
        nodeId: ScenarioNodeId,
    )(f: (LedgerNodeInfo) => LedgerNodeInfo): LedgerData =
      copy(
        nodeInfos = nodeInfos
          .get(nodeId)
          .map(ni => nodeInfos.updated(nodeId, f(ni)))
          .getOrElse(nodeInfos),
      )

    def markAsActive(coid: AbsoluteContractId): LedgerData =
      copy(activeContracts = activeContracts + coid)

    def markAsInactive(coid: AbsoluteContractId): LedgerData =
      copy(activeContracts = activeContracts - coid)

    def createdIn(coid: AbsoluteContractId, nodeId: ScenarioNodeId): LedgerData =
      copy(coidToNodeId = coidToNodeId + (coid -> nodeId))

    def addKey(key: GlobalKey, acoid: AbsoluteContractId): LedgerData =
      copy(activeKeys = activeKeys + (key -> acoid))

    def removeKey(key: GlobalKey): LedgerData =
      copy(activeKeys = activeKeys - key)

  }

  case class UniqueKeyViolation(gk: GlobalKey)

  private def processTransaction(
      trId: ScenarioTransactionId,
      richTr: RichTransaction,
      ledgerData: LedgerData,
  ): Either[UniqueKeyViolation, LedgerData] = {
    type ExerciseNodeProcessing = (Option[ScenarioNodeId], List[ScenarioNodeId])

    @tailrec
    def processNodes(
        mbCache0: Either[UniqueKeyViolation, LedgerData],
        enps: List[ExerciseNodeProcessing],
    ): Either[UniqueKeyViolation, LedgerData] = {
      mbCache0 match {
        case Left(err) => Left(err)
        case Right(cache0) =>
          enps match {
            case Nil => Right(cache0)
            case (_, Nil) :: restENPs => processNodes(Right(cache0), restENPs)
            case (mbParentId, nodeId :: restOfNodeIds) :: restENPs =>
              richTr.nodes.get(nodeId) match {
                case None =>
                  crash(s"processTransaction: non-existent node '$nodeId'.")
                case Some(node) =>
                  val newLedgerNodeInfo = LedgerNodeInfo(
                    node = node,
                    transaction = trId,
                    effectiveAt = richTr.effectiveAt,
                    observingSince = Map.empty,
                    referencedBy = Set.empty,
                    consumedBy = None,
                    parent = mbParentId,
                  )
                  val newCache =
                    cache0.copy(nodeInfos = cache0.nodeInfos + (nodeId -> newLedgerNodeInfo))
                  val idsToProcess = (mbParentId -> restOfNodeIds) :: restENPs

                  node match {
                    case nc: NodeCreate.WithTxValue[AbsoluteContractId] =>
                      val newCache1 =
                        newCache
                          .markAsActive(nc.coid)
                          .createdIn(nc.coid, nodeId)
                      val mbNewCache2 = nc.key match {
                        case None => Right(newCache1)
                        case Some(keyWithMaintainers) =>
                          val gk = GlobalKey(
                            nc.coinst.template,
                            // FIXME: we probably should never crash here !
                            assertNoContractId(keyWithMaintainers.key),
                          )
                          newCache1.activeKeys.get(gk) match {
                            case None => Right(newCache1.addKey(gk, nc.coid))
                            case Some(_) => Left(UniqueKeyViolation(gk))
                          }
                      }
                      processNodes(mbNewCache2, idsToProcess)

                    case NodeFetch(referencedCoid, templateId @ _, optLoc @ _, _, _, _) =>
                      val newCacheP =
                        newCache.updateLedgerNodeInfo(referencedCoid)(info =>
                          info.copy(referencedBy = info.referencedBy + nodeId))

                      processNodes(Right(newCacheP), idsToProcess)

                    case ex: NodeExercises.WithTxValue[ScenarioNodeId, AbsoluteContractId] =>
                      val newCache0 =
                        newCache.updateLedgerNodeInfo(ex.targetCoid)(
                          info =>
                            info.copy(
                              referencedBy = info.referencedBy + nodeId,
                              consumedBy = if (ex.consuming) Some(nodeId) else info.consumedBy,
                          ))
                      val newCache1 =
                        if (ex.consuming) {
                          val newCache0_1 = newCache0.markAsInactive(ex.targetCoid)
                          val nc = newCache0_1
                            .nodeInfoByCoid(ex.targetCoid)
                            .node
                            .asInstanceOf[NodeCreate[
                              AbsoluteContractId,
                              Transaction.Value[
                                AbsoluteContractId
                              ]]]
                          nc.key match {
                            case None => newCache0_1
                            case Some(keyWithMaintainers) =>
                              newCache0_1.removeKey(
                                GlobalKey(
                                  ex.templateId,
                                  // FIXME: we probably should'nt crash here !
                                  assertNoContractId(keyWithMaintainers.key),
                                ),
                              )
                          }
                        } else newCache0

                      processNodes(
                        Right(newCache1),
                        (Some(nodeId) -> ex.children.toList) :: idsToProcess,
                      )

                    case nlkup: NodeLookupByKey.WithTxValue[AbsoluteContractId] =>
                      nlkup.result match {
                        case None =>
                          processNodes(Right(newCache), idsToProcess)
                        case Some(referencedCoid) =>
                          val newCacheP =
                            newCache.updateLedgerNodeInfo(referencedCoid)(info =>
                              info.copy(referencedBy = info.referencedBy + nodeId))

                          processNodes(Right(newCacheP), idsToProcess)
                      }
                  }
              }
          }

      }
    }

    val mbCacheAfterProcess =
      processNodes(Right(ledgerData), List(None -> richTr.roots.toList))

    mbCacheAfterProcess.map { cacheAfterProcess =>
      val globalImplicitDisclosure = richTr.globalImplicitDisclosure.map {
        case (cid, parties) => ledgerData.coidToNodeId(cid) -> parties
      }
      Relation
        .union(
          Relation
            .union(richTr.localImplicitDisclosure, richTr.explicitDisclosure),
          globalImplicitDisclosure,
        )
        .foldLeft(cacheAfterProcess) {
          case (cacheP, (nodeId, witnesses)) =>
            cacheP.updateLedgerNodeInfo(nodeId)(_.addObservers(witnesses.map(_ -> trId).toMap))
        }
    }
  }
}
