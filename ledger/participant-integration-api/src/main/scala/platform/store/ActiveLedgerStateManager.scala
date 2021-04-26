// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.ledger.api.domain.RejectionReason
import com.daml.ledger.api.domain.RejectionReason.{Inconsistent, InvalidLedgerTime}
import com.daml.ledger.participant.state.v1.ContractInst
import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.transaction.{CommittedTransaction, NodeId, Node => N}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.platform.store.Contract.ActiveContract

/** A helper for updating an [[ActiveLedgerState]] with new transactions:
  * - Validates the transaction against the [[ActiveLedgerState]].
  * - Updates the [[ActiveLedgerState]].
  */
private[platform] class ActiveLedgerStateManager[ALS <: ActiveLedgerState[ALS]](
    initialState: => ALS
) {

  /** State that is restored on rollback.
    * @param createdIds The set of local contract ids created in the transaction.
    * @param archivedIds The set of contract ids
    *  archived by the transaction or local contracts whose create has been rolled back.
    *  In other words, contracts that we know won’t be active at the
    *  end of the transaction.
    * @param als The active ledger state
    */
  private case class RollbackState(
      createdIds: Set[ContractId],
      archivedIds: Set[ContractId],
      als: Option[ALS],
  ) {
    def mapAcs(f: ALS => ALS): RollbackState =
      copy(als = als.map(f))
  }

  /** The accumulator state used while validating the transaction
    * and to record the required changes to the ledger state.
    * @param currentState The current state
    * @param rollbackStates The stack of states at the beginning of a rollback node
    *  so we can restore the previous state when leaving a rollback node.
    *  The most recent rollback state comes first.
    * @param errs The list of errors produced during validation.
    * @param parties The set of parties that are used
    *  in the transaction and will be implicitly allocated
    */
  private case class AddTransactionState(
      currentState: RollbackState,
      rollbackStates: List[RollbackState],
      errs: Set[RejectionReason],
      parties: Set[Party],
  ) {

    def mapAcs(f: ALS => ALS): AddTransactionState =
      copy(currentState = currentState.mapAcs(f), rollbackStates = rollbackStates.map(_.mapAcs(f)))

    def result: Either[Set[RejectionReason], ALS] = {
      if (!rollbackStates.isEmpty) {
        sys.error(s"IMPOSSIBLE finished transaction but rollback states is not empty")
      }
      currentState.als match {
        case None =>
          if (errs.isEmpty) {
            sys.error(s"IMPOSSIBLE no acc and no errors either!")
          }
          Left(errs)
        case Some(acc_) =>
          if (errs.isEmpty) {
            Right(acc_)
          } else {
            Left(errs)
          }
      }
    }
  }

  private object AddTransactionState {
    def apply(acs: ALS): AddTransactionState =
      AddTransactionState(RollbackState(Set.empty, Set.empty, Some(acs)), List(), Set(), Set.empty)
  }

  /** A higher order function to update an abstract active ledger state (ALS) with the effects of the given transaction.
    * Makes sure that there are no double spends or timing errors.
    */
  def addTransaction(
      let: Instant,
      transactionId: TransactionId,
      workflowId: Option[WorkflowId],
      actAs: List[Party],
      transaction: CommittedTransaction,
      disclosure: Relation[NodeId, Party],
      divulgence: Relation[ContractId, Party],
      divulgedContracts: List[(Value.ContractId, ContractInst)],
  ): Either[Set[RejectionReason], ALS] = {
    // If some node requires a contract, check that we have that contract, and check that that contract is not
    // created after the current let.
    def contractCheck(als: ALS, cid: ContractId): Option[RejectionReason] =
      als.lookupContractLet(cid) match {
        case Some(Let(otherContractLet)) =>
          // Existing active contract, check its LET
          if (otherContractLet.isAfter(let)) {
            Some(
              InvalidLedgerTime(
                s"Encountered contract [$cid] with LET [$otherContractLet] greater than the LET of the transaction [$let]"
              )
            )
          } else {
            None
          }
        case Some(LetUnknown) =>
          // Contract divulged in the past
          None
        case None if divulgedContracts.exists(_._1 == cid) =>
          // Contract is going to be divulged in this transaction
          None
        case None =>
          // Contract not known
          Some(Inconsistent(s"Could not lookup contract $cid"))
      }

    def handleLeaf(
        state: AddTransactionState,
        nodeId: NodeId,
        node: N.LeafOnlyActionNode[ContractId],
    ): AddTransactionState =
      state.currentState.als match {
        case None => state
        case Some(als) =>
          node match {
            case nf: N.NodeFetch[ContractId] =>
              val nodeParties = nf.signatories
                .union(nf.stakeholders)
                .union(nf.actingParties)
              state.copy(
                errs = contractCheck(als, nf.coid).fold(state.errs)(state.errs + _),
                parties = state.parties.union(nodeParties),
              )
            case nc: N.NodeCreate[ContractId] =>
              val nodeParties = nc.signatories
                .union(nc.stakeholders)
                .union(nc.key.map(_.maintainers).getOrElse(Set.empty))
              val activeContract = ActiveContract(
                id = nc.coid,
                let = let,
                transactionId = transactionId,
                nodeId = nodeId,
                workflowId = workflowId,
                contract = nc.versionedCoinst,
                witnesses = disclosure(nodeId),
                // A contract starts its life without being divulged at all.
                divulgences = Map.empty,
                key = nc.versionedKey.map(
                  _.assertNoCid(coid => s"Contract ID $coid found in contract key")
                ),
                signatories = nc.signatories,
                observers = nc.stakeholders.diff(nc.signatories),
                agreementText = nc.coinst.agreementText,
              )
              activeContract.key match {
                case None =>
                  state.copy(
                    currentState = state.currentState.copy(
                      als = Some(als.addContract(activeContract, None))
                    ),
                    parties = state.parties.union(nodeParties),
                  )
                case Some(key) =>
                  val gk = GlobalKey(activeContract.contract.template, key.key.value)
                  if (als.lookupContractByKey(gk).isDefined) {
                    state.copy(
                      currentState = state.currentState.copy(
                        als = None
                      ),
                      errs = state.errs + Inconsistent("DuplicateKey: contract key is not unique"),
                      parties = state.parties.union(nodeParties),
                    )
                  } else {
                    state.copy(
                      currentState = state.currentState.copy(
                        als = Some(als.addContract(activeContract, Some(gk)))
                      ),
                      parties = state.parties.union(nodeParties),
                    )
                  }
              }
            case nlkup: N.NodeLookupByKey[ContractId] =>
              // Check that the stored lookup result matches the current result
              val key = nlkup.key.key.ensureNoCid.fold(
                coid => throw new IllegalStateException(s"Contract ID $coid found in contract key"),
                identity,
              )
              val gk = GlobalKey(nlkup.templateId, key)
              val nodeParties = nlkup.key.maintainers

              if (actAs.nonEmpty) {
                // If the submitter is known, look up the contract
                // Submitters being know means the transaction was submitted on this participant.
                val currentResult = als.lookupContractByKey(gk)
                if (currentResult == nlkup.result) {
                  state.copy(
                    parties = state.parties.union(nodeParties)
                  )
                } else {
                  state.copy(
                    errs = state.errs + Inconsistent(
                      s"Contract key lookup with different results: expected [${nlkup.result}], actual [${currentResult}]"
                    )
                  )
                }
              } else {
                // Otherwise, trust that the lookup was valid.
                // The submitter being unknown means the transaction was submitted on a different participant,
                // and (A) this participant may not know the authoritative answer to whether the key exists and
                // (B) this code is called from a Indexer and not from the sandbox ledger.
                state.copy(
                  parties = state.parties.union(nodeParties)
                )
              }
          }
      }

    val st =
      transaction
        .foldInExecutionOrder[AddTransactionState](AddTransactionState(initialState))(
          exerciseBegin = (acc, _, ne) => {
            acc.currentState.als match {
              case None => (acc, true)
              case Some(als) =>
                val nodeParties = ne.signatories
                  .union(ne.stakeholders)
                  .union(ne.actingParties)
                val newState = acc.copy(
                  errs = contractCheck(als, ne.targetCoid).fold(acc.errs)(acc.errs + _),
                  currentState = acc.currentState.copy(
                    als = Some(if (ne.consuming) {
                      als.removeContract(ne.targetCoid)
                    } else {
                      als
                    }),
                    archivedIds =
                      if (ne.consuming) acc.currentState.archivedIds + ne.targetCoid
                      else acc.currentState.archivedIds,
                  ),
                  parties = acc.parties.union(nodeParties),
                )
                (newState, true)
            }
          },
          rollbackBegin = (acc, _, _) => {
            (acc.copy(rollbackStates = acc.currentState +: acc.rollbackStates), true)
          },
          leaf = (acc, nodeId, node) => handleLeaf(acc, nodeId, node),
          exerciseEnd = (acc, _, _) => acc,
          rollbackEnd = (acc, _, _) => {
            val (beforeRollback, rest) = acc.rollbackStates match {
              case Seq() => sys.error("IMPOSSIBLE: Rollback end but rollback stack is empty")
              case head +: tail => (head, tail)
            }
            // Discard archives in the rollback but mark contracts created in the rollback as archived.
            // This means that at the end of the traversal, we can use archivedIds to filter out
            // both local and global contracts that are no longer active.
            val newArchived =
              beforeRollback.archivedIds union (acc.currentState.createdIds diff beforeRollback.createdIds)
            acc.copy(
              currentState = beforeRollback.copy(
                archivedIds = newArchived
              ),
              rollbackStates = rest,
            )
          },
        )
    val divulgedContractIds = divulgence -- st.currentState.archivedIds
    st.mapAcs(
      _.divulgeAlreadyCommittedContracts(transactionId, divulgedContractIds, divulgedContracts)
    ).mapAcs(_ addParties st.parties)
      .result
  }

}
