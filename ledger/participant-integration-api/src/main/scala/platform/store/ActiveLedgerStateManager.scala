// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.ledger.api.domain.RejectionReason
import com.daml.ledger.api.domain.RejectionReason.{Inconsistent, InvalidLedgerTime}
import com.daml.lf.data.Ref
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.{CommittedTransaction, GlobalKey, NodeId, Node}
import com.daml.lf.value.Value.ContractId
import com.daml.platform.store.Contract.ActiveContract

/** A helper for updating an [[ActiveLedgerState]] with new transactions:
  * - Validates the transaction against the [[ActiveLedgerState]].
  * - Updates the [[ActiveLedgerState]].
  *
  *  This class is only used in two places:
  *  1. Sandbox Classic in-memory backend.
  *  2. A very old migration for the index database.
  */
private[platform] class ActiveLedgerStateManager[ALS <: ActiveLedgerState[ALS]](
    initialState: => ALS
) {

  /** State that is restored on rollback.
    * @param createdIds The set of local contract ids created in the transaction.
    * @param als The active ledger state
    */
  private case class RollbackState(
      createdIds: Set[ContractId],
      als: Option[ALS],
  ) {
    def mapAcs(f: ALS => ALS): RollbackState =
      copy(als = als.map(f))
    def cloneState(): RollbackState =
      mapAcs(_.cloneState())
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
      parties: Set[Ref.Party],
  ) {

    def mapAcs(f: ALS => ALS): AddTransactionState =
      copy(currentState = currentState.mapAcs(f), rollbackStates = rollbackStates.map(_.mapAcs(f)))

    def result: Either[Set[RejectionReason], ALS] = {
      if (rollbackStates.nonEmpty) {
        sys.error("IMPOSSIBLE finished transaction but rollback states is not empty")
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
      AddTransactionState(
        RollbackState(Set.empty, Some(acs)),
        List(),
        Set.empty,
        Set.empty,
      )
  }

  /** A higher order function to update an abstract active ledger state (ALS) with the effects of the given transaction.
    * Makes sure that there are no double spends or timing errors.
    */
  def addTransaction(
      let: Instant,
      transactionId: Ref.TransactionId,
      workflowId: Option[Ref.WorkflowId],
      actAs: List[Ref.Party],
      transaction: CommittedTransaction,
      disclosure: Relation[NodeId, Ref.Party],
      divulgence: Relation[ContractId, Ref.Party],
      divulgedContracts: ActiveLedgerState.ReferencedContracts,
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
                s"Encountered contract [${cid.coid}] with LET [$otherContractLet] greater than the LET of the transaction [$let]"
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
          Some(Inconsistent(s"Could not lookup contract ${cid.coid}"))
      }

    def handleLeaf(
        state: AddTransactionState,
        nodeId: NodeId,
        node: Node.LeafOnlyAction,
    ): AddTransactionState =
      state.currentState.als match {
        case None => state
        case Some(als) =>
          node match {
            case nf: Node.Fetch =>
              val nodeParties = nf.signatories
                .union(nf.stakeholders)
                .union(nf.actingParties)
              state.copy(
                errs = contractCheck(als, nf.coid).fold(state.errs)(state.errs + _),
                parties = state.parties.union(nodeParties),
              )
            case nc: Node.Create =>
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
                key = nc.versionedKey,
                signatories = nc.signatories,
                observers = nc.stakeholders.diff(nc.signatories),
                agreementText = nc.agreementText,
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
                  val gk =
                    GlobalKey(activeContract.contract.unversioned.template, key.key.unversioned)
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
            case nlkup: Node.LookupByKey =>
              // Check that the stored lookup result matches the current result
              val key = nlkup.key.key
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
                      s"Contract key lookup with different results: expected [${nlkup.result}], actual [$currentResult]"
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
                    })
                  ),
                  parties = acc.parties.union(nodeParties),
                )
                (newState, true)
            }
          },
          rollbackBegin = (acc, _, _) => {
            (acc.copy(rollbackStates = acc.currentState.cloneState() +: acc.rollbackStates), true)
          },
          leaf = (acc, nodeId, node) => handleLeaf(acc, nodeId, node),
          exerciseEnd = (acc, _, _) => acc,
          rollbackEnd = (acc, _, _) => {
            val (beforeRollback, rest) = acc.rollbackStates match {
              case Nil => sys.error("IMPOSSIBLE: Rollback end but rollback stack is empty")
              case head :: tail => (head, tail)
            }
            acc.copy(
              currentState = beforeRollback,
              rollbackStates = rest,
            )
          },
        )
    val divulgedContractIds = divulgence -- transaction.inactiveContracts
    st.mapAcs(
      _.divulgeAlreadyCommittedContracts(transactionId, divulgedContractIds, divulgedContracts)
    ).mapAcs(_ addParties st.parties)
      .result
  }

}
