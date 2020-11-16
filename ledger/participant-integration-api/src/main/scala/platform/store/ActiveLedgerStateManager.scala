// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

/**
  * A helper for updating an [[ActiveLedgerState]] with new transactions:
  * - Validates the transaction against the [[ActiveLedgerState]].
  * - Updates the [[ActiveLedgerState]].
  */
private[platform] class ActiveLedgerStateManager[ALS <: ActiveLedgerState[ALS]](
    initialState: => ALS) {

  private case class AddTransactionState(
      acc: Option[ALS],
      errs: Set[RejectionReason],
      parties: Set[Party],
      archivedIds: Set[ContractId]) {

    def mapAcs(f: ALS => ALS): AddTransactionState = copy(acc = acc map f)

    def result: Either[Set[RejectionReason], ALS] = {
      acc match {
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
      AddTransactionState(Some(acs), Set(), Set.empty, Set.empty)
  }

  /**
    * A higher order function to update an abstract active ledger state (ALS) with the effects of the given transaction.
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
      divulgedContracts: List[(Value.ContractId, ContractInst)])
    : Either[Set[RejectionReason], ALS] = {
    val st =
      transaction
        .fold[AddTransactionState](AddTransactionState(initialState)) {
          case (ats @ AddTransactionState(None, _, _, _), _) => ats
          case (ats @ AddTransactionState(Some(acc), errs, parties, archivedIds), (nodeId, node)) =>
            // If some node requires a contract, check that we have that contract, and check that that contract is not
            // created after the current let.
            def contractCheck(cid: ContractId): Option[RejectionReason] =
              acc lookupContractLet cid match {
                case Some(Let(otherContractLet)) =>
                  // Existing active contract, check its LET
                  if (otherContractLet.isAfter(let)) {
                    Some(InvalidLedgerTime(
                      s"Encountered contract [$cid] with LET [$otherContractLet] greater than the LET of the transaction [$let]"))
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

            node match {
              case nf: N.NodeFetch.WithTxValue[ContractId] =>
                val nodeParties = nf.signatories
                  .union(nf.stakeholders)
                  .union(nf.actingParties)
                AddTransactionState(
                  Some(acc),
                  contractCheck(nf.coid).fold(errs)(errs + _),
                  parties.union(nodeParties),
                  archivedIds
                )
              case nc: N.NodeCreate.WithTxValue[ContractId] =>
                val nodeParties = nc.signatories
                  .union(nc.stakeholders)
                  .union(nc.key.map(_.maintainers).getOrElse(Set.empty))
                val activeContract = ActiveContract(
                  id = nc.coid,
                  let = let,
                  transactionId = transactionId,
                  nodeId = nodeId,
                  workflowId = workflowId,
                  contract = nc.coinst,
                  witnesses = disclosure(nodeId),
                  // A contract starts its life without being divulged at all.
                  divulgences = Map.empty,
                  key =
                    nc.key.map(_.assertNoCid(coid => s"Contract ID $coid found in contract key")),
                  signatories = nc.signatories,
                  observers = nc.stakeholders.diff(nc.signatories),
                  agreementText = nc.coinst.agreementText
                )
                activeContract.key match {
                  case None =>
                    ats.copy(
                      acc = Some(acc.addContract(activeContract, None)),
                      parties = parties.union(nodeParties))
                  case Some(key) =>
                    val gk = GlobalKey(activeContract.contract.template, key.key.value)
                    if (acc.lookupContractByKey(gk).isDefined) {
                      AddTransactionState(
                        None,
                        errs + Inconsistent("DuplicateKey: contract key is not unique"),
                        parties.union(nodeParties),
                        archivedIds)
                    } else {
                      ats.copy(
                        acc = Some(acc.addContract(activeContract, Some(gk))),
                        parties = parties.union(nodeParties)
                      )
                    }
                }
              case ne: N.NodeExercises.WithTxValue[_, ContractId] =>
                val nodeParties = ne.signatories
                  .union(ne.stakeholders)
                  .union(ne.actingParties)
                ats.copy(
                  errs = contractCheck(ne.targetCoid).fold(errs)(errs + _),
                  acc = Some(if (ne.consuming) {
                    acc.removeContract(ne.targetCoid)
                  } else {
                    acc
                  }),
                  parties = parties.union(nodeParties),
                  archivedIds = if (ne.consuming) archivedIds + ne.targetCoid else archivedIds
                )
              case nlkup: N.NodeLookupByKey.WithTxValue[ContractId] =>
                // Check that the stored lookup result matches the current result
                val key = nlkup.key.key.ensureNoCid.fold(
                  coid =>
                    throw new IllegalStateException(s"Contract ID $coid found in contract key"),
                  identity
                )
                val gk = GlobalKey(nlkup.templateId, key.value)
                val nodeParties = nlkup.key.maintainers

                if (actAs.nonEmpty) {
                  // If the submitter is known, look up the contract
                  // Submitters being know means the transaction was submitted on this participant.
                  val currentResult = acc.lookupContractByKey(gk)
                  if (currentResult == nlkup.result) {
                    ats.copy(
                      parties = parties.union(nodeParties),
                    )
                  } else {
                    ats.copy(
                      errs = errs + Inconsistent(
                        s"Contract key lookup with different results: expected [${nlkup.result}], actual [${currentResult}]")
                    )
                  }
                } else {
                  // Otherwise, trust that the lookup was valid.
                  // The submitter being unknown means the transaction was submitted on a different participant,
                  // and (A) this participant may not know the authoritative answer to whether the key exists and
                  // (B) this code is called from a Indexer and not from the sandbox ledger.
                  ats.copy(
                    parties = parties.union(nodeParties),
                  )
                }
            }
        }

    val divulgedContractIds = divulgence -- st.archivedIds
    st.mapAcs(
        _ divulgeAlreadyCommittedContracts (transactionId, divulgedContractIds, divulgedContracts))
      .mapAcs(_ addParties st.parties)
      .result
  }

}
