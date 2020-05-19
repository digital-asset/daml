// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.ledger.api.domain.RejectionReason
import com.daml.ledger.api.domain.RejectionReason.{Disputed, Inconsistent, InvalidLedgerTime}
import com.daml.ledger.participant.state.v1.AbsoluteContractInst
import com.daml.ledger.{EventId, TransactionId, WorkflowId}
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.transaction.{GenTransaction, Node => N}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.AbsoluteContractId
import com.daml.platform.store.Contract.ActiveContract

/**
  * A helper for updating an [[ActiveLedgerState]] with new transactions:
  * - Validates the transaction against the [[ActiveLedgerState]].
  * - Updates the [[ActiveLedgerState]].
  */
class ActiveLedgerStateManager[ALS <: ActiveLedgerState[ALS]](initialState: => ALS) {

  private case class AddTransactionState(
      acc: Option[ALS],
      errs: Set[RejectionReason],
      parties: Set[Party],
      archivedIds: Set[AbsoluteContractId]) {

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
      submitter: Option[Party],
      transaction: GenTransaction.WithTxValue[EventId, AbsoluteContractId],
      disclosure: Relation[EventId, Party],
      globalDivulgence: Relation[AbsoluteContractId, Party],
      divulgedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)])
    : Either[Set[RejectionReason], ALS] = {
    // NOTE(RA): `globalImplicitDisclosure` was meant to refer to contracts created in previous transactions.
    // However, because we have translated relative to absolute IDs at this point, `globalImplicitDisclosure`
    // will also point to contracts created in the same transaction.
    //
    // This is dealt with as follows:
    // - First, all transaction nodes are traversed without updating divulgence info.
    //   - When validating a fetch/exercise node, both the set of previously divulged contracts and
    //     the newly divulged contracts is used.
    //   - While traversing consuming exercise nodes, the set of all contracts archived in this transaction is collected.
    // - Finally, divulgence information is updated using `globalImplicitDisclosure` minus the set of contracts
    //   archived in this transaction.
    val st =
      transaction
        .fold[AddTransactionState](AddTransactionState(initialState)) {
          case (ats @ AddTransactionState(None, _, _, _), _) => ats
          case (ats @ AddTransactionState(Some(acc), errs, parties, archivedIds), (nodeId, node)) =>
            // If some node requires a contract, check that we have that contract, and check that that contract is not
            // created after the current let.
            def contractCheck(cid: AbsoluteContractId): Option[RejectionReason] =
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
              case nf: N.NodeFetch.WithTxValue[AbsoluteContractId] =>
                val nodeParties = nf.signatories
                  .union(nf.stakeholders)
                  .union(nf.actingParties.getOrElse(Set.empty))
                AddTransactionState(
                  Some(acc),
                  contractCheck(nf.coid).fold(errs)(errs + _),
                  parties.union(nodeParties),
                  archivedIds
                )
              case nc: N.NodeCreate.WithTxValue[AbsoluteContractId] =>
                val nodeParties = nc.signatories
                  .union(nc.stakeholders)
                  .union(nc.key.map(_.maintainers).getOrElse(Set.empty))
                val activeContract = ActiveContract(
                  id = nc.coid,
                  let = let,
                  transactionId = transactionId,
                  eventId = nodeId,
                  workflowId = workflowId,
                  contract = nc.coinst,
                  witnesses = disclosure(nodeId),
                  // The divulgences field used to be filled with data coming from the `localDivulgence` field of the blinding info.
                  // But this field is always empty in transactions with only absolute contract ids.
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
                        errs + Disputed("DuplicateKey: contract key is not unique"),
                        parties.union(nodeParties),
                        archivedIds)
                    } else {
                      ats.copy(
                        acc = Some(acc.addContract(activeContract, Some(gk))),
                        parties = parties.union(nodeParties)
                      )
                    }
                }
              case ne: N.NodeExercises.WithTxValue[EventId, AbsoluteContractId] =>
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
              case nlkup: N.NodeLookupByKey.WithTxValue[AbsoluteContractId] =>
                // Check that the stored lookup result matches the current result
                val key = nlkup.key.key.ensureNoCid.fold(
                  coid =>
                    throw new IllegalStateException(s"Contract ID $coid found in contract key"),
                  identity
                )
                val gk = GlobalKey(nlkup.templateId, key.value)
                val nodeParties = nlkup.key.maintainers

                submitter match {
                  case Some(_) =>
                    // If the submitter is known, look up the contract
                    // Submitter being know means the transaction was submitted on this participant.
                    val currentResult = acc.lookupContractByKey(gk)
                    if (currentResult == nlkup.result) {
                      ats.copy(
                        parties = parties.union(nodeParties),
                      )
                    } else {
                      ats.copy(
                        errs = errs + Disputed(
                          s"Contract key lookup with different results: expected [${nlkup.result}], actual [${currentResult}]")
                      )
                    }
                  // Otherwise, trust that the lookup was valid.
                  // The submitter being unknown means the transaction was submitted on a different participant,
                  // and (A) this participant may not know the authoritative answer to whether the key exists and
                  // (B) this code is called from a Indexer and not from the sandbox ledger.
                  case None =>
                    ats.copy(
                      parties = parties.union(nodeParties),
                    )
                }
            }
        }

    val divulgedContractIds = globalDivulgence -- st.archivedIds
    st.mapAcs(
        _ divulgeAlreadyCommittedContracts (transactionId, divulgedContractIds, divulgedContracts))
      .mapAcs(_ addParties st.parties)
      .result
  }

}
