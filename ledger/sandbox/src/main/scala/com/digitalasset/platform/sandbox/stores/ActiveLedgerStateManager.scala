// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.time.Instant

import com.daml.ledger.participant.state.v1.AbsoluteContractInst
import com.digitalasset.daml.lf.data.Ref.{Party, TransactionIdString}
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.{GenTransaction, Node => N}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.{EventId, WorkflowId}
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState._
import com.digitalasset.platform.sandbox.stores.ledger.SequencingError
import com.digitalasset.platform.sandbox.stores.ledger.SequencingError.PredicateType.{
  Exercise,
  Fetch
}
import com.digitalasset.platform.sandbox.stores.ledger.SequencingError.{
  DuplicateKey,
  InactiveDependencyError,
  PredicateType,
  TimeBeforeError
}

/**
  * A helper for updating an [[ActiveLedgerState]] with new transactions:
  * - Validates the transaction against the [[ActiveLedgerState]].
  * - Updates the [[ActiveLedgerState].
  */
class ActiveLedgerStateManager[ALS](initialState: => ALS)(
    implicit ACS: ALS => ActiveLedgerState[ALS]) {

  private case class AddTransactionState(
      acc: Option[ALS],
      errs: Set[SequencingError],
      parties: Set[Party],
      archivedIds: Set[AbsoluteContractId]) {

    def mapAcs(f: ALS => ALS): AddTransactionState = copy(acc = acc map f)

    def result: Either[Set[SequencingError], ALS] = {
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
      transactionId: TransactionIdString,
      workflowId: Option[WorkflowId],
      transaction: GenTransaction.WithTxValue[EventId, AbsoluteContractId],
      disclosure: Relation[EventId, Party],
      localDivulgence: Relation[EventId, Party],
      globalDivulgence: Relation[AbsoluteContractId, Party],
      divulgedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)])
    : Either[Set[SequencingError], ALS] = {
    // NOTE(RC): `globalImplicitDisclosure` was meant to refer to contracts created in previous transactions.
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
        .fold[AddTransactionState](GenTransaction.TopDown, AddTransactionState(initialState)) {
          case (ats @ AddTransactionState(None, _, _, _), _) => ats
          case (ats @ AddTransactionState(Some(acc), errs, parties, archivedIds), (nodeId, node)) =>
            // If some node requires a contract, check that we have that contract, and check that that contract is not
            // created after the current let.
            def contractCheck(
                cid: AbsoluteContractId,
                predType: PredicateType): Option[SequencingError] =
              acc lookupContractLet cid match {
                case Some(Let(otherContractLet)) =>
                  // Existing active contract, check its LET
                  if (otherContractLet.isAfter(let)) {
                    Some(TimeBeforeError(cid, otherContractLet, let, predType))
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
                  Some(InactiveDependencyError(cid, predType))
              }

            node match {
              case nf: N.NodeFetch[AbsoluteContractId] =>
                val nodeParties = nf.signatories
                  .union(nf.stakeholders)
                  .union(nf.actingParties.getOrElse(Set.empty))
                val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(nf.coid)
                AddTransactionState(
                  Some(acc),
                  contractCheck(absCoid, Fetch).fold(errs)(errs + _),
                  parties.union(nodeParties),
                  archivedIds
                )
              case nc: N.NodeCreate.WithTxValue[AbsoluteContractId] =>
                val nodeParties = nc.signatories
                  .union(nc.stakeholders)
                  .union(nc.key.map(_.maintainers).getOrElse(Set.empty))
                val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(nc.coid)
                val withoutStakeHolders = localDivulgence
                  .getOrElse(nodeId, Set.empty) diff nc.stakeholders
                val withStakeHolders = localDivulgence
                  .getOrElse(nodeId, Set.empty)

                assert(withoutStakeHolders == withStakeHolders)

                val activeContract = ActiveContract(
                  id = absCoid,
                  let = let,
                  transactionId = transactionId,
                  eventId = nodeId,
                  workflowId = workflowId,
                  contract = nc.coinst.mapValue(
                    _.mapContractId(SandboxEventIdFormatter.makeAbsCoid(transactionId))),
                  witnesses = disclosure(nodeId),
                  // we need to `getOrElse` here because the `Nid` might include absolute
                  // contract ids, and those are never present in the local disclosure.
                  divulgences = (localDivulgence
                    .getOrElse(nodeId, Set.empty) diff nc.stakeholders).toList
                    .map(p => p -> transactionId)
                    .toMap,
                  key = nc.key,
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
                    val gk = GlobalKey(activeContract.contract.template, key.key)
                    if (acc keyExists gk) {
                      AddTransactionState(
                        None,
                        errs + DuplicateKey(gk),
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
                val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(ne.targetCoid)
                ats.copy(
                  errs = contractCheck(absCoid, Exercise).fold(errs)(errs + _),
                  acc = Some(if (ne.consuming) {
                    acc.removeContract(absCoid)
                  } else {
                    acc
                  }),
                  parties = parties.union(nodeParties),
                  archivedIds = if (ne.consuming) archivedIds + absCoid else archivedIds
                )
              case nlkup: N.NodeLookupByKey.WithTxValue[AbsoluteContractId] =>
                // NOTE(FM) we do not need to check anything, since
                // * this is a lookup, it does not matter if the key exists or not
                // * if the key exists, we have it as an internal invariant that the backing coid exists.
                ats
            }
        }

    val divulgedContractIds = globalDivulgence -- st.archivedIds
    st.mapAcs(
        _ divulgeAlreadyCommittedContracts (transactionId, divulgedContractIds, divulgedContracts))
      .mapAcs(_ addParties st.parties)
      .result
  }

}
