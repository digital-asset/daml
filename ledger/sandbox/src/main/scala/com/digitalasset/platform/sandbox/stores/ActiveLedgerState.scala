// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.{Party, TransactionIdString}
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.Node.{GlobalKey, KeyWithMaintainers}
import com.digitalasset.daml.lf.transaction.{GenTransaction, Node => N}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.digitalasset.ledger.WorkflowId
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

class ActiveContractsManager[ACS](initialState: => ACS)(
    implicit ACS: ACS => ActiveLedgerState[ACS]) {

  private case class AddTransactionState(
      acc: Option[ACS],
      errs: Set[SequencingError],
      parties: Set[Party]) {

    def mapAcs(f: ACS => ACS): AddTransactionState = copy(acc = acc map f)

    def result: Either[Set[SequencingError], ACS] = {
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
    def apply(acs: ACS): AddTransactionState =
      AddTransactionState(Some(acs), Set(), Set.empty)
  }

  /**
    * A higher order function to update an abstract active contract set (ACS) with the effects of the given transaction.
    * Makes sure that there are no double spends or timing errors.
    */
  def addTransaction[Nid](
      let: Instant,
      transactionId: TransactionIdString,
      workflowId: Option[WorkflowId],
      transaction: GenTransaction.WithTxValue[Nid, AbsoluteContractId],
      explicitDisclosure: Relation[Nid, Party],
      localImplicitDisclosure: Relation[Nid, Party],
      globalImplicitDisclosure: Relation[AbsoluteContractId, Party])
    : Either[Set[SequencingError], ACS] = {
    val st =
      transaction
        .fold[AddTransactionState](GenTransaction.TopDown, AddTransactionState(initialState)) {
          case (ats @ AddTransactionState(None, _, _), _) => ats
          case (ats @ AddTransactionState(Some(acc), errs, parties), (nodeId, node)) =>
            // if some node requires a contract, check that we have that contract, and check that that contract is not
            // created after the current let.
            def contractCheck(
                cid: AbsoluteContractId,
                predType: PredicateType): Option[SequencingError] =
              acc lookupContract cid match {
                case None => Some(InactiveDependencyError(cid, predType))
                case Some(otherTx) =>
                  if (otherTx.let.isAfter(let)) {
                    Some(TimeBeforeError(cid, otherTx.let, let, predType))
                  } else {
                    None
                  }
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
                  parties.union(nodeParties)
                )
              case nc: N.NodeCreate.WithTxValue[AbsoluteContractId] =>
                val nodeParties = nc.signatories
                  .union(nc.stakeholders)
                  .union(nc.key.map(_.maintainers).getOrElse(Set.empty))
                val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(nc.coid)
                val activeContract = ActiveContract(
                  let = let,
                  transactionId = transactionId,
                  workflowId = workflowId,
                  contract = nc.coinst.mapValue(
                    _.mapContractId(SandboxEventIdFormatter.makeAbsCoid(transactionId))),
                  witnesses = explicitDisclosure(nodeId),
                  // we need to `getOrElse` here because the `Nid` might include absolute
                  // contract ids, and those are never present in the local disclosure.
                  divulgences = (localImplicitDisclosure
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
                    ats.copy(acc = Some(acc.addContract(absCoid, activeContract, None)))
                  case Some(key) =>
                    val gk = GlobalKey(activeContract.contract.template, key.key)
                    if (acc keyExists gk) {
                      AddTransactionState(None, errs + DuplicateKey(gk), parties.union(nodeParties))
                    } else {
                      ats.copy(
                        acc = Some(acc.addContract(absCoid, activeContract, Some(gk))),
                        parties = parties.union(nodeParties)
                      )
                    }
                }
              case ne: N.NodeExercises.WithTxValue[Nid, AbsoluteContractId] =>
                val nodeParties = ne.signatories
                  .union(ne.stakeholders)
                  .union(ne.actingParties)
                val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(ne.targetCoid)
                ats.copy(
                  errs = contractCheck(absCoid, Exercise).fold(errs)(errs + _),
                  acc = Some(if (ne.consuming) {
                    acc.removeContract(absCoid, (acc lookupContract absCoid).flatMap(_.key) match {
                      case None => None
                      case Some(key) => Some(GlobalKey(ne.templateId, key.key))
                    })
                  } else {
                    acc
                  }),
                  parties = parties.union(nodeParties)
                )
              case nlkup: N.NodeLookupByKey.WithTxValue[AbsoluteContractId] =>
                // NOTE(FM) we do not need to check anything, since
                // * this is a lookup, it does not matter if the key exists or not
                // * if the key exists, we have it as an internal invariant that the backing coid exists.
                ats
            }
        }

    st.mapAcs(_ divulgeAlreadyCommittedContract (transactionId, globalImplicitDisclosure))
      .mapAcs(_ addParties st.parties)
      .result
  }

}

/**
  * An abstract representation of the active ledger state:
  * - Active contracts
  * - Divulged contracts
  * - Contract keys
  * - Known parties
  *
  * The active ledger state is used for validating transactions,
  * see [[ActiveContractsManager]].
  *
  * The active ledger state could be derived from the transaction stream,
  * we keep track of it explicitly for performance reasons.
  */
trait ActiveLedgerState[+Self] { this: ActiveLedgerState[Self] =>

  /** Callback to query a contract, used for transaction validation */
  def lookupContract(cid: AbsoluteContractId): Option[ActiveContract]

  /** Callback to query a contract key, used for transaction validation */
  def keyExists(key: GlobalKey): Boolean

  /** Called when a new contract is created */
  def addContract(cid: AbsoluteContractId, c: ActiveContract, keyO: Option[GlobalKey]): Self

  /** Called when the given contract is archived */
  def removeContract(cid: AbsoluteContractId, keyO: Option[GlobalKey]): Self

  /** Called once for each transaction with the set of parties found in that transaction.
    * As the sandbox has an open world of parties, any party name mentioned in a transaction
    * will implicitly add that name to the list of known parties.
    */
  def addParties(parties: Set[Party]): Self

  /** Note that this method is about disclosing contracts _that have already been
    * committed_. Implementors of `ActiveContracts` must take care to also store
    * divulgence information already present in `ActiveContract#divulgences` in the `addContract`
    * method.
    */
  def divulgeAlreadyCommittedContract(
      transactionId: TransactionIdString,
      global: Relation[AbsoluteContractId, Party]): Self
}

object ActiveLedgerState {

  case class ActiveContract(
      let: Instant, // time when the contract was committed
      transactionId: TransactionIdString, // transaction id where the contract originates
      workflowId: Option[WorkflowId], // workflow id from where the contract originates
      contract: ContractInst[VersionedValue[AbsoluteContractId]],
      witnesses: Set[Party],
      divulgences: Map[Party, TransactionIdString], // for each party, the transaction id at which the contract was divulged
      key: Option[KeyWithMaintainers[VersionedValue[AbsoluteContractId]]],
      signatories: Set[Party],
      observers: Set[Party],
      agreementText: String)

}
