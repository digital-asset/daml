// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.TransactionId
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.Node.{GlobalKey, KeyWithMaintainers}
import com.digitalasset.daml.lf.transaction.{GenTransaction, Node => N}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter
import com.digitalasset.platform.sandbox.stores.ActiveContracts._
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
import scalaz.syntax.std.map._

case class ActiveContractsInMemory(
    contracts: Map[AbsoluteContractId, ActiveContract],
    keys: Map[GlobalKey, AbsoluteContractId])
    extends ActiveContracts[ActiveContractsInMemory] {

  override def lookupContract(cid: AbsoluteContractId) = contracts.get(cid)

  override def keyExists(key: GlobalKey) = keys.contains(key)

  override def addContract(cid: AbsoluteContractId, c: ActiveContract, keyO: Option[GlobalKey]) =
    keyO match {
      case None => copy(contracts = contracts + (cid -> c))
      case Some(key) =>
        copy(contracts = contracts + (cid -> c), keys = keys + (key -> cid))
    }

  override def removeContract(cid: AbsoluteContractId, keyO: Option[GlobalKey]) = keyO match {
    case None => copy(contracts = contracts - cid)
    case Some(key) => copy(contracts = contracts - cid, keys = keys - key)
  }

  override def divulgeAlreadyCommittedContract(
      transactionId: String,
      global: Relation[AbsoluteContractId, Ref.Party]): ActiveContractsInMemory =
    if (global.nonEmpty)
      copy(
        contracts = contracts ++
          contracts.intersectWith(global) { (ac, parties) =>
            ac copy (divulgences = parties.foldLeft(ac.divulgences)((m, e) =>
              if (m.contains(e)) m else m + (e -> transactionId)))
          })
    else this

  private val acManager =
    new ActiveContractsManager(this)

  /** adds a transaction to the ActiveContracts, make sure that there are no double spends or
    * timing errors. this check is leveraged to achieve higher concurrency, see LedgerState
    */
  def addTransaction[Nid](
      let: Instant,
      transactionId: TransactionId,
      workflowId: String,
      transaction: GenTransaction.WithTxValue[Nid, AbsoluteContractId],
      explicitDisclosure: Relation[Nid, Ref.Party],
      localImplicitDisclosure: Relation[Nid, Ref.Party],
      globalImplicitDisclosure: Relation[AbsoluteContractId, Ref.Party]
  ): Either[Set[SequencingError], ActiveContractsInMemory] =
    acManager.addTransaction(
      let,
      transactionId,
      workflowId,
      transaction,
      explicitDisclosure,
      localImplicitDisclosure,
      globalImplicitDisclosure)

}

object ActiveContractsInMemory {
  def empty: ActiveContractsInMemory = ActiveContractsInMemory(Map(), Map())
}

class ActiveContractsManager[ACS](initialState: => ACS)(implicit ACS: ACS => ActiveContracts[ACS]) {

  private case class AddTransactionState(acc: Option[ACS], errs: Set[SequencingError]) {

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
      AddTransactionState(Some(acs), Set())
  }

  /**
    * A higher order function to update an abstract active contract set (ACS) with the effects of the given transaction.
    * Makes sure that there are no double spends or timing errors.
    */
  def addTransaction[Nid](
      let: Instant,
      transactionId: TransactionId,
      workflowId: String,
      transaction: GenTransaction.WithTxValue[Nid, AbsoluteContractId],
      explicitDisclosure: Relation[Nid, Ref.Party],
      localImplicitDisclosure: Relation[Nid, Ref.Party],
      globalImplicitDisclosure: Relation[AbsoluteContractId, Ref.Party])
    : Either[Set[SequencingError], ACS] = {
    val st =
      transaction
        .fold[AddTransactionState](GenTransaction.TopDown, AddTransactionState(initialState)) {
          case (ats @ AddTransactionState(None, _), _) => ats
          case (ats @ AddTransactionState(Some(acc), errs), (nodeId, node)) =>
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
                val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(nf.coid)
                AddTransactionState(Some(acc), contractCheck(absCoid, Fetch).fold(errs)(errs + _))
              case nc: N.NodeCreate.WithTxValue[AbsoluteContractId] =>
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
                  key = nc.key
                )
                activeContract.key match {
                  case None =>
                    ats.copy(acc = Some(acc.addContract(absCoid, activeContract, None)))
                  case Some(key) =>
                    val gk = GlobalKey(activeContract.contract.template, key.key)
                    if (acc keyExists gk) {
                      AddTransactionState(None, errs + DuplicateKey(gk))
                    } else {
                      ats.copy(acc = Some(acc.addContract(absCoid, activeContract, Some(gk))))
                    }
                }
              case ne: N.NodeExercises.WithTxValue[Nid, AbsoluteContractId] =>
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
                  })
                )
              case nlkup: N.NodeLookupByKey.WithTxValue[AbsoluteContractId] =>
                // NOTE(FM) we do not need to check anything, since
                // * this is a lookup, it does not matter if the key exists or not
                // * if the key exists, we have it as an internal invariant that the backing coid exists.
                ats
            }
        }

    st.mapAcs(_ divulgeAlreadyCommittedContract (transactionId, globalImplicitDisclosure)).result
  }

}

trait ActiveContracts[+Self] { this: ActiveContracts[Self] =>
  def lookupContract(cid: AbsoluteContractId): Option[ActiveContract]
  def keyExists(key: GlobalKey): Boolean
  def addContract(cid: AbsoluteContractId, c: ActiveContract, keyO: Option[GlobalKey]): Self
  def removeContract(cid: AbsoluteContractId, keyO: Option[GlobalKey]): Self

  /** Note that this method is about disclosing contracts _that have already been
    * committed_. Implementors of `ActiveContracts` must take care to also store
    * divulgence information already present in `ActiveContract#divulgences` in the `addContract`
    * method.
    */
  def divulgeAlreadyCommittedContract(
      transactionId: String,
      global: Relation[AbsoluteContractId, Ref.Party]): Self
}

object ActiveContracts {

  case class ActiveContract(
      let: Instant, // time when the contract was committed
      transactionId: String, // transaction id where the contract originates
      workflowId: String, // workflow id from where the contract originates
      contract: ContractInst[VersionedValue[AbsoluteContractId]],
      witnesses: Set[Ref.Party],
      divulgences: Map[Ref.Party, String], // for each party, the transaction id at which the contract was divulged
      key: Option[KeyWithMaintainers[VersionedValue[AbsoluteContractId]]])

}
