// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref
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

case class ActiveContracts(
    contracts: Map[AbsoluteContractId, ActiveContract],
    keys: Map[GlobalKey, AbsoluteContractId]) {

  private def lookupContract(acs: ActiveContracts, cid: AbsoluteContractId) = acs.contracts.get(cid)

  private def keyExists(acs: ActiveContracts, key: GlobalKey) = acs.keys.contains(key)

  private def addContract(
      acs: ActiveContracts,
      cid: AbsoluteContractId,
      c: ActiveContract,
      keyO: Option[GlobalKey]) = keyO match {
    case None => acs.copy(contracts = acs.contracts + (cid -> c))
    case Some(key) =>
      acs.copy(contracts = acs.contracts + (cid -> c), keys = acs.keys + (key -> cid))
  }

  private def removeContract(
      acs: ActiveContracts,
      cid: AbsoluteContractId,
      keyO: Option[GlobalKey]) = keyO match {
    case None => acs.copy(contracts = acs.contracts - cid)
    case Some(key) => acs.copy(contracts = acs.contracts - cid, keys = acs.keys - key)
  }

  private def implicitlyDisclose(global: Relation[AbsoluteContractId, Ref.Party]): ActiveContracts =
    if (global.nonEmpty)
      copy(
        contracts = contracts ++
          contracts.intersectWith(global) { (ac, parties) =>
            ac copy (witnesses = ac.witnesses union parties)
          })
    else this

  private val acManager =
    new ActiveContractsManager(this)(
      lookupContract,
      keyExists,
      addContract,
      removeContract,
      _ implicitlyDisclose _)

  /** adds a transaction to the ActiveContracts, make sure that there are no double spends or
    * timing errors. this check is leveraged to achieve higher concurrency, see LedgerState
    */
  def addTransaction[Nid](
      let: Instant,
      transactionId: String,
      workflowId: String,
      transaction: GenTransaction[Nid, AbsoluteContractId, VersionedValue[AbsoluteContractId]],
      explicitDisclosure: Relation[Nid, Ref.Party],
      globalImplicitDisclosure: Relation[AbsoluteContractId, Ref.Party]
  ): Either[Set[SequencingError], ActiveContracts] =
    acManager.addTransaction(
      let,
      transactionId,
      workflowId,
      transaction,
      explicitDisclosure,
      globalImplicitDisclosure)

}

class ActiveContractsManager[ACS](initialState: => ACS)(
    lookupContract: (ACS, AbsoluteContractId) => Option[ActiveContract],
    keyExists: (ACS, GlobalKey) => Boolean,
    addContract: (ACS, AbsoluteContractId, ActiveContract, Option[GlobalKey]) => ACS,
    removeContract: (ACS, AbsoluteContractId, Option[GlobalKey]) => ACS,
    implicitlyDisclose: (ACS, Relation[AbsoluteContractId, Ref.Party]) => ACS) {

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
      transactionId: String,
      workflowId: String,
      transaction: GenTransaction[Nid, AbsoluteContractId, VersionedValue[AbsoluteContractId]],
      explicitDisclosure: Relation[Nid, Ref.Party],
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
              lookupContract(acc, cid) match {
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
              case nc: N.NodeCreate[AbsoluteContractId, VersionedValue[AbsoluteContractId]] =>
                val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(nc.coid)
                val activeContract = ActiveContract(
                  let = let,
                  transactionId = transactionId,
                  workflowId = workflowId,
                  contract = nc.coinst.mapValue(
                    _.mapContractId(SandboxEventIdFormatter.makeAbsCoid(transactionId))),
                  witnesses = explicitDisclosure(nodeId).intersect(nc.stakeholders),
                  key = nc.key
                )
                activeContract.key match {
                  case None =>
                    ats.copy(acc = Some(addContract(acc, absCoid, activeContract, None)))
                  case Some(key) =>
                    val gk = GlobalKey(activeContract.contract.template, key.key)
                    if (keyExists(acc, gk)) {
                      AddTransactionState(None, errs + DuplicateKey(gk))
                    } else {
                      ats.copy(acc = Some(addContract(acc, absCoid, activeContract, Some(gk))))
                    }
                }
              case ne: N.NodeExercises[
                    Nid,
                    AbsoluteContractId,
                    VersionedValue[AbsoluteContractId]] =>
                val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(ne.targetCoid)
                ats.copy(
                  errs = contractCheck(absCoid, Exercise).fold(errs)(errs + _),
                  acc = Some(if (ne.consuming) {
                    removeContract(acc, absCoid, lookupContract(acc, absCoid).flatMap(_.key) match {
                      case None => None
                      case Some(key) => Some(GlobalKey(ne.templateId, key.key))
                    })
                  } else {
                    acc
                  })
                )
              case nlkup: N.NodeLookupByKey[
                    AbsoluteContractId,
                    VersionedValue[AbsoluteContractId]] =>
                // NOTE(FM) we do not need to check anything, since
                // * this is a lookup, it does not matter if the key exists or not
                // * if the key exists, we have it as an internal invariant that the backing coid exists.
                ats
            }
        }

    st.mapAcs(implicitlyDisclose(_, globalImplicitDisclosure)).result
  }

}

object ActiveContracts {

  case class ActiveContract(
      let: Instant, // time when the contract was committed
      transactionId: String, // transaction id where the contract originates
      workflowId: String, // workflow id from where the contract originates
      contract: ContractInst[VersionedValue[AbsoluteContractId]],
      witnesses: Set[Ref.Party],
      key: Option[KeyWithMaintainers[VersionedValue[AbsoluteContractId]]])

  def empty: ActiveContracts = ActiveContracts(Map(), Map())

}
