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

case class ActiveContracts(
    contracts: Map[AbsoluteContractId, ActiveContract],
    keys: Map[GlobalKey, AbsoluteContractId]) {

  /** adds a transaction to the ActiveContracts, make sure that there are no double spends or
    * timing errors. this check is leveraged to achieve higher concurrency, see LedgerState
    */
  def addTransaction[Nid](
      let: Instant,
      transactionId: String,
      workflowId: String,
      transaction: GenTransaction[Nid, AbsoluteContractId, VersionedValue[AbsoluteContractId]],
      explicitDisclosure: Relation[Nid, Ref.Party])
    : Either[Set[SequencingError], ActiveContracts] = {
    val st =
      transaction.fold[AddTransactionState](GenTransaction.TopDown, AddTransactionState(this)) {
        case (ats @ AddTransactionState(None, _), _) => ats
        case (ats @ AddTransactionState(Some(acc), errs), (nodeId, node)) =>
          // if some node requires a contract, check that we have that contract, and check that that contract is not
          // created after the current let.
          def contractCheck(
              cid: AbsoluteContractId,
              predType: PredicateType): Option[SequencingError] =
            acc.contracts.get(cid) match {
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
              ats.addContract(absCoid, activeContract)
            case ne: N.NodeExercises[Nid, AbsoluteContractId, VersionedValue[AbsoluteContractId]] =>
              val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(ne.targetCoid)
              ats.copy(
                errs = contractCheck(absCoid, Exercise).fold(errs)(errs + _),
                acc = Some(if (ne.consuming) {
                  AddTransactionAcc(acc.contracts - absCoid, acc.contracts(absCoid).key match {
                    case None => acc.keys
                    case Some(key) => acc.keys - GlobalKey(ne.templateId, key.key)
                  })
                } else {
                  acc
                })
              )
            case nlkup: N.NodeLookupByKey[AbsoluteContractId, VersionedValue[AbsoluteContractId]] =>
              // NOTE(FM) we do not need to check anything, since
              // * this is a lookup, it does not matter if the key exists or not
              // * if the key exists, we have it as an internal invariant that the backing coid exists.
              ats
          }
      }

    st.result
  }

}

sealed abstract class ActiveContractsAction

final case class ActiveContractsAdd(abcCoid: AbsoluteContractId, contract: ActiveContract)
    extends ActiveContractsAction

final case class ActiveContractsRemove(abcCoid: AbsoluteContractId) extends ActiveContractsAction

/** If some node requires a contract, check that we have that contract, and check that that contract is not created after the given transaction. */
final case class ActiveContractsCheck(cid: AbsoluteContractId, let: Instant)
    extends ActiveContractsAction

object ActiveContracts {

  private case class AddTransactionAcc(
      contracts: Map[AbsoluteContractId, ActiveContract],
      keys: Map[GlobalKey, AbsoluteContractId])

  private case class AddTransactionState(
      acc: Option[AddTransactionAcc],
      errs: Set[SequencingError]) {
    def addContract(coid: AbsoluteContractId, contract: ActiveContract): AddTransactionState =
      acc match {
        case None => this
        case Some(AddTransactionAcc(contracts, keys)) =>
          contract.key match {
            case None =>
              this.copy(acc = Some(AddTransactionAcc(contracts + (coid -> contract), keys)))
            case Some(key) =>
              val gk = GlobalKey(contract.contract.template, key.key)
              if (keys.contains(gk)) {
                AddTransactionState(None, errs + DuplicateKey(gk))
              } else {
                this.copy(acc =
                  Some(AddTransactionAcc(contracts + (coid -> contract), keys + (gk -> coid))))
              }
          }
      }

    def result: Either[Set[SequencingError], ActiveContracts] = {
      acc match {
        case None =>
          if (errs.isEmpty) {
            sys.error(s"IMPOSSIBLE no acc and no errors either!")
          }
          Left(errs)
        case Some(acc_) =>
          if (errs.isEmpty) {
            Right(ActiveContracts(acc_.contracts, acc_.keys))
          } else {
            Left(errs)
          }
      }
    }
  }

  private object AddTransactionState {
    def apply(acs: ActiveContracts): AddTransactionState =
      AddTransactionState(Some(AddTransactionAcc(acs.contracts, acs.keys)), Set())
  }

  case class ActiveContract(
      let: Instant, // time when the contract was committed
      transactionId: String, // transaction id where the contract originates
      workflowId: String, // workflow id from where the contract originates
      contract: ContractInst[VersionedValue[AbsoluteContractId]],
      witnesses: Set[Ref.Party],
      key: Option[KeyWithMaintainers[VersionedValue[AbsoluteContractId]]])

  def empty: ActiveContracts = ActiveContracts(Map(), Map())

  /**
    * Processes a transaction, returning a list of actions that a persistence layer needs to perform.
    * The reason why a list of actions is returned is that some persistence layers may want to batch these
    * operations, or execute them asynchronously.
    */
  def addAcsActions[Nid](
      let: Instant,
      transactionId: String,
      workflowId: String,
      transaction: GenTransaction[Nid, AbsoluteContractId, VersionedValue[AbsoluteContractId]],
      explicitDisclosure: Relation[Nid, Ref.Party],
  ): List[ActiveContractsAction] = {
    type Acc = List[ActiveContractsAction]
    transaction.fold[Acc](GenTransaction.TopDown, List.empty) {
      case (changes, (nodeId, node)) =>
        node match {
          case nf: N.NodeFetch[AbsoluteContractId] =>
            val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(nf.coid)
            changes :+ ActiveContractsCheck(absCoid, let)
          case nc: N.NodeCreate[AbsoluteContractId, VersionedValue[AbsoluteContractId]] =>
            val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(nc.coid)
            val activeContract = ActiveContract(
              let,
              transactionId,
              workflowId,
              nc.coinst.mapValue(
                _.mapContractId(SandboxEventIdFormatter.makeAbsCoid(transactionId))),
              explicitDisclosure(nodeId).intersect(nc.stakeholders),
              None
            )
            changes :+ ActiveContractsAdd(absCoid, activeContract)
          case ne: N.NodeExercises[Nid, AbsoluteContractId, VersionedValue[AbsoluteContractId]] =>
            val absCoid = SandboxEventIdFormatter.makeAbsCoid(transactionId)(ne.targetCoid)
            if (ne.consuming) {
              changes :+ ActiveContractsCheck(absCoid, let) :+ ActiveContractsRemove(absCoid)
            } else {
              changes :+ ActiveContractsCheck(absCoid, let)
            }
          case _: N.NodeLookupByKey[AbsoluteContractId, VersionedValue[AbsoluteContractId]] =>
            sys.error("Contract Keys are not implemented yet")
        }
    }
  }

}
