// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1.impl.reference

import com.daml.ledger.participant.state.v1.{SubmittedTransaction, SubmitterInfo, TransactionMeta}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.engine.{
  Blinding,
  ContractNotFound,
  Engine,
  Error,
  Result,
  ResultDone,
  ResultError,
  ResultNeedContract,
  ResultNeedKey,
  ResultNeedPackage
}
import com.digitalasset.daml.lf.lfpackage.Ast
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.digitalasset.daml_lf.DamlLf.Archive

/**
  * This module provides functions for transaction validation.
  */
object Validation {

  type AbsContractInst = ContractInst[VersionedValue[AbsoluteContractId]]

  sealed trait ValidationResult
  final case object Success extends ValidationResult
  final case class Failure(msg: String) extends ValidationResult

  /*
   * Runs a pre-commit validation of evaluated transactions in the DAMLe execution context
   */
  def validate(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
      contracts: Map[AbsoluteContractId, AbsContractInst],
      packages: Map[PackageId, (Ast.Package, Archive)]): ValidationResult = {

    def stepResult[A](result: Result[A]): Either[Error, A] = {
      result match {
        case ResultNeedKey(_, _) =>
          sys.error("Contract Keys are not implemented yet")
        case ResultNeedPackage(packageId, resume) =>
          stepResult(resume(packages.get(packageId).map(_._1)))
        case ResultDone(completedResult) => Right(completedResult)
        case ResultNeedContract(acoid, resume) =>
          contracts.get(acoid) match {
            case Some(contractData) =>
              stepResult(resume(Some(contractData)))
            case None =>
              stepResult(ResultError(ContractNotFound(acoid)))
          }
        case ResultError(err) => Left(err)
      }
    }

    val engine = new Engine()
    val submitter = submitterInfo.submitter

    val result = for {
      // check the transaction is conformant
      result <- stepResult(engine.validate(transaction, transactionMeta.ledgerEffectiveTime))
      // check transaction is well-authorized
      blinding <- Blinding.checkAuthorizationAndBlind(
        engine.ledgerFeatureFlags(),
        transaction,
        Set(submitter))
    } yield (blinding, result)

    result match {
      case Right(_) => Success
      case Left(err) => Failure(err.detailMsg)
    }
  }

}
