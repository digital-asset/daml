// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.example

import com.digitalasset.daml.lf.data.Ref.SimpleString
import com.digitalasset.daml.lf.data.Time.Timestamp
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
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.digitalasset.ledger.backend.api.v1.TransactionSubmission
import com.digitalasset.platform.sandbox.config.DamlPackageContainer

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
      submission: TransactionSubmission,
      contracts: Map[AbsoluteContractId, AbsContractInst],
      packages: DamlPackageContainer): ValidationResult = {

    def stepResult[A](result: Result[A]): Either[Error, A] = {
      result match {
        case ResultNeedKey(_, _) =>
          sys.error("Contract Keys are not implemented yet")
        case ResultNeedPackage(packageId, resume) =>
          stepResult(resume(packages.getPackage(packageId)))
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
    val submitter = SimpleString.assertFromString(submission.submitter)
    val tx = submission.transaction

    val result = for {
      // check the transaction is conformant
      result <- stepResult(
        engine.validate(tx, Timestamp.assertFromInstant(submission.ledgerEffectiveTime)))
      // check transaction is well-authorized
      blinding <- Blinding.checkAuthorizationAndBlind(
        engine.ledgerFeatureFlags(),
        tx,
        Set(submitter))
    } yield (blinding, result)

    result match {
      case Right(_) => Success
      case Left(err) => Failure(err.detailMsg)
    }
  }

}
