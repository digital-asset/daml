// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package speedy

import data.Ref.PackageId
import data.Time
import SResult._
import com.daml.lf.language.{Ast, PackageInterface}
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.validation.{Validation, ValidationError}
import com.daml.logging.LoggingContext
import transaction.{GlobalKeyWithMaintainers, SubmittedTransaction}
import value.Value
import scalautil.Statement.discard

import scala.annotation.tailrec

private[speedy] object SpeedyTestLib {

  sealed abstract class Error(msg: String) extends RuntimeException("SpeedyTestLib.run:" + msg)

  final case object UnexpectedSResultNeedTime extends Error("unexpected SResultNeedTime")

  final case class UnknownContract(contractId: Value.ContractId)
      extends Error(s"unknown contract '$contractId'")

  final case class UnknownPackage(packageId: PackageId)
      extends Error(s"unknown package '$packageId'")

  final case object UnexpectedSResultScenarioX extends Error("unexpected SResultScenarioX")

  implicit def loggingContext: LoggingContext = LoggingContext.ForTesting

  @throws[SError.SErrorCrash]
  def run(
      machine: Speedy.Machine,
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, SValue] = {
    runTx(machine, getPkg, getContract, getKey, getTime) match {
      case Left(e) => Left(e)
      case Right(SResultFinal(v, _)) => Right(v)
    }
  }

  @throws[SError.SErrorCrash]
  def runTx(
      machine: Speedy.Machine,
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, SResultFinal] = {
    @tailrec
    def loop: Either[SError.SError, SResultFinal] = {
      machine.run() match {
        case SResultNeedTime(callback) =>
          getTime.lift(()) match {
            case Some(value) =>
              callback(value)
              loop
            case None =>
              throw UnexpectedSResultNeedTime
          }
        case SResultNeedContract(contractId, _, callback) =>
          getContract.lift(contractId) match {
            case Some(value) =>
              callback(value.unversioned)
              loop
            case None =>
              throw UnknownContract(contractId)
          }
        case SResultNeedPackage(pkg, _, callback) =>
          getPkg.lift(pkg) match {
            case Some(value) =>
              callback(value)
              loop
            case None =>
              throw UnknownPackage(pkg)
          }
        case SResultNeedKey(key, _, callback) =>
          discard(callback(getKey.lift(key)))
          loop
        case fv: SResultFinal =>
          Right(fv)
        case SResultError(err) =>
          Left(err)
        case _: SResultFinal | _: SResultScenarioGetParty | _: SResultScenarioPassTime |
            _: SResultScenarioSubmit =>
          throw UnexpectedSResultScenarioX
      }
    }

    loop
  }

  @throws[SError.SErrorCrash]
  def buildTransaction(
      machine: Speedy.Machine,
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, SubmittedTransaction] =
    runTx(machine, getPkg, getContract, getKey, getTime) match {
      case Right(SResultFinal(_, None)) =>
        throw SError.SErrorCrash("buildTransaction", "unexpected missing transaction")
      case Right(SResultFinal(_, Some(ctx))) =>
        ctx match {
          case PartialTransaction.Result(tx, _, _, _, _) =>
            Right(tx)
        }
      case Left(err) =>
        Left(err)
    }

  @throws[ValidationError]
  def typeAndCompile(pkgs: Map[PackageId, Ast.Package]): PureCompiledPackages = {
    Validation.unsafeCheckPackages(PackageInterface(pkgs), pkgs)
    PureCompiledPackages.assertBuild(
      pkgs,
      Compiler.Config.Dev.copy(stacktracing = Compiler.FullStackTrace),
    )
  }

  @throws[ValidationError]
  def typeAndCompile[X](pkg: Ast.Package)(implicit
      parserParameter: ParserParameters[X]
  ): PureCompiledPackages =
    typeAndCompile(Map(parserParameter.defaultPackageId -> pkg))
}
