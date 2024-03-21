// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package speedy

import data.Ref.PackageId
import data.Time
import SResult._
import com.daml.lf.data.Ref.Party
import com.daml.lf.language.{Ast, LanguageMajorVersion, PackageInterface}
import com.daml.lf.speedy.Speedy.{ContractInfo, UpdateMachine}
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.validation.{Validation, ValidationError}
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import transaction.{GlobalKey, GlobalKeyWithMaintainers, SubmittedTransaction}
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
      machine: Speedy.Machine[Question.Update],
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, SValue] = {
    runCollectRequests(machine, getPkg, getContract, getKey, getTime) match {
      case Left(e) => Left(e)
      case Right((v, _, _)) => Right(v)
    }
  }

  @throws[SError.SErrorCrash]
  def buildTransaction(
      machine: Speedy.UpdateMachine,
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, SubmittedTransaction] =
    buildTransactionCollectRequests(machine, getPkg, getContract, getKey, getTime) match {
      case Right((tx, _, _)) =>
        Right(tx)
      case Left(err) =>
        Left(err)
    }

  case class AuthRequest(
      holding: Set[Party],
      requesting: Set[Party],
  )

  case class UpgradeVerificationRequest(
      coid: ContractId,
      signatories: Set[Party],
      observers: Set[Party],
      keyOpt: Option[GlobalKeyWithMaintainers],
  )

  @throws[SError.SErrorCrash]
  def buildTransactionCollectRequests(
      machine: Speedy.UpdateMachine,
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[
    SError.SError,
    (SubmittedTransaction, List[AuthRequest], List[UpgradeVerificationRequest]),
  ] =
    runCollectRequests(machine, getPkg, getContract, getKey, getTime) match {
      case Right((_, authRequests, upgradeVerificationrequests)) =>
        machine.finish.map(_.tx) match {
          case Left(err) =>
            Left(err)
          case Right(tx) =>
            Right((tx, authRequests, upgradeVerificationrequests))
        }
      case Left(err) =>
        Left(err)
    }

  @throws[SError.SErrorCrash]
  def runCollectRequests(
      machine: Speedy.Machine[Question.Update],
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, (SValue, List[AuthRequest], List[UpgradeVerificationRequest])] = {

    var authRequests: List[AuthRequest] = List.empty
    var upgradeVerificationRequests: List[UpgradeVerificationRequest] = List.empty

    def onQuestion(question: Question.Update): Unit = question match {
      case Question.Update.NeedAuthority(holding @ _, requesting @ _, callback) =>
        authRequests = AuthRequest(holding, requesting) :: authRequests
        callback()
      case Question.Update.NeedTime(callback) =>
        getTime.lift(()) match {
          case Some(value) =>
            callback(value)
          case None =>
            throw UnexpectedSResultNeedTime
        }
      case Question.Update.NeedContract(contractId, _, callback) =>
        getContract.lift(contractId) match {
          case Some(value) =>
            callback(value.unversioned)
          case None =>
            throw UnknownContract(contractId)
        }
      case Question.Update.NeedUpgradeVerification(
            coid,
            signatories,
            observers,
            keyOpt,
            callback,
          ) =>
        upgradeVerificationRequests = UpgradeVerificationRequest(
          coid,
          signatories,
          observers,
          keyOpt,
        ) :: upgradeVerificationRequests
        callback(None)

      case Question.Update.NeedPackage(pkg, _, callback) =>
        getPkg.lift(pkg) match {
          case Some(value) =>
            callback(value)
          case None =>
            throw UnknownPackage(pkg)
        }
      case Question.Update.NeedKey(key, _, callback) =>
        discard(callback(getKey.lift(key)))
      case Question.Update.NeedPackageId(_, _, _) =>
        // TODO https://github.com/digital-asset/daml/issues/16154 (dynamic-exercise)
        ???
    }
    runTxQ(onQuestion, machine) match {
      case Left(e) => Left(e)
      case Right(fv) => Right((fv, authRequests.reverse, upgradeVerificationRequests.reverse))
    }
  }

  @throws[SError.SErrorCrash]
  def runTxQ[Q](
      onQuestion: Q => Unit,
      machine: Speedy.Machine[Q],
  ): Either[SError.SError, SValue] = {
    @tailrec
    def loop: Either[SError.SError, SValue] = {
      machine.run() match {
        case SResultQuestion(question) =>
          onQuestion(question)
          loop
        case SResultFinal(v) =>
          Right(v)
        case SResultInterruption =>
          loop
        case SResultError(err) =>
          Left(err)
      }
    }
    loop
  }

  @throws[ValidationError]
  def typeAndCompile(
      majorLanguageVersion: LanguageMajorVersion,
      pkgs: Map[PackageId, Ast.Package],
  ): PureCompiledPackages = {
    require(
      pkgs.values.forall(pkg => pkg.languageVersion.major == majorLanguageVersion), {
        val wrongPackages = pkgs.view
          .mapValues(_.languageVersion)
          .filter { case (_, v) => v.major != majorLanguageVersion }
          .toList
        s"these packages don't have the expected major language version $majorLanguageVersion: $wrongPackages"
      },
    )
    Validation.unsafeCheckPackages(PackageInterface(pkgs), pkgs)
    PureCompiledPackages.assertBuild(
      pkgs,
      Compiler.Config
        .Dev(majorLanguageVersion)
        .copy(
          stacktracing = Compiler.FullStackTrace
        ),
    )
  }

  @throws[ValidationError]
  def typeAndCompile[X](pkg: Ast.Package)(implicit
      parserParameter: ParserParameters[X]
  ): PureCompiledPackages =
    typeAndCompile(
      pkg.languageVersion.major,
      Map(parserParameter.defaultPackageId -> pkg),
    )

  private[speedy] object Implicits {

    implicit class AddTestMethodsToMachine(machine: UpdateMachine) {

      private[lf] def withWarningLog(warningLog: WarningLog): UpdateMachine =
        new UpdateMachine(
          sexpr = machine.sexpr,
          traceLog = machine.traceLog,
          warningLog = warningLog,
          compiledPackages = machine.compiledPackages,
          profile = machine.profile,
          validating = machine.validating,
          submissionTime = machine.submissionTime,
          contractKeyUniqueness = machine.contractKeyUniqueness,
          ptx = machine.ptx,
          committers = machine.committers,
          readAs = machine.readAs,
          commitLocation = machine.commitLocation,
          limits = machine.limits,
          iterationsBetweenInterruptions = machine.iterationsBetweenInterruptions,
        )

      private[speedy] def withLocalContractKey(
          contractId: ContractId,
          key: GlobalKey,
      ): UpdateMachine = {
        machine.ptx = machine.ptx.copy(
          contractState = machine.ptx.contractState.copy(
            locallyCreated = machine.ptx.contractState.locallyCreated + contractId,
            activeState = machine.ptx.contractState.activeState.createKey(key, contractId),
          )
        )
        machine
      }

      private[speedy] def withDisclosedContractKeys(
          disclosedContractKeys: (ContractId, ContractInfo)*
      ): UpdateMachine = {
        disclosedContractKeys.foreach { case (contractId, contract) =>
          machine.addDisclosedContracts(contractId, contract)
        }
        machine
      }
    }
  }
}
