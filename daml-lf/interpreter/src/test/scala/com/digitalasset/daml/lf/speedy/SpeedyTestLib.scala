// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package speedy

import data.Ref.{PackageId, TypeConName}
import data.Time
import SResult._
import com.daml.lf.language.{Ast, PackageInterface}
import com.daml.lf.speedy.Speedy.{CachedContract, OnLedgerMachine}
import com.daml.lf.testing.parser.ParserParameters
import com.daml.lf.validation.{Validation, ValidationError}
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.nameof.NameOf
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
      machine: Speedy.Machine,
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Value.ContractId] = PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, SValue] = {
    runTx(machine, getPkg, getContract, getKey, getTime) match {
      case Left(e) => Left(e)
      case Right(SResultFinal(v)) => Right(v)
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
      case Right(SResultFinal(_)) =>
        machine.asOnLedger(NameOf.qualifiedNameOfCurrentFunc)(_.finish.map(_.tx))
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

  private[speedy] object Implicits {

    implicit class AddTestMethodsToMachine(machine: OnLedgerMachine) {

      private[lf] def withWarningLog(warningLog: WarningLog): OnLedgerMachine =
        new OnLedgerMachine(
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
          disclosureKeyTable = machine.disclosureKeyTable,
        )

      def withCachedContracts(cachedContracts: (ContractId, CachedContract)*): OnLedgerMachine = {
        for {
          entry <- cachedContracts
          (contractId, cachedContract) = entry
        } machine.updateCachedContracts(contractId, cachedContract)
        machine
      }

      private[speedy] def withLocalContractKey(
          contractId: ContractId,
          key: GlobalKey,
      ): OnLedgerMachine = {
        machine.ptx = machine.ptx.copy(
          contractState = machine.ptx.contractState.copy(
            locallyCreated = machine.ptx.contractState.locallyCreated + contractId,
            activeState = machine.ptx.contractState.activeState.createKey(key, contractId),
          )
        )
        machine
      }

      private[speedy] def withDisclosedContractKeys(
          templateId: TypeConName,
          disclosedContractKeys: (crypto.Hash, ContractId)*
      ): OnLedgerMachine = {
        for {
          entry <- disclosedContractKeys
          (keyHash, contractId) = entry
        } machine.disclosureKeyTable.addContractKey(templateId, keyHash, contractId)

        machine
      }
    }
  }
}
