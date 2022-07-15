// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package speedy

import data.Ref.{Location, PackageId}
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
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

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
    @tailrec
    def loop: Either[SError.SError, SValue] = {
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
        case SResultFinalValue(v) =>
          Right(v)
        case SResultError(err) =>
          Left(err)
        case _: SResultScenarioGetParty | _: SResultScenarioPassTime | _: SResultScenarioSubmit =>
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
    run(machine, getPkg, getContract, getKey, getTime) match {
      case Right(_) =>
        machine.withOnLedger("buildTransaction") { onLedger =>
          onLedger.ptx.finish match {
            case PartialTransaction.IncompleteTransaction(_) =>
              throw SError.SErrorCrash("buildTransaction", "unexpected IncompleteTransaction")
            case PartialTransaction.CompleteTransaction(tx, _, _, _, _) =>
              Right(tx)
          }
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

  object Implicits {
    implicit def addSpeedyMachineHelpers(machine: Speedy.Machine): SpeedyMachineTestHelpers =
      new SpeedyMachineTestHelpers(machine)

    private[speedy] class SpeedyMachineTestHelpers(machine: Speedy.Machine) {
      def traceDisclosureTable(traceLog: TestTraceLog): Speedy.Machine = {
        new Speedy.Machine(
          machine.ctrl,
          machine.returnValue,
          machine.frame,
          machine.actuals,
          machine.env,
          machine.envBase,
          machine.kontStack,
          machine.lastLocation,
          machine.traceLog,
          machine.warningLog,
          machine.loggingContext,
          machine.compiledPackages,
          machine.steps,
          machine.track,
          machine.profile,
          machine.submissionTime,
          machine.ledgerMode,
          Speedy.DisclosureTable(
            contractIdByKey =
              traceLog.traceMap("contractIdByKey queried", machine.disclosureTable.contractIdByKey),
            contractById =
              traceLog.traceMap("contractById queried", machine.disclosureTable.contractById),
          ),
        )
      }
    }
  }
}

class TestTraceLog extends TraceLog {
  private val messages: ArrayBuffer[(String, Option[Location])] = new ArrayBuffer()

  override def add(message: String, optLocation: Option[Location])(implicit
      loggingContext: LoggingContext
  ): Unit = {
    discard(messages += ((message, optLocation)))
  }

  def tracePF[X, Y](text: String, pf: PartialFunction[X, Y]): PartialFunction[X, Y] = {
    case x if { add(text, None)(LoggingContext.ForTesting); pf.isDefinedAt(x) } => pf(x)
  }

  def traceMap[X, Y](text: String, pf: Map[X, Y]): Map[X, Y] = new Map[X, Y] {
    override def apply(key: X): Y = {
      add(text, None)(LoggingContext.ForTesting)
      pf(key)
    }

    override def get(key: X): Option[Y] = {
      add(text, None)(LoggingContext.ForTesting)
      pf.get(key)
    }

    override def iterator: Iterator[(X, Y)] = pf.iterator

    override def removed(key: X): Map[X, Y] = pf.removed(key)

    override def updated[Y1 >: Y](key: X, value: Y1): Map[X, Y1] = pf.updated(key, value)
  }

  override def iterator: Iterator[(String, Option[Location])] = messages.iterator

  def getMessages: Seq[String] = messages.view.map(_._1).toSeq
}
