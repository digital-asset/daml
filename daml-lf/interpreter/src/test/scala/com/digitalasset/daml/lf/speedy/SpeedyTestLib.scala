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
import transaction.{GlobalKeyWithMaintainers, SubmittedTransaction}
import value.Value
import scalautil.Statement.discard

import scala.annotation.tailrec

private[speedy] object SpeedyTestLib {

  @throws[SError.SErrorCrash]
  def run(
      machine: Speedy.Machine,
      getPkg: PartialFunction[PackageId, CompiledPackages] = PartialFunction.empty,
      getContract: PartialFunction[Value.ContractId, Value.VersionedContractInstance] =
        PartialFunction.empty,
      getKey: PartialFunction[GlobalKeyWithMaintainers, Option[Value.ContractId]] =
        PartialFunction.empty,
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
              throw new IllegalStateException("SpeedyTestLib.run: unexpected SResultNeedTime")
          }
        case SResultNeedContract(contractId, _, _, callback) =>
          getContract.lift(contractId) match {
            case Some(value) =>
              callback(value)
              loop
            case None =>
              throw new IllegalStateException(s"SpeedyTestLib.run: unknown contract '$contractId'")
          }
        case SResultNeedPackage(pkg, _, callback) =>
          getPkg.lift(pkg) match {
            case Some(value) =>
              callback(value)
              loop
            case None =>
              throw new IllegalStateException(s"SpeedyTestLib.run: unknown package '$pkg'")
          }
        case SResultNeedKey(key, _, callback) =>
          getKey.lift(key) match {
            case Some(value) =>
              discard(callback(value))
              loop
            case None =>
              throw new IllegalStateException("SpeedyTestLib.run: unexpected SResultNeedKey")
          }
        case SResultFinalValue(v) =>
          Right(v)
        case SResultError(err) =>
          Left(err)
        case otherwise =>
          throw new IllegalStateException(s"SpeedyTestLib.run: unexpected ${otherwise.getClass}")
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
      getKey: PartialFunction[GlobalKeyWithMaintainers, Option[Value.ContractId]] =
        PartialFunction.empty,
      getTime: PartialFunction[Unit, Time.Timestamp] = PartialFunction.empty,
  ): Either[SError.SError, SubmittedTransaction] =
    run(machine, getPkg, getContract, getKey, getTime) match {
      case Right(_) =>
        machine.withOnLedger("buildTransaction") { onLedger =>
          onLedger.ptx.finish match {
            case PartialTransaction.IncompleteTransaction(_) =>
              throw SError.SErrorCrash("buildTransaction", "unexpected IncompleteTransaction")
            case PartialTransaction.CompleteTransaction(tx, _, _) =>
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

}
