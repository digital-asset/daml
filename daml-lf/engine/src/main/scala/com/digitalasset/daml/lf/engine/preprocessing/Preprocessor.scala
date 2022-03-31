// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import java.util
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.{Ast, LookupError}
import com.daml.lf.speedy.SValue
import com.daml.lf.transaction.SubmittedTransaction
import com.daml.lf.value.Value
import com.daml.nameof.NameOf

import scala.annotation.tailrec

/** The Command Preprocessor is responsible of the following tasks:
  *  - normalizes value representation (e.g. resolves missing type
  *    reference in record/variant/enumeration, infers missing labeled
  *    record fields, orders labeled record fields, ...);
  *  - checks value nesting does not overpass 100;
  *  - checks a LF command/value is properly typed according the
  *    Daml-LF package definitions;
  *  - checks for Contract ID suffix (see [[requireV1ContractIdSuffix]]);
  *  - translates a LF command/value into speedy command/value; and
  *  - translates a complete transaction into a list of speedy
  *    commands.
  *
  * @param compiledPackages a [[MutableCompiledPackages]] contains the
  *   Daml-LF package definitions against the command should
  *   resolved/typechecked. It is updated dynamically each time the
  *   [[ResultNeedPackage]] continuation is called.
  * @param requireV1ContractIdSuffix when `true` the preprocessor will reject
  *   any value/command/transaction that contains V1 Contract IDs
  *   without suffixed.
  */
private[engine] final class Preprocessor(
    compiledPackages: MutableCompiledPackages,
    requireV1ContractIdSuffix: Boolean = true,
) {

  import Preprocessor._

  import compiledPackages.interface

  val commandPreprocessor =
    new CommandPreprocessor(
      interface = interface,
      requireV1ContractIdSuffix = requireV1ContractIdSuffix,
    )
  val transactionPreprocessor = new TransactionPreprocessor(commandPreprocessor)

  @tailrec
  private[this] def collectNewPackagesFromTypes(
      types: List[Ast.Type],
      acc: Map[Ref.PackageId, language.Reference] = Map.empty,
  ): Result[List[(Ref.PackageId, language.Reference)]] =
    types match {
      case typ :: rest =>
        typ match {
          case Ast.TTyCon(tycon) =>
            val pkgId = tycon.packageId
            val newAcc =
              if (compiledPackages.packageIds(pkgId) || acc.contains(pkgId))
                acc
              else
                acc.updated(pkgId, language.Reference.DataType(tycon))
            collectNewPackagesFromTypes(rest, newAcc)
          case Ast.TApp(tyFun, tyArg) =>
            collectNewPackagesFromTypes(tyFun :: tyArg :: rest, acc)
          case Ast.TNat(_) | Ast.TBuiltin(_) | Ast.TVar(_) =>
            collectNewPackagesFromTypes(rest, acc)
          case Ast.TSynApp(_, _) | Ast.TForall(_, _) | Ast.TStruct(_) =>
            // We assume that collectPackages is always given serializable types
            ResultError(
              Error.Preprocessing
                .Internal(
                  NameOf.qualifiedNameOfCurrentFunc,
                  s"unserializable type ${typ.pretty}",
                  None,
                )
            )
        }
      case Nil =>
        ResultDone(acc.toList)
    }

  @tailrec
  private[this] def collectNewPackagesFromTemplateIds(
      templateIds: List[Ref.TypeConName],
      acc: Map[Ref.PackageId, language.Reference] = Map.empty,
  ): Result[List[(Ref.PackageId, language.Reference)]] =
    templateIds match {
      case templateId :: rest =>
        val pkgId = templateId.packageId
        val newAcc =
          if (compiledPackages.packageIds(pkgId) || acc.contains(pkgId))
            acc
          else
            acc.updated(pkgId, language.Reference.TemplateOrInterface(templateId))
        collectNewPackagesFromTemplateIds(rest, newAcc)
      case Nil =>
        ResultDone(acc.toList)
    }

  private[this] def pullPackages(
      pkgIds: List[(Ref.PackageId, language.Reference)]
  ): Result[Unit] =
    pkgIds match {
      case (pkgId, context) :: rest =>
        ResultNeedPackage(
          pkgId,
          {
            case Some(pkg) =>
              compiledPackages.addPackage(pkgId, pkg).flatMap(_ => pullPackages(rest))
            case None =>
              ResultError(Error.Package.MissingPackage(pkgId, context))
          },
        )
      case Nil =>
        ResultDone.Unit
    }

  private[this] def pullTypePackages(typ: Ast.Type): Result[Unit] =
    collectNewPackagesFromTypes(List(typ)).flatMap(pullPackages)

  private[this] def pullTemplatePackage(tyCons: List[Ref.TypeConName]): Result[Unit] =
    collectNewPackagesFromTemplateIds(tyCons).flatMap(pullPackages)

  /** Translates the LF value `v0` of type `ty0` to a speedy value.
    * Fails if the nesting is too deep or if v0 does not match the type `ty0`.
    * Assumes ty0 is a well-formed serializable typ.
    */
  def translateValue(ty0: Ast.Type, v0: Value): Result[SValue] =
    safelyRun(pullTypePackages(ty0)) {
      commandPreprocessor.valueTranslator.unsafeTranslateValue(ty0, v0)
    }

  private[engine] def preprocessApiCommand(
      cmd: command.ApiCommand
  ): Result[speedy.Command] =
    safelyRun(pullTemplatePackage(List(cmd.templateId))) {
      commandPreprocessor.unsafePreprocessApiCommand(cmd)
    }

  /** Translates  LF commands to a speedy commands.
    */
  def preprocessApiCommands(
      cmds: data.ImmArray[command.ApiCommand]
  ): Result[ImmArray[speedy.Command]] =
    safelyRun(pullTemplatePackage(cmds.toSeq.view.map(_.templateId).toList)) {
      commandPreprocessor.unsafePreprocessApiCommands(cmds)
    }

  private[engine] def preprocessReplayCommand(
      cmd: command.ReplayCommand
  ): Result[speedy.Command] =
    safelyRun(pullTemplatePackage(List(cmd.templateId))) {
      commandPreprocessor.unsafePreprocessReplayCommand(cmd)
    }

  /** Translates a complete transaction. Assumes no contract ID suffixes are used */
  def translateTransactionRoots(
      tx: SubmittedTransaction
  ): Result[ImmArray[speedy.Command]] =
    safelyRun(
      pullTemplatePackage(
        tx.rootNodes.toSeq.view.map(_.templateId) ++: tx.byInterfaceNodes.map(_.templateId)
      )
    ) {
      transactionPreprocessor.unsafeTranslateTransactionRoots(tx)
    }

}

private[preprocessing] object Preprocessor {

  private[preprocessing] def ArrayList[X](as: X*): util.ArrayList[X] = {
    val a = new util.ArrayList[X](as.length)
    as.foreach(a.add)
    a
  }

  @throws[Error.Preprocessing.Error]
  def handleLookup[X](either: Either[LookupError, X]): X = either match {
    case Right(v) => v
    case Left(error) => throw Error.Preprocessing.Lookup(error)
  }

  @inline
  def safelyRun[X](handleMissingPackages: => Result[_])(unsafeRun: => X): Result[X] = {

    def start(first: Boolean): Result[X] =
      try {
        ResultDone(unsafeRun)
      } catch {
        case Error.Preprocessing.Lookup(LookupError.MissingPackage(_, _)) if first =>
          handleMissingPackages.flatMap(_ => start(false))
        case e: Error.Preprocessing.Error =>
          ResultError(e)
      }

    start(first = true)
  }

  @inline
  def safelyRun[X](unsafeRun: => X): Either[Error.Preprocessing.Error, X] =
    try {
      Right(unsafeRun)
    } catch {
      case e: Error.Preprocessing.Error =>
        Left(e)
    }

}
