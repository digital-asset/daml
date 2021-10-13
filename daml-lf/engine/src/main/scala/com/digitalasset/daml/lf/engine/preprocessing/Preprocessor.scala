// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  * @param forbidV0ContractId when `true` the preprocessor will reject
  *   any value/command/transaction that contains V0 Contract IDs
  *   without suffixed.
  * @param requireV1ContractIdSuffix when `true` the preprocessor will reject
  *   any value/command/transaction that contains V1 Contract IDs
  *   without suffixed.
  */
private[engine] final class Preprocessor(
    compiledPackages: MutableCompiledPackages,
    forbidV0ContractId: Boolean = true,
    requireV1ContractIdSuffix: Boolean = true,
) {

  import Preprocessor._

  import compiledPackages.interface

  val commandPreprocessor =
    new CommandPreprocessor(
      interface = interface,
      forbidV0ContractId = forbidV0ContractId,
      requireV1ContractIdSuffix = requireV1ContractIdSuffix,
    )
  val transactionPreprocessor = new TransactionPreprocessor(commandPreprocessor)

  // This pulls all the dependencies of in `typesToProcess0` and `tyConAlreadySeen0`
  private def getDependencies(
      typesToProcess0: List[Ast.Type],
      tmplToProcess0: List[Ref.TypeConName],
      tyConAlreadySeen0: Set[Ref.TypeConName] = Set.empty,
      tmplAlreadySeen0: Set[Ref.TypeConName] = Set.empty,
  ): Result[(Set[Ref.TypeConName], Set[Ref.TypeConName])] = {

    @tailrec
    def go(
        typesToProcess0: List[Ast.Type],
        tmplToProcess0: List[Ref.TypeConName],
        tyConAlreadySeen0: Set[Ref.TypeConName],
        tmplsAlreadySeen0: Set[Ref.TypeConName],
    ): Result[(Set[Ref.TypeConName], Set[Ref.TypeConName])] = {
      def pullPackage(pkgId: Ref.PackageId, context: language.Reference) =
        ResultNeedPackage(
          pkgId,
          {
            case Some(pkg) =>
              for {
                _ <- compiledPackages.addPackage(pkgId, pkg)
                r <- getDependencies(
                  typesToProcess0,
                  tmplToProcess0,
                  tyConAlreadySeen0,
                  tmplsAlreadySeen0,
                )
              } yield r
            case None =>
              ResultError(Error.Package.MissingPackage(pkgId, context))
          },
        )

      typesToProcess0 match {
        case typ :: typesToProcess =>
          typ match {
            case Ast.TApp(fun, arg) =>
              go(fun :: arg :: typesToProcess, tmplToProcess0, tyConAlreadySeen0, tmplsAlreadySeen0)
            case Ast.TTyCon(tyCon) if !tyConAlreadySeen0(tyCon) =>
              interface.lookupDataType(tyCon) match {
                case Right(Ast.DDataType(_, _, dataType)) =>
                  val typesToProcess = dataType match {
                    case Ast.DataRecord(fields) =>
                      fields.foldRight(typesToProcess0)(_._2 :: _)
                    case Ast.DataVariant(variants) =>
                      variants.foldRight(typesToProcess0)(_._2 :: _)
                    case Ast.DataEnum(_) =>
                      typesToProcess0
                    case Ast.DataInterface =>
                      typesToProcess0
                  }
                  go(
                    typesToProcess,
                    tmplToProcess0,
                    tyConAlreadySeen0 + tyCon,
                    tmplsAlreadySeen0,
                  )
                case Left(LookupError.MissingPackage(pkgId, context)) =>
                  pullPackage(pkgId, context)
                case Left(e) =>
                  ResultError(Error.Preprocessing.Lookup(e))
              }
            case Ast.TTyCon(_) | Ast.TNat(_) | Ast.TBuiltin(_) | Ast.TVar(_) =>
              go(typesToProcess, tmplToProcess0, tyConAlreadySeen0, tmplsAlreadySeen0)
            case Ast.TSynApp(_, _) | Ast.TForall(_, _) | Ast.TStruct(_) =>
              // We assume that getDependencies is always given serializable types
              ResultError(
                Error.Preprocessing
                  .Internal(NameOf.qualifiedNameOfCurrentFunc, s"unserializable type ${typ.pretty}")
              )
          }
        case Nil =>
          tmplToProcess0 match {
            case tmplId :: tmplsToProcess if tmplsAlreadySeen0(tmplId) =>
              go(Nil, tmplsToProcess, tyConAlreadySeen0, tmplsAlreadySeen0)
            case tmplId :: tmplsToProcess =>
              interface.lookupTemplate(tmplId) match {
                case Right(template) =>
                  val typs0 = template.choices.map(_._2.argBinder._2).toList
                  val typs1 =
                    if (tyConAlreadySeen0(tmplId)) typs0 else Ast.TTyCon(tmplId) :: typs0
                  val typs2 = template.key.fold(typs1)(_.typ :: typs1)
                  go(typs2, tmplsToProcess, tyConAlreadySeen0, tmplsAlreadySeen0)
                case Left(LookupError.MissingPackage(pkgId, context)) =>
                  pullPackage(pkgId, context)
                case Left(error) =>
                  ResultError(Error.Preprocessing.Lookup(error))
              }
            case Nil =>
              ResultDone(tyConAlreadySeen0 -> tmplsAlreadySeen0)
          }
      }
    }

    go(typesToProcess0, tmplToProcess0, tyConAlreadySeen0, tmplAlreadySeen0)
  }

  /** Translates the LF value `v0` of type `ty0` to a speedy value.
    * Fails if the nesting is too deep or if v0 does not match the type `ty0`.
    * Assumes ty0 is a well-formed serializable typ.
    */
  def translateValue(ty0: Ast.Type, v0: Value): Result[SValue] =
    safelyRun(getDependencies(List(ty0), List.empty)) {
      commandPreprocessor.valueTranslator.unsafeTranslateValue(ty0, v0)
    }

  private[engine] def preprocessCommand(
      cmd: command.Command
  ): Result[speedy.Command] =
    safelyRun(getDependencies(List.empty, List(cmd.templateId))) {
      commandPreprocessor.unsafePreprocessCommand(cmd)
    }

  /** Translates  LF commands to a speedy commands.
    */
  def preprocessCommands(
      cmds: data.ImmArray[command.ApiCommand]
  ): Result[ImmArray[speedy.Command]] =
    safelyRun(getDependencies(List.empty, cmds.map(_.templateId).toList)) {
      commandPreprocessor.unsafePreprocessCommands(cmds)
    }

  /** Translates a complete transaction. Assumes no contract ID suffixes are used */
  def translateTransactionRoots(
      tx: SubmittedTransaction
  ): Result[ImmArray[speedy.Command]] =
    safelyRun(
      getDependencies(List.empty, tx.rootNodes.toList.map(_.templateId))
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
  def safelyRun[X](
      handleMissingPackages: Result[_]
  )(unsafeRun: => X): Result[X] = {

    def start: Result[X] =
      try {
        ResultDone(unsafeRun)
      } catch {
        case Error.Preprocessing.Lookup(LookupError.MissingPackage(_, _)) =>
          handleMissingPackages.flatMap(_ => start)
        case e: Error.Preprocessing.Error =>
          ResultError(e)
      }

    start
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
