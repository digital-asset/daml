// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.preprocessing

import com.daml.lf.{language, command, data, speedy}
import com.daml.lf.engine._
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.{MutableCompiledPackages, ResultError}
import com.daml.lf.language.{Ast, LookupError}
import com.daml.lf.speedy.SValue
import com.daml.lf.transaction.{Node, SubmittedTransaction}
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

  import compiledPackages.pkgInterface

  val commandPreprocessor =
    new CommandPreprocessor(
      pkgInterface = pkgInterface,
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
              if (compiledPackages.contains(pkgId) || acc.contains(pkgId))
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

  private[this] def collectNewPackagesFromTemplatesOrInterfaces(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      tyRefs: Iterable[Ref.TypeConRef],
  ): List[(Ref.PackageId, language.Reference)] =
    tyRefs
      .foldLeft(Map.empty[Ref.PackageId, language.Reference]) { (acc, tycon) =>
        val pkgId = tycon.pkgRef match {
          case Ref.PackageRef.Name(name) =>
            pkgResolution.get(name)
          case Ref.PackageRef.Id(id) =>
            Some(id)
        }
        pkgId match {
          case Some(id) if !compiledPackages.contains(id) && !acc.contains(id) =>
            acc.updated(
              id,
              language.Reference.TemplateOrInterface(Ref.TypeConName(id, tycon.qName)),
            )
          case _ =>
            acc
        }
      }
      .toList

  private[this] def collectNewPackagesFromTemplatesOrInterfaces(
      tycons: Iterable[Ref.TypeConName]
  ): List[(Ref.PackageId, language.Reference)] =
    tycons
      .foldLeft(Map.empty[Ref.PackageId, language.Reference]) { (acc, tycon) =>
        val pkgId = tycon.packageId
        if (compiledPackages.contains(pkgId) || acc.contains(pkgId))
          acc
        else
          acc.updated(pkgId, language.Reference.TemplateOrInterface(tycon))
      }
      .toList

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

  private[this] def pullPackage(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      tyCons: Iterable[Ref.TypeConRef],
  ): Result[Unit] =
    pullPackages(collectNewPackagesFromTemplatesOrInterfaces(pkgResolution, tyCons))

  private[this] def pullPackage(tyCons: Iterable[Ref.TypeConName]): Result[Unit] =
    pullPackages(collectNewPackagesFromTemplatesOrInterfaces(tyCons))

  /** Translates the LF value `v0` of type `ty0` to a speedy value.
    * Fails if the nesting is too deep or if v0 does not match the type `ty0`.
    * Assumes ty0 is a well-formed serializable typ.
    */
  def translateValue(ty0: Ast.Type, v0: Value): Result[SValue] =
    safelyRun(pullTypePackages(ty0)) {
      // this is used only by the value enricher, strict translation is the way to go
      commandPreprocessor.unsafeStrictTranslateValue(ty0, v0)
    }

  private[engine] def preprocessApiCommand(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmd: command.ApiCommand,
  ): Result[speedy.Command] =
    safelyRun(pullPackage(pkgResolution, List(cmd.typeRef))) {
      commandPreprocessor.unsafePreprocessApiCommand(pkgResolution, cmd)
    }

  private[lf] val EmptyPackageResolution: Result[Map[Ref.PackageName, Ref.PackageId]] = ResultDone(
    Map.empty
  )

  def buildPackageResolution(
      packageMap: Map[Ref.PackageId, (Ref.PackageName, Ref.PackageVersion)] = Map.empty,
      packagePreference: Set[Ref.PackageId] = Set.empty,
  ): Result[Map[Ref.PackageName, Ref.PackageId]] =
    packagePreference.foldLeft(EmptyPackageResolution)((acc, pkgId) =>
      for {
        pkgName <- packageMap.get(pkgId) match {
          case Some((pkgName, _)) => ResultDone(pkgName)
          case None =>
            ResultError(Error.Preprocessing.Lookup(language.LookupError.MissingPackage(pkgId)))
        }
        m <- acc
        _ <- m.get(pkgName) match {
          case None => Result.unit
          case Some(pkgId0) =>
            ResultError(
              Error.Preprocessing.Internal(
                NameOf.qualifiedNameOfCurrentFunc,
                s"package $pkgId0 and $pkgId have the same name $pkgName",
                None,
              )
            )
        }
      } yield m.updated(pkgName, pkgId)
    )

  /** Translates  LF commands to a speedy commands.
    */
  def preprocessApiCommands(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmds: data.ImmArray[command.ApiCommand],
  ): Result[ImmArray[speedy.Command]] =
    safelyRun(pullPackage(pkgResolution, cmds.toSeq.view.map(_.typeRef))) {
      commandPreprocessor.unsafePreprocessApiCommands(pkgResolution, cmds)
    }

  def preprocessDisclosedContracts(
      discs: data.ImmArray[command.DisclosedContract]
  ): Result[ImmArray[speedy.DisclosedContract]] =
    safelyRun(pullPackage(discs.toSeq.view.map(_.templateId))) {
      commandPreprocessor.unsafePreprocessDisclosedContracts(discs)
    }

  private[engine] def preprocessReplayCommand(
      cmd: command.ReplayCommand
  ): Result[speedy.Command] =
    safelyRun(pullPackage(List(cmd.templateId))) {
      commandPreprocessor.unsafePreprocessReplayCommand(cmd)
    }

  /** Translates a complete transaction. Assumes no contract ID suffixes are used */
  def translateTransactionRoots(
      tx: SubmittedTransaction
  ): Result[ImmArray[speedy.Command]] =
    safelyRun(
      pullPackage(
        tx.nodes.values.collect { case action: Node.Action => action.templateId }
      )
    ) {
      transactionPreprocessor.unsafeTranslateTransactionRoots(tx)
    }

  def preprocessInterfaceView(
      templateId: Ref.Identifier,
      argument: Value,
      interfaceId: Ref.Identifier,
  ): Result[speedy.InterfaceView] =
    safelyRun(
      pullPackage(Seq(templateId)).flatMap(_ => pullPackage(Seq(interfaceId)))
    ) {
      commandPreprocessor.unsafePreprocessInterfaceView(templateId, argument, interfaceId)
    }
}

private[preprocessing] object Preprocessor {

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
