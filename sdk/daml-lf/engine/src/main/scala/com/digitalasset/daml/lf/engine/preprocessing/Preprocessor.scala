// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.command.{ApiCommand, ReplayCommand}
import com.daml.lf.data.Ref.{PackageRef, TypeConRef}
import com.daml.lf.data.{ImmArray, Ref}
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
      checkV1ContractIdSuffix = requireV1ContractIdSuffix,
    )

  val transactionPreprocessor = new TransactionPreprocessor(commandPreprocessor)

  @tailrec
  private[this] def collectPackagesRefFromTypes(
      types: List[Ast.Type],
      acc: Set[language.Reference] = Set.empty,
  ): Result[Set[language.Reference]] =
    types match {
      case typ :: rest =>
        typ match {
          case Ast.TTyCon(tycon) =>
            collectPackagesRefFromTypes(rest, acc + language.Reference.DataType(tycon))
          case Ast.TApp(tyFun, tyArg) =>
            collectPackagesRefFromTypes(tyFun :: tyArg :: rest, acc)
          case Ast.TNat(_) | Ast.TBuiltin(_) | Ast.TVar(_) =>
            collectPackagesRefFromTypes(rest, acc)
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
        ResultDone(acc)
    }

  @tailrec
  private[this] def collectPkgRefInValues(
      values: List[Value],
      acc0: Set[language.Reference],
  ): Set[language.Reference] =
    values match {
      case head :: tail =>
        head match {
          case leaf: Value.ValueCidlessLeaf =>
            val acc = leaf match {
              case Value.ValueEnum(tycon, _) =>
                tycon.fold(acc0)(tyCon =>
                  acc0 + language.Reference.DataEnum(Ref.TypeConRef.fromIdentifier(tyCon))
                )
              case _: Value.ValueInt64 | _: Value.ValueNumeric | _: Value.ValueText |
                  _: Value.ValueTimestamp | _: Value.ValueDate | _: Value.ValueParty |
                  _: Value.ValueBool | Value.ValueUnit =>
                acc0
            }
            collectPkgRefInValues(tail, acc)
          case Value.ValueRecord(tycon, fields) =>
            collectPkgRefInValues(
              fields.foldRight(tail) { case ((_, v), tail) => v :: tail },
              tycon.fold(acc0)(tyCon =>
                acc0 + language.Reference.DataRecord(Ref.TypeConRef.fromIdentifier(tyCon))
              ),
            )
          case Value.ValueVariant(tycon, _, value) =>
            collectPkgRefInValues(
              value :: tail,
              tycon.fold(acc0)(tyCon =>
                acc0 + language.Reference.DataVariant(Ref.TypeConRef.fromIdentifier(tyCon))
              ),
            )
          case Value.ValueContractId(_) =>
            collectPkgRefInValues(tail, acc0)
          case Value.ValueList(values) =>
            collectPkgRefInValues(values.iterator.foldLeft(tail) { (tail, v) => v :: tail }, acc0)
          case Value.ValueOptional(value) =>
            value match {
              case Some(value) => collectPkgRefInValues(value :: tail, acc0)
              case None => collectPkgRefInValues(tail, acc0)
            }
          case Value.ValueTextMap(entries) =>
            collectPkgRefInValues(
              entries.toImmArray.foldRight(tail) { case ((_, v), tail) => v :: tail },
              acc0,
            )
          case Value.ValueGenMap(entries) =>
            collectPkgRefInValues(
              entries.foldRight(tail) { case ((k, v), tail) => k :: v :: tail },
              acc0,
            )
        }
      case Nil =>
        acc0
    }

  private def collectPackagesInCmds(cmds: ImmArray[ApiCommand]): Set[language.Reference] =
    cmds.foldRight(Set.empty[language.Reference]) { (cmd, acc) =>
      cmd match {
        case ApiCommand.Create(tmplRef, arg) =>
          collectPkgRefInValues(arg :: Nil, acc + language.Reference.Template(tmplRef))
        case ApiCommand.Exercise(typeRef, _, _, arg) =>
          collectPkgRefInValues(arg :: Nil, acc + language.Reference.TemplateOrInterface(typeRef))
        case ApiCommand.ExerciseByKey(tmplRef, key, _, arg) =>
          collectPkgRefInValues(key :: arg :: Nil, acc + language.Reference.Template(tmplRef))
        case ApiCommand.CreateAndExercise(tmplRef, createArg, _, choiceArg) =>
          collectPkgRefInValues(
            createArg :: choiceArg :: Nil,
            acc + language.Reference.Template(tmplRef),
          )
      }
    }

  private[this] def collectPackagesInCmd(cmd: ReplayCommand): Set[language.Reference] =
    cmd match {
      case ReplayCommand.Create(tmplId, _) =>
        Set(language.Reference.Template(tmplId))
      case ReplayCommand.Exercise(tmplId, mbIfaceId, _, _, _) =>
        mbIfaceId match {
          case Some(ifaceId) =>
            Set(language.Reference.InterfaceInstance(tmplId, ifaceId))
          case None =>
            Set(language.Reference.Template(tmplId))
        }
      case ReplayCommand.ExerciseByKey(tmplId, _, choiceId, _) =>
        Set(language.Reference.TemplateChoice(tmplId, choiceId))
      case ReplayCommand.Fetch(tmplId, mbIfaceId, _) =>
        mbIfaceId match {
          case Some(ifaceId) =>
            Set(language.Reference.InterfaceInstance(tmplId, ifaceId))
          case None =>
            Set(language.Reference.Template(tmplId))
        }
      case ReplayCommand.FetchByKey(tmplId, _) =>
        Set(language.Reference.TemplateKey(tmplId))
      case ReplayCommand.LookupByKey(tmplId, _) =>
        Set(language.Reference.TemplateKey(tmplId))
    }

  private[this] def collectPackagesInNodes(nodes: Iterable[Node]): Set[language.Reference] =
    nodes.collect {
      case create: Node.Create =>
        language.Reference.Template(create.templateId)
      case fetch: Node.Fetch =>
        fetch.interfaceId match {
          case Some(ifaceId) =>
            language.Reference.InterfaceInstance(fetch.templateId, ifaceId)
          case None =>
            language.Reference.Template(fetch.templateId)
        }
      case lookup: Node.LookupByKey =>
        language.Reference.TemplateKey(lookup.templateId)
      case exe: Node.Exercise =>
        exe.interfaceId match {
          case Some(ifaceId) =>
            language.Reference.InterfaceInstance(exe.templateId, ifaceId)
          case None =>
            language.Reference.Template(exe.templateId)
        }
    }.toSet

  private[this] def collectPkgInDisclosure(
      contracts: ImmArray[command.DisclosedContract]
  ): Set[language.Reference] =
    contracts.iterator
      .map(contract => language.Reference.Template(TypeConRef.fromIdentifier(contract.templateId)))
      .toSet

  private[this] def pullPackages(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      refs: Set[language.Reference],
  ): Result[Unit] = {
    val unknownPkgIds = for {
      ref <- refs.iterator
      pkgRef <- ref.pkgRefs
      pkgId <- pkgRef match {
        case PackageRef.Name(name) =>
          pkgResolution.get(name).toList
        case PackageRef.Id(id) =>
          List(id)
      }
      if !compiledPackages.packageIds.contains(pkgId)
    } yield pkgId -> ref

    def loop(unknownPkgIds: List[(Ref.PackageId, language.Reference)]): Result[Unit] =
      unknownPkgIds match {
        case (pkgId, ref) :: rest =>
          ResultNeedPackage(
            pkgId,
            {
              case Some(pkg) =>
                compiledPackages.addPackage(pkgId, pkg).flatMap(_ => loop(rest))
              case None =>
                ResultError(Error.Package.MissingPackage(pkgId, ref))
            },
          )
        case Nil =>
          ResultDone.Unit
      }

    loop(unknownPkgIds.toMap.toList)
  }

  /** Translates the LF value `v0` of type `ty0` to a speedy value.
    * Fails if the nesting is too deep or if v0 does not match the type `ty0`.
    * Assumes ty0 is a well-formed serializable typ.
    */
  def translateValue(ty0: Ast.Type, v0: Value): Result[SValue] =
    safelyRun(collectPackagesRefFromTypes(List(ty0)).flatMap(pullPackages(Map.empty, _))) {
      // this is used only by the value enricher
      commandPreprocessor.unsafeTranslateValue(ty0, v0)
    }

  private[engine] def preprocessApiCommand(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmd: command.ApiCommand,
  ): Result[speedy.Command] =
    safelyRun(pullPackages(pkgResolution, collectPackagesInCmds(ImmArray(cmd)))) {
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
    safelyRun(pullPackages(pkgResolution, collectPackagesInCmds(cmds))) {
      commandPreprocessor.unsafePreprocessApiCommands(pkgResolution, cmds)
    }

  def preprocessDisclosedContracts(
      discs: data.ImmArray[command.DisclosedContract]
  ): Result[ImmArray[speedy.DisclosedContract]] =
    safelyRun(pullPackages(Map.empty, collectPkgInDisclosure(discs))) {
      commandPreprocessor.unsafePreprocessDisclosedContracts(discs)
    }

  private[engine] def preprocessReplayCommand(
      cmd: command.ReplayCommand
  ): Result[speedy.Command] = {
    safelyRun(pullPackages(Map.empty, collectPackagesInCmd(cmd))) {
      commandPreprocessor.unsafePreprocessReplayCommand(cmd)
    }
  }

  /** Translates a complete transaction. Assumes no contract ID suffixes are used */
  def translateTransactionRoots(
      tx: SubmittedTransaction
  ): Result[ImmArray[speedy.Command]] =
    safelyRun(
      pullPackages(
        Map.empty,
        collectPackagesInNodes(tx.nodes.values.toList),
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
      pullPackages(Map.empty, Set(language.Reference.InterfaceInstance(templateId, interfaceId)))
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
