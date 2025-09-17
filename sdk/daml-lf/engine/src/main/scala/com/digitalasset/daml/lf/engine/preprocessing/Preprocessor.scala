// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine
package preprocessing

import com.digitalasset.daml.lf.command.{ApiContractKey, ReplayCommand}
import com.digitalasset.daml.lf.data.{Bytes, FrontStack, ImmArray, Ref, SortedLookupList, Time}
import com.digitalasset.daml.lf.language.{Ast, LookupError}
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  FatContractInstanceImpl,
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  SubmittedTransaction,
}
import com.digitalasset.daml.lf.value.Value
import com.daml.nameof.NameOf
import com.digitalasset.daml.lf.crypto.Hash

import scala.annotation.tailrec
import scala.collection.immutable.TreeSet
import scala.language.implicitConversions

/** The Command Preprocessor is responsible of the following tasks:
  *  - normalizes value representation (e.g. resolves missing type
  *    reference in record/variant/enumeration, infers missing labeled
  *    record fields, orders labeled record fields, ...);
  *  - checks value nesting does not overpass 100;
  *  - checks a LF command/value is properly typed according the
  *    Daml-LF package definitions;
  *  - checks for Contract ID suffix (see [[forbidLocalContractIds]]);
  *  - translates a LF command/value into speedy command/value; and
  *  - translates a complete transaction into a list of speedy
  *    commands.
  *
  * @param compiledPackages a [[MutableCompiledPackages]] contains the
  *   Daml-LF package definitions against the command should
  *   resolved/typechecked. It is updated dynamically each time the
  *   [[ResultNeedPackage]] continuation is called.
  * @param forbidLocalContractIds when `true` the preprocessor will reject
  *   any value/command/transaction that contains a local Contract ID.
  */
private[engine] final class Preprocessor(
    compiledPackages: CompiledPackages,
    loadPackage: (Ref.PackageId, language.Reference) => Result[Unit],
    forbidLocalContractIds: Boolean = true,
    initialGasBudget: Preprocessor.CostModel.Cost = 0L,
) {

  import Preprocessor._
  import Preprocessor.CostModel.StructuralCostModelImplicits._

  import compiledPackages.pkgInterface

  private[this] var gasBudget: CostModel.Cost = initialGasBudget

  def updateGasBudget[A](value: A)(implicit cost: A => CostModel.Cost): Unit = {
    gasBudget += cost(value)
  }

  val commandPreprocessor =
    new CommandPreprocessor(
      pkgInterface = pkgInterface,
      forbidLocalContractIds = forbidLocalContractIds,
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
        val pkgId = tycon.pkg match {
          case Ref.PackageRef.Name(name) =>
            pkgResolution.get(name)
          case Ref.PackageRef.Id(id) =>
            Some(id)
        }
        pkgId match {
          case Some(id) if !compiledPackages.contains(id) && !acc.contains(id) =>
            acc.updated(
              id,
              language.Reference.TemplateOrInterface(tycon.copy(Ref.PackageRef.Id(id))),
            )
          case _ =>
            acc
        }
      }
      .toList

  private[this] def collectNewPackagesFromTemplatesOrInterfaces(
      tycons: Iterable[Ref.TypeConId]
  ): List[(Ref.PackageId, language.Reference)] =
    tycons
      .foldLeft(Map.empty[Ref.PackageId, language.Reference]) { (acc, tycon) =>
        val pkgId = tycon.packageId
        if (compiledPackages.contains(pkgId) || acc.contains(pkgId))
          acc
        else
          acc.updated(pkgId, language.Reference.TemplateOrInterface(tycon.toRef))
      }
      .toList

  private[this] def pullPackages(
      pkgIds: List[(Ref.PackageId, language.Reference)]
  ): Result[Unit] =
    pkgIds match {
      case (pkgId, context) :: rest =>
        loadPackage(pkgId, context).flatMap(_ => pullPackages(rest))
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

  private[this] def pullPackage(tyCons: Iterable[Ref.TypeConId]): Result[Unit] =
    pullPackages(collectNewPackagesFromTemplatesOrInterfaces(tyCons))

  /** Translates the LF value `v0` of type `ty0` to a speedy value.
    * Fails if the nesting is too deep or if v0 does not match the type `ty0`.
    * Assumes ty0 is a well-formed serializable typ.
    */
  def translateValue(ty0: Ast.Type, v0: Value): Result[SValue] =
    safelyRun(pullTypePackages(ty0)) {
      // this is used only by the value enricher, strict translation is the way to go
      commandPreprocessor.unsafeTranslateValue(
        ty0,
        v0,
        extendLocalIdForbiddanceToRelativeV2 = false,
      )
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
  ): Result[Map[Ref.PackageName, Ref.PackageId]] = {
    updateGasBudget(packageMap)
    updateGasBudget(packagePreference)

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
  }

  def buildGlobalKey(
      templateId: Ref.TypeConId,
      contractKey: Value,
      extendLocalIdForbiddanceToRelativeV2: Boolean,
  ): Result[GlobalKey] = {
    safelyRun(pullPackage(Seq(templateId))) {
      commandPreprocessor.unsafePreprocessContractKey(
        contractKey,
        templateId,
        extendLocalIdForbiddanceToRelativeV2,
      )
    }
  }

  /** Translates  LF commands to a speedy commands.
    */
  def preprocessApiCommands(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      cmds: data.ImmArray[command.ApiCommand],
  ): Result[ImmArray[speedy.ApiCommand]] = {
    updateGasBudget(cmds)

    safelyRun(pullPackage(pkgResolution, cmds.toSeq.view.map(_.typeRef))) {
      commandPreprocessor.unsafePreprocessApiCommands(pkgResolution, cmds)
    }
  }

  def preprocessDisclosedContracts(
      discs: data.ImmArray[FatContractInstance]
  ): Result[(ImmArray[speedy.DisclosedContract], Set[Value.ContractId], Set[Hash])] = {
    updateGasBudget(discs)

    safelyRun(pullPackage(discs.toSeq.view.map(_.templateId))) {
      commandPreprocessor.unsafePreprocessDisclosedContracts(discs)
    }
  }

  private[engine] def preprocessReplayCommand(
      cmd: ReplayCommand
  ): Result[speedy.Command] = {
    def templateAndInterfaceIds =
      cmd match {
        case ReplayCommand.Create(templateId, _) => List(templateId)
        case ReplayCommand.Exercise(templateId, interfaceId, _, _, _) =>
          templateId :: interfaceId.toList
        case ReplayCommand.ExerciseByKey(templateId, _, _, _) => List(templateId)
        case ReplayCommand.Fetch(templateId, interfaceId, _) =>
          templateId :: interfaceId.toList
        case ReplayCommand.FetchByKey(templateId, _) => List(templateId)
        case ReplayCommand.LookupByKey(templateId, _) => List(templateId)
      }
    safelyRun(pullPackage(templateAndInterfaceIds)) {
      commandPreprocessor.unsafePreprocessReplayCommand(cmd)
    }
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

  def preprocessApiContractKeys(
      pkgResolution: Map[Ref.PackageName, Ref.PackageId],
      keys: Seq[ApiContractKey],
  ): Result[Seq[GlobalKey]] = {
    updateGasBudget(keys)

    safelyRun(pullPackage(pkgResolution, keys.view.map(_.templateRef))) {
      commandPreprocessor.unsafePreprocessApiContractKeys(pkgResolution, keys)
    }
  }

  // TODO: do we need to calculate pre-processor sizes for pre-fetching?
  private[engine] def prefetchContractIdsAndKeys(
      commands: ImmArray[speedy.ApiCommand],
      prefetchKeys: Seq[GlobalKey],
      disclosedContractIds: Set[Value.ContractId],
      disclosedKeyHashes: Set[Hash],
  ): Result[Unit] =
    safelyRun(
      ResultError(
        Error.Preprocessing.Internal(
          NameOf.qualifiedNameOfCurrentFunc,
          "unsafePrefetchKeys should not need packages",
          None,
        )
      )
    ) {
      val keysToPrefetch = unsafePrefetchKeys(commands, prefetchKeys, disclosedKeyHashes)
      val contractIdsToPrefetch = unsafePrefetchContractIds(commands, disclosedContractIds)
      (keysToPrefetch, contractIdsToPrefetch)
    }.flatMap { case (keysToPrefetch, contractIdsToPrefetch) =>
      if (keysToPrefetch.nonEmpty || contractIdsToPrefetch.nonEmpty)
        ResultPrefetch(contractIdsToPrefetch.toSeq, keysToPrefetch, () => ResultDone.Unit)
      else ResultDone.Unit
    }

  private def unsafePrefetchContractIds(
      commands: ImmArray[speedy.ApiCommand],
      disclosedContractIds: Set[Value.ContractId],
  ): Set[Value.ContractId] = {
    val contractIdsInCommands =
      commands.iterator.foldLeft(Set.empty[Value.ContractId])((acc, cmd) =>
        cmd match {
          case speedy.Command.ExerciseTemplate(_, contractId, _, argument) =>
            SValue.addContractIds(argument, acc + contractId.value)
          case speedy.Command.ExerciseInterface(_, contractId, _, argument) =>
            SValue.addContractIds(argument, acc + contractId.value)
          case speedy.Command.ExerciseByKey(_, _, _, argument) =>
            // No need to look at the key because keys cannot contain contract IDs
            SValue.addContractIds(argument, acc)
          case speedy.Command.Create(_, argument) =>
            SValue.addContractIds(argument, acc)
          case speedy.Command.CreateAndExercise(_, createArgument, _, choiceArgument) =>
            SValue.addContractIds(choiceArgument, SValue.addContractIds(createArgument, acc))
        }
      )
    val prefetchContractIds = contractIdsInCommands -- disclosedContractIds
    prefetchContractIds
  }

  private def unsafePrefetchKeys(
      commands: ImmArray[speedy.Command],
      prefetchKeys: Seq[GlobalKey],
      disclosedKeyHashes: Set[crypto.Hash],
  ): Seq[GlobalKey] = {
    val exercisedKeys = commands.iterator.collect {
      case speedy.Command.ExerciseByKey(templateId, contractKey, _, _) =>
        speedy.Speedy.Machine
          .globalKey(pkgInterface, templateId, contractKey)
          .getOrElse(
            throw Error.Preprocessing.ContractIdInContractKey(contractKey.toUnnormalizedValue)
          )
    }
    val undisclosedKeys =
      (exercisedKeys ++ prefetchKeys).filterNot(key => disclosedKeyHashes.contains(key.hash))
    undisclosedKeys.distinct.toSeq
  }
}

private[lf] object Preprocessor {

  object CostModel {
    type Cost = Long

    object StructuralCostModelImplicits {
      implicit def costOfPackageVersion(value: Ref.PackageVersion): Cost =
        1 + value.segments.length.toLong

      implicit def costOfLanguageVersion(value: language.LanguageVersion): Cost =
        1 + value.pretty.length.toLong

      implicit def costOfTypeConRef(value: Ref.TypeConRef): Cost = 1 + value.toString.length.toLong

      implicit def costOfTypeConId(value: Ref.TypeConId): Cost = 1 + value.toString.length.toLong

      implicit def costOfString(value: String): Cost = value.length.toLong

      implicit def costOfContractId(value: Value.ContractId): Cost = 1 + value.coid.length.toLong

      implicit def costOfDate(value: Time.Date): Cost = 1 + costOfInt(value.days)

      implicit def costOfTimestamp(value: Time.Timestamp): Cost = 1 + costOfLong(value.micros)

      implicit def costOfCreationTime(value: CreationTime): Cost =
        1 + costOfLong(CreationTime.encode(value))

      implicit def costOfInt(value: Int): Cost = 4

      implicit def costOfLong(value: Long): Cost = 8

      implicit def costOfUnit(value: Unit): Cost = 0

      // FIXME: make stack safe?
      implicit def costOfValue(value: Value): Cost = value match {
        case Value.ValueBool(_) =>
          1 + 1
        case Value.ValueText(txt) =>
          1 + txt.length.toLong
        case Value.ValueEnum(tycon, value) =>
          1 + costOfOption(tycon) + costOfString(value)
        case Value.ValueContractId(cid) =>
          1 + costOfContractId(cid)
        case Value.ValueDate(date) =>
          1 + costOfDate(date)
        case Value.ValueGenMap(map) =>
          1 + costOfImmArray(map)
        case Value.ValueInt64(n) =>
          1 + costOfLong(n)
        case Value.ValueList(value) =>
          1 + costOfFrontStack(value)(costOfValue)
        case Value.ValueNumeric(value) =>
          1 + 42 // FIXME:
        case Value.ValueOptional(opt) =>
          1 + costOfOption(opt)(costOfValue)
        case Value.ValueParty(p) =>
          1 + costOfString(p)
        case Value.ValueRecord(tyCon, fields) =>
          implicit def costOfFieldEntry(value: (Option[Ref.Name], Value)): Cost = {
            costOfTuple2(value)(costOfOption, costOfValue)
          }
          1 + costOfOption(tyCon) + costOfImmArray(fields)
        case Value.ValueTextMap(ls) =>
          1 + costOfSortedList(ls)(costOfValue)
        case Value.ValueTimestamp(ts) =>
          1 + costOfTimestamp(ts)
        case Value.ValueUnit =>
          1 + costOfUnit(())
        case Value.ValueVariant(tycon, variant, value) =>
          1 + costOfOption(tycon) + costOfString(variant) + costOfValue(value)
      }

      implicit def costOfFatContractInstance(value: FatContractInstance): Cost = {
        val FatContractInstanceImpl(
          version,
          contractId,
          pkgName,
          templateId,
          createArg,
          signatories,
          stakeholders,
          contractKey,
          createdAt,
          authData,
        ) = value

        1 + costOfLanguageVersion(version) + costOfContractId(contractId) + costOfString(
          pkgName
        ) + costOfTypeConId(templateId) + costOfValue(createArg) + costOfTreeSet(
          signatories
        ) + costOfTreeSet(stakeholders) + costOfOption(contractKey) + costOfCreationTime(
          createdAt
        ) + costOfBytes(authData)
      }

      implicit def costOfApiCommand(value: command.ApiCommand): Cost = value match {
        case command.ApiCommand.Create(templateRef, arg) =>
          1 + costOfTypeConRef(templateRef) + costOfValue(arg)

        case command.ApiCommand.Exercise(typeRef, contractId, choiceId, arg) =>
          1 + costOfTypeConRef(typeRef) + costOfContractId(contractId) + costOfString(
            choiceId
          ) + costOfValue(arg)

        case command.ApiCommand.ExerciseByKey(templateRef, contractKey, choiceId, arg) =>
          1 + costOfTypeConRef(templateRef) + costOfValue(contractKey) + costOfString(
            choiceId
          ) + costOfValue(arg)

        case command.ApiCommand.CreateAndExercise(templateRef, createArg, choiceId, choiceArg) =>
          1 + costOfTypeConRef(templateRef) + costOfValue(createArg) + costOfString(
            choiceId
          ) + costOfValue(choiceArg)
      }

      implicit def costOfApiContractKey(value: ApiContractKey): Cost = {
        val ApiContractKey(templateRef, contractKey) = value

        1 + costOfTypeConRef(templateRef) + costOfValue(contractKey)
      }

      implicit def costOfBytes(value: Bytes): Cost = 1 + value.length.toLong

      implicit def costOfHash(value: crypto.Hash): Cost = 1 + value.bytes.length.toLong

      implicit def costOfGlobalKeyWithMaintainers(value: GlobalKeyWithMaintainers): Cost = {
        val GlobalKeyWithMaintainers(key, maintainers) = value
        val costOfGlobalKey =
          1 + costOfTypeConId(key.templateId) + costOfString(key.packageName) + costOfValue(
            key.key
          ) + costOfHash(key.hash)

        1 + costOfGlobalKey + costOfSet(maintainers)
      }

      implicit def costOfTuple2[A, B](
          value: (A, B)
      )(implicit fstCost: A => Cost, sndCost: B => Cost): Cost =
        1 + fstCost(value._1) + sndCost(value._2)

      implicit def costOfOption[A](value: Option[A])(implicit elemCost: A => Cost): Cost =
        1 + value.map(elemCost).getOrElse(0L)

      implicit def costOfMap[A, B](
          value: Map[A, B]
      )(implicit keyCost: A => Cost, valueCost: B => Cost): Cost =
        1 + value.keys.map(keyCost).sum.toLong + value.values.map(valueCost).sum.toLong

      implicit def costOfImmArray[A](value: ImmArray[A])(implicit elemCost: A => Cost): Cost =
        1 + value.toSeq.map(elemCost).sum.toLong

      implicit def costOfSeq[A](value: Seq[A])(implicit elemCost: A => Cost): Cost =
        1 + value.map(elemCost).sum.toLong

      implicit def costOfSortedList[A](value: SortedLookupList[A])(implicit
          elemCost: A => Cost
      ): Cost = 1 + costOfImmArray(value.toImmArray)

      implicit def costOfFrontStack[A](value: FrontStack[A])(implicit elemCost: A => Cost): Cost =
        1 + costOfImmArray(value.toImmArray)(elemCost)

      implicit def costOfSet[A](value: Set[A])(implicit elemCost: A => Cost): Cost =
        1 + value.map(elemCost).sum.toLong

      implicit def costOfTreeSet[A](value: TreeSet[A])(implicit elemCost: A => Cost): Cost =
        1 + value.map(elemCost).sum.toLong
    }
  }

  def forTesting(compilerConfig: speedy.Compiler.Config): Preprocessor =
    forTesting(new ConcurrentCompiledPackages(compilerConfig))

  def forTesting(pkgs: MutableCompiledPackages): Preprocessor =
    new Preprocessor(
      pkgs,
      (pkgId, _) =>
        ResultNeedPackage(
          pkgId,
          {
            case Some(pkg) => pkgs.addPackage(pkgId, pkg)
            case None =>
              ResultError(
                Error.Preprocessing(
                  Error.Preprocessing.Lookup(LookupError.MissingPackage(pkgId))
                )
              )
          },
        ),
    )

  @throws[Error.Preprocessing.Error]
  private[preprocessing] def handleLookup[X](either: Either[LookupError, X]): X = either match {
    case Right(v) => v
    case Left(error) => throw Error.Preprocessing.Lookup(error)
  }

  @inline
  private[preprocessing] def safelyRun[X](
      handleMissingPackages: => Result[_]
  )(unsafeRun: => X): Result[X] = {

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
  private[preprocessing] def safelyRun[X](unsafeRun: => X): Either[Error.Preprocessing.Error, X] =
    try {
      Right(unsafeRun)
    } catch {
      case e: Error.Preprocessing.Error =>
        Left(e)
    }

}
