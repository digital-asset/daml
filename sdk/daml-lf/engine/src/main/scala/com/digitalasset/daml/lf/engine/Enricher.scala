// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import cats.data.NonEmptySet
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{Identifier, Name, PackageId, PackageRef, QualifiedName}
import com.digitalasset.daml.lf.interpretation.{Error => IError}
import com.digitalasset.daml.lf.language.{Ast, LookupError}
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.value.Value

// Provide methods to add missing information in values (and value containers):
// - type constructor in records, variants, and enums
// - Records' field names

object Enricher {

  // impoverish remove redundant information from a value added by
  // enrichValue.
  // we have the following invariant:
  // impoverish(enrich(type, impoverish(value))) == impoverish(value)
  def impoverish(value: Value): Value = {
    def go(value0: Value, nesting: Int): Value =
      if (nesting > Value.MAXIMUM_NESTING) {
        throw Error.Preprocessing.ValueNesting(value)
      } else {
        val newNesting = nesting + 1
        value0 match {
          case Value.ValueEnum(_, cons) =>
            Value.ValueEnum(None, cons)
          case _: Value.ValueCidlessLeaf | _: Value.ValueContractId => value0
          case Value.ValueRecord(_, fields) =>
            val n = fields.reverseIterator.dropWhile(_._2 == Value.ValueNone).size
            Value.ValueRecord(
              tycon = None,
              fields = fields.iterator
                .take(n)
                .map { case (_, v) => None -> go(v, newNesting) }
                .to(ImmArray),
            )
          case Value.ValueVariant(_, variant, value) =>
            Value.ValueVariant(
              tycon = None,
              variant = variant,
              value = go(value, newNesting),
            )
          case Value.ValueList(values) =>
            Value.ValueList(
              values = values.map(go(_, newNesting))
            )
          case Value.ValueOptional(value) =>
            Value.ValueOptional(
              value = value.map(go(_, newNesting))
            )
          case Value.ValueTextMap(value) =>
            Value.ValueTextMap(
              value = value.mapValue(go(_, newNesting))
            )
          case Value.ValueGenMap(entries) =>
            Value.ValueGenMap(
              entries = entries.map { case (k, v) =>
                go(k, newNesting) -> go(v, newNesting)
              }
            )
        }
      }

    go(value, 0)
  }

  private def impoverish(key: GlobalKey): GlobalKey =
    GlobalKey.assertBuild(
      templateId = key.templateId,
      key = impoverish(key.key),
      key.packageName,
    )

  private def impoverish(key: GlobalKeyWithMaintainers): GlobalKeyWithMaintainers =
    GlobalKeyWithMaintainers(
      globalKey = impoverish(key.globalKey),
      maintainers = key.maintainers,
    )

  private def impoverish(create: Node.Create): Node.Create = {
    import create._
    Node.Create(
      coid = coid,
      packageName = packageName,
      templateId = templateId,
      arg = impoverish(arg),
      signatories = signatories,
      stakeholders = stakeholders,
      keyOpt = keyOpt.map(impoverish),
      version = version,
    )
  }

  private def impoverish(node: Node): Node =
    node match {
      case create: Node.Create =>
        impoverish(create)
      case fetch: Node.Fetch =>
        import fetch._
        Node.Fetch(
          coid = coid,
          packageName = packageName,
          templateId = templateId,
          actingParties = actingParties,
          signatories = signatories,
          stakeholders = stakeholders,
          keyOpt = keyOpt.map(impoverish),
          byKey = byKey,
          interfaceId = interfaceId,
          version = version,
        )
      case lookup: Node.LookupByKey =>
        import lookup._
        Node.LookupByKey(
          packageName = packageName,
          templateId = templateId,
          key = impoverish(key),
          result = result,
          version = version,
        )
      case exe: Node.Exercise =>
        import exe._
        Node.Exercise(
          targetCoid = targetCoid,
          packageName = packageName,
          templateId = templateId,
          interfaceId = interfaceId,
          choiceId = choiceId,
          consuming = consuming,
          actingParties = actingParties,
          chosenValue = impoverish(chosenValue),
          stakeholders = stakeholders,
          signatories = signatories,
          choiceObservers = choiceObservers,
          choiceAuthorizers = choiceAuthorizers,
          children = children,
          exerciseResult = exerciseResult.map(impoverish),
          keyOpt = keyOpt.map(impoverish),
          byKey = byKey,
          version = version,
        )
      case rb: Node.Rollback => rb
    }

  def impoverish(tx: VersionedTransaction): VersionedTransaction =
    VersionedTransaction(
      version = tx.version,
      nodes = tx.nodes.map { case (nid, node) => nid -> impoverish(node) },
      roots = tx.roots,
    )

  def impoverish(contract: FatContractInstance): FatContractInstance = {
    val create = impoverish(contract.toCreateNode)
    FatContractInstance.fromCreateNode(
      create = create,
      createTime = contract.createdAt,
      authenticationData = contract.authenticationData,
    )
  }
}

final class Enricher(
    compiledPackages: CompiledPackages,
    loadPackage: (PackageId, language.Reference) => Result[Unit],
    addTypeInfo: Boolean,
    addFieldNames: Boolean,
    addTrailingNoneFields: Boolean,
    forbidLocalContractIds: Boolean,
) {

  def this(
      engine: Engine,
      addTypeInfo: Boolean = true,
      addFieldNames: Boolean = true,
      addTrailingNoneFields: Boolean = true,
      forbidLocalContractIds: Boolean = true,
  ) =
    this(
      engine.compiledPackages(),
      engine.loadPackage,
      addTypeInfo = addTypeInfo,
      addFieldNames = addFieldNames,
      addTrailingNoneFields = addTrailingNoneFields,
      forbidLocalContractIds,
    )

  val preprocessor = new preprocessing.Preprocessor(
    compiledPackages,
    loadPackage,
    forbidLocalContractIds = forbidLocalContractIds,
  )

  private[this] def toValue(sValue: SValue): Value =
    sValue.toValue(
      keepTypeInfo = addTypeInfo,
      keepFieldName = addFieldNames,
      keepTrailingNoneFields = addTrailingNoneFields,
    )

  def enrichValue(typ: Ast.Type, value: Value): Result[Value] =
    preprocessor
      .translateValue(typ, value)
      .map(toValue)

  def enrichVersionedValue(
      typ: Ast.Type,
      versionedValue: Value.VersionedValue,
  ): Result[Value.VersionedValue] =
    for {
      value <- enrichValue(typ, versionedValue.unversioned)
    } yield versionedValue.map(_ => value)

  def enrichContract(
      contract: Value.ThinContractInstance
  ): Result[Value.ThinContractInstance] =
    for {
      arg <- enrichContract(contract.template, contract.arg)
    } yield contract.copy(arg = arg)

  def enrichContract(
      contract: FatContractInstance
  ): Result[FatContractInstance] =
    enrichCreate(contract.toCreateNode).map(create =>
      FatContractInstance.fromCreateNode(create, contract.createdAt, contract.authenticationData)
    )

  /** Verifies that [contract] hashes to the same value according to all packages in [packageIds] (i.e. renders to
    * the same value modulo package IDs) and returns the contract enriched by the smallest package ID.
    *
    * Returns a Left if:
    *   - the contract hashes to different values according to different packages
    *   - the contract contains a key.
    *
    * Returns a ResultError if the enrichment fails for unexpected reasons (e.g. typechecking fails).
    */
  def enrichContractWithPackages(
      contract: FatContractInstance,
      packageIds: NonEmptySet[PackageId],
  ): Result[Either[String, FatContractInstance]] = {
    enrichCreateWithPackages(contract.toCreateNode, packageIds).map(createOrError =>
      createOrError.map(create =>
        FatContractInstance.fromCreateNode(create, contract.createdAt, contract.authenticationData)
      )
    )
  }

  def enrichVersionedContract(
      contract: Value.VersionedThinContractInstance
  ): Result[Value.VersionedThinContractInstance] =
    for {
      arg <- enrichValue(Ast.TTyCon(contract.unversioned.template), contract.unversioned.arg)
    } yield contract.map(_.copy(arg = arg))

  def enrichView(
      interfaceId: Identifier,
      viewValue: Value,
  ): Result[Value] = for {
    iface <- handleLookup(
      compiledPackages.pkgInterface.lookupInterface(interfaceId)
    )
    r <- enrichValue(iface.view, viewValue)
  } yield r

  def enrichVersionedView(
      interfaceId: Identifier,
      viewValue: Value.VersionedValue,
  ): Result[Value.VersionedValue] = for {
    view <- enrichView(interfaceId, viewValue.unversioned)
  } yield viewValue.copy(unversioned = view)

  def enrichContract(tyCon: Identifier, value: Value): Result[Value] =
    enrichValue(Ast.TTyCon(tyCon), value)

  private[this] def pkgInterface = compiledPackages.pkgInterface

  private[this] def handleLookup[X](lookup: => Either[LookupError, X]): Result[X] = lookup match {
    case Right(value) => ResultDone(value)
    case Left(LookupError.MissingPackage(PackageRef.Id(pkgId), context)) =>
      loadPackage(pkgId, context)
        .flatMap(_ =>
          lookup match {
            case Right(value) => ResultDone(value)
            case Left(err) => ResultError(Error.Preprocessing.Lookup(err))
          }
        )
    case Left(error) =>
      ResultError(Error.Preprocessing.Lookup(error))
  }

  // deprecated
  def enrichChoiceArgument(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Result[Value] = enrichChoiceArgument(
    templateId.packageId,
    templateId.qualifiedName,
    interfaceId,
    choiceName,
    value,
  )

  def enrichChoiceArgument(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Result[Value] =
    handleLookup(
      pkgInterface.lookupChoice(Identifier(packageId, templateName), interfaceId, choiceName)
    )
      .flatMap(choice => enrichValue(choice.argBinder._2, value))

  // deprecated
  def enrichChoiceResult(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Result[Value] = enrichChoiceResult(
    templateId.packageId,
    templateId.qualifiedName,
    interfaceId,
    choiceName,
    value,
  )

  def enrichChoiceResult(
      packageId: PackageId,
      templateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Result[Value] =
    handleLookup(
      pkgInterface.lookupChoice(Identifier(packageId, templateName), interfaceId, choiceName)
    )
      .flatMap(choice => enrichValue(choice.returnType, value))

  def enrichContractKey(tyCon: Identifier, value: Value): Result[Value] =
    handleLookup(pkgInterface.lookupTemplateKey(tyCon))
      .flatMap(key => enrichValue(key.typ, value))

  private val ResultNone = ResultDone(None)

  def enrichContractKey(
      key: GlobalKeyWithMaintainers
  ): Result[GlobalKeyWithMaintainers] =
    enrichContractKey(key.globalKey.templateId, key.globalKey.key).map(normalizedKey =>
      key.copy(globalKey = GlobalKey.assertWithRenormalizedValue(key.globalKey, normalizedKey))
    )

  def enrichContractKey(
      key: Option[GlobalKeyWithMaintainers]
  ): Result[Option[GlobalKeyWithMaintainers]] =
    key match {
      case Some(k) =>
        enrichContractKey(k).map(Some(_))
      case None =>
        ResultNone
    }

  def enrichVersionedContractKey(
      key: Versioned[GlobalKeyWithMaintainers]
  ): Result[Versioned[GlobalKeyWithMaintainers]] =
    enrichContractKey(key.unversioned).map(normalizedValue => key.map(_ => normalizedValue))

  def enrichVersionedContractKey(
      key: Option[Versioned[GlobalKeyWithMaintainers]]
  ): Result[Option[Versioned[GlobalKeyWithMaintainers]]] =
    key match {
      case Some(k) =>
        enrichVersionedContractKey(k).map(Some(_))
      case None =>
        ResultNone
    }

  def enrichCreate(create: Node.Create): Result[Node.Create] =
    for {
      arg <- enrichValue(Ast.TTyCon(create.templateId), create.arg)
      key <- enrichContractKey(create.keyOpt)
    } yield create.copy(arg = arg, keyOpt = key)

  /** Verifies that [create] hashes to the same value according to all packages in [packageIds] (i.e. renders to
    * the same value modulo package IDs) and returns the Create node enriched by the smallest package ID.
    *
    * Returns a Left if:
    *   - the contract hashes to different values according to different packages
    *   - the contract contains a key.
    *
    * Returns a ResultError if the enrichment fails for unexpected reasons (e.g. typechecking fails).
    */
  def enrichCreateWithPackages(
      create: Node.Create,
      packageIds: NonEmptySet[PackageId],
  ): Result[Either[String, Node.Create]] = {
    import Result.ResultInstances._
    import cats.implicits._

    def translateWithPackage(pkgId: PackageId): Result[(PackageId, SValue)] =
      for {
        _ <-
          if (!compiledPackages.contains(pkgId))
            loadPackage(pkgId, language.Reference.Package(PackageRef.Id(pkgId)))
          else ResultDone(())
        res <- preprocessor.translateValue(
          Ast.TTyCon(Identifier(pkgId, create.templateId.qualifiedName)),
          create.arg,
        )
      } yield pkgId -> res

    def hashSValue(pkgId: PackageId, sValue: SValue): Result[Hash] =
      SValueHash
        .hashContractInstance(
          compiledPackages.signatures(pkgId).pkgName,
          create.templateId.qualifiedName,
          sValue,
        )
        .left
        .map(error =>
          Error.Interpretation(
            Error.Interpretation.DamlException(
              IError.ContractHashingError(
                create.coid,
                create.templateId.copy(pkg = pkgId),
                create.arg,
                error.msg,
              )
            ),
            None,
          )
        )
        .fold(ResultError(_), ResultDone(_))

    create.keyOpt match {
      case Some(_) =>
        ResultDone(Left("enrichCreateWithPackages does not support contract keys"))
      case None =>
        val packageIdList = packageIds.toNonEmptyList
        val targetPkgId = packageIdList.iterator.min
        for {
          result <- translateWithPackage(targetPkgId)
          (_, resultSValue) = result
          others <- packageIdList.filter(_ != targetPkgId).traverse(translateWithPackage)
          resultHash <- hashSValue(targetPkgId, resultSValue)
          otherHashes <- others.traverse { case (pkgId, sval) => hashSValue(pkgId, sval) }
        } yield {
          otherHashes.find(_ != resultHash) match {
            case Some(otherPkgId) =>
              Left(
                s"Contract ${create.coid} enriches to different values for packages ${targetPkgId} and ${otherPkgId}"
              )
            case None =>
              Right(create.copy(arg = toValue(resultSValue)))
          }
        }
    }
  }

  private def enrichNode(node: Node): Result[Node] =
    node match {
      case rb @ Node.Rollback(_) =>
        ResultDone(rb)
      case create: Node.Create =>
        enrichCreate(create)
      case fetch: Node.Fetch =>
        for {
          key <- enrichContractKey(fetch.keyOpt)
        } yield fetch.copy(keyOpt = key)
      case lookup: Node.LookupByKey =>
        for {
          key <- enrichContractKey(lookup.key)
        } yield lookup.copy(key = key)
      case exe: Node.Exercise =>
        for {
          choiceArg <- enrichChoiceArgument(
            exe.templateId,
            exe.interfaceId,
            exe.choiceId,
            exe.chosenValue,
          )
          result <- exe.exerciseResult match {
            case Some(exeResult) =>
              enrichChoiceResult(exe.templateId, exe.interfaceId, exe.choiceId, exeResult).map(
                Some(_)
              )
            case None =>
              ResultNone
          }
          key <- enrichContractKey(exe.keyOpt)
        } yield exe.copy(chosenValue = choiceArg, exerciseResult = result, keyOpt = key)
    }

  def enrichTransaction(tx: Transaction): Result[Transaction] =
    for {
      normalizedNodes <-
        tx.nodes.foldLeft[Result[Map[NodeId, Node]]](ResultDone(Map.empty)) {
          case (acc, (nid, node)) =>
            for {
              nodes <- acc
              normalizedNode <- enrichNode(node)
            } yield nodes.updated(nid, normalizedNode)
        }
    } yield Transaction(
      nodes = normalizedNodes,
      roots = tx.roots,
    )

  def enrichVersionedTransaction(versionedTx: VersionedTransaction): Result[VersionedTransaction] =
    enrichTransaction(Transaction(versionedTx.nodes, versionedTx.roots)).map {
      case Transaction(nodes, roots) =>
        VersionedTransaction(versionedTx.version, nodes, roots)
    }

  def enrichIncompleteTransaction(
      incompleteTx: IncompleteTransaction
  ): Result[IncompleteTransaction] =
    enrichTransaction(incompleteTx.transaction).map(transaction =>
      incompleteTx.copy(transaction = transaction)
    )
}
