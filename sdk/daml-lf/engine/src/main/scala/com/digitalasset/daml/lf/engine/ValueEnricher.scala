// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, Name, PackageId, QualifiedName}
import com.daml.lf.language.{Ast, LookupError}
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  IncompleteTransaction,
  Node,
  NodeId,
  Transaction,
  TransactionVersion,
  VersionedTransaction,
}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.VersionedValue
import com.daml.lf.speedy.SValue

// Provide methods to add missing information in values (and value containers):
// - type constructor in records, variants, and enums
// - Records' field names

final class ValueEnricher(
    compiledPackages: CompiledPackages,
    translateValue: (Ast.Type, Boolean, Value) => Result[SValue],
    loadPackage: (PackageId, language.Reference) => Result[Unit],
) {

  def this(engine: Engine) =
    this(
      engine.compiledPackages(),
      engine.preprocessor.translateValue,
      engine.loadPackage,
    )

  def enrichValue(typ: Ast.Type, upgradable: Boolean, value: Value): Result[Value] =
    translateValue(typ, upgradable, value).map(_.toUnnormalizedValue)

  def enrichVersionedValue(
      typ: Ast.Type,
      upgradable: Boolean,
      versionedValue: VersionedValue,
  ): Result[VersionedValue] =
    for {
      value <- enrichValue(typ, upgradable, versionedValue.unversioned)
    } yield versionedValue.map(_ => value)

  def enrichContract(
      contract: Value.ContractInstance
  ): Result[Value.ContractInstance] =
    for {
      arg <- enrichContract(contract.template, contract.arg)
    } yield contract.copy(arg = arg)

  def enrichVersionedContract(
      contract: Value.VersionedContractInstance
  ): Result[Value.VersionedContractInstance] =
    for {
      pkg <- handleLookup(
        compiledPackages.pkgInterface.lookupPackage(contract.unversioned.template.packageId)
      )
      arg <- enrichValue(
        Ast.TTyCon(contract.unversioned.template),
        pkg.upgradable,
        contract.unversioned.arg,
      )
    } yield contract.map(_.copy(arg = arg))

  def enrichView(
      interfaceId: Identifier,
      viewValue: Value,
  ): Result[Value] = for {
    pkg <- handleLookup(
      compiledPackages.pkgInterface.lookupPackage(interfaceId.packageId)
    )
    iface <- handleLookup(
      compiledPackages.pkgInterface.lookupInterface(interfaceId)
    )
    r <- enrichValue(iface.view, pkg.upgradable, viewValue)
  } yield r

  def enrichVersionedView(
      interfaceId: Identifier,
      viewValue: VersionedValue,
  ): Result[VersionedValue] = for {
    view <- enrichView(interfaceId, viewValue.unversioned)
  } yield viewValue.copy(unversioned = view)

  def enrichContract(tyCon: Identifier, value: Value): Result[Value] =
    for {
      pkg <- handleLookup(
        compiledPackages.pkgInterface.lookupPackage(tyCon.packageId)
      )
      enrichedValue <- enrichValue(Ast.TTyCon(tyCon), pkg.upgradable, value)
    } yield enrichedValue

  private[this] def pkgInterface = compiledPackages.pkgInterface

  private[this] def handleLookup[X](lookup: => Either[LookupError, X]) = lookup match {
    case Right(value) => ResultDone(value)
    case Left(LookupError.MissingPackage(pkgId, context)) =>
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

  def enrichChoiceArgument(
      choicePackageId: PackageId,
      qualifiedTemplateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Result[Value] = for {
    choice <- handleLookup(
      pkgInterface.lookupChoice(
        Identifier(choicePackageId, qualifiedTemplateName),
        interfaceId,
        choiceName,
      )
    )
    pkg <- handleLookup(
      compiledPackages.pkgInterface.lookupPackage(choicePackageId)
    )
    enrichedValue <- enrichValue(choice.argBinder._2, pkg.upgradable, value)
  } yield enrichedValue

  // Deprecated
  def enrichChoiceArgument(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Result[Value] =
    enrichChoiceArgument(
      templateId.packageId,
      templateId.qualifiedName,
      interfaceId,
      choiceName,
      value,
    )

  def enrichChoiceResult(
      choicePackageId: PackageId,
      qualifiedTemplateName: QualifiedName,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Result[Value] = for {
    choice <- handleLookup(
      pkgInterface.lookupChoice(
        Identifier(choicePackageId, qualifiedTemplateName),
        interfaceId,
        choiceName,
      )
    )
    pkg <- handleLookup(pkgInterface.lookupPackage(choicePackageId))
    enrichedValue <- enrichValue(choice.returnType, pkg.upgradable, value)
  } yield enrichedValue

  // Deprecated
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

  def enrichContractKey(tyCon: Identifier, value: Value): Result[Value] =
    for {
      key <- handleLookup(pkgInterface.lookupTemplateKey(tyCon))
      pkg <- handleLookup(pkgInterface.lookupPackage(tyCon.packageId))
      enrichedValue <- enrichValue(key.typ, pkg.upgradable, value)
    } yield enrichedValue

  private val ResultNone = ResultDone(None)

  def enrichContractKey(
      key: GlobalKeyWithMaintainers,
      packageName: KeyPackageName,
  ): Result[GlobalKeyWithMaintainers] =
    enrichContractKey(key.globalKey.templateId, key.globalKey.key).map(normalizedKey =>
      key.copy(globalKey =
        GlobalKey.assertWithRenormalizedValue(key.globalKey, normalizedKey, packageName)
      )
    )

  def enrichContractKey(
      key: Option[GlobalKeyWithMaintainers],
      version: TransactionVersion,
      packageName: Option[Ref.PackageName],
  ): Result[Option[GlobalKeyWithMaintainers]] =
    key match {
      case Some(k) =>
        enrichContractKey(k, KeyPackageName(packageName, version)).map(Some(_))
      case None =>
        ResultNone
    }

  def enrichNode(node: Node): Result[Node] =
    node match {
      case rb @ Node.Rollback(_) =>
        ResultDone(rb)
      case create: Node.Create =>
        for {
          pkg <- handleLookup(pkgInterface.lookupPackage(create.templateId.packageId))
          arg <- enrichValue(Ast.TTyCon(create.templateId), pkg.upgradable, create.arg)
          key <- enrichContractKey(create.keyOpt, create.version, create.packageName)
        } yield create.copy(arg = arg, keyOpt = key)
      case fetch: Node.Fetch =>
        for {
          key <- enrichContractKey(fetch.keyOpt, fetch.version, fetch.packageName)
        } yield fetch.copy(keyOpt = key)
      case lookup: Node.LookupByKey =>
        for {
          key <- enrichContractKey(lookup.key, KeyPackageName(lookup.packageName, lookup.version))
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
          key <- enrichContractKey(exe.keyOpt, exe.version, exe.packageName)
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
