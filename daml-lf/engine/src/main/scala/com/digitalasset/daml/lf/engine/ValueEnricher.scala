// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, Name, PackageId}
import com.daml.lf.language.{Ast, LookupError}
import com.daml.lf.transaction.{
  GlobalKey,
  GlobalKeyWithMaintainers,
  IncompleteTransaction,
  Node,
  NodeId,
  Transaction,
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
    translateValue: (Ast.Type, Value) => Result[SValue],
    loadPackage: (PackageId, language.Reference) => Result[Unit],
) {

  def this(engine: Engine) =
    this(
      engine.compiledPackages(),
      engine.preprocessor.translateValue,
      engine.loadPackage,
    )

  def enrichValue(typ: Ast.Type, value: Value): Result[Value] =
    translateValue(typ, value).map(_.toUnnormalizedValue)

  def enrichVersionedValue(
      typ: Ast.Type,
      versionedValue: VersionedValue,
  ): Result[VersionedValue] =
    for {
      value <- enrichValue(typ, versionedValue.unversioned)
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
      viewValue: VersionedValue,
  ): Result[VersionedValue] = for {
    view <- enrichView(interfaceId, viewValue.unversioned)
  } yield viewValue.copy(unversioned = view)

  def enrichContract(tyCon: Identifier, value: Value): Result[Value] =
    enrichValue(Ast.TTyCon(tyCon), value)

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
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Result[Value] =
    handleLookup(pkgInterface.lookupChoice(templateId, interfaceId, choiceName))
      .flatMap(choice => enrichValue(choice.argBinder._2, value))

  def enrichChoiceResult(
      templateId: Identifier,
      interfaceId: Option[Identifier],
      choiceName: Name,
      value: Value,
  ): Result[Value] =
    handleLookup(pkgInterface.lookupChoice(templateId, interfaceId, choiceName))
      .flatMap(choice => enrichValue(choice.returnType, value))

  def enrichContractKey(tyCon: Identifier, value: Value): Result[Value] =
    handleLookup(pkgInterface.lookupTemplateKey(tyCon))
      .flatMap(key => enrichValue(key.typ, value))

  private val ResultNone = ResultDone(None)

  def enrichContractKey(
      key: GlobalKeyWithMaintainers,
      packageName: Option[Ref.PackageName],
  ): Result[GlobalKeyWithMaintainers] =
    enrichContractKey(key.globalKey.templateId, key.globalKey.key).map(normalizedKey =>
      key.copy(globalKey =
        GlobalKey.assertWithRenormalizedValue(key.globalKey, normalizedKey, packageName)
      )
    )

  def enrichContractKey(
      key: Option[GlobalKeyWithMaintainers],
      packageName: Option[Ref.PackageName],
  ): Result[Option[GlobalKeyWithMaintainers]] =
    key match {
      case Some(k) =>
        enrichContractKey(k, packageName).map(Some(_))
      case None =>
        ResultNone
    }

  def enrichNode(node: Node): Result[Node] =
    node match {
      case rb @ Node.Rollback(_) =>
        ResultDone(rb)
      case create: Node.Create =>
        for {
          arg <- enrichValue(Ast.TTyCon(create.templateId), create.arg)
          key <- enrichContractKey(create.keyOpt, create.packageName)
        } yield create.copy(arg = arg, keyOpt = key)
      case fetch: Node.Fetch =>
        for {
          key <- enrichContractKey(fetch.keyOpt, fetch.packageName)
        } yield fetch.copy(keyOpt = key)
      case lookup: Node.LookupByKey =>
        for {
          key <- enrichContractKey(lookup.key, lookup.packageName)
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
          key <- enrichContractKey(exe.keyOpt, exe.packageName)
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
