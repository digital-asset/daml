// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.{Ast, LookupError}
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
  IncompleteTransaction,
  Node,
  NodeId,
  Transaction,
  TransactionVersion,
  Versioned,
  VersionedTransaction,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.VersionedValue
import com.digitalasset.daml.lf.speedy.SValue

// Provide methods to add missing information in values (and value containers):
// - type constructor in records, variants, and enums
// - Records' field names

final class ValuePostprocessor(
    compiledPackages: CompiledPackages,
    translateValue: (Ast.Type, Value) => Result[SValue],
    loadPackage: (Ref.PackageId, language.Reference) => Result[Unit],
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
      interfaceId: Ref.TypeConName,
      viewValue: Value,
  ): Result[Value] = for {
    iface <- handleLookup(
      compiledPackages.pkgInterface.lookupInterface(interfaceId)
    )
    r <- enrichValue(iface.view, viewValue)
  } yield r

  def enrichVersionedView(
      interfaceId: Ref.TypeConName,
      viewValue: VersionedValue,
  ): Result[VersionedValue] = for {
    view <- enrichView(interfaceId, viewValue.unversioned)
  } yield viewValue.copy(unversioned = view)

  def enrichContract(tyCon: Ref.TypeConName, value: Value): Result[Value] =
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
      templateId: Ref.TypeConName,
      interfaceId: Option[Ref.TypeConName],
      choiceName: Ref.Name,
      value: Value,
  ): Result[Value] =
    handleLookup(pkgInterface.lookupChoice(templateId, interfaceId, choiceName))
      .flatMap(choice => enrichValue(choice.argBinder._2, value))

  def enrichChoiceResult(
      templateId: Ref.TypeConName,
      interfaceId: Option[Ref.TypeConName],
      choiceName: Ref.Name,
      value: Value,
  ): Result[Value] =
    handleLookup(pkgInterface.lookupChoice(templateId, interfaceId, choiceName))
      .flatMap(choice => enrichValue(choice.returnType, value))

  def enrichContractKey(tyCon: Ref.TypeConName, value: Value): Result[Value] =
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

  def enrichNode(node: Node): Result[Node] =
    node match {
      case rb @ Node.Rollback(_) =>
        ResultDone(rb)
      case create: Node.Create =>
        for {
          arg <- enrichValue(Ast.TTyCon(create.templateId), create.arg)
          key <- enrichContractKey(create.keyOpt)
        } yield create.copy(arg = arg, keyOpt = key)
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

  /*
    This will renormalize a contract w.r.t a (up/down)grade template.
    In practice this potentially
     - adds/removes none fields
     - change the transaction version.
   */
  private[this] def reversionValue(
      typ: Ast.Type,
      value: Value,
      dstVer: TransactionVersion,
  ): Result[Value] =
    translateValue(typ, value).map(_.toNormalizedValue(dstVer))

  // This
  def reversion(
      contract: FatContractInstance,
      dstTmplId: Ref.TypeConName,
  ): Result[FatContractInstance] = {
    if (contract.templateId == dstTmplId)
      ResultDone(contract)
    else
      for {
        dstPkg <- handleLookup(compiledPackages.pkgInterface.lookupPackage(dstTmplId.packageId))
        _ <-
          if (
            dstPkg.pkgName == contract.packageName && dstTmplId.qualifiedName == contract.templateId.qualifiedName
          )
            Result.unit
          else
            ResultError(
              Error.Postprocessing.ReversioningMismatch(
                contract.contractId,
                dstPkg.pkgName,
                dstTmplId.qualifiedName,
                contract.packageName,
                contract.templateId.qualifiedName,
              )
            )
        dstTxVersion = TransactionVersion.assignNodeVersion(dstPkg.languageVersion)
        dstArgType = Ast.TTyCon(dstTmplId)
        createArg <- reversionValue(dstArgType, contract.createArg, dstTxVersion)
        keyOpt <- contract.contractKeyWithMaintainers match {
          case Some(GlobalKeyWithMaintainers(globalKey, maintainers)) =>
            for {
              keyDef <- handleLookup(compiledPackages.pkgInterface.lookupContractKey(dstTmplId))
              keyVal <- reversionValue(keyDef.typ, globalKey.key, dstTxVersion)
            } yield Some(
              GlobalKeyWithMaintainers.assertBuild(
                dstTmplId,
                keyVal,
                maintainers,
                dstPkg.pkgName,
              )
            )
          case None => ResultNone
        }
      } yield contract.toImplementation.copy(
        createArg = createArg,
        contractKeyWithMaintainers = keyOpt,
        version = dstTxVersion,
      )
  }

}
