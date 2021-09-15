// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref.{Identifier, Name, PackageId}
import com.daml.lf.language.{Ast, LookupError}
import com.daml.lf.transaction.Node.{GenNode, KeyWithMaintainers}
import com.daml.lf.transaction.{CommittedTransaction, Node, NodeId, VersionedTransaction}
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

  def this(engine: Engine) = {
    this(
      engine.compiledPackages(),
      engine.preprocessor.translateValue,
      engine.loadPackage,
    )
  }

  def enrichValue(typ: Ast.Type, value: Value): Result[Value] = {
    translateValue(typ, value).map(_.toUnnormalizedValue)
  }

  def enrichVersionedValue(
      typ: Ast.Type,
      versionedValue: VersionedValue,
  ): Result[VersionedValue] =
    for {
      value <- enrichValue(typ, versionedValue.value)
    } yield versionedValue.copy(value = value)

  def enrichContract(
      contract: Value.ContractInst[Value]
  ): Result[Value.ContractInst[Value]] =
    for {
      arg <- enrichContract(contract.template, contract.arg)
    } yield contract.copy(arg = arg)

  def enrichVersionedContract(
      contract: Value.ContractInst[VersionedValue]
  ): Result[Value.ContractInst[VersionedValue]] =
    for {
      arg <- enrichVersionedValue(Ast.TTyCon(contract.template), contract.arg)
    } yield contract.copy(arg = arg)

  def enrichContract(tyCon: Identifier, value: Value): Result[Value] =
    enrichValue(Ast.TTyCon(tyCon), value)

  private[this] def interface = compiledPackages.interface

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
      tyCon: Identifier,
      choiceName: Name,
      value: Value,
  ): Result[Value] =
    handleLookup(interface.lookupChoice(tyCon, choiceName))
      .flatMap(choice => enrichValue(choice.argBinder._2, value))

  def enrichChoiceResult(
      tyCon: Identifier,
      choiceName: Name,
      value: Value,
  ): Result[Value] =
    handleLookup(interface.lookupChoice(tyCon, choiceName)).flatMap(choice =>
      enrichValue(choice.returnType, value)
    )

  def enrichContractKey(tyCon: Identifier, value: Value): Result[Value] =
    handleLookup(interface.lookupTemplateKey(tyCon))
      .flatMap(key => enrichValue(key.typ, value))

  def enrichVersionedContractKey(
      tyCon: Identifier,
      value: VersionedValue,
  ): Result[VersionedValue] =
    handleLookup(interface.lookupTemplateKey(tyCon))
      .flatMap(key => enrichVersionedValue(key.typ, value))

  private val ResultNone = ResultDone(None)

  def enrichContractKey(
      tyCon: Identifier,
      key: KeyWithMaintainers[Value],
  ): Result[KeyWithMaintainers[Value]] =
    enrichContractKey(tyCon, key.key).map(normalizedKey => key.copy(key = normalizedKey))

  def enrichContractKey(
      tyCon: Identifier,
      key: Option[KeyWithMaintainers[Value]],
  ): Result[Option[KeyWithMaintainers[Value]]] =
    key match {
      case Some(k) =>
        enrichContractKey(tyCon, k).map(Some(_))
      case None =>
        ResultNone
    }

  def enrichVersionedContractKey(
      tyCon: Identifier,
      key: KeyWithMaintainers[VersionedValue],
  ): Result[KeyWithMaintainers[VersionedValue]] =
    enrichVersionedContractKey(tyCon, key.key).map(normalizedKey => key.copy(key = normalizedKey))

  def enrichVersionedContractKey(
      tyCon: Identifier,
      key: Option[KeyWithMaintainers[VersionedValue]],
  ): Result[Option[KeyWithMaintainers[VersionedValue]]] =
    key match {
      case Some(k) =>
        enrichVersionedContractKey(tyCon, k).map(Some(_))
      case None =>
        ResultNone
    }

  def enrichNode[Nid](node: GenNode[Nid]): Result[GenNode[Nid]] =
    node match {
      case rb @ Node.NodeRollback(_) =>
        ResultDone(rb)
      case create: Node.NodeCreate =>
        for {
          arg <- enrichValue(Ast.TTyCon(create.templateId), create.arg)
          key <- enrichContractKey(create.templateId, create.key)
        } yield create.copy(arg = arg, key = key)
      case fetch: Node.NodeFetch =>
        for {
          key <- enrichContractKey(fetch.templateId, fetch.key)
        } yield fetch.copy(key = key)
      case lookup: Node.NodeLookupByKey =>
        for {
          key <- enrichContractKey(lookup.templateId, lookup.key)
        } yield lookup.copy(key = key)
      case exe: Node.NodeExercises[Nid] =>
        for {
          choiceArg <- enrichChoiceArgument(exe.templateId, exe.choiceId, exe.chosenValue)
          result <- exe.exerciseResult match {
            case Some(exeResult) =>
              enrichChoiceResult(exe.templateId, exe.choiceId, exeResult).map(Some(_))
            case None =>
              ResultNone
          }
          key <- enrichContractKey(exe.templateId, exe.key)
        } yield exe.copy(chosenValue = choiceArg, exerciseResult = result, key = key)
    }

  def enrichTransaction(tx: CommittedTransaction): Result[CommittedTransaction] = {
    for {
      normalizedNodes <-
        tx.nodes.foldLeft[Result[Map[NodeId, GenNode[NodeId]]]](ResultDone(Map.empty)) {
          case (acc, (nid, node)) =>
            for {
              nodes <- acc
              normalizedNode <- enrichNode(node)
            } yield nodes.updated(nid, normalizedNode)
        }
    } yield CommittedTransaction(
      VersionedTransaction(
        version = tx.version,
        nodes = normalizedNodes,
        roots = tx.roots,
      )
    )
  }

}
