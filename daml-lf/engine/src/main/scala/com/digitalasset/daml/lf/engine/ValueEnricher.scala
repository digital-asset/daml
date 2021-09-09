// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.data.Ref.{Identifier, Name}
import com.daml.lf.language.{Ast, LookupError}
import com.daml.lf.transaction.Node.{GenNode, KeyWithMaintainers}
import com.daml.lf.transaction.{CommittedTransaction, Node, NodeId, VersionedTransaction}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, VersionedValue}

// Provide methods to add missing information in values (and value containers):
// - type constructor in records, variants, and enums
// - Records' field names
final class ValueEnricher(engine: Engine) {

  def enrichValue(typ: Ast.Type, value: Value[ContractId]): Result[Value[ContractId]] =
    engine.preprocessor.translateValue(typ, value).map(_.toUnnormalizedValue)

  def enrichVersionedValue(
      typ: Ast.Type,
      versionedValue: VersionedValue[ContractId],
  ): Result[VersionedValue[ContractId]] =
    for {
      value <- enrichValue(typ, versionedValue.value)
    } yield versionedValue.copy(value = value)

  def enrichContract(
      contract: Value.ContractInst[Value[ContractId]]
  ): Result[Value.ContractInst[Value[ContractId]]] =
    for {
      arg <- enrichContract(contract.template, contract.arg)
    } yield contract.copy(arg = arg)

  def enrichVersionedContract(
      contract: Value.ContractInst[VersionedValue[ContractId]]
  ): Result[Value.ContractInst[VersionedValue[ContractId]]] =
    for {
      arg <- enrichVersionedValue(Ast.TTyCon(contract.template), contract.arg)
    } yield contract.copy(arg = arg)

  def enrichContract(tyCon: Identifier, value: Value[ContractId]): Result[Value[ContractId]] =
    enrichValue(Ast.TTyCon(tyCon), value)

  private[this] def interface = engine.compiledPackages().interface

  private[this] def handleLookup[X](lookup: => Either[LookupError, X]) = lookup match {
    case Right(value) => ResultDone(value)
    case Left(LookupError.MissingPackage(pkgId, context)) =>
      engine
        .loadPackage(pkgId, context)
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
      value: Value[ContractId],
  ): Result[Value[ContractId]] =
    handleLookup(interface.lookupChoice(tyCon, choiceName))
      .flatMap(choice => enrichValue(choice.argBinder._2, value))

  def enrichChoiceResult(
      tyCon: Identifier,
      choiceName: Name,
      value: Value[ContractId],
  ): Result[Value[ContractId]] =
    handleLookup(interface.lookupChoice(tyCon, choiceName)).flatMap(choice =>
      enrichValue(choice.returnType, value)
    )

  def enrichContractKey(tyCon: Identifier, value: Value[ContractId]): Result[Value[ContractId]] =
    handleLookup(interface.lookupTemplateKey(tyCon))
      .flatMap(key => enrichValue(key.typ, value))

  def enrichVersionedContractKey(
      tyCon: Identifier,
      value: VersionedValue[ContractId],
  ): Result[VersionedValue[ContractId]] =
    handleLookup(interface.lookupTemplateKey(tyCon))
      .flatMap(key => enrichVersionedValue(key.typ, value))

  private val ResultNone = ResultDone(None)

  def enrichContractKey(
      tyCon: Identifier,
      key: KeyWithMaintainers[Value[ContractId]],
  ): Result[KeyWithMaintainers[Value[ContractId]]] =
    enrichContractKey(tyCon, key.key).map(normalizedKey => key.copy(key = normalizedKey))

  def enrichContractKey(
      tyCon: Identifier,
      key: Option[KeyWithMaintainers[Value[ContractId]]],
  ): Result[Option[KeyWithMaintainers[Value[ContractId]]]] =
    key match {
      case Some(k) =>
        enrichContractKey(tyCon, k).map(Some(_))
      case None =>
        ResultNone
    }

  def enrichVersionedContractKey(
      tyCon: Identifier,
      key: KeyWithMaintainers[VersionedValue[ContractId]],
  ): Result[KeyWithMaintainers[VersionedValue[ContractId]]] =
    enrichVersionedContractKey(tyCon, key.key).map(normalizedKey => key.copy(key = normalizedKey))

  def enrichVersionedContractKey(
      tyCon: Identifier,
      key: Option[KeyWithMaintainers[VersionedValue[ContractId]]],
  ): Result[Option[KeyWithMaintainers[VersionedValue[ContractId]]]] =
    key match {
      case Some(k) =>
        enrichVersionedContractKey(tyCon, k).map(Some(_))
      case None =>
        ResultNone
    }

  def enrichNode[Nid](node: GenNode[Nid, ContractId]): Result[GenNode[Nid, ContractId]] =
    node match {
      case rb @ Node.NodeRollback(_) =>
        ResultDone(rb)
      case create: Node.NodeCreate[ContractId] =>
        for {
          arg <- enrichValue(Ast.TTyCon(create.templateId), create.arg)
          key <- enrichContractKey(create.templateId, create.key)
        } yield create.copy(arg = arg, key = key)
      case fetch: Node.NodeFetch[ContractId] =>
        for {
          key <- enrichContractKey(fetch.templateId, fetch.key)
        } yield fetch.copy(key = key)
      case lookup: Node.NodeLookupByKey[ContractId] =>
        for {
          key <- enrichContractKey(lookup.templateId, lookup.key)
        } yield lookup.copy(key = key)
      case exe: Node.NodeExercises[Nid, ContractId] =>
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
        tx.nodes.foldLeft[Result[Map[NodeId, GenNode[NodeId, ContractId]]]](ResultDone(Map.empty)) {
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
