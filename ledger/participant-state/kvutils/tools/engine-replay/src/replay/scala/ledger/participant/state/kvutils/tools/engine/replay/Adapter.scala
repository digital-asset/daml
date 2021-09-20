// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.replay

import com.daml.lf.data._
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.transaction.test.{TransactionBuilder => TxBuilder}
import com.daml.lf.transaction.{GlobalKey, Node, NodeId, SubmittedTransaction, Transaction => Tx}
import com.daml.lf.value.Value

import scala.collection.mutable

private[replay] final class Adapter(
    packages: Map[Ref.PackageId, Ast.Package],
    pkgLangVersion: Ref.PackageId => LanguageVersion,
) {

  private val interface = com.daml.lf.language.Interface(packages)

  def adapt(tx: Tx.Transaction): SubmittedTransaction =
    tx.foldWithPathState(TxBuilder(pkgLangVersion), Option.empty[NodeId])(
      (builder, parent, _, node) =>
        (builder, Some(parent.fold(builder.add(adapt(node)))(builder.add(adapt(node), _))))
    ).buildSubmitted()

  // drop value version and children
  private[this] def adapt(node: Tx.Node): Node.GenNode[NodeId] =
    node match {
      case rollback: Node.NodeRollback[_] =>
        rollback.copy(children = ImmArray.Empty)
      case create: Node.NodeCreate =>
        create.copy(
          templateId = adapt(create.templateId),
          arg = adapt(create.arg),
          key = create.key.map(adapt),
        )
      case exe: Node.NodeExercises[NodeId] =>
        exe.copy(
          templateId = adapt(exe.templateId),
          chosenValue = adapt(exe.chosenValue),
          children = ImmArray.Empty,
          exerciseResult = exe.exerciseResult.map(adapt),
          key = exe.key.map(adapt),
        )
      case fetch: Node.NodeFetch =>
        fetch.copy(
          templateId = adapt(fetch.templateId),
          key = fetch.key.map(adapt),
        )
      case lookup: Node.NodeLookupByKey =>
        lookup
          .copy(
            templateId = adapt(lookup.templateId),
            key = adapt(lookup.key),
          )
    }

  // drop value version
  private[this] def adapt(
      k: Node.KeyWithMaintainers[Value]
  ): Node.KeyWithMaintainers[Value] =
    k.copy(adapt(k.key))

  def adapt(coinst: Tx.ContractInst): Tx.ContractInst =
    coinst.copy(
      template = adapt(coinst.template),
      arg = coinst.arg.copy(value = adapt(coinst.arg.value)),
    )

  def adapt(gkey: GlobalKey): GlobalKey =
    GlobalKey.assertBuild(adapt(gkey.templateId), adapt(gkey.key))

  private[this] def adapt(value: Value): Value =
    value match {
      case Value.ValueEnum(tycon, value) =>
        Value.ValueEnum(tycon.map(adapt), value)
      case Value.ValueRecord(tycon, fields) =>
        Value.ValueRecord(tycon.map(adapt), fields.map { case (f, v) => f -> adapt(v) })
      case Value.ValueVariant(tycon, variant, value) =>
        Value.ValueVariant(tycon.map(adapt), variant, adapt(value))
      case Value.ValueList(values) =>
        Value.ValueList(values.map(adapt))
      case Value.ValueOptional(value) =>
        Value.ValueOptional(value.map(adapt))
      case Value.ValueTextMap(value) =>
        Value.ValueTextMap(value.mapValue(adapt))
      case Value.ValueGenMap(entries) =>
        Value.ValueGenMap(entries.map { case (k, v) => adapt(k) -> adapt(v) })
      case _: Value.ValueCidlessLeaf | _: Value.ValueContractId =>
        value
    }

  private[this] val cache = mutable.Map.empty[Ref.Identifier, Ref.Identifier]

  private[this] def adapt(id: Ref.Identifier): Ref.Identifier =
    cache.getOrElseUpdate(id, assertRight(lookup(id)))

  private[this] def lookup(id: Ref.Identifier): Either[String, Ref.Identifier] = {
    val pkgIds = packages.keysIterator.flatMap { pkgId =>
      val renamed = id.copy(packageId = pkgId)
      if (interface.lookupDefinition(renamed).isRight)
        List(renamed)
      else
        List.empty
    }
    pkgIds.toSeq match {
      case Seq(newId) => Right(newId)
      case Seq() => Left(s"no package found for ${id.qualifiedName}")
      case _ => Left(s"2 or more packages found for ${id.qualifiedName}")
    }
  }

}
