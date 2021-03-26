// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.replay

import com.daml.lf.data._
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.transaction.test.{TransactionBuilder => TxBuilder}
import com.daml.lf.transaction.{GlobalKey, Node, NodeId, SubmittedTransaction, Transaction => Tx}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

import scala.collection.mutable

private[replay] final class Adapter(
    packages: Map[Ref.PackageId, Ast.Package],
    pkgLangVersion: Ref.PackageId => LanguageVersion,
) {

  def adapt(tx: Tx.Transaction): SubmittedTransaction =
    tx.foldWithPathState(TxBuilder(pkgLangVersion), Option.empty[NodeId])(
      (builder, parent, _, node) =>
        (builder, Some(parent.fold(builder.add(adapt(node)))(builder.add(adapt(node), _))))
    ).buildSubmitted()

  // drop value version and children
  private[this] def adapt(node: Tx.Node): Node.GenNode[NodeId, ContractId] =
    node match {
      case _: Node.NodeRollback[_] =>
        // TODO https://github.com/digital-asset/daml/issues/8020
        sys.error("rollback nodes are not supported")
      case create: Node.NodeCreate[ContractId] =>
        create.copy(
          templateId = adapt(create.templateId),
          arg = adapt(create.arg),
          optLocation = None,
          key = create.key.map(adapt),
        )
      case exe: Node.NodeExercises[NodeId, ContractId] =>
        exe.copy(
          templateId = adapt(exe.templateId),
          optLocation = None,
          chosenValue = adapt(exe.chosenValue),
          children = ImmArray.empty,
          exerciseResult = exe.exerciseResult.map(adapt),
          key = exe.key.map(adapt),
        )
      case fetch: Node.NodeFetch[ContractId] =>
        fetch.copy(
          templateId = adapt(fetch.templateId),
          optLocation = None,
          key = fetch.key.map(adapt),
        )
      case lookup: Node.NodeLookupByKey[ContractId] =>
        lookup
          .copy(
            templateId = adapt(lookup.templateId),
            optLocation = None,
            key = adapt(lookup.key),
          )
    }

  // drop value version
  private[this] def adapt(
      k: Node.KeyWithMaintainers[Value[ContractId]]
  ): Node.KeyWithMaintainers[Value[ContractId]] =
    k.copy(adapt(k.key))

  def adapt(coinst: Tx.ContractInst[ContractId]): Tx.ContractInst[ContractId] =
    coinst.copy(
      template = adapt(coinst.template),
      arg = coinst.arg.copy(value = adapt(coinst.arg.value)),
    )

  def adapt(gkey: GlobalKey): GlobalKey =
    GlobalKey.assertBuild(adapt(gkey.templateId), adapt(gkey.key))

  private[this] def adapt(value: Value[ContractId]): Value[ContractId] =
    value match {
      case Value.ValueEnum(tycon, value) =>
        Value.ValueEnum(tycon.map(adapt), value)
      case Value.ValueRecord(tycon, fields) =>
        Value.ValueRecord(tycon.map(adapt), fields.map { case (f, v) => f -> adapt(v) })
      case Value.ValueVariant(tycon, variant, value) =>
        Value.ValueVariant(tycon.map(adapt), variant, adapt(value))
      case Value.ValueBuiltinException(tag, value) =>
        Value.ValueBuiltinException(tag, adapt(value))
      case Value.ValueList(values) =>
        Value.ValueList(values.map(adapt))
      case Value.ValueOptional(value) =>
        Value.ValueOptional(value.map(adapt))
      case Value.ValueTextMap(value) =>
        Value.ValueTextMap(value.mapValue(adapt))
      case Value.ValueGenMap(entries) =>
        Value.ValueGenMap(entries.map { case (k, v) => adapt(k) -> adapt(v) })
      case _: Value.ValueCidlessLeaf | _: Value.ValueContractId[ContractId] =>
        value
    }

  private[this] val cache = mutable.Map.empty[Ref.Identifier, Ref.Identifier]

  private[this] def adapt(id: Ref.Identifier): Ref.Identifier =
    cache.getOrElseUpdate(id, assertRight(lookup(id)))

  private[this] def lookup(id: Ref.Identifier): Either[String, Ref.Identifier] = {
    val pkgIds = packages.collect {
      case (pkgId, pkg) if pkg.lookupDefinition(id.qualifiedName).isRight => pkgId
    }
    pkgIds.toSeq match {
      case Seq(pkgId) => Right(id.copy(packageId = pkgId))
      case Seq() => Left(s"no package found for ${id.qualifiedName}")
      case _ => Left(s"2 or more packages found for ${id.qualifiedName}")
    }
  }

}
