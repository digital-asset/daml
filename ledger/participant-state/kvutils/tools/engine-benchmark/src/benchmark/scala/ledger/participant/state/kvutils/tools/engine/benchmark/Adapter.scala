// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.benchmark

import com.daml.lf.data._
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.transaction.test.{TransactionBuilder => TxBuilder}
import com.daml.lf.transaction.{GlobalKey, Node, NodeId, SubmittedTransaction, Transaction => Tx}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

import scala.collection.mutable

private[benchmark] final class Adapter(
    packages: Map[Ref.PackageId, Ast.Package],
    pkgLangVersion: Ref.PackageId => LanguageVersion
) {

  def adapt(tx: Tx.Transaction): SubmittedTransaction =
    tx.foldWithPathState(TxBuilder(pkgLangVersion), Option.empty[NodeId])(
        (builder, parent, _, node) =>
          (builder, Some(parent.fold(builder.add(adapt(node)))(builder.add(adapt(node), _))))
      )
      .buildSubmitted()

  // drop value version and children
  private[this] def adapt(node: Tx.Node): Node.GenNode[NodeId, ContractId, Value[ContractId]] =
    node match {
      case create: Node.NodeCreate.WithTxValue[ContractId] =>
        create.copy(
          coinst = create.coinst.copy(adapt(create.coinst.template), adapt(create.coinst.arg.value)),
          optLocation = None,
          key = create.key.map(adapt)
        )
      case exe: Node.NodeExercises.WithTxValue[NodeId, ContractId] =>
        exe.copy(
          templateId = adapt(exe.templateId),
          optLocation = None,
          chosenValue = adapt(exe.chosenValue.value),
          children = ImmArray.empty,
          exerciseResult = exe.exerciseResult.map(v => adapt(v.value)),
          key = exe.key.map(adapt),
        )
      case fetch: Node.NodeFetch.WithTxValue[ContractId] =>
        fetch.copy(
          templateId = adapt(fetch.templateId),
          optLocation = None,
          key = fetch.key.map(adapt),
        )
      case lookup: Node.NodeLookupByKey.WithTxValue[ContractId] =>
        lookup.copy(
          templateId = adapt(lookup.templateId),
          optLocation = None,
          key = adapt(lookup.key),
        )
    }

  // drop value version
  private[this] def adapt(
      k: Node.KeyWithMaintainers[Tx.Value[ContractId]],
  ): Node.KeyWithMaintainers[Value[ContractId]] =
    k.copy(adapt(k.key.value))

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
      case Value.ValueList(values) =>
        Value.ValueList(values.map(adapt))
      case Value.ValueOptional(value) =>
        Value.ValueOptional(value.map(adapt))
      case Value.ValueTextMap(value) =>
        Value.ValueTextMap(value.mapValue(adapt))
      case Value.ValueGenMap(entries) =>
        Value.ValueGenMap(entries.map { case (k, v) => adapt(k) -> adapt(v) })
      case Value.ValueStruct(fields) =>
        Value.ValueStruct(fields.mapValues(adapt))
      case _: Value.ValueCidlessLeaf | _: Value.ValueContractId[ContractId] =>
        value
    }

  private[this] val cache = mutable.Map.empty[Ref.Identifier, Ref.Identifier]

  private[this] def adapt(id: Ref.Identifier): Ref.Identifier =
    cache.getOrElseUpdate(id, assertRight(lookup(id)))

  private[this] def lookup(id: Ref.Identifier): Either[String, Ref.Identifier] = {
    val pkgIds = packages.collect {
      case (pkgId, pkg) if pkg.lookupIdentifier(id.qualifiedName).isRight => pkgId
    }
    pkgIds.toSeq match {
      case Seq(pkgId) => Right(id.copy(packageId = pkgId))
      case Seq() => Left(s"no package foud for ${id.qualifiedName}")
      case _ => Left(s"2 or more packages found for ${id.qualifiedName}")
    }
  }

}
