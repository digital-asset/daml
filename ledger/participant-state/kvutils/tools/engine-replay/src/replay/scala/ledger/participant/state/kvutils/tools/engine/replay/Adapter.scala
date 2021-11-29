// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.engine.replay

import com.daml.lf.data._
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.transaction.test.{TransactionBuilder => TxBuilder}
import com.daml.lf.transaction.{GlobalKey, Node, NodeId, SubmittedTransaction, VersionedTransaction}
import com.daml.lf.value.Value

import scala.collection.mutable

private[replay] final class Adapter(
    packages: Map[Ref.PackageId, Ast.Package],
    pkgLangVersion: Ref.PackageId => LanguageVersion,
) {

  private val interface = com.daml.lf.language.PackageInterface(packages)

  def adaptTransaction(tx: VersionedTransaction): SubmittedTransaction =
    tx.unversioned
      .foldWithPathState(TxBuilder(pkgLangVersion), Option.empty[NodeId])(
        (builder, parent, _, node) =>
          (
            builder,
            Some(parent.fold(builder.add(adaptNode(node)))(builder.add(adaptNode(node), _))),
          )
      )
      .buildSubmitted()

  // drop value version and children
  private[this] def adaptNode(node: Node): Node =
    node match {
      case rollback: Node.Rollback =>
        rollback.copy(children = ImmArray.Empty)
      case create: Node.Create =>
        create.copy(
          templateId = adaptId(create.templateId),
          arg = adaptValue(create.arg),
          key = create.key.map(adaptKey),
        )
      case exe: Node.Exercise =>
        exe.copy(
          templateId = adaptId(exe.templateId),
          chosenValue = adaptValue(exe.chosenValue),
          children = ImmArray.Empty,
          exerciseResult = exe.exerciseResult.map(adaptValue),
          key = exe.key.map(adaptKey),
        )
      case fetch: Node.Fetch =>
        fetch.copy(
          templateId = adaptId(fetch.templateId),
          key = fetch.key.map(adaptKey),
        )
      case lookup: Node.LookupByKey =>
        lookup
          .copy(
            templateId = adaptId(lookup.templateId),
            key = adaptKey(lookup.key),
          )
    }

  // drop value version
  private[this] def adaptKey(
      k: Node.KeyWithMaintainers
  ): Node.KeyWithMaintainers =
    k.copy(adaptValue(k.key))

  def adaptCoinst(coinst: Value.VersionedContractInstance): Value.VersionedContractInstance =
    coinst.map(unversioned =>
      unversioned.copy(template = adaptId(unversioned.template), arg = adaptValue(unversioned.arg))
    )

  def adaptKey(gkey: GlobalKey): GlobalKey =
    GlobalKey.assertBuild(adaptId(gkey.templateId), adaptValue(gkey.key))

  private[this] def adaptValue(value: Value): Value =
    value match {
      case Value.ValueEnum(tycon, value) =>
        Value.ValueEnum(tycon.map(adaptId), value)
      case Value.ValueRecord(tycon, fields) =>
        Value.ValueRecord(tycon.map(adaptId), fields.map { case (f, v) => f -> adaptValue(v) })
      case Value.ValueVariant(tycon, variant, value) =>
        Value.ValueVariant(tycon.map(adaptId), variant, adaptValue(value))
      case Value.ValueList(values) =>
        Value.ValueList(values.map(adaptValue))
      case Value.ValueOptional(value) =>
        Value.ValueOptional(value.map(adaptValue))
      case Value.ValueTextMap(value) =>
        Value.ValueTextMap(value.mapValue(adaptValue))
      case Value.ValueGenMap(entries) =>
        Value.ValueGenMap(entries.map { case (k, v) => adaptValue(k) -> adaptValue(v) })
      case _: Value.ValueCidlessLeaf | _: Value.ValueContractId =>
        value
    }

  private[this] val cache = mutable.Map.empty[Ref.Identifier, Ref.Identifier]

  private[this] def adaptId(id: Ref.Identifier): Ref.Identifier =
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
