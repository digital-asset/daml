// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package testing.snapshot

import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.transaction._
import com.daml.lf.transaction.test.{TransactionBuilder => TxBuilder}
import com.daml.lf.value.Value

import scala.collection.mutable

final class Adapter(
    packages: Map[Ref.PackageId, Ast.Package]
) {

  private val interface = com.daml.lf.language.PackageInterface(packages)

  def adapt(tx: VersionedTransaction): SubmittedTransaction =
    tx.foldWithPathState(TxBuilder(interface.packageLanguageVersion), Option.empty[NodeId])(
      (builder, parent, _, node) =>
        (builder, Some(parent.fold(builder.add(adapt(node)))(builder.add(adapt(node), _))))
    ).buildSubmitted()

  // drop value version and children
  private[this] def adapt(node: Node): Node =
    node match {
      case rollback: Node.Rollback =>
        rollback.copy(children = ImmArray.Empty)
      case create: Node.Create =>
        create.copy(
          templateId = adapt(create.templateId),
          arg = adapt(create.arg),
          key = create.key.map(adapt),
        )
      case exe: Node.Exercise =>
        exe.copy(
          templateId = adapt(exe.templateId),
          chosenValue = adapt(exe.chosenValue),
          children = ImmArray.Empty,
          exerciseResult = exe.exerciseResult.map(adapt),
          key = exe.key.map(adapt),
        )
      case fetch: Node.Fetch =>
        fetch.copy(
          templateId = adapt(fetch.templateId),
          key = fetch.key.map(adapt),
        )
      case lookup: Node.LookupByKey =>
        lookup
          .copy(
            templateId = adapt(lookup.templateId),
            key = adapt(lookup.key),
          )
    }

  // drop value version
  private[this] def adapt(
      k: Node.KeyWithMaintainers
  ): Node.KeyWithMaintainers =
    k.copy(adapt(k.key))

  def adapt(coinst: Value.VersionedContractInstance): Value.VersionedContractInstance =
    coinst.map(unversioned =>
      unversioned.copy(template = adapt(unversioned.template), arg = adapt(unversioned.arg))
    )

  def adapt(gkey: GlobalKeyWithMaintainers): GlobalKeyWithMaintainers =
    GlobalKeyWithMaintainers(adapt(gkey.globalKey), gkey.maintainers)

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
