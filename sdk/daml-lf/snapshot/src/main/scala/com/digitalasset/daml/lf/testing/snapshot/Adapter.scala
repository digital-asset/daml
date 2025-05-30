// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.testing.snapshot.Adapter.TxBuilder
import com.digitalasset.daml.lf.transaction._
import com.digitalasset.daml.lf.transaction.test.{NodeIdTransactionBuilder, TestNodeBuilder}
import com.digitalasset.daml.lf.value.Value

import scala.collection.mutable

final class Adapter(
    packages: Map[Ref.PackageId, Ast.Package]
) {

  private val interface = com.digitalasset.daml.lf.language.PackageInterface(packages)

  def adapt(tx: VersionedTransaction): SubmittedTransaction =
    tx.foldWithPathState(new TxBuilder(interface.packageLanguageVersion), Option.empty[NodeId])(
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
          keyOpt = create.keyOpt.map(adapt),
        )
      case exe: Node.Exercise =>
        exe.copy(
          templateId = adapt(exe.templateId),
          chosenValue = adapt(exe.chosenValue),
          children = ImmArray.Empty,
          exerciseResult = exe.exerciseResult.map(adapt),
          keyOpt = exe.keyOpt.map(adapt),
        )
      case fetch: Node.Fetch =>
        fetch.copy(
          templateId = adapt(fetch.templateId),
          keyOpt = fetch.keyOpt.map(adapt),
        )
      case lookup: Node.LookupByKey =>
        lookup
          .copy(
            templateId = adapt(lookup.templateId),
            key = adapt(lookup.key),
          )
    }

  def adapt(k: GlobalKeyWithMaintainers): GlobalKeyWithMaintainers =
    k.copy(globalKey = adapt(k.globalKey))

  def adapt(coinst: Value.VersionedThinContractInstance): Value.VersionedThinContractInstance =
    coinst.map(unversioned =>
      unversioned.copy(template = adapt(unversioned.template), arg = adapt(unversioned.arg))
    )

  def adapt(coinst: FatContractInstance): FatContractInstance =
    coinst.toImplementation.copy(
      templateId = adapt(coinst.templateId),
      createArg = adapt(coinst.createArg),
      contractKeyWithMaintainers = coinst.contractKeyWithMaintainers.map(adapt),
    )

  def adapt(gkey: GlobalKey): GlobalKey =
    GlobalKey.assertBuild(adapt(gkey.templateId), adapt(gkey.key), gkey.packageName)

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
      val renamed = id.copy(pkg = pkgId)
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

object Adapter {
  private class TxBuilder(pkgLangVer: Ref.PackageId => LanguageVersion)
      extends NodeIdTransactionBuilder
      with TestNodeBuilder {
    override def transactionVersion(packageId: Ref.PackageId): Option[TransactionVersion] = {
      Some(pkgLangVer(packageId))
    }
  }
}
