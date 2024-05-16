// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf

import com.daml.lf.crypto.Hash.KeyPackageName
import com.daml.lf.data.Ref
import com.daml.lf.transaction.{GlobalKeyWithMaintainers, Node, TransactionVersion}

import Ordering.Implicits._

object NodeVersionUpdater {

  private def defaultUpgradePkgName: Ref.PackageName =
    Ref.PackageName.assertFromString("updater-package-name")

  trait VersionUpdater[T] {
    def apply(
        node: T,
        version: TransactionVersion,
        packageName: Option[Ref.PackageName],
    ): T
  }

  private def rehash(version: TransactionVersion, packageName: Option[Ref.PackageName])(
      gk: GlobalKeyWithMaintainers
  ): GlobalKeyWithMaintainers = {
    GlobalKeyWithMaintainers.assertBuild(
      gk.globalKey.templateId,
      gk.value,
      gk.maintainers,
      KeyPackageName(packageName, version),
    )
  }

  implicit object CreateUpdater extends VersionUpdater[Node.Create] {
    override def apply(
        n: Node.Create,
        version: TransactionVersion,
        packageName: Option[Ref.PackageName],
    ): Node.Create =
      n.copy(
        version = version,
        packageNameVersion = packageName.map((_, Ref.PackageVersion.Dummy)),
        keyOpt = n.keyOpt.map(rehash(version, packageName)),
      )
  }
  implicit object FetchUpdater extends VersionUpdater[Node.Fetch] {
    override def apply(
        n: Node.Fetch,
        version: TransactionVersion,
        packageName: Option[Ref.PackageName],
    ): Node.Fetch =
      n.copy(
        version = version,
        packageName = packageName,
        keyOpt = n.keyOpt.map(rehash(version, packageName)),
      )
  }
  implicit object ExerciseUpdater extends VersionUpdater[Node.Exercise] {
    override def apply(
        n: Node.Exercise,
        version: TransactionVersion,
        packageName: Option[Ref.PackageName],
    ): Node.Exercise =
      n.copy(
        version = version,
        packageName = packageName,
        keyOpt = n.keyOpt.map(rehash(version, packageName)),
      )
  }
  implicit object LookupByKeyUpdater extends VersionUpdater[Node.LookupByKey] {
    override def apply(
        n: Node.LookupByKey,
        version: TransactionVersion,
        packageName: Option[Ref.PackageName],
    ): Node.LookupByKey =
      n.copy(
        version = version,
        packageName = packageName,
        key = rehash(version, packageName)(n.key),
      )
  }

  implicit object ActionUpdater extends VersionUpdater[Node.Action] {
    override def apply(
        node: Node.Action,
        version: TransactionVersion,
        packageName: Option[Ref.PackageName],
    ): Node.Action = node match {
      case n: Node.Create => CreateUpdater(n, version, packageName)
      case n: Node.Fetch => FetchUpdater(n, version, packageName)
      case n: Node.Exercise => ExerciseUpdater(n, version, packageName)
      case n: Node.LookupByKey => LookupByKeyUpdater(n, version, packageName)
    }

  }

  implicit class Ops[T <: Node.Action](node: T)(implicit versionUpdater: VersionUpdater[T]) {

    def updateVersion(
        version: TransactionVersion,
        upgradePkgName: Ref.PackageName = defaultUpgradePkgName,
    ): T = {

      val packageName = if (version >= TransactionVersion.minUpgrade) {
        node.packageName.orElse(Some(upgradePkgName))
      } else None

      versionUpdater(node, version, packageName)
    }
  }

}
