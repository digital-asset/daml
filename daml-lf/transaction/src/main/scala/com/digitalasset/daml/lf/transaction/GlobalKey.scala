// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.TypeConName
import com.daml.lf.value.Value

import scala.language.implicitConversions

/** Useful in various circumstances -- basically this is what a ledger implementation must use as
  * a key. The 'hash' is guaranteed to be stable over time.
  */
final class GlobalKey private (
    val templateId: Ref.TypeConName,
    val key: Value,
    val hash: crypto.Hash,
) extends data.NoCopy {
  override def equals(obj: Any): Boolean = obj match {
    case that: GlobalKey => this.hash == that.hash
    case _ => false
  }

  // Ready for refactoring where packageId becomes optional (#14486)
  def packageId: Option[Ref.PackageId] = Some(templateId.packageId)
  def qualifiedName: Ref.QualifiedName = templateId.qualifiedName
  def shared: SharedGlobalKey =
    SharedGlobalKey(Some(templateId.packageId), templateId.qualifiedName, key, hash)

  override def hashCode(): Int = hash.hashCode()

  override def toString: String = s"GlobalKey($templateId, $key)"
}

// TODO: https://github.com/digital-asset/daml/issues/17661 - the target state for GlobalKey
//   once all call sites support SharedGlobalKey
final case class SharedGlobalKey private (
    val packageId: Option[Ref.PackageId],
    val qualifiedName: Ref.QualifiedName,
    val key: Value,
    val hash: crypto.Hash,
) extends data.NoCopy {

  override def equals(obj: Any): Boolean = obj match {
    case that: SharedGlobalKey => this.hash == that.hash
    case _ => false
  }

  override def hashCode(): Int = hash.hashCode()

  override def toString: String =
    s"SharedGlobalKey($qualifiedName${packageId.fold("")(p => s"@$p")}, $key)"

  def assertGlobalKey: GlobalKey =
    GlobalKey.assertBuild(Ref.TypeConName(packageId.get, qualifiedName), key)
}

object SharedGlobalKey {

  implicit def globalKeyToShared(gk: GlobalKey): SharedGlobalKey =
    SharedGlobalKey(gk.packageId, gk.qualifiedName, gk.key, gk.hash)

  def build(
      packageId: Option[Ref.PackageId],
      qualifiedName: Ref.QualifiedName,
      key: Value,
  ): Either[crypto.Hash.HashingError, SharedGlobalKey] = {
    crypto.Hash
      .hashContractKey(packageId, qualifiedName, key)
      .map(new SharedGlobalKey(packageId, qualifiedName, key, _))
  }

  // Like `build` but,  in case of error, throws an exception instead of returning a message.
  @throws[IllegalArgumentException]
  def assertBuild(
      packageId: Option[Ref.PackageId],
      qualifiedName: Ref.QualifiedName,
      key: Value,
  ): SharedGlobalKey =
    data.assertRight(build(packageId, qualifiedName, key).left.map(_.msg))

  private[lf] def unapply(
      globalKey: SharedGlobalKey
  ): Some[(Option[Ref.PackageId], Ref.QualifiedName, Value)] =
    Some((globalKey.packageId, globalKey.qualifiedName, globalKey.key))

}

object GlobalKey {

  // Will fail if key contains contract ids
  def build(templateId: Ref.TypeConName, key: Value): Either[crypto.Hash.HashingError, GlobalKey] =
    crypto.Hash.hashContractKey(templateId, key).map(new GlobalKey(templateId, key, _))

  // Like `build` but,  in case of error, throws an exception instead of returning a message.
  @throws[IllegalArgumentException]
  def assertBuild(templateId: Ref.TypeConName, key: Value): GlobalKey =
    data.assertRight(build(templateId, key).left.map(_.msg))

  private[lf] def unapply(globalKey: GlobalKey): Some[(TypeConName, Value)] =
    Some((globalKey.templateId, globalKey.key))
}

final case class GlobalKeyWithMaintainers(
    globalKey: GlobalKey,
    maintainers: Set[Ref.Party],
) {
  def value: Value = globalKey.key
}

object GlobalKeyWithMaintainers {
  def assertBuild(
      templateId: Ref.TypeConName,
      value: Value,
      maintainers: Set[Ref.Party],
  ): GlobalKeyWithMaintainers =
    data.assertRight(build(templateId, value, maintainers).left.map(_.msg))

  def build(
      templateId: Ref.TypeConName,
      value: Value,
      maintainers: Set[Ref.Party],
  ): Either[Hash.HashingError, GlobalKeyWithMaintainers] =
    GlobalKey.build(templateId, value).map(GlobalKeyWithMaintainers(_, maintainers))
}

/** Controls whether the engine should error out when it encounters duplicate keys.
  * This is always turned on with the exception of Canton which allows turning this on or off
  * and forces it to be turned off in multi-domain mode.
  */
sealed abstract class ContractKeyUniquenessMode extends Product with Serializable

object ContractKeyUniquenessMode {

  /** Disable key uniqueness checks and only consider byKey operations.
    * Note that no stable semantics are provided for off mode.
    */
  case object Off extends ContractKeyUniquenessMode

  /** Considers all nodes mentioning keys as byKey operations and checks for contract key uniqueness. */
  case object Strict extends ContractKeyUniquenessMode
}
