// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref
import com.daml.lf.value.Value

/** Useful in various circumstances -- basically this is what a ledger implementation must use as
  * a key. The 'hash' is guaranteed to be stable over time.
  */
final class GlobalKey private (
    val packageId: Option[Ref.PackageId],
    val qualifiedName: Ref.QualifiedName,
    val key: Value,
    val hash: crypto.Hash,
) extends data.NoCopy {
  override def equals(obj: Any): Boolean = obj match {
    case that: GlobalKey => this.hash == that.hash
    case _ => false
  }

  override def hashCode(): Int = hash.hashCode()

  override def toString: String = s"GlobalKey($qualifiedName, $key)"
}

object GlobalKey {

  // Will fail if key contains contract ids
  def build(
      packageId: Option[Ref.PackageId],
      qualifiedName: Ref.QualifiedName,
      key: Value,
  ): Either[crypto.Hash.HashingError, GlobalKey] =
    crypto.Hash
      .hashContractKey(packageId, qualifiedName, key)
      .map(new GlobalKey(packageId, qualifiedName, key, _))

  def build(templateId: Ref.TypeConName, key: Value): Either[crypto.Hash.HashingError, GlobalKey] =
    build(Some(templateId.packageId), templateId.qualifiedName, key)

  // Like `build` but,  in case of error, throws an exception instead of returning a message.
  @throws[IllegalArgumentException]
  def assertBuild(
      packageId: Option[Ref.PackageId],
      qualifiedName: Ref.QualifiedName,
      key: Value,
  ): GlobalKey =
    data.assertRight(build(packageId, qualifiedName, key).left.map(_.msg))

  def assertBuild(templateId: Ref.TypeConName, key: Value): GlobalKey =
    data.assertRight(build(templateId, key).left.map(_.msg))

  private[lf] def unapply(
      globalKey: GlobalKey
  ): Some[(Option[Ref.PackageId], Ref.QualifiedName, Value)] =
    Some((globalKey.packageId, globalKey.qualifiedName, globalKey.key))
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
    GlobalKeyWithMaintainers(GlobalKey.assertBuild(templateId, value), maintainers)
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
