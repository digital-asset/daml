// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageName, Party, TypeConId}
import com.digitalasset.daml.lf.value.Value

/** Useful in various circumstances -- basically this is what a ledger implementation must use as
  * a key. The 'hash' is guaranteed to be stable over time.
  */
final class GlobalKey private (
    val templateId: Ref.TypeConId,
    val packageName: Ref.PackageName,
    val key: Value,
    val hash: crypto.Hash,
) extends data.NoCopy {
  override def equals(obj: Any): Boolean = obj match {
    case that: GlobalKey => this.hash == that.hash
    case _ => false
  }

  def qualifiedName: Ref.QualifiedName = templateId.qualifiedName

  override def hashCode(): Int = hash.hashCode()

  override def toString: String = s"GlobalKey($templateId, $packageName, $key)"
}

object GlobalKey {

  def assertWithRenormalizedValue(key: GlobalKey, value: Value): GlobalKey = {
    new GlobalKey(key.templateId, key.packageName, value, key.hash)
  }

  // Will fail if key contains contract ids
  def build(
      templateId: TypeConId,
      packageName: PackageName,
      key: Value,
      keyHash: crypto.Hash,
  ): Either[crypto.Hash.HashingError, GlobalKey] =
    Right(new GlobalKey(templateId, packageName, key, keyHash))

  def assertBuild(
      templateId: TypeConId,
      packageName: PackageName,
      key: Value,
      keyHash: crypto.Hash,
  ): GlobalKey = {
    data.assertRight(build(templateId, packageName, key, keyHash).left.map(_.msg))
  }

  private[lf] def unapply(globalKey: GlobalKey): Some[(TypeConId, Value)] =
    Some((globalKey.templateId, globalKey.key))

}

final case class GlobalKeyWithMaintainers(
    globalKey: GlobalKey,
    maintainers: Set[Ref.Party],
) {
  def value: Value = globalKey.key

  def nonVerbose: GlobalKeyWithMaintainers =
    GlobalKeyWithMaintainers(
      GlobalKey
        .assertWithRenormalizedValue(globalKey, globalKey.key.nonVerboseWithoutTrailingNones),
      maintainers,
    )
}

object GlobalKeyWithMaintainers {

  def assertBuild(
      templateId: TypeConId,
      value: Value,
      valueHash: crypto.Hash,
      maintainers: Set[Party],
      packageName: PackageName,
  ): GlobalKeyWithMaintainers =
    data.assertRight(build(templateId, value, valueHash, maintainers, packageName).left.map(_.msg))

  def build(
      templateId: TypeConId,
      value: Value,
      valueHash: crypto.Hash,
      maintainers: Set[Party],
      packageName: PackageName,
  ): Either[Hash.HashingError, GlobalKeyWithMaintainers] =
    GlobalKey
      .build(templateId, packageName, value, valueHash)
      .map(GlobalKeyWithMaintainers(_, maintainers))
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
