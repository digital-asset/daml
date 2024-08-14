// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{Party, TypeConName}
import com.digitalasset.daml.lf.value.Value

/** Useful in various circumstances -- basically this is what a ledger implementation must use as
  * a key. The 'hash' is guaranteed to be stable over time.
  */
final class GlobalKey private (
    val templateId: Ref.TypeConName,
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

  def withRenormalizedValue(key: GlobalKey, value: Value): Either[String, GlobalKey] = {
    Either.cond(
      key.key == value ||
        Hash.assertHashContractKey(key.templateId, key.packageName, value) == key.hash,
      new GlobalKey(key.templateId, key.packageName, value, key.hash),
      s"Hash must not change as a result of value renormalization key=$key, value=$value",
    )
  }

  def assertWithRenormalizedValue(key: GlobalKey, value: Value): GlobalKey = {
    data.assertRight(withRenormalizedValue(key, value))
  }

  // Will fail if key contains contract ids
  def build(
      templateId: TypeConName,
      key: Value,
      packageName: Ref.PackageName,
  ): Either[crypto.Hash.HashingError, GlobalKey] = {
    crypto.Hash
      .hashContractKey(templateId, packageName, key)
      .map(new GlobalKey(templateId, packageName, key, _))
  }

  def assertBuild(
      templateId: TypeConName,
      key: Value,
      packageName: Ref.PackageName,
  ): GlobalKey = {
    data.assertRight(build(templateId, key, packageName).left.map(_.msg))
  }

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
      templateId: TypeConName,
      value: Value,
      maintainers: Set[Party],
      packageName: Ref.PackageName,
  ): GlobalKeyWithMaintainers =
    data.assertRight(build(templateId, value, maintainers, packageName).left.map(_.msg))

  def build(
      templateId: TypeConName,
      value: Value,
      maintainers: Set[Party],
      packageName: Ref.PackageName,
  ): Either[Hash.HashingError, GlobalKeyWithMaintainers] =
    GlobalKey.build(templateId, value, packageName).map(GlobalKeyWithMaintainers(_, maintainers))

  def withRenormalizedValue(
      key: GlobalKeyWithMaintainers,
      value: Value,
  ): Either[String, GlobalKeyWithMaintainers] = {
    GlobalKey
      .withRenormalizedValue(key.globalKey, value)
      .map(GlobalKeyWithMaintainers(_, key.maintainers))
  }
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
