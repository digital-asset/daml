// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Party, TypeConName}
import com.daml.lf.value.Value

import scala.annotation.nowarn

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

  override def hashCode(): Int = hash.hashCode()

  override def toString: String = s"GlobalKey($templateId, $key)"
}

object GlobalKey {

  def assertWithRenormalizedValue(key: GlobalKey, value: Value): GlobalKey = {
    if (
      key.key != value &&
      Hash.assertHashContractKey(key.templateId, value) != key.hash &&
      Hash.assertHashContractKey(key.templateId, value) != key.hash
    ) {
      throw new IllegalArgumentException(
        s"Hash must not change as a result of value renormalization key=$key, value=$value"
      )
    }

    new GlobalKey(key.templateId, value, key.hash)

  }

  // Will fail if key contains contract ids
  def build(templateId: TypeConName, key: Value): Either[crypto.Hash.HashingError, GlobalKey] =
    crypto.Hash
      .hashContractKey(templateId, key)
      .map(new GlobalKey(templateId, key, _))

  // TODO(https://github.com/digital-asset/daml/issues/18240) remove this method once canton stops
  //  using this it.
  @nowarn("cat=unused")
  def build(
      templateId: Ref.TypeConName,
      key: Value,
      shared: Boolean,
  ): Either[crypto.Hash.HashingError, GlobalKey] =
    build(templateId, key)

  // Like `build` but,  in case of error, throws an exception instead of returning a message.
  @throws[IllegalArgumentException]
  def assertBuild(templateId: TypeConName, key: Value): GlobalKey =
    data.assertRight(build(templateId, key).left.map(_.msg))

  // TODO(https://github.com/digital-asset/daml/issues/18240) remove this method once canton stops
  //  using this it.
  @throws[IllegalArgumentException]
  @nowarn("cat=unused")
  def assertBuild(templateId: TypeConName, key: Value, shared: Boolean): GlobalKey =
    assertBuild(templateId, key)

  private[lf] def unapply(globalKey: GlobalKey): Some[(TypeConName, Value)] =
    Some((globalKey.templateId, globalKey.key))

  def isShared(key: GlobalKey): Boolean =
    Hash.hashContractKey(key.templateId, key.key) == Right(key.hash)

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
  ): GlobalKeyWithMaintainers =
    data.assertRight(build(templateId, value, maintainers).left.map(_.msg))

  // TODO(https://github.com/digital-asset/daml/issues/18240) remove this method once canton stops
  //  using this it.
  @nowarn("cat=unused")
  def assertBuild(
      templateId: Ref.TypeConName,
      value: Value,
      maintainers: Set[Ref.Party],
      shared: Boolean,
  ): GlobalKeyWithMaintainers =
    assertBuild(templateId, value, maintainers)

  def build(
      templateId: TypeConName,
      value: Value,
      maintainers: Set[Party],
  ): Either[Hash.HashingError, GlobalKeyWithMaintainers] =
    GlobalKey.build(templateId, value).map(GlobalKeyWithMaintainers(_, maintainers))

  // TODO(https://github.com/digital-asset/daml/issues/18240) remove this method once canton stops
  //  using this it.
  @nowarn("cat=unused")
  def build(
      templateId: Ref.TypeConName,
      value: Value,
      maintainers: Set[Ref.Party],
      shared: Boolean,
  ): Either[Hash.HashingError, GlobalKeyWithMaintainers] =
    build(templateId, value, maintainers)
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
