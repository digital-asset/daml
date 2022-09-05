// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref
import com.daml.lf.value.Value

/** Useful in various circumstances -- basically this is what a ledger implementation must use as
  * a key. The 'hash' is guaranteed to be stable over time.
  */
final class GlobalKey private (
    val templateId: Ref.TypeConName,
    val key: Value,
    val hash: crypto.Hash,
) extends {
  override def equals(obj: Any): Boolean = obj match {
    case that: GlobalKey => this.hash == that.hash
    case _ => false
  }

  override def hashCode(): Int = hash.hashCode()

  override def toString: String = s"GlobalKey($templateId, $key)"
}

object GlobalKey {

  def apply(templateId: Ref.TypeConName, key: Value): GlobalKey =
    new GlobalKey(templateId, key, crypto.Hash.safeHashContractKey(templateId, key))

  // Will fail if key contains contract ids
  def build(templateId: Ref.TypeConName, key: Value): Either[String, GlobalKey] =
    crypto.Hash.hashContractKey(templateId, key).map(new GlobalKey(templateId, key, _))

  // Like `build` but,  in case of error, throws an exception instead of returning a message.
  @throws[IllegalArgumentException]
  def assertBuild(templateId: Ref.TypeConName, key: Value): GlobalKey =
    data.assertRight(build(templateId, key))
}

final case class GlobalKeyWithMaintainers(
    globalKey: GlobalKey,
    maintainers: Set[Ref.Party],
)

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
