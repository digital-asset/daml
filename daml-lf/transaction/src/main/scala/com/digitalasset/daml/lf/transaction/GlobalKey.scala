// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId

/** Useful in various circumstances -- basically this is what a ledger implementation must use as
  * a key. The 'hash' is guaranteed to be stable over time.
  */
final class GlobalKey private (
    val templateId: Ref.TypeConName,
    val key: Value[ContractId],
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

  def apply(templateId: Ref.TypeConName, key: Value[Nothing]): GlobalKey =
    new GlobalKey(templateId, key, crypto.Hash.safeHashContractKey(templateId, key))

  // Will fail if key contains contract ids
  def build(templateId: Ref.TypeConName, key: Value[ContractId]): Either[String, GlobalKey] =
    crypto.Hash.hashContractKey(templateId, key).map(new GlobalKey(templateId, key, _))

  // Like `build` but,  in case of error, throws an exception instead of returning a message.
  @throws[IllegalArgumentException]
  def assertBuild(templateId: Ref.TypeConName, key: Value[ContractId]): GlobalKey =
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
sealed trait ContractKeyUniquenessMode
object ContractKeyUniquenessMode {
  case object On extends ContractKeyUniquenessMode
  case object Off extends ContractKeyUniquenessMode
}
