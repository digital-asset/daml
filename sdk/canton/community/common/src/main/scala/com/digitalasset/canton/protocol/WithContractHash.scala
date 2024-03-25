// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.logging.pretty.Pretty

final case class WithContractHash[+A](private val x: A, contractHash: LfHash) {
  def unwrap: A = x

  def map[B](f: A => B): WithContractHash[B] = WithContractHash(f(x), contractHash)

}

object WithContractHash {

  implicit def prettyWithContractHash[A: Pretty]: Pretty[WithContractHash[A]] = {
    import Pretty.*
    prettyOfClass(
      unnamedParam(_.x),
      param("contract hash", _.contractHash),
    )
  }

  def fromContract[A](contract: SerializableContract, x: A): WithContractHash[A] =
    WithContractHash(x, contract.rawContractInstance.contractHash)
}
