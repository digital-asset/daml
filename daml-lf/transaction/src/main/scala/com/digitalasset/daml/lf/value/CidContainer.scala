// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import com.daml.lf.data.Bytes
import Value.ContractId

import scala.util.control.NoStackTrace

trait CidContainer[+A] {

  protected def self: A

  def mapCid(f: ContractId => ContractId): A

  def foreachCid(f: ContractId => Unit) = {
    mapCid(cid => {
      f(cid)
      cid
    })
    ()
  }

  // We cheat using exceptions, to get a cheap implementation of traverse using the `map` function above.
  // In practice, we abort the traversal using an exception as soon as we find an input we cannot map.
  def traverseCid[L](f: ContractId => Either[L, ContractId]): Either[L, A] = {
    case class Ball(x: L) extends Throwable with NoStackTrace
    try {
      Right(mapCid(x => f(x).fold(y => throw Ball(y), identity)))
    } catch {
      case Ball(x) => Left(x)
    }
  }

  final def ensureNoCid: Either[Value.ContractId, A] =
    traverseCid[Value.ContractId](Left(_))

  final def assertNoCid(message: Value.ContractId => String): A =
    data.assertRight(ensureNoCid.left.map(message))

  // Sets the suffix of any the V1 ContractId `coid` of the container that are not already suffixed.
  // Uses `f(coid.discriminator)` as suffix.
  final def suffixCid(f: crypto.Hash => Bytes): Either[String, A] = {
    traverseCid[String] {
      case Value.ContractId.V1(discriminator, Bytes.Empty) =>
        Value.ContractId.V1.build(discriminator, f(discriminator))
      case acoid @ Value.ContractId.V1(_, _) => Right(acoid)
      case acoid @ Value.ContractId.V0(_) => Right(acoid)
    }
  }

}
