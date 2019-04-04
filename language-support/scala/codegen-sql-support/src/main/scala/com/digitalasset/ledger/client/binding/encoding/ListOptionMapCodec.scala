// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import com.digitalasset.ledger.client.binding.{Primitive => P}

import scala.language.higherKinds

trait ListOptionMapCodec[T, F[_]] {
  def encodeList[A: F](as: P.List[A]): T

  def decodeList[A: F](t: T): P.List[A]

  def encodeOption[A: F](as: P.Optional[A]): T

  def decodeOption[A: F](t: T): P.Optional[A]

  def encodeMap[A: F](as: P.Map[A]): T

  def decodeMap[A: F](t: T): P.Map[A]
}
