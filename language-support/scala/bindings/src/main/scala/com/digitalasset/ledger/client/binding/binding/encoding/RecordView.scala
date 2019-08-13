// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding.encoding

import scala.language.higherKinds

import scalaz.~>

/** The API of [[LfEncodable.ViaFields#view]]. */
trait RecordView[C[_], Self[_[_]]] { this: Self[C] =>
  def hoist[D[_]](f: C ~> D): Self[D]
}

object RecordView {
  def Empty[C[_]]: Empty[C] = new Empty
  final class Empty[C[_]] extends RecordView[C, Empty] {
    override def hoist[D[_]](f: C ~> D): Empty[D] = new Empty
  }
}
