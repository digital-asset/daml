// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.google.protobuf.MessageLite
import com.daml.caching.{Cache, Weight}

package object caching {

  implicit object `Bytes Weight` extends Weight[Bytes] {
    override def weigh(value: Bytes): Cache.Size =
      value.size().toLong
  }

  implicit object `Message Weight` extends Weight[MessageLite] {
    override def weigh(value: MessageLite): Cache.Size =
      value.getSerializedSize.toLong
  }

}
