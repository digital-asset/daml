// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.caching.Cache.Size
import com.daml.caching.{Cache, Weight}
import com.google.protobuf.MessageLite

package object caching {

  implicit object `Raw.Bytes Weight` extends Weight[Raw.Bytes] {
    override def weigh(rawBytes: Raw.Bytes): Size =
      rawBytes.size
  }

  implicit object `Message Weight` extends Weight[MessageLite] {
    override def weigh(value: MessageLite): Cache.Size =
      value.getSerializedSize.toLong
  }

}
