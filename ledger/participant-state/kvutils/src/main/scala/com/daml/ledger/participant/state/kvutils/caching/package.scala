// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import com.daml.caching.Cache.Size
import com.daml.caching.{Cache, Weight}
import com.daml.ledger.validator.Raw
import com.google.protobuf.MessageLite

package object caching {

  implicit object `Bytes Weight` extends Weight[Bytes] {
    override def weigh(value: Bytes): Cache.Size =
      value.size().toLong
  }

  implicit object `Key Weight` extends Weight[Raw.Key] {
    override def weigh(key: Raw.Key): Size =
      key.bytes.size().toLong
  }

  implicit object `Value Weight` extends Weight[Raw.Value] {
    override def weigh(value: Raw.Value): Size =
      value.bytes.size().toLong
  }

  implicit object `Message Weight` extends Weight[MessageLite] {
    override def weigh(value: MessageLite): Cache.Size =
      value.getSerializedSize.toLong
  }

}
