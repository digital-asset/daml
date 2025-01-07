// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.ValueDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString

object LfHashSyntax {

  implicit class LfHashSyntax(private val hash: LfHash) extends AnyVal {
    def toProtoPrimitive: ByteString = hash.bytes.toByteString
  }

  implicit class LfHashObjectSyntax(private val lfHash: LfHash.type) extends AnyVal {
    def fromProtoPrimitive(
        fieldName: String,
        bytes: ByteString,
    ): ParsingResult[LfHash] =
      LfHash
        .fromBytes(Bytes.fromByteString(bytes))
        .leftMap(err => ValueDeserializationError(fieldName, err))
  }
}
