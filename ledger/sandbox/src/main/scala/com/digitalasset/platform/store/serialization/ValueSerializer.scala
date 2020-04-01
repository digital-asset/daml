// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.serialization

import java.io.InputStream

import com.daml.lf.archive.{Decode, Reader}
import com.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.daml.lf.value.{ValueCoder, ValueOuterClass}

object ValueSerializer {

  def serializeValue(
      value: VersionedValue[AbsoluteContractId],
      errorContext: => String,
  ): Array[Byte] =
    ValueCoder
      .encodeVersionedValueWithCustomVersion(ValueCoder.CidEncoder, value)
      .fold(error => sys.error(s"$errorContext (${error.errorMessage})"), _.toByteArray)

  private def deserializeValueHelper(
      stream: InputStream,
      errorContext: => Option[String],
  ): VersionedValue[AbsoluteContractId] =
    ValueCoder
      .decodeVersionedValue(
        ValueCoder.AbsCidDecoder,
        ValueOuterClass.VersionedValue.parseFrom(
          Decode.damlLfCodedInputStream(stream, Reader.PROTOBUF_RECURSION_LIMIT)))
      .fold(
        error =>
          sys.error(errorContext.fold(error.errorMessage)(ctx => s"$ctx (${error.errorMessage})")),
        identity
      )

  def deserializeValue(
      stream: InputStream,
  ): VersionedValue[AbsoluteContractId] =
    deserializeValueHelper(stream, None)

  def deserializeValue(
      stream: InputStream,
      errorContext: => String,
  ): VersionedValue[AbsoluteContractId] =
    deserializeValueHelper(stream, Some(errorContext))

}
