// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.serialization

import java.io.InputStream

import com.digitalasset.daml.lf.archive.{Decode, Reader}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass}

object ValueSerializer {

  def serializeValue(
      value: VersionedValue[AbsoluteContractId],
      errorContext: => String,
  ): Array[Byte] =
    ValueCoder
      .encodeVersionedValueWithCustomVersion(ValueCoder.CidEncoder, value)
      .fold(error => sys.error(s"$errorContext (${error.errorMessage})"), _.toByteArray)

  private def deserializeValue(
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
    deserializeValue(stream, None)

  def deserializeValue(
      stream: InputStream,
      errorContext: => String,
  ): VersionedValue[AbsoluteContractId] =
    deserializeValue(stream, Some(errorContext))

}
