// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.serialization

import java.io.InputStream

import com.digitalasset.daml.lf.archive.{Decode, Reader}
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass}

trait ValueSerializer {
  def serializeValue(
      value: VersionedValue[AbsoluteContractId]
  ): Either[ValueCoder.EncodeError, Array[Byte]]

  def deserializeValue(stream: InputStream): Either[DecodeError, VersionedValue[AbsoluteContractId]]
}

/**
  * This is a preliminary serializer using protobuf as a payload type. Our goal on the long run is to use JSON as a payload.
  */
object ValueSerializer extends ValueSerializer {

  override def serializeValue(
      value: VersionedValue[AbsoluteContractId]): Either[ValueCoder.EncodeError, Array[Byte]] =
    ValueCoder
      .encodeVersionedValueWithCustomVersion(ValueCoder.CidEncoder, value)
      .map(_.toByteArray())

  override def deserializeValue(
      stream: InputStream
  ): Either[DecodeError, VersionedValue[AbsoluteContractId]] =
    ValueCoder
      .decodeVersionedValue(
        ValueCoder.AbsCidDecoder,
        ValueOuterClass.VersionedValue.parseFrom(
          Decode.damlLfCodedInputStream(stream, Reader.PROTOBUF_RECURSION_LIMIT)))

}
