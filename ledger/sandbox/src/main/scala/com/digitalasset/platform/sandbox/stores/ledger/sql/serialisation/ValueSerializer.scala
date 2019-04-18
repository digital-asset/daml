// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation

import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, VersionedValue}
import com.digitalasset.daml.lf.value.ValueCoder.DecodeError
import com.digitalasset.daml.lf.value.{ValueCoder, ValueOuterClass}

trait ValueSerializer {
  def serialiseValue(
      value: VersionedValue[AbsoluteContractId]): Either[ValueCoder.EncodeError, Array[Byte]]

  def deserialiseValue(blob: Array[Byte]): Either[DecodeError, VersionedValue[AbsoluteContractId]]
}

/**
  * This is a preliminary serializer using protobuf as a payload type. Our goal on the long run is to use JSON as a payload.
  */
object ValueSerializer extends ValueSerializer {

  override def serialiseValue(
      value: VersionedValue[AbsoluteContractId]): Either[ValueCoder.EncodeError, Array[Byte]] =
    ValueCoder
      .encodeVersionedValueWithCustomVersion(defaultCidEncode, value)
      .map(_.toByteArray())

  override def deserialiseValue(
      blob: Array[Byte]): Either[DecodeError, VersionedValue[AbsoluteContractId]] =
    ValueCoder
      .decodeVersionedValue(defaultCidDecode, ValueOuterClass.VersionedValue.parseFrom(blob))

  val defaultCidEncode: ValueCoder.EncodeCid[AbsoluteContractId] = ValueCoder.EncodeCid(
    _.coid,
    acid => (acid.coid, false)
  )

  val defaultCidDecode: ValueCoder.DecodeCid[AbsoluteContractId] = ValueCoder.DecodeCid(
    { i: String =>
      Right(AbsoluteContractId(i))
    }, { (i, r) =>
      if (r)
        sys.error("found relative contract id in stored contract instance")
      else Right(AbsoluteContractId(i))
    }
  )

}
