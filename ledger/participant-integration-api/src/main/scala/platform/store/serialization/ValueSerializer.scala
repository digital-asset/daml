// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.serialization

import java.io.InputStream

import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.google.protobuf.CodedInputStream

private[platform] object ValueSerializer {

  def serializeValue(
      value: VersionedValue[ContractId],
      errorContext: => String,
  ): Array[Byte] =
    ValueCoder
      .encodeVersionedValue(ValueCoder.CidEncoder, value)
      .fold(error => sys.error(s"$errorContext (${error.errorMessage})"), _.toByteArray)

  private[this] val PROTOBUF_RECURSION_LIMIT = 1000

  private[this] def lfValueCodedInputStream(stream: InputStream) = {
    val cos = CodedInputStream.newInstance(stream)
    cos.setRecursionLimit(PROTOBUF_RECURSION_LIMIT)
    cos
  }

  private def deserializeValueHelper(
      stream: InputStream,
      errorContext: => Option[String],
  ): VersionedValue[ContractId] =
    ValueCoder
      .decodeVersionedValue(
        ValueCoder.CidDecoder,
        ValueOuterClass.VersionedValue.parseFrom(lfValueCodedInputStream(stream)),
      )
      .fold(
        error =>
          sys.error(errorContext.fold(error.errorMessage)(ctx => s"$ctx (${error.errorMessage})")),
        identity,
      )

  def deserializeValue(
      stream: InputStream
  ): VersionedValue[ContractId] =
    deserializeValueHelper(stream, None)

  def deserializeValue(
      stream: InputStream,
      errorContext: => String,
  ): VersionedValue[ContractId] =
    deserializeValueHelper(stream, Some(errorContext))

}
