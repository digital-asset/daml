// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.serialization

import java.io.InputStream

import com.daml.lf.value.Value.VersionedValue
import com.daml.lf.value.{ValueCoder, ValueOuterClass}

private[platform] object ValueSerializer {

  def serializeValue(
      value: VersionedValue,
      errorContext: => String,
  ): Array[Byte] =
    ValueCoder
      .encodeVersionedValue(ValueCoder.CidEncoder, value)
      .fold(error => sys.error(s"$errorContext (${error.errorMessage})"), _.toByteArray)

  private def deserializeValueHelper(
      stream: InputStream,
      errorContext: => Option[String],
  ): VersionedValue =
    ValueCoder
      .decodeVersionedValue(
        ValueCoder.CidDecoder,
        ValueOuterClass.VersionedValue.parseFrom(stream),
      )
      .fold(
        error =>
          sys.error(errorContext.fold(error.errorMessage)(ctx => s"$ctx (${error.errorMessage})")),
        identity,
      )

  def deserializeValue(
      stream: InputStream
  ): VersionedValue =
    deserializeValueHelper(stream, None)

  def deserializeValue(
      stream: InputStream,
      errorContext: => String,
  ): VersionedValue =
    deserializeValueHelper(stream, Some(errorContext))

}
