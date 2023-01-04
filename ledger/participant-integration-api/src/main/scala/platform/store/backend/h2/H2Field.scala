// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.h2

import java.io.{ByteArrayInputStream, InputStream}

import com.daml.platform.store.backend.common.Field
import com.daml.platform.store.interning.StringInterning

private[h2] case class IntArray[FROM](extract: StringInterning => FROM => Iterable[Int])
    extends Field[FROM, Iterable[Int], Array[java.lang.Integer]] {
  override def convert: Iterable[Int] => Array[java.lang.Integer] = _.view.map(Int.box).toArray
}

private[h2] case class IntArrayOptional[FROM](
    extract: StringInterning => FROM => Option[Iterable[Int]]
) extends Field[FROM, Option[Iterable[Int]], Array[java.lang.Integer]] {
  override def convert: Option[Iterable[Int]] => Array[java.lang.Integer] =
    _.map(_.view.map(Int.box).toArray).orNull
}

private[h2] case class H2Bytea[FROM](extract: StringInterning => FROM => Array[Byte])
    extends Field[FROM, Array[Byte], InputStream] {
  override def convert: Array[Byte] => InputStream = new ByteArrayInputStream(_)
}

private[h2] case class H2ByteaOptional[FROM](
    extract: StringInterning => FROM => Option[Array[Byte]]
) extends Field[FROM, Option[Array[Byte]], InputStream] {
  override def convert: Option[Array[Byte]] => InputStream =
    _.map(new ByteArrayInputStream(_)).orNull
}
