// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import com.google.protobuf.{ByteString, CodedInputStream}

import scala.util.Using

final class GenReader[X] private[archive] (val fromCodedInputStream: CodedInputStream => X) {

  // close the stream unconditionally
  def fromInputStream(is: java.io.InputStream): X =
    fromCodedInputStream(CodedInputStream.newInstance(is))

  def fromByteArray(bytes: Array[Byte]): X =
    fromCodedInputStream(CodedInputStream.newInstance(bytes))

  def fromByteString(bytes: ByteString): X =
    fromCodedInputStream(CodedInputStream.newInstance(bytes.asReadOnlyByteBuffer()))

  def fromBytes(bytes: data.Bytes): X =
    fromByteString(bytes.toByteString)

  def fromFile(file: java.nio.file.Path): X =
    Using.resource(java.nio.file.Files.newInputStream(file))(fromInputStream)

  def fromFile(file: java.io.File): X =
    fromFile(file.toPath)

  private[archive] def andThen[Y](f: X => Y): GenReader[Y] =
    new GenReader(x => f(fromCodedInputStream(x)))

}
