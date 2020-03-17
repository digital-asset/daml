// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform

import java.io.{StringReader, StringWriter}

import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.daml.lf.data.Ref
import com.google.common.io.{BaseEncoding, ByteStreams}

import scala.util.Try

object ApiOffset {

  def fromString(s: String): Try[Offset] = Try {
    Offset.fromInputStream(BaseEncoding.base16.decodingStream(new StringReader(s)))
  }

  def assertFromString(s: String): Offset = fromString(s).get

  def toApiString(offset: Offset): Ref.LedgerString = {
    val writer = new StringWriter()
    val os = BaseEncoding.base16.encodingStream(writer)
    ByteStreams.copy(offset.toInputStream, os)
    Ref.LedgerString.assertFromString(writer.toString)
  }

  implicit class ApiOffsetConverter(val offset: Offset) {
    def toApiString: Ref.LedgerString = ApiOffset.toApiString(offset)
  }

}
