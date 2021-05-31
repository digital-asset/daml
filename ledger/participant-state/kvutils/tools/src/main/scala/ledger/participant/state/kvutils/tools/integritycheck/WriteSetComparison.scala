// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.export.WriteSet

trait WriteSetComparison {

  def compareWriteSets(expectedWriteSet: WriteSet, actualWriteSet: WriteSet): Option[String]

  def checkEntryIsReadable(rawKey: Raw.Key, rawEnvelope: Raw.Envelope): Either[String, Unit]

}

object WriteSetComparison {

  def rawHexString(raw: Raw.Bytes): String =
    raw.bytes.toByteArray.map(byte => "%02x".format(byte)).mkString

}
