// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import java.security.MessageDigest

import com.daml.ledger.participant.state.kvutils.Raw
import com.daml.ledger.participant.state.kvutils.store.DamlLogEntryId
import com.google.protobuf.ByteString

/** Computes log entry IDs from raw submission envelopes. */
trait LogEntryIdComputationStrategy {
  def compute(value: Raw.Envelope): DamlLogEntryId
}

/** While the log entry ID is no longer the basis for deriving absolute contract IDs,
  * it is used for keying log entries / fragments. We may want to consider content addressing
  * instead and remove the whole concept of log entry identifiers.
  * For now this implementation uses a sha256 hash of the submission envelope in order to generate
  * deterministic log entry IDs.
  */
object HashingLogEntryIdComputationStrategy extends LogEntryIdComputationStrategy {

  private val LogEntryIdPrefix = "0"

  override def compute(rawEnvelope: Raw.Envelope): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(hash(rawEnvelope))
      .build

  private def hash(rawEnvelope: Raw.Envelope): ByteString = {
    val messageDigest = MessageDigest
      .getInstance("SHA-256")
    messageDigest.update(rawEnvelope.bytes.asReadOnlyByteBuffer())
    val hash = messageDigest
      .digest()
      .map("%02x" format _)
      .mkString
    ByteString.copyFromUtf8(LogEntryIdPrefix + hash)
  }
}
