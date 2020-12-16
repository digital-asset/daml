// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration
import java.util.UUID

import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.committer.CommitContext
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, TimeModel}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.google.protobuf.ByteString

object TestHelpers {
  def name(value: String): Ref.Name = Ref.Name.assertFromString(value)

  def party(value: String): Ref.Party = Ref.Party.assertFromString(value)

  val badArchive: DamlLf.Archive =
    DamlLf.Archive.newBuilder
      .setHash("blablabla")
      .build

  val theRecordTime: Timestamp = Timestamp.Epoch
  val theDefaultConfig: Configuration = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault,
    maxDeduplicationTime = Duration.ofDays(1),
  )

  def mkEntryId(n: Int): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(n.toString))
      .build

  def mkParticipantId(n: Int): ParticipantId =
    Ref.ParticipantId.assertFromString(s"participant-$n")

  def randomLedgerString: Ref.LedgerString =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)

  def createCommitContext(
      recordTime: Option[Timestamp],
      inputs: DamlStateMap = Map.empty,
      participantId: Int = 0,
  ): CommitContext =
    CommitContext(inputs, recordTime, mkParticipantId(participantId))
}
