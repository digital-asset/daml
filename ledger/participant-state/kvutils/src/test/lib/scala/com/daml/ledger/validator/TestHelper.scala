// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlCommandDedupKey,
  DamlContractKey,
  DamlLogEntry,
  DamlLogEntryId,
  DamlPartyAllocationEntry,
  DamlStateKey,
  DamlSubmission,
  DamlSubmissionDedupKey
}
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.lf.value.ValueOuterClass
import com.google.protobuf.ByteString

private[validator] object TestHelper {

  lazy val aParticipantId: ParticipantId = ParticipantId.assertFromString("aParticipantId")

  lazy val aLogEntry: DamlLogEntry =
    DamlLogEntry
      .newBuilder()
      .setPartyAllocationEntry(
        DamlPartyAllocationEntry.newBuilder().setParty("aParty").setParticipantId(aParticipantId))
      .build()

  lazy val allDamlStateKeyTypes: Seq[DamlStateKey] = Seq(
    DamlStateKey.newBuilder
      .setPackageId("a package ID"),
    DamlStateKey.newBuilder
      .setContractId("a contract ID"),
    DamlStateKey.newBuilder
      .setCommandDedup(DamlCommandDedupKey.newBuilder.setCommandId("an ID")),
    DamlStateKey.newBuilder
      .setParty("a party"),
    DamlStateKey.newBuilder
      .setContractKey(
        DamlContractKey.newBuilder.setTemplateId(
          ValueOuterClass.Identifier.newBuilder.addName("a name"))),
    DamlStateKey.newBuilder.setConfiguration(com.google.protobuf.Empty.getDefaultInstance),
    DamlStateKey.newBuilder.setSubmissionDedup(
      DamlSubmissionDedupKey.newBuilder.setSubmissionId("a submission ID"))
  ).map(_.build)

  lazy val anInvalidEnvelope: ByteString = ByteString.copyFromUtf8("invalid data")

  def makePartySubmission(party: String): DamlSubmission = {
    val builder = DamlSubmission.newBuilder
    builder.setSubmissionSeed(ByteString.EMPTY)
    builder.addInputDamlStateBuilder().setParty(party)
    val submissionId = s"$party-submission"
    builder
      .addInputDamlStateBuilder()
      .getSubmissionDedupBuilder
      .setParticipantId(aParticipantId)
      .setSubmissionId(submissionId)
    builder.getPartyAllocationEntryBuilder
      .setSubmissionId(submissionId)
      .setParticipantId(aParticipantId)
      .setDisplayName(party)
      .setParty(party)
    builder.build
  }

  def aLogEntryId(): DamlLogEntryId = SubmissionValidator.allocateRandomLogEntryId()
}
