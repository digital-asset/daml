package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission
import com.daml.ledger.participant.state.v1.ParticipantId
import com.google.protobuf.ByteString

private[validator] object TestHelper {

  lazy val aParticipantId: ParticipantId = ParticipantId.assertFromString("aParticipantId")

  lazy val anInvalidEnvelope: ByteString = ByteString.copyFromUtf8("invalid data")
  lazy val invalidEnvelope: ByteString = anInvalidEnvelope

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
}
