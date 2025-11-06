// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.digitalasset.canton.config.GeneratorsConfig
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, Update}
import com.digitalasset.canton.participant.admin.data.{ActiveContract, ActiveContractOld}
import com.digitalasset.canton.participant.protocol.party.{
  PartyReplicationSourceParticipantMessage,
  PartyReplicationTargetParticipantMessage,
}
import com.digitalasset.canton.participant.protocol.submission.TransactionSubmissionTrackingData.{
  CauseWithTemplate,
  RejectionCause,
  TimeoutCause,
}
import com.digitalasset.canton.participant.protocol.submission.{
  SubmissionTrackingData,
  TransactionSubmissionTrackingData,
}
import com.digitalasset.canton.protocol.GeneratorsProtocol
import com.digitalasset.canton.topology.{GeneratorsTopology, PhysicalSynchronizerId, SynchronizerId}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{GeneratorsLf, LedgerUserId, LfPartyId, ReassignmentCounter}
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsParticipant(
    generatorsTopology: GeneratorsTopology,
    generatorsLf: GeneratorsLf,
    generatorsProtocol: GeneratorsProtocol,
    version: ProtocolVersion,
) {

  import GeneratorsConfig.*
  import com.digitalasset.canton.Generators.*
  import generatorsTopology.*
  import generatorsLf.*
  import generatorsProtocol.*
  import com.digitalasset.canton.ledger.api.GeneratorsApi.*

  implicit val completionInfoArb: Arbitrary[CompletionInfo] = Arbitrary {
    for {
      actAs <- boundedListGen[LfPartyId]
      userId <- Arbitrary.arbitrary[LedgerUserId]
      commandId <- ledgerSubmissionIdArb.arbitrary
      optDedupPeriod <- Gen.option(Arbitrary.arbitrary[DeduplicationPeriod])
      submissionId <- Gen.option(lfSubmissionIdArb.arbitrary)
    } yield CompletionInfo(actAs, userId, commandId, optDedupPeriod, submissionId)
  }

  implicit val finalReasonArb: Arbitrary[Update.CommandRejected.FinalReason] =
    Arbitrary(
      for {
        statusCode <- Gen.oneOf(
          com.google.rpc.Code.INVALID_ARGUMENT_VALUE,
          com.google.rpc.Code.CANCELLED_VALUE,
        )
      } yield Update.CommandRejected.FinalReason(com.google.rpc.status.Status(statusCode))
    )

  val causeWithTemplateGen: Gen[CauseWithTemplate] = for {
    template <- Arbitrary.arbitrary[Update.CommandRejected.FinalReason]
  } yield CauseWithTemplate(template)
  val timeoutCauseGen: Gen[TimeoutCause.type] = Gen.const(TimeoutCause)
  implicit val rejectionCauseArb: Arbitrary[RejectionCause] =
    arbitraryForAllSubclasses(classOf[RejectionCause])(
      GeneratorForClass(causeWithTemplateGen, classOf[CauseWithTemplate]),
      GeneratorForClass(timeoutCauseGen, classOf[TimeoutCause.type]),
    )

  val transactionSubmissionTrackingDataGen: Gen[TransactionSubmissionTrackingData] =
    for {
      completionInfo <- Arbitrary.arbitrary[CompletionInfo]
      rejectionCause <- Arbitrary.arbitrary[RejectionCause]
      physicalSynchronizerId <- Arbitrary.arbitrary[PhysicalSynchronizerId]
    } yield TransactionSubmissionTrackingData(
      completionInfo,
      rejectionCause,
      physicalSynchronizerId,
    )

  implicit val submissionTrackingDataArg: Arbitrary[SubmissionTrackingData] =
    arbitraryForAllSubclasses(classOf[SubmissionTrackingData])(
      GeneratorForClass(
        transactionSubmissionTrackingDataGen,
        classOf[TransactionSubmissionTrackingData],
      )
    )

  implicit val reassignmentCounterArb: Arbitrary[ReassignmentCounter] =
    Arbitrary(Gen.chooseNum(0L, Long.MaxValue).map(ReassignmentCounter(_)))

  import com.daml.ledger.api.v2.state_service.ActiveContract as LapiActiveContract
  implicit val activeContractArb: Arbitrary[ActiveContract] =
    Arbitrary(
      for {
        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        reassignmentCounter <- Arbitrary.arbitrary[ReassignmentCounter]
        // TODO(#26599): Add generator for LapiActiveContract
        lapiActiveContract <- Gen.const(
          LapiActiveContract(None, synchronizerId.toProtoPrimitive, reassignmentCounter.unwrap)
        )
      } yield ActiveContract.create(lapiActiveContract)(version)
    )

  implicit val activeContractOldArb: Arbitrary[ActiveContractOld] =
    Arbitrary(
      for {
        synchronizerId <- Arbitrary.arbitrary[SynchronizerId]
        serializableContract <- serializableContractArb(canHaveEmptyKey = true).arbitrary
        reassignmentCounter <- Arbitrary.arbitrary[ReassignmentCounter]
      } yield ActiveContractOld.create(
        synchronizerId,
        serializableContract,
        reassignmentCounter,
      )(version)
    )

  // If this pattern match is not exhaustive anymore, update the message generator below
  {
    ((_: PartyReplicationSourceParticipantMessage.DataOrStatus) match {
      case _: PartyReplicationSourceParticipantMessage.AcsBatch => ()
      case PartyReplicationSourceParticipantMessage.EndOfACS => ()
    }).discard
  }

  implicit val partyReplicationSourceParticipantMessageArb
      : Arbitrary[PartyReplicationSourceParticipantMessage] =
    Arbitrary(
      for {
        acsBatch <- nonEmptyListGen[ActiveContract]
        message <- Gen
          .oneOf[PartyReplicationSourceParticipantMessage.DataOrStatus](
            PartyReplicationSourceParticipantMessage.AcsBatch(
              acsBatch
            ),
            Gen.const(PartyReplicationSourceParticipantMessage.EndOfACS),
          )
      } yield PartyReplicationSourceParticipantMessage.apply(
        message,
        version,
      )
    )

  // If this pattern match is not exhaustive anymore, update the instruction generator below
  {
    ((_: PartyReplicationTargetParticipantMessage.Instruction) match {
      case _: PartyReplicationTargetParticipantMessage.Initialize => ()
      case _: PartyReplicationTargetParticipantMessage.SendAcsUpTo => ()
    }).discard
  }

  implicit val partyReplicationTargetParticipantMessageArb
      : Arbitrary[PartyReplicationTargetParticipantMessage] = Arbitrary(
    for {
      contractOrdinal <- nonNegativeIntArb.arbitrary
      instruction <- Gen
        .oneOf[PartyReplicationTargetParticipantMessage.Instruction](
          PartyReplicationTargetParticipantMessage.Initialize(contractOrdinal),
          PartyReplicationTargetParticipantMessage.SendAcsUpTo(contractOrdinal),
        )
    } yield PartyReplicationTargetParticipantMessage.apply(instruction, version)
  )

}
