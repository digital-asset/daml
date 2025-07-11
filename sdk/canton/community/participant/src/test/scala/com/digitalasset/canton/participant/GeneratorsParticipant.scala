// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.DeduplicationPeriod
import com.digitalasset.canton.ledger.participant.state.{CompletionInfo, Update}
import com.digitalasset.canton.participant.protocol.submission.TransactionSubmissionTrackingData.{
  CauseWithTemplate,
  RejectionCause,
  TimeoutCause,
}
import com.digitalasset.canton.participant.protocol.submission.{
  SubmissionTrackingData,
  TransactionSubmissionTrackingData,
}
import com.digitalasset.canton.topology.{GeneratorsTopology, PhysicalSynchronizerId}
import com.digitalasset.canton.version.SerializationDeserializationTestHelpers
import com.digitalasset.canton.{GeneratorsLf, LedgerUserId, LfPartyId}
import org.scalacheck.{Arbitrary, Gen}

final class GeneratorsParticipant(
    generatorsTopology: GeneratorsTopology,
    generatorsLf: GeneratorsLf,
) {

  import GeneratorsParticipant.*

  import com.digitalasset.canton.Generators.*
  import generatorsTopology.*
  import generatorsLf.*
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
}

object GeneratorsParticipant {
  sealed trait GeneratorForClass {
    type Typ <: AnyRef
    def gen: Gen[Typ]
    def clazz: Class[Typ]
  }
  object GeneratorForClass {
    def apply[T <: AnyRef](generator: Gen[T], claz: Class[T]): GeneratorForClass { type Typ = T } =
      new GeneratorForClass {
        override type Typ = T
        override def gen: Gen[Typ] = generator
        override def clazz: Class[Typ] = claz
      }
  }

  def arbitraryForAllSubclasses[T](clazz: Class[T])(
      g0: GeneratorForClass { type Typ <: T },
      moreGens: GeneratorForClass { type Typ <: T }*
  ): Arbitrary[T] = {
    val availableGenerators = (g0 +: moreGens).map(_.clazz.getName).toSet

    val foundSubclasses = SerializationDeserializationTestHelpers
      .findSubClassesOf(clazz, "com.digitalasset.canton")
      .map(_.getName)
      .toSet
    val missingGeneratorsForSubclasses = foundSubclasses -- availableGenerators
    require(
      missingGeneratorsForSubclasses.isEmpty,
      s"No generators for subclasses $missingGeneratorsForSubclasses of ${clazz.getName}",
    )
    val redundantGenerators = availableGenerators -- foundSubclasses
    require(
      redundantGenerators.isEmpty,
      s"Reflection did not find the subclasses $redundantGenerators of ${clazz.getName}",
    )

    val allGen = NonEmpty.from(moreGens) match {
      case None => g0.gen
      case Some(moreGensNE) =>
        Gen.oneOf[T](
          g0.gen,
          moreGensNE.head1.gen,
          moreGensNE.tail1.map(_.gen) *,
        )
    }
    // Returns an Arbitrary instead of a Gen so that the exhaustiveness check is forced upon creation
    // rather than first usage of the Arbitrary's arbitrary.
    Arbitrary(allGen)
  }
}
