// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.ContractAuthenticationFailed
import com.digitalasset.canton.participant.protocol.TransactionProcessor.TransactionProcessorError
import com.digitalasset.canton.participant.protocol.submission.TransactionConfirmationRequestFactory
import com.digitalasset.canton.participant.protocol.validation.*
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.protocol.{ContractMetadata, LfContractId, SerializableContract}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.daml.lf.transaction.FatContractInstance
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

class TransactionProcessingStepsTest extends AsyncWordSpec with BaseTest {
  private val synchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("the::synchronizer")
  )
  private val participantId: ParticipantId = ParticipantId("participant")

  private def buildTestInstance(
      behaviors: Map[SerializableContract, Either[String, Unit]]
  ) = new TransactionProcessingSteps(
    synchronizerId = synchronizerId,
    participantId = participantId,
    confirmationRequestFactory = mock[TransactionConfirmationRequestFactory],
    confirmationResponseFactory = mock[TransactionConfirmationResponseFactory],
    modelConformanceChecker = mock[ModelConformanceChecker],
    staticSynchronizerParameters = defaultStaticSynchronizerParameters,
    crypto = mock[SynchronizerCryptoClient],
    metrics = ParticipantTestMetrics.synchronizer.transactionProcessing,
    serializableContractAuthenticator = new ContractAuthenticator {
      override def authenticateSerializable(contract: SerializableContract): Either[String, Unit] =
        behaviors.getOrElse(
          contract,
          fail(s"authenticateSerializable did not find ${contract.contractId}"),
        )
      override def authenticateFat(contract: FatContractInstance): Either[String, Unit] = fail(
        "unexpected"
      )
      override def verifyMetadata(
          contract: SerializableContract,
          metadata: ContractMetadata,
      ): Either[String, Unit] = Either.unit
    },
    transactionEnricher = tx => _ => EitherT.pure(tx),
    createNodeEnricher = node => _ => EitherT.pure(node),
    new AuthorizationValidator(participantId, true),
    new InternalConsistencyChecker(
      loggerFactory
    ),
    CommandProgressTracker.NoOp,
    loggerFactory = loggerFactory,
    FutureSupervisor.Noop,
  )

  "authenticateInputContracts" when {
    val c1, c2 = mock[SerializableContract]
    val contractId1 = LfContractId.assertFromString("00" * 33 + "00")
    val contractId2 = LfContractId.assertFromString("00" * 33 + "01")
    val inputContracts = Map(contractId1 -> c1, contractId2 -> c2)

    "provided with valid input contracts" should {
      "succeed" in {
        val testInstance =
          buildTestInstance(Map(c1 -> Either.unit, c2 -> Either.unit))

        val result = testInstance.authenticateInputContractsInternal(inputContracts)
        result.value.map(_ shouldBe Right[TransactionProcessorError, Unit](()))
      }
    }

    "provided with contracts failing authentication" must {
      "convert failure and raise alarm" in {
        val testInstance =
          buildTestInstance(
            Map(c1 -> Either.unit, c2 -> Left("some authentication failure"))
          )

        val (expectedLog, expectedResult) = {
          val expectedLog: LogEntry => Assertion =
            _.shouldBeCantonError(
              ContractAuthenticationFailed,
              _ should include(
                s"Contract with id (${contractId2.coid}) could not be authenticated: some authentication failure"
              ),
            )

          val expectedError =
            ContractAuthenticationFailed.Error(contractId2, "some authentication failure")

          Some(expectedLog) -> Left(expectedError)
        }

        loggerFactory
          .assertLogs(
            testInstance.authenticateInputContractsInternal(inputContracts).value,
            expectedLog.toList *,
          )
          .map(_ shouldBe expectedResult)
      }
    }
  }
}
