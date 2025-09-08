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
import com.digitalasset.canton.protocol.{ExampleContractFactory, LfHash}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.util.ContractAuthenticator
import com.digitalasset.daml.lf.transaction.FatContractInstance
import com.digitalasset.daml.lf.value.Value.ContractId
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

class TransactionProcessingStepsTest extends AsyncWordSpec with BaseTest {
  private val synchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("the::synchronizer")
  ).toPhysical
  private val participantId: ParticipantId = ParticipantId("participant")

  private def buildTestInstance(
      behaviors: Map[ContractId, Either[String, Unit]]
  ) = new TransactionProcessingSteps(
    psid = synchronizerId,
    participantId = participantId,
    confirmationRequestFactory = mock[TransactionConfirmationRequestFactory],
    confirmationResponsesFactory = mock[TransactionConfirmationResponsesFactory],
    modelConformanceChecker = mock[ModelConformanceChecker],
    staticSynchronizerParameters = defaultStaticSynchronizerParameters,
    crypto = mock[SynchronizerCryptoClient],
    metrics = ParticipantTestMetrics.synchronizer.transactionProcessing,
    serializableContractAuthenticator = new ContractAuthenticator {
      override def legacyAuthenticate(contract: FatContractInstance): Either[String, Unit] =
        behaviors.getOrElse(
          contract.contractId,
          fail(s"contract authentication did not find ${contract.contractId}"),
        )
      override def authenticate(
          instance: FatContractInstance,
          contractHash: LfHash,
      ): Either[String, Unit] =
        behaviors.getOrElse(
          instance.contractId,
          fail(s"contract authentication did not find ${instance.contractId}"),
        )
    },
    transactionEnricher = tx => _ => EitherT.pure(tx),
    createNodeEnricher = node => _ => EitherT.pure(node),
    new AuthorizationValidator(participantId),
    new InternalConsistencyChecker(
      loggerFactory
    ),
    CommandProgressTracker.NoOp,
    loggerFactory = loggerFactory,
    FutureSupervisor.Noop,
  )

  "authenticateInputContracts" when {
    val c1 = ExampleContractFactory.build()
    val c2 = ExampleContractFactory.build()
    val inputContracts = Map(c1.contractId -> c1, c2.contractId -> c2)

    "provided with valid input contracts" should {
      "succeed" in {
        val testInstance =
          buildTestInstance(Map(c1.contractId -> Either.unit, c2.contractId -> Either.unit))

        val result = testInstance.authenticateInputContractsInternal(inputContracts)
        result.value.map(_ shouldBe Right[TransactionProcessorError, Unit](()))
      }
    }

    "provided with contracts failing authentication" must {
      "convert failure and raise alarm" in {
        val testInstance =
          buildTestInstance(
            Map(c1.contractId -> Either.unit, c2.contractId -> Left("some authentication failure"))
          )

        val (expectedLog, expectedResult) = {
          val expectedLog: LogEntry => Assertion =
            _.shouldBeCantonError(
              ContractAuthenticationFailed,
              _ should include(
                s"Contract with id (${c2.contractId.coid}) could not be authenticated: some authentication failure"
              ),
            )

          val expectedError =
            ContractAuthenticationFailed.Error(c2.contractId, "some authentication failure")

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
