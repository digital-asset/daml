// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.TransactionProcessor.SubmissionErrors.ContractAuthenticationFailed
import com.digitalasset.canton.participant.protocol.TransactionProcessor.TransactionProcessorError
import com.digitalasset.canton.participant.protocol.submission.ConfirmationRequestFactory
import com.digitalasset.canton.participant.protocol.validation.*
import com.digitalasset.canton.participant.store.StoredContractManager
import com.digitalasset.canton.protocol.{ContractMetadata, LfContractId, SerializableContract}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

class TransactionProcessingStepsTest extends AsyncWordSpec with BaseTest {
  private val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("the::domain"))
  private val participantId: ParticipantId = ParticipantId("participant")

  private def buildTestInstance(
      contractAuthenticatorBehaviors: (SerializableContract, Either[String, Unit])*
  ) = new TransactionProcessingSteps(
    domainId = domainId,
    participantId = participantId,
    confirmationRequestFactory = mock[ConfirmationRequestFactory],
    confirmationResponseFactory = mock[ConfirmationResponseFactory],
    modelConformanceChecker = mock[ModelConformanceChecker],
    staticDomainParameters = defaultStaticDomainParameters,
    crypto = mock[DomainSyncCryptoClient],
    storedContractManager = mock[StoredContractManager],
    metrics = ParticipantTestMetrics.domain.transactionProcessing,
    serializableContractAuthenticator = new SerializableContractAuthenticator {
      val behaviors: Map[SerializableContract, Either[String, Unit]] =
        contractAuthenticatorBehaviors.toMap
      override def authenticate(contract: SerializableContract): Either[String, Unit] = behaviors(
        contract
      )
      override def verifyMetadata(
          contract: SerializableContract,
          metadata: ContractMetadata,
      ): Either[String, Unit] = Right(())
    },
    new AuthenticationValidator(),
    new AuthorizationValidator(participantId, enableContractUpgrading = false),
    new InternalConsistencyChecker(
      defaultStaticDomainParameters.uniqueContractKeys,
      defaultStaticDomainParameters.protocolVersion,
      loggerFactory,
    ),
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
        val testInstance = buildTestInstance(c1 -> Right(()), c2 -> Right(()))

        val result = testInstance.authenticateInputContractsInternal(inputContracts)
        result.value.map(_ shouldBe Right[TransactionProcessorError, Unit](()))
      }
    }

    "provided with contracts failing authentication" must {
      "convert failure and raise alarm" in {
        val testInstance =
          buildTestInstance(c1 -> Right(()), c2 -> Left("some authentication failure"))

        val (expectedLog, expectedResult) =
          if (testedProtocolVersion >= ProtocolVersion.v4) {
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
          } else {
            // Contract id authentication not performed prior to PV4
            None -> Right(())
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
