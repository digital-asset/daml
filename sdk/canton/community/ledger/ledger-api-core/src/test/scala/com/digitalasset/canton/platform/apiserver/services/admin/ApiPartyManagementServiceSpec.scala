// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import cats.syntax.traverse.*
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.admin.party_management_service.AllocateExternalPartyRequest.SignedTransaction
import com.daml.ledger.api.v2.admin.party_management_service.{
  AllocateExternalPartyRequest,
  AllocatePartyRequest,
  GenerateExternalPartyTopologyRequest,
  GenerateExternalPartyTopologyResponse,
  PartyDetails as ProtoPartyDetails,
}
import com.daml.ledger.api.v2.crypto as lapicrypto
import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss
import com.daml.nonempty.NonEmpty
import com.daml.tracing.TelemetrySpecBase.*
import com.daml.tracing.{DefaultOpenTelemetry, NoOpTelemetry}
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.base.error.utils.ErrorDetails.RetryInfoDetail
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{HashOps, SigningKeyUsage, SigningPublicKey, TestHash}
import com.digitalasset.canton.interactive.ExternalPartyUtils
import com.digitalasset.canton.ledger.api.{IdentityProviderId, ObjectMeta}
import com.digitalasset.canton.ledger.localstore.api.{PartyRecord, PartyRecordStore}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.{
  IndexPartyManagementService,
  IndexUpdateService,
  IndexerPartyDetails,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementService.blindAndConvertToProto
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementServiceSpec.*
import com.digitalasset.canton.platform.apiserver.services.admin.PartyAllocation
import com.digitalasset.canton.platform.apiserver.services.tracking.{InFlight, StreamTracker}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{
  DecentralizedNamespaceDefinition,
  DelegationRestriction,
  HostingParticipant,
  NamespaceDelegation,
  ParticipantPermission,
  PartyHostingLimits,
  PartyToKeyMapping,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ExternalPartyOnboardingDetails,
  ParticipantId,
  PartyId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutorService}
import com.digitalasset.daml.lf.data.Ref
import com.google.protobuf.ByteString
import io.grpc.Status.Code
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.scalaland.chimney.dsl.*
import org.mockito.{ArgumentMatchers, ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalapb.lenses.{Lens, Mutation}

import java.security.KeyPairGenerator
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class ApiPartyManagementServiceSpec
    extends AsyncWordSpec
    with MockitoSugar
    with Matchers
    with ScalaFutures
    with ArgumentMatchersSugar
    with PekkoBeforeAndAfterAll
    with ErrorsAssertions
    with BaseTest
    with BeforeAndAfterEach
    with ExternalPartyUtils
    with HasExecutorService {

  override def externalPartyExecutionContext: ExecutionContext = executorService

  var testTelemetrySetup: TestTelemetrySetup = _
  val partiesPageSize = PositiveInt.tryCreate(100)

  val aSubmissionId = Ref.SubmissionId.assertFromString("aSubmissionId")

  lazy val (
    _mockIndexTransactionsService,
    mockIdentityProviderExists,
    mockIndexPartyManagementService,
    mockPartyRecordStore,
  ) = mockedServices()
  val partyAllocationTracker = makePartyAllocationTracker(loggerFactory)

  lazy val apiService = ApiPartyManagementService.createApiService(
    mockIndexPartyManagementService,
    mockIdentityProviderExists,
    partiesPageSize,
    mockPartyRecordStore,
    TestPartySyncService(testTelemetrySetup.tracer),
    oneHour,
    ApiPartyManagementService.CreateSubmissionId.fixedForTests(aSubmissionId),
    new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
    partyAllocationTracker,
    loggerFactory = loggerFactory,
  )

  override def beforeEach(): Unit =
    testTelemetrySetup = new TestTelemetrySetup()

  override def afterEach(): Unit =
    testTelemetrySetup.close()

  private implicit val ec: ExecutionContext = directExecutionContext

  "ApiPartyManagementService" should {
    def blind(
        idpId: IdentityProviderId,
        partyDetails: IndexerPartyDetails,
        partyRecord: Option[PartyRecord],
    ): ProtoPartyDetails =
      blindAndConvertToProto(idpId)((partyDetails, partyRecord))

    "translate basic input to the output" in {
      blind(IdentityProviderId.Default, partyDetails, Some(partyRecord)) shouldBe protoPartyDetails
    }

    "blind identity_provider_id for non default IDP" in {
      blind(IdentityProviderId("idp_1"), partyDetails, Some(partyRecord)) shouldBe protoPartyDetails
        .copy(isLocal = false)
    }

    "blind identity_provider_id if record is for non default IDP" in {
      blind(
        IdentityProviderId.Default,
        partyDetails,
        Some(partyRecord.copy(identityProviderId = IdentityProviderId("idp_1"))),
      ) shouldBe protoPartyDetails.copy(identityProviderId = "")
    }

    "not blind `isLocal` if local record does not exist" in {
      blind(IdentityProviderId.Default, partyDetails, None) shouldBe protoPartyDetails
    }

    "blind `isLocal` if local record does not exist for non default IDP" in {
      blind(IdentityProviderId("idp_1"), partyDetails, None) shouldBe protoPartyDetails
        .copy(isLocal = false)
    }

    "validate allocateExternalParty request" when {
      def testAllocateExternalPartyValidation(
          requestTransform: Lens[
            AllocateExternalPartyRequest,
            AllocateExternalPartyRequest,
          ] => Mutation[AllocateExternalPartyRequest],
          expectedFailure: PartyId => Option[String],
          decentralizedOwners: Option[PositiveInt] = None,
      ) = {
        val (onboarding, partyE) = generateExternalPartyOnboardingTransactions(
          "alice",
          participantId,
          decentralizedOwners = decentralizedOwners,
        )
        val request = onboarding
          .toAllocateExternalPartyRequest(synchronizerId = DefaultTestIdentities.synchronizerId)
          .update(requestTransform)

        apiService
          .allocateExternalParty(request)
          .transform {
            case Failure(e: io.grpc.StatusRuntimeException) =>
              expectedFailure(partyE.partyId) match {
                case Some(value) =>
                  e.getStatus.getCode.value() shouldBe io.grpc.Status.INVALID_ARGUMENT.getCode
                    .value()
                  e.getStatus.getDescription should include(value)
                  Success(succeed)
                case None =>
                  fail(s"Expected success but allocation failed with $e")
              }
            case Failure(other) => fail(s"expected a gRPC exception but got $other")
            case Success(_) if expectedFailure(partyE.partyId).isDefined =>
              fail("Expected a failure but got a success")
            case Success(_) => Success(succeed)
          }
      }

      val bobKey = generateNamespaceSigningKey
      val bobParty = PartyId.tryCreate("bob", bobKey.fingerprint)

      "fail if missing synchronizerId" in {
        testAllocateExternalPartyValidation(
          _.synchronizer.modify(_ => ""),
          _ => Some("The submitted command is missing a mandatory field: synchronizer_id"),
        )
      }

      "fail if missing a party to participant" in {
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.filterNot(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToParticipant]
                .nonEmpty
            )
          ),
          _ => Some("One transaction of type PartyToParticipant must be provided, got 0"),
        )
      }

      "allow a single P2P" in {
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.filter(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToParticipant]
                .nonEmpty
            )
          ),
          _ => None,
        )
      }

      "refuse a P2P with Submission rights" in {
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.map(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToParticipant]
                .map { p2p =>
                  TopologyTransaction(
                    p2p.operation,
                    p2p.serial,
                    PartyToParticipant.tryCreate(
                      p2p.mapping.partyId,
                      p2p.mapping.threshold,
                      Seq(HostingParticipant(participantId, ParticipantPermission.Submission)),
                    ),
                    testedProtocolVersion,
                  )
                }
                .map { updatedTx =>
                  SignedTransaction(updatedTx.toByteString, tx.signatures)
                }
                .getOrElse(tx)
            )
          ),
          _ =>
            Some(
              "The PartyToParticipant transaction must not contain any node with Submission permission. Nodes with submission permission: PAR::participant1::participant1..."
            ),
        )
      }

      "refuse a non multi-hosted party submitted to another node" in {
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.map(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToParticipant]
                .map { p2p =>
                  TopologyTransaction(
                    p2p.operation,
                    p2p.serial,
                    PartyToParticipant.tryCreate(
                      p2p.mapping.partyId,
                      p2p.mapping.threshold,
                      Seq(
                        HostingParticipant(
                          DefaultTestIdentities.participant2,
                          ParticipantPermission.Confirmation,
                        )
                      ),
                    ),
                    testedProtocolVersion,
                  )
                }
                .map { updatedTx =>
                  SignedTransaction(updatedTx.toByteString, tx.signatures)
                }
                .getOrElse(tx)
            )
          ),
          _ =>
            Some(
              "The party is to be hosted on a single participant (PAR::participant2::participant2...) that is not this participant (PAR::participant1::participant1...). Submit the allocation request on PAR::participant2::participant2... instead."
            ),
        )
      }

      "refuse a multi-hosted party with no confirming node" in {
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.map(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToParticipant]
                .map { p2p =>
                  TopologyTransaction(
                    p2p.operation,
                    p2p.serial,
                    PartyToParticipant.tryCreate(
                      p2p.mapping.partyId,
                      p2p.mapping.threshold,
                      Seq(
                        HostingParticipant(
                          participantId,
                          ParticipantPermission.Observation,
                        ),
                        HostingParticipant(
                          DefaultTestIdentities.participant2,
                          ParticipantPermission.Observation,
                        ),
                      ),
                    ),
                    testedProtocolVersion,
                  )
                }
                .map { updatedTx =>
                  SignedTransaction(updatedTx.toByteString, tx.signatures)
                }
                .getOrElse(tx)
            )
          ),
          _ =>
            Some(
              "The PartyToParticipant transaction must contain at least one node with Confirmation permission"
            ),
        )
      }

      "refuse mismatching party namespace and p2p namespace" in {
        val updatedTransaction = TopologyTransaction(
          Replace,
          PositiveInt.one,
          NamespaceDelegation.tryCreate(
            namespace = bobParty.namespace,
            target = bobKey,
            restriction = DelegationRestriction.CanSignAllMappings,
          ),
          testedProtocolVersion,
        )
        val signatures = signTxAs(
          updatedTransaction.hash.hash.getCryptographicEvidence,
          Seq(bobKey.fingerprint),
          keyUsage = SigningKeyUsage.All,
        )
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.map(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[NamespaceDelegation]
                .map(_ => updatedTransaction)
                .map { updatedTx =>
                  SignedTransaction(
                    updatedTx.toByteString,
                    signatures.map(_.toProtoV30.transformInto[iss.Signature]),
                  )
                }
                .getOrElse(tx)
            )
          ),
          partyId =>
            Some(
              s"The Party namespace (${bobParty.namespace}) does not match the PartyToParticipant namespace (${partyId.namespace})"
            ),
        )
      }

      "refuse mismatching p2k namespace and p2p namespace" in {
        def updatedTransaction(signingKeys: NonEmpty[Seq[SigningPublicKey]]) = TopologyTransaction(
          Replace,
          PositiveInt.one,
          PartyToKeyMapping.tryCreate(
            partyId = bobParty,
            threshold = PositiveInt.one,
            signingKeys = signingKeys,
          ),
          testedProtocolVersion,
        )
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.map(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToKeyMapping]
                .map(p2k => updatedTransaction(p2k.mapping.signingKeys))
                .map { updatedTx =>
                  SignedTransaction(updatedTx.toByteString, tx.signatures)
                }
                .getOrElse(tx)
            )
          ),
          partyId =>
            Some(
              s"The PartyToKeyMapping namespace (${bobParty.namespace}) does not match the PartyToParticipant namespace (${partyId.namespace})"
            ),
        )
      }

      "refuse mismatching decentralized namespace and p2p namespace" in {
        val decentralizedNamespace =
          DecentralizedNamespaceDefinition.computeNamespace(Set(bobParty.namespace))
        val updatedTransaction = TopologyTransaction(
          Replace,
          PositiveInt.one,
          DecentralizedNamespaceDefinition.tryCreate(
            decentralizedNamespace = decentralizedNamespace,
            threshold = PositiveInt.one,
            owners = NonEmpty.mk(Set, bobParty.namespace),
          ),
          testedProtocolVersion,
        )
        val signatures = signTxAs(
          updatedTransaction.hash.hash.getCryptographicEvidence,
          Seq(bobKey.fingerprint),
          keyUsage = SigningKeyUsage.All,
        )
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.map(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[DecentralizedNamespaceDefinition]
                .map(_ => updatedTransaction)
                .map { updatedTx =>
                  SignedTransaction(
                    updatedTx.toByteString,
                    signatures.map(_.toProtoV30.transformInto[iss.Signature]),
                  )
                }
                .getOrElse(tx)
            )
          ),
          partyId =>
            Some(
              s"The Party namespace ($decentralizedNamespace) does not match the PartyToParticipant namespace (${partyId.namespace})"
            ),
          decentralizedOwners = Some(PositiveInt.one),
        )
      }

      "refuse decentralized namespace with too many owners" in {
        val max = ExternalPartyOnboardingDetails.maxDecentralizedOwnersSize
        val input = max + PositiveInt.one
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(identity),
          _ =>
            Some(
              s"Decentralized namespaces cannot have more than ${max.value} individual namespace owners, got ${input.value}"
            ),
          decentralizedOwners = Some(input),
        )
      }

      "refuse unwanted transactions" in {
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.appended(
              SignedTransaction(
                TopologyTransaction(
                  TopologyChangeOp.Replace,
                  PositiveInt.one,
                  PartyHostingLimits.apply(
                    DefaultTestIdentities.synchronizerId,
                    DefaultTestIdentities.party1,
                  ),
                  testedProtocolVersion,
                ).toByteString,
                Seq.empty,
              )
            )
          ),
          _ =>
            Some(
              s"Unsupported transactions found: PartyHostingLimits. Supported transactions are: NamespaceDelegation, DecentralizedNamespaceDefinition, PartyToParticipant, PartyToKeyMapping"
            ),
        )
      }
    }

    "propagate trace context" in {
      val span = testTelemetrySetup.anEmptySpan()
      val scope = span.makeCurrent()

      // Kick the interaction off
      val future = apiService
        .allocateParty(AllocatePartyRequest("aParty", None, ""))
        .thereafter { _ =>
          scope.close()
          span.end()
        }

      // Allow the tracker to complete
      partyAllocationTracker.onStreamItem(
        PartyAllocation.Completed(
          PartyAllocation.TrackerKey.forTests(aSubmissionId),
          IndexerPartyDetails(aParty, isLocal = true),
        )
      )

      // Wait for tracker to complete
      future.futureValue

      testTelemetrySetup.reportedSpanAttributes should contain(anUserIdSpanAttribute)
    }

    "close while allocating party" in {
      val (
        mockIndexTransactionsService,
        mockIdentityProviderExists,
        mockIndexPartyManagementService,
        mockPartyRecordStore,
      ) = mockedServices()
      val partyAllocationTracker = makePartyAllocationTracker(loggerFactory)
      val apiPartyManagementService = ApiPartyManagementService.createApiService(
        mockIndexPartyManagementService,
        mockIdentityProviderExists,
        partiesPageSize,
        mockPartyRecordStore,
        TestPartySyncService(testTelemetrySetup.tracer),
        oneHour,
        ApiPartyManagementService.CreateSubmissionId.fixedForTests(aSubmissionId.toString),
        NoOpTelemetry,
        partyAllocationTracker,
        loggerFactory = loggerFactory,
      )

      // Kick the interaction off
      val future = apiPartyManagementService.allocateParty(AllocatePartyRequest("aParty", None, ""))

      // Close the service
      apiPartyManagementService.close()

      // Assert that it caused the appropriate failure
      future
        .transform {
          case Success(_) =>
            fail("Expected a failure, but received success")
          case Failure(err: StatusRuntimeException) =>
            assertError(
              actual = err,
              expectedStatusCode = Code.UNAVAILABLE,
              expectedMessage = "SERVER_IS_SHUTTING_DOWN(1,0): Server is shutting down",
              expectedDetails = List(
                ErrorDetails.ErrorInfoDetail(
                  "SERVER_IS_SHUTTING_DOWN",
                  Map(
                    "parties" -> "['aParty']",
                    "category" -> "1",
                    "definite_answer" -> "false",
                    "test" -> s"'${getClass.getSimpleName}'",
                  ),
                ),
                RetryInfoDetail(1.second),
              ),
              verifyEmptyStackTrace = true,
            )
            Success(succeed)
          case Failure(other) =>
            fail("Unexpected error", other)
        }
    }

    "generate-external-topology" when {
      def createService = {
        val keyGen = KeyPairGenerator.getInstance("Ed25519")
        val keyPair = keyGen.generateKeyPair()
        val apiPartyManagementService = ApiPartyManagementService.createApiService(
          mock[IndexPartyManagementService],
          mock[IdentityProviderExists],
          partiesPageSize,
          mock[PartyRecordStore],
          TestPartySyncService(testTelemetrySetup.tracer),
          oneHour,
          ApiPartyManagementService.CreateSubmissionId.fixedForTests(aSubmissionId.toString),
          NoOpTelemetry,
          mock[PartyAllocation.Tracker],
          loggerFactory = loggerFactory,
        )
        val signingKey = Some(
          lapicrypto.SigningPublicKey(
            format = lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
            keyData = ByteString.copyFrom(keyPair.getPublic.getEncoded),
            keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519,
          )
        )
        (apiPartyManagementService, signingKey)
      }
      def getMappingsFromResponse(response: GenerateExternalPartyTopologyResponse) = {
        response.topologyTransactions should have length (3)
        val txs = response.topologyTransactions.toList
          .traverse(tx =>
            TopologyTransaction
              .fromByteString(ProtocolVersion.latest, tx)
          )
          .valueOrFail("unable to parse topology txs")
          .map(_.mapping)
        txs match {
          case (nd: NamespaceDelegation) :: (pk: PartyToKeyMapping) :: (pp: PartyToParticipant) :: Nil =>
            (nd, pk, pp)
          case other => fail("unexpected mappings: " + other)
        }
      }
      "correctly pass through all fields" in {
        val (service, publicKey) = createService
        val syncId = DefaultTestIdentities.synchronizerId

        for {
          response <- service.generateExternalPartyTopology(
            GenerateExternalPartyTopologyRequest(
              synchronizer = syncId.toProtoPrimitive,
              partyHint = "alice",
              publicKey = publicKey,
              localParticipantObservationOnly = false,
              otherConfirmingParticipantUids =
                Seq(DefaultTestIdentities.participant2.uid.toProtoPrimitive),
              confirmationThreshold = 2,
              observingParticipantUids =
                Seq(DefaultTestIdentities.participant3.uid.toProtoPrimitive),
            )
          )
        } yield {
          val (nd, pk, pp) = getMappingsFromResponse(response)
          pk.party shouldBe pp.partyId
          pk.party.namespace shouldBe nd.namespace
          nd.namespace.fingerprint shouldBe nd.target.fingerprint
          pk.threshold.value shouldBe 1
          pp.participants.toSet shouldBe Set(
            HostingParticipant(
              DefaultTestIdentities.participant1,
              ParticipantPermission.Confirmation,
            ),
            HostingParticipant(
              DefaultTestIdentities.participant2,
              ParticipantPermission.Confirmation,
            ),
            HostingParticipant(
              DefaultTestIdentities.participant3,
              ParticipantPermission.Observation,
            ),
          )
          pp.threshold.value shouldBe 2

        }
      }
      "correctly interpret local observer" in {
        val (service, publicKey) = createService
        val syncId = DefaultTestIdentities.synchronizerId

        for {
          response <- service.generateExternalPartyTopology(
            GenerateExternalPartyTopologyRequest(
              synchronizer = syncId.toProtoPrimitive,
              partyHint = "alice",
              publicKey = publicKey,
              localParticipantObservationOnly = true,
              otherConfirmingParticipantUids =
                Seq(DefaultTestIdentities.participant2.uid.toProtoPrimitive),
              confirmationThreshold = 1,
              observingParticipantUids = Seq(),
            )
          )
        } yield {
          val (_, _, pp) = getMappingsFromResponse(response)
          pp.participants.toSet shouldBe Set(
            HostingParticipant(
              DefaultTestIdentities.participant1,
              ParticipantPermission.Observation,
            ),
            HostingParticipant(
              DefaultTestIdentities.participant2,
              ParticipantPermission.Confirmation,
            ),
          )
          pp.threshold.value shouldBe 1
        }
      }
      "correctly reject invalid threshold" in {
        val (service, publicKey) = createService
        val syncId = DefaultTestIdentities.synchronizerId

        for {
          response <- service
            .generateExternalPartyTopology(
              GenerateExternalPartyTopologyRequest(
                synchronizer = syncId.toProtoPrimitive,
                partyHint = "alice",
                publicKey = publicKey,
                localParticipantObservationOnly = false,
                otherConfirmingParticipantUids =
                  Seq(DefaultTestIdentities.participant2.uid.toProtoPrimitive),
                confirmationThreshold = 3,
                observingParticipantUids = Seq(),
              )
            )
            .failed
        } yield {
          response.getMessage should include(
            "Confirmation threshold exceeds number of confirming participants"
          )
        }
      }
      "fail gracefully on invalid synchronizer-ids" in {
        val (service, publicKey) = createService
        for {
          response1 <- service
            .generateExternalPartyTopology(
              GenerateExternalPartyTopologyRequest(
                synchronizer = "",
                partyHint = "alice",
                publicKey = publicKey,
                localParticipantObservationOnly = false,
                otherConfirmingParticipantUids = Seq(),
                confirmationThreshold = 1,
                observingParticipantUids = Seq(),
              )
            )
            .failed
          response2 <- service
            .generateExternalPartyTopology(
              GenerateExternalPartyTopologyRequest(
                synchronizer = SynchronizerId.tryFromString("not::valid").toProtoPrimitive,
                partyHint = "alice",
                publicKey = publicKey,
                localParticipantObservationOnly = false,
                otherConfirmingParticipantUids = Seq(),
                confirmationThreshold = 1,
                observingParticipantUids = Seq(),
              )
            )
            .failed
        } yield {
          response1.getMessage should include("Empty string is not a valid unique identifier")
          response2.getMessage should include("Unknown or not connected synchronizer not::valid")
        }
      }
      "fail gracefully on invalid party hints" in {
        val (service, publicKey) = createService
        val syncId = DefaultTestIdentities.synchronizerId
        for {
          response1 <- service
            .generateExternalPartyTopology(
              GenerateExternalPartyTopologyRequest(
                synchronizer = syncId.toProtoPrimitive,
                partyHint = "",
                publicKey = publicKey,
                localParticipantObservationOnly = false,
                otherConfirmingParticipantUids = Seq(),
                confirmationThreshold = 1,
                observingParticipantUids = Seq(),
              )
            )
            .failed
          response2 <- service
            .generateExternalPartyTopology(
              GenerateExternalPartyTopologyRequest(
                synchronizer = syncId.toProtoPrimitive,
                partyHint =
                  "Aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                publicKey = publicKey,
                localParticipantObservationOnly = false,
                otherConfirmingParticipantUids = Seq(),
                confirmationThreshold = 1,
                observingParticipantUids = Seq(),
              )
            )
            .failed
        } yield {
          response1.getMessage should include("Party hint is empty")
          response2.getMessage should include("is too long")
        }
      }
      "fail gracefully on empty keys" in {
        val (service, _) = createService
        val syncId = DefaultTestIdentities.synchronizerId
        for {
          response1 <- service
            .generateExternalPartyTopology(
              GenerateExternalPartyTopologyRequest(
                synchronizer = syncId.toProtoPrimitive,
                partyHint = "alice",
                publicKey = None,
                localParticipantObservationOnly = false,
                otherConfirmingParticipantUids = Seq(),
                confirmationThreshold = 1,
                observingParticipantUids = Seq(),
              )
            )
            .failed
        } yield {
          response1.getMessage should include("Field `public_key` is not set")
        }
      }
      "fail gracefully on invalid duplicate participant ids" in {
        val (service, publicKey) = createService
        val syncId = DefaultTestIdentities.synchronizerId
        for {
          response1 <- service
            .generateExternalPartyTopology(
              GenerateExternalPartyTopologyRequest(
                synchronizer = syncId.toProtoPrimitive,
                partyHint = "alice",
                publicKey = publicKey,
                localParticipantObservationOnly = false,
                otherConfirmingParticipantUids =
                  Seq(DefaultTestIdentities.participant1.uid.toProtoPrimitive),
                confirmationThreshold = 1,
                observingParticipantUids = Seq(),
              )
            )
            .failed
          response2 <- service
            .generateExternalPartyTopology(
              GenerateExternalPartyTopologyRequest(
                synchronizer = syncId.toProtoPrimitive,
                partyHint = "alice",
                publicKey = publicKey,
                localParticipantObservationOnly = false,
                otherConfirmingParticipantUids = Seq(),
                confirmationThreshold = 1,
                observingParticipantUids =
                  Seq(DefaultTestIdentities.participant1.uid.toProtoPrimitive),
              )
            )
            .failed
        } yield {
          response1.getMessage should include("Duplicate participant ids")
          response2.getMessage should include("Duplicate participant ids")
        }
      }

    }

  }

  private def makePartyAllocationTracker(
      loggerFactory: NamedLoggerFactory
  ): PartyAllocation.Tracker =
    StreamTracker.withTimer[PartyAllocation.TrackerKey, PartyAllocation.Completed](
      timer = new java.util.Timer("test-timer"),
      itemKey = (_ => Some(PartyAllocation.TrackerKey.forTests(aSubmissionId))),
      inFlightCounter = InFlight.Limited(100, mock[com.daml.metrics.api.MetricHandle.Counter]),
      loggerFactory,
    )

  private def mockedServices(): (
      IndexUpdateService,
      IdentityProviderExists,
      IndexPartyManagementService,
      PartyRecordStore,
  ) = {
    val mockIndexUpdateService = mock[IndexUpdateService]
    when(mockIndexUpdateService.currentLedgerEnd())
      .thenReturn(Future.successful(None))

    val mockIdentityProviderExists = mock[IdentityProviderExists]
    when(
      mockIdentityProviderExists.apply(ArgumentMatchers.eq(IdentityProviderId.Default))(
        any[LoggingContextWithTrace]
      )
    )
      .thenReturn(Future.successful(true))

    val mockIndexPartyManagementService = mock[IndexPartyManagementService]

    val mockPartyRecordStore = mock[PartyRecordStore]
    when(
      mockPartyRecordStore.createPartyRecord(any[PartyRecord])(any[LoggingContextWithTrace])
    ).thenReturn(
      Future.successful(
        Right(PartyRecord(aParty, ObjectMeta.empty, IdentityProviderId.Default))
      )
    )
    when(
      mockPartyRecordStore.getPartyRecordO(any[Ref.Party])(any[LoggingContextWithTrace])
    ).thenReturn(Future.successful(Right(None)))

    (
      mockIndexUpdateService,
      mockIdentityProviderExists,
      mockIndexPartyManagementService,
      mockPartyRecordStore,
    )
  }
}

object ApiPartyManagementServiceSpec {

  val participantId = DefaultTestIdentities.participant1

  val partyDetails: IndexerPartyDetails = IndexerPartyDetails(
    party = Ref.Party.assertFromString("Bob"),
    isLocal = true,
  )
  val partyRecord: PartyRecord = PartyRecord(
    party = Ref.Party.assertFromString("Bob"),
    ObjectMeta.empty,
    IdentityProviderId.Default,
  )
  val protoPartyDetails: ProtoPartyDetails = ProtoPartyDetails(
    party = "Bob",
    localMetadata = Some(new com.daml.ledger.api.v2.admin.object_meta.ObjectMeta("", Map.empty)),
    isLocal = true,
    identityProviderId = "",
  )

  val aParty = Ref.Party.assertFromString("aParty")

  val oneHour = FiniteDuration(1, java.util.concurrent.TimeUnit.HOURS)

  private final case class TestPartySyncService(tracer: Tracer) extends state.PartySyncService {
    override def allocateParty(
        hint: Ref.Party,
        submissionId: Ref.SubmissionId,
        externalPartyOnboardingDetails: Option[ExternalPartyOnboardingDetails],
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[state.SubmissionResult] = {
      val telemetryContext = traceContext.toDamlTelemetryContext(tracer)
      telemetryContext.setAttribute(
        anUserIdSpanAttribute._1,
        anUserIdSpanAttribute._2,
      )
      FutureUnlessShutdown.pure(state.SubmissionResult.Acknowledged)
    }

    override def protocolVersionForSynchronizerId(
        synchronizerId: SynchronizerId
    ): Option[ProtocolVersion] =
      Option.when(synchronizerId == DefaultTestIdentities.synchronizerId)(ProtocolVersion.latest)

    override def participantId: ParticipantId = DefaultTestIdentities.participant1

    override def hashOps: HashOps = TestHash
  }
}
