// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.daml.ledger.api.v2.crypto.SignatureFormat.SIGNATURE_FORMAT_RAW
import com.daml.ledger.api.v2.{crypto, crypto as lapicrypto}
import com.daml.nonempty.NonEmpty
import com.daml.tracing.TelemetrySpecBase.*
import com.daml.tracing.{DefaultOpenTelemetry, NoOpTelemetry}
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.base.error.utils.ErrorDetails
import com.digitalasset.base.error.utils.ErrorDetails.RetryInfoDetail
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.v30.SigningKeyScheme.SIGNING_KEY_SCHEME_UNSPECIFIED
import com.digitalasset.canton.crypto.{
  Fingerprint,
  HashOps,
  SigningKeyUsage,
  SigningPublicKey,
  TestHash,
  v30,
}
import com.digitalasset.canton.ledger.api.{IdentityProviderId, ObjectMeta}
import com.digitalasset.canton.ledger.localstore.api.{
  PartyRecord,
  PartyRecordStore,
  UserManagementStore,
}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationEvent.Added
import com.digitalasset.canton.ledger.participant.state.Update.TopologyTransactionEffective.AuthorizationLevel.Submission
import com.digitalasset.canton.ledger.participant.state.index.{
  IndexPartyManagementService,
  IndexerPartyDetails,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{
  LoggingContextWithTrace,
  NamedLoggerFactory,
  SuppressionRule,
}
import com.digitalasset.canton.platform.apiserver.services.admin.ApiPartyManagementService.{
  CreateSubmissionId,
  blindAndConvertToProto,
}
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
  TopologyMapping,
  TopologyTransaction,
}
import com.digitalasset.canton.topology.{
  DefaultTestIdentities,
  ExternalPartyOnboardingDetails,
  Namespace,
  ParticipantId,
  PartyId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.{TestTelemetrySetup, TraceContext}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutorService, LfPartyId}
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
import org.slf4j.event.Level
import scalapb.lenses.{Lens, Mutation}

import java.security.{KeyPair, KeyPairGenerator, Signature}
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
    with HasExecutorService {

  var testTelemetrySetup: TestTelemetrySetup = _
  val partiesPageSize = PositiveInt.tryCreate(100)

  val aPartyAllocationTracker =
    PartyAllocation.TrackerKey(
      DefaultTestIdentities.party1.toLf,
      DefaultTestIdentities.participant1.toLf,
      Added(Submission),
    )
  val createSubmissionId = new CreateSubmissionId {
    override def apply(
        partyIdHint: LfPartyId,
        authorizationLevel: TopologyTransactionEffective.AuthorizationLevel,
    ): PartyAllocation.TrackerKey = aPartyAllocationTracker
  }

  lazy val (
    _mockIndexTransactionsService,
    mockIdentityProviderExists,
    mockIndexPartyManagementService,
    mockPartyRecordStore,
  ) = mockedServices()
  val partyAllocationTracker = makePartyAllocationTracker(loggerFactory)

  lazy val apiService = ApiPartyManagementService.createApiService(
    mock[IndexPartyManagementService],
    mock[UserManagementStore],
    mock[IdentityProviderExists],
    partiesPageSize,
    NonNegativeInt.tryCreate(0),
    mock[PartyRecordStore],
    TestPartySyncService(testTelemetrySetup.tracer),
    oneHour,
    createSubmissionId,
    NoOpTelemetry,
    mock[PartyAllocation.Tracker],
    loggerFactory = loggerFactory,
  )

  override def beforeEach(): Unit =
    testTelemetrySetup = new TestTelemetrySetup()

  override def afterEach(): Unit =
    testTelemetrySetup.close()

  private implicit val ec: ExecutionContext = directExecutionContext

  val ApiPartyManagementServiceSuppressionRule: SuppressionRule =
    SuppressionRule.LoggerNameContains("ApiPartyManagementService") &&
      SuppressionRule.Level(Level.ERROR)

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

    def createSigningKey: (Option[crypto.SigningPublicKey], KeyPair) = {
      val keyGen = KeyPairGenerator.getInstance("Ed25519")
      val keyPair = keyGen.generateKeyPair()
      val protoKey = Some(
        lapicrypto.SigningPublicKey(
          format = lapicrypto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
          keyData = ByteString.copyFrom(keyPair.getPublic.getEncoded),
          keySpec = lapicrypto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519,
        )
      )
      (protoKey, keyPair)
    }

    def cantonSigningPublicKey(publicKey: crypto.SigningPublicKey) =
      SigningPublicKey
        .fromProtoV30(
          com.digitalasset.canton.crypto.v30.SigningPublicKey(
            format =
              publicKey.format.transformInto[com.digitalasset.canton.crypto.v30.CryptoKeyFormat],
            publicKey = publicKey.keyData,
            // Deprecated field
            scheme = SIGNING_KEY_SCHEME_UNSPECIFIED,
            usage = Seq(SigningKeyUsage.Namespace.toProtoEnum),
            keySpec =
              publicKey.keySpec.transformInto[com.digitalasset.canton.crypto.v30.SigningKeySpec],
          )
        )
        .value

    def sign(keyPair: KeyPair, data: ByteString, signedBy: Fingerprint) = {
      val signatureInstance = Signature.getInstance("Ed25519")
      signatureInstance.initSign(keyPair.getPrivate)
      signatureInstance.update(data.toByteArray)
      lapicrypto.Signature(
        format = SIGNATURE_FORMAT_RAW,
        signature = ByteString.copyFrom(signatureInstance.sign()),
        signedBy = signedBy.toProtoPrimitive,
        signingAlgorithmSpec = lapicrypto.SigningAlgorithmSpec.SIGNING_ALGORITHM_SPEC_ED25519,
      )
    }

    "validate allocateExternalParty request" when {
      def testAllocateExternalPartyValidation(
          requestTransform: Lens[
            AllocateExternalPartyRequest,
            AllocateExternalPartyRequest,
          ] => Mutation[AllocateExternalPartyRequest],
          expectedFailure: PartyId => Option[String],
      ) = {
        val (publicKey, keyPair) = createSigningKey
        val cantonPublicKey = cantonSigningPublicKey(publicKey.value)
        val partyId = PartyId.tryCreate("alice", cantonPublicKey.fingerprint)
        for {
          generatedTransactions <- apiService.generateExternalPartyTopology(
            GenerateExternalPartyTopologyRequest(
              synchronizer = DefaultTestIdentities.synchronizerId.toProtoPrimitive,
              partyHint = "alice",
              publicKey = publicKey,
              localParticipantObservationOnly = false,
              otherConfirmingParticipantUids =
                Seq(DefaultTestIdentities.participant2.uid.toProtoPrimitive),
              confirmationThreshold = 1,
              observingParticipantUids =
                Seq(DefaultTestIdentities.participant3.uid.toProtoPrimitive),
            )
          )
          signature = sign(keyPair, generatedTransactions.multiHash, partyId.fingerprint)
          request = AllocateExternalPartyRequest(
            synchronizer = DefaultTestIdentities.synchronizerId.toProtoPrimitive,
            onboardingTransactions = generatedTransactions.topologyTransactions.map(tx =>
              AllocateExternalPartyRequest.SignedTransaction(tx, Seq.empty)
            ),
            multiHashSignatures = Seq(signature),
            identityProviderId = "",
          ).update(requestTransform)
          result <- apiService
            .allocateExternalParty(request)
            .transform {
              case Failure(e: io.grpc.StatusRuntimeException) =>
                expectedFailure(partyId) match {
                  case Some(value) =>
                    e.getStatus.getCode.value() shouldBe io.grpc.Status.INVALID_ARGUMENT.getCode
                      .value()
                    e.getStatus.getDescription should include(value)
                    Success(succeed)
                  case None =>
                    fail(s"Expected success but allocation failed with $e")
                }
              case Failure(other) => fail(s"expected a gRPC exception but got $other")
              case Success(_) if expectedFailure(partyId).isDefined =>
                fail("Expected a failure but got a success")
              case Success(_) => Success(succeed)
            }
        } yield result
      }

      def mkDecentralizedTx(ownerSize: Int): (SignedTransaction, Namespace) = {
        val ownersKeys = Seq.fill(ownerSize)(createSigningKey).map { case (publicKey, keyPair) =>
          (cantonSigningPublicKey(publicKey.value), keyPair)
        }
        val namespaceOwners = ownersKeys.map(_._1.fingerprint).toSet.map(Namespace(_))
        val decentralizedNamespace =
          DecentralizedNamespaceDefinition.computeNamespace(namespaceOwners)
        val decentralizedTx = TopologyTransaction(
          Replace,
          PositiveInt.one,
          DecentralizedNamespaceDefinition.tryCreate(
            decentralizedNamespace = decentralizedNamespace,
            threshold = PositiveInt.one,
            owners = NonEmpty.from(namespaceOwners).value,
          ),
          testedProtocolVersion,
        )
        val signatures = ownersKeys.map { case (publicKey, keyPair) =>
          sign(keyPair, decentralizedTx.getCryptographicEvidence, publicKey.fingerprint)
        }
        (
          SignedTransaction(
            decentralizedTx.toByteString,
            signatures,
          ),
          decentralizedNamespace,
        )
      }

      "fail if missing synchronizerId" in {
        testAllocateExternalPartyValidation(
          _.synchronizer.modify(_ => ""),
          _ => Some("The submitted command is missing a mandatory field: synchronizer"),
        )
      }

      "fail if missing a party to participant" in {
        val (decentralizedNamespaceTx, _) = mkDecentralizedTx(1)

        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.filterNot(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToParticipant]
                .nonEmpty
            ) // Add another type of tx, otherwise it fails with "empty transaction field"
              .appended(decentralizedNamespaceTx)
          ),
          _ => Some("One transaction of type PartyToParticipant must be provided, got 0"),
        )
      }

      "fail on invalid key usages for party namespace key" in {
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.map(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToParticipant]
                .map(
                  _.toProtoV30.mapping.value.update(
                    _.partyToParticipant.partySigningKeys.keys.modify(
                      _.map(_.copy(usage = Seq(v30.SigningKeyUsage.SIGNING_KEY_USAGE_PROTOCOL)))
                    )
                  )
                )
                .map(TopologyMapping.fromProtoV30(_).value)
                .map(
                  TopologyTransaction(
                    TopologyChangeOp.Replace,
                    PositiveInt.one,
                    _,
                    testedProtocolVersion,
                  )
                )
                .map { updatedTx =>
                  SignedTransaction(updatedTx.toByteString, tx.signatures)
                }
                .getOrElse(tx)
            )
          ),
          _ => Some("Missing Namespace and Protocol usage on the party namespace key"),
        )
      }

      "fail on invalid key usages for protocol keys" in {
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.map(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToParticipant]
                .map(
                  _.toProtoV30.mapping.value.update(
                    _.partyToParticipant.partySigningKeys.keys.modify(
                      _.map(_.copy(usage = Seq(v30.SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE)))
                    )
                  )
                )
                .map(TopologyMapping.fromProtoV30(_).value)
                .map(
                  TopologyTransaction(
                    TopologyChangeOp.Replace,
                    PositiveInt.one,
                    _,
                    testedProtocolVersion,
                  )
                )
                .map { updatedTx =>
                  SignedTransaction(updatedTx.toByteString, tx.signatures)
                }
                .getOrElse(tx)
            )
          ),
          _ => Some("Missing Protocol usage on signing keys"),
        )
      }

      "fail on empty protocol keys" in {
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            _.map(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[PartyToParticipant]
                .map(
                  _.toProtoV30.mapping.value.update(
                    _.partyToParticipant.optionalPartySigningKeys.set(None)
                  )
                )
                .map(TopologyMapping.fromProtoV30(_).value)
                .map(
                  TopologyTransaction(
                    TopologyChangeOp.Replace,
                    PositiveInt.one,
                    _,
                    testedProtocolVersion,
                  )
                )
                .map { updatedTx =>
                  SignedTransaction(updatedTx.toByteString, tx.signatures)
                }
                .getOrElse(tx)
            )
          ),
          _ =>
            Some(
              "Party signing keys must be supplied either in the PartyToParticipant or in a PartyToKeyMapping transaction. Not in both."
            ),
        )
      }

      "fail on protocol keys in both PTK and PTP" in {
        val (publicKey, keyPair) = createSigningKey
        val cantonPublicKey = cantonSigningPublicKey(publicKey.value)

        def mkPtkTransaction(partyId: PartyId) = TopologyTransaction(
          Replace,
          PositiveInt.one,
          PartyToKeyMapping.tryCreate(
            partyId = partyId,
            threshold = PositiveInt.one,
            signingKeys = NonEmpty.mk(Seq, cantonPublicKey),
          ),
          testedProtocolVersion,
        )
        def mkSignature(
            ptkTransaction: TopologyTransaction[TopologyChangeOp.Replace, PartyToKeyMapping],
            partyId: PartyId,
        ) = sign(
          keyPair,
          ptkTransaction.hash.hash.getCryptographicEvidence,
          partyId.fingerprint,
        )

        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify { transactions =>
            val partyId = transactions
              .flatMap(tx =>
                TopologyTransaction
                  .fromByteString(testedProtocolVersion, tx.transaction)
                  .value
                  .selectMapping[PartyToParticipant]
              )
              .loneElement
              .mapping
              .partyId

            val ptk = mkPtkTransaction(partyId)
            val signature = mkSignature(ptk, partyId)

            transactions :+ SignedTransaction(ptk.toByteString, Seq(signature))
          },
          _ =>
            Some(
              "Party signing keys must be supplied either in the PartyToParticipant or in a PartyToKeyMapping transaction. Not in both."
            ),
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

      "refuse mismatching ptk namespace and ptp namespace" in {
        val (publicKey, keyPair) = createSigningKey
        val cantonPublicKey = cantonSigningPublicKey(publicKey.value)
        val ptkPartyId = PartyId.tryCreate("alice", cantonPublicKey.fingerprint)

        val ptkTransaction = TopologyTransaction(
          Replace,
          PositiveInt.one,
          PartyToKeyMapping.tryCreate(
            partyId = ptkPartyId,
            threshold = PositiveInt.one,
            signingKeys = NonEmpty.mk(Seq, cantonPublicKey),
          ),
          testedProtocolVersion,
        )
        val signature = sign(
          keyPair,
          ptkTransaction.hash.hash.getCryptographicEvidence,
          ptkPartyId.fingerprint,
        )
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(transactions =>
            transactions :+ SignedTransaction(
              ptkTransaction.toByteString,
              Seq(signature),
            )
          ),
          partyId =>
            Some(
              s"The PartyToKeyMapping namespace (${ptkPartyId.namespace}) does not match the PartyToParticipant namespace (${partyId.namespace})"
            ),
        )
      }

      "refuse mismatching party namespace and p2p namespace" in {
        val (publicKey, keyPair) = createSigningKey
        val cantonPublicKey = cantonSigningPublicKey(publicKey.value)
        val nsdPartyId = PartyId.tryCreate("alice", cantonPublicKey.fingerprint)

        val nsdTransaction = TopologyTransaction(
          Replace,
          PositiveInt.one,
          NamespaceDelegation.tryCreate(
            namespace = nsdPartyId.namespace,
            target = cantonPublicKey,
            restriction = DelegationRestriction.CanSignAllMappings,
          ),
          testedProtocolVersion,
        )
        val signature = sign(
          keyPair,
          nsdTransaction.hash.hash.getCryptographicEvidence,
          nsdPartyId.fingerprint,
        )
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(transactions =>
            transactions :+ SignedTransaction(
              nsdTransaction.toByteString,
              Seq(signature),
            )
          ),
          partyId =>
            Some(
              s"The Party namespace (${nsdPartyId.namespace}) does not match the PartyToParticipant namespace (${partyId.namespace})"
            ),
        )
      }

      "refuse mismatching decentralized namespace and p2p namespace" in {
        val (decentralizedNamespaceTx, namespace) = mkDecentralizedTx(1)
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            // Remove the Namespace delegation generated by default
            _.filterNot(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[NamespaceDelegation]
                .isDefined
            )
              // replace it with a decentralized namespace
              .appended(decentralizedNamespaceTx)
          ),
          partyId =>
            Some(
              s"The Party namespace ($namespace) does not match the PartyToParticipant namespace (${partyId.namespace})"
            ),
        )
      }

      "refuse decentralized namespace with too many owners" in {
        val max = ExternalPartyOnboardingDetails.maxDecentralizedOwnersSize
        val (decentralizedNamespaceTx, namespace) = mkDecentralizedTx(max.increment.value)
        testAllocateExternalPartyValidation(
          _.onboardingTransactions.modify(
            // Remove the Namespace delegation generated by default
            _.filterNot(tx =>
              TopologyTransaction
                .fromByteString(testedProtocolVersion, tx.transaction)
                .value
                .selectMapping[NamespaceDelegation]
                .isDefined
            )
              // replace it with a decentralized namespace with too many owners
              .appended(decentralizedNamespaceTx)
          ),
          partyId =>
            Some(
              s"The Party namespace ($namespace) does not match the PartyToParticipant namespace (${partyId.namespace})"
            ),
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
      val (
        mockIdentityProviderExists,
        mockIndexPartyManagementService,
        mockUserManagementStore,
        mockPartyRecordStore,
      ) = mockedServices()
      val partyAllocationTracker = makePartyAllocationTracker(loggerFactory)
      val apiService = ApiPartyManagementService.createApiService(
        mockIndexPartyManagementService,
        mockUserManagementStore,
        mockIdentityProviderExists,
        partiesPageSize,
        NonNegativeInt.tryCreate(0),
        mockPartyRecordStore,
        TestPartySyncService(testTelemetrySetup.tracer),
        oneHour,
        createSubmissionId,
        new DefaultOpenTelemetry(OpenTelemetrySdk.builder().build()),
        partyAllocationTracker,
        loggerFactory = loggerFactory,
      )

      loggerFactory.suppress(
        ApiPartyManagementServiceSuppressionRule
      ) {

        val span = testTelemetrySetup.anEmptySpan()
        val scope = span.makeCurrent()

        // Kick the interaction off
        val future = apiService
          .allocateParty(AllocatePartyRequest("aParty", None, "", "", ""))
          .thereafter { _ =>
            scope.close()
            span.end()
          }

        // Allow the tracker to complete
        partyAllocationTracker.onStreamItem(
          PartyAllocation.Completed(
            aPartyAllocationTracker,
            IndexerPartyDetails(aParty, isLocal = true),
          )
        )

        // Wait for tracker to complete
        future.futureValue

        testTelemetrySetup.reportedSpanAttributes should contain(anUserIdSpanAttribute)
      }
    }

    "close while allocating party" in {
      val (
        mockIdentityProviderExists,
        mockIndexPartyManagementService,
        mockUserManagementStore,
        mockPartyRecordStore,
      ) = mockedServices()
      val partyAllocationTracker = makePartyAllocationTracker(loggerFactory)
      val apiPartyManagementService = ApiPartyManagementService.createApiService(
        mockIndexPartyManagementService,
        mockUserManagementStore,
        mockIdentityProviderExists,
        partiesPageSize,
        NonNegativeInt.tryCreate(0),
        mockPartyRecordStore,
        TestPartySyncService(testTelemetrySetup.tracer),
        oneHour,
        createSubmissionId,
        NoOpTelemetry,
        partyAllocationTracker,
        loggerFactory = loggerFactory,
      )

      loggerFactory.suppress(
        ApiPartyManagementServiceSuppressionRule
      ) {
        // Kick the interaction off
        val future =
          apiPartyManagementService.allocateParty(AllocatePartyRequest("aParty", None, "", "", ""))

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
                expectedMessage = "ABORTED_DUE_TO_SHUTDOWN(1,0): request aborted due to shutdown",
                expectedDetails = List(
                  ErrorDetails.ErrorInfoDetail(
                    "ABORTED_DUE_TO_SHUTDOWN",
                    Map(
                      "parties" -> "['aParty']",
                      "category" -> "1",
                      "test" -> s"'${getClass.getSimpleName}'",
                    ),
                  ),
                  RetryInfoDetail(10.seconds),
                ),
                verifyEmptyStackTrace = true,
              )
              Success(succeed)
            case Failure(other) =>
              fail("Unexpected error", other)
          }
      }
    }

    "generate-external-topology" when {
      def getMappingFromResponse(response: GenerateExternalPartyTopologyResponse) = {
        response.topologyTransactions should have length 1
        val txs = response.topologyTransactions.toList
          .traverse(tx =>
            TopologyTransaction
              .fromByteString(ProtocolVersion.latest, tx)
          )
          .valueOrFail("unable to parse topology txs")
          .map(_.mapping)
        txs match {
          case (pp: PartyToParticipant) :: Nil => pp
          case other => fail("unexpected mappings: " + other)
        }
      }
      "correctly pass through all fields" in {
        val (publicKey, _) = createSigningKey
        val syncId = DefaultTestIdentities.synchronizerId

        for {
          response <- apiService.generateExternalPartyTopology(
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
          val pp = getMappingFromResponse(response)
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
        val (publicKey, _) = createSigningKey
        val syncId = DefaultTestIdentities.synchronizerId

        for {
          response <- apiService.generateExternalPartyTopology(
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
          val pp = getMappingFromResponse(response)
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
        val (publicKey, _) = createSigningKey
        val syncId = DefaultTestIdentities.synchronizerId

        for {
          response <- apiService
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
        val (publicKey, _) = createSigningKey
        for {
          response1 <- apiService
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
          response2 <- apiService
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
        val (publicKey, _) = createSigningKey
        val syncId = DefaultTestIdentities.synchronizerId
        for {
          response1 <- apiService
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
          response2 <- apiService
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
        val syncId = DefaultTestIdentities.synchronizerId
        for {
          response1 <- apiService
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
        val (publicKey, _) = createSigningKey
        val syncId = DefaultTestIdentities.synchronizerId
        for {
          response1 <- apiService
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
          response2 <- apiService
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
          response3 <- apiService
            .generateExternalPartyTopology(
              GenerateExternalPartyTopologyRequest(
                synchronizer = syncId.toProtoPrimitive,
                partyHint = "alice",
                publicKey = publicKey,
                localParticipantObservationOnly = false,
                otherConfirmingParticipantUids =
                  Seq(DefaultTestIdentities.participant2.uid.toProtoPrimitive),
                confirmationThreshold = 1,
                observingParticipantUids =
                  Seq(DefaultTestIdentities.participant2.uid.toProtoPrimitive),
              )
            )
            .failed
        } yield {
          response1.getMessage should include(
            s"This participant node ($participantId) is also listed in 'otherConfirmingParticipantUids'." +
              s" By sending the request to this node, it is de facto a hosting node" +
              s" and must not be listed in 'otherConfirmingParticipantUids'."
          )
          response2.getMessage should include(
            "This participant node (PAR::participant1::participant1...) is also listed in 'observingParticipantUids'." +
              " By sending the request to this node, it is de facto a hosting node" +
              " and must not be listed in 'observingParticipantUids'."
          )
          response3.getMessage should include(
            "The following participant IDs are referenced multiple times in the request:" +
              " participant2::participant2.... " +
              "Please ensure all IDs are referenced only once across" +
              " 'otherConfirmingParticipantUids' and 'observingParticipantUids' fields."
          )
        }
      }

    }

  }

  private def makePartyAllocationTracker(
      loggerFactory: NamedLoggerFactory
  ): PartyAllocation.Tracker =
    StreamTracker.withTimer[PartyAllocation.TrackerKey, PartyAllocation.Completed](
      timer = new java.util.Timer("test-timer"),
      itemKey = (_ => Some(aPartyAllocationTracker)),
      inFlightCounter = InFlight.Limited(100, mock[com.daml.metrics.api.MetricHandle.Counter]),
      loggerFactory,
    )

  private def mockedServices(): (
      IdentityProviderExists,
      IndexPartyManagementService,
      UserManagementStore,
      PartyRecordStore,
  ) = {
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

    val mockUserManagementStore = mock[UserManagementStore]

    (
      mockIdentityProviderExists,
      mockIndexPartyManagementService,
      mockUserManagementStore,
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
        partyId: PartyId,
        submissionId: Ref.SubmissionId,
        synchronizerIdO: Option[SynchronizerId],
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
