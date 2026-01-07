// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{
  SigningKeysWithThreshold,
  SigningPublicKey,
  v30 as cryptoProto,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{EnvironmentDefinition, TestConsoleEnvironment}
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyTransaction,
}
import com.google.protobuf.ByteString

import java.security.KeyPairGenerator
import scala.concurrent.Future

final class AllocateExternalPartyAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with HasExecutionContext {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  override def baseEnvironmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P2_S1M1
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition.withSetup { implicit env =>
      import env.*
      participant2.synchronizers.connect_local(sequencer1, alias = daName)
    }

  override def serviceCallName: String = "PartyManagementService#AllocateExternalParty"

  private val signingKey: SigningPublicKey = {
    val keyGen = KeyPairGenerator.getInstance("Ed25519")
    val keyPair = keyGen.generateKeyPair()
    val protoKey =
      cryptoProto.SigningPublicKey(
        format = cryptoProto.CryptoKeyFormat.CRYPTO_KEY_FORMAT_DER_X509_SUBJECT_PUBLIC_KEY_INFO,
        publicKey = ByteString.copyFrom(keyPair.getPublic.getEncoded),
        scheme = cryptoProto.SigningKeyScheme.SIGNING_KEY_SCHEME_ED25519,
        keySpec = cryptoProto.SigningKeySpec.SIGNING_KEY_SPEC_EC_CURVE25519,
        usage = Seq(
          cryptoProto.SigningKeyUsage.SIGNING_KEY_USAGE_NAMESPACE,
          cryptoProto.SigningKeyUsage.SIGNING_KEY_USAGE_PROTOCOL,
        ),
      )
    SigningPublicKey.fromProtoV30(protoKey).value
  }

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(PartyManagementServiceGrpc.stub(channel), context.token)
      .allocateExternalParty(
        AllocateExternalPartyRequest(
          synchronizer = env.synchronizer1Id.toProtoPrimitive,
          onboardingTransactions = Seq(
            AllocateExternalPartyRequest.SignedTransaction(
              TopologyTransaction(
                op = TopologyChangeOp.Replace,
                serial = PositiveInt.one,
                PartyToParticipant.tryCreate(
                  DefaultTestIdentities.party1,
                  PositiveInt.one,
                  Seq(
                    HostingParticipant(env.participant1.id, Confirmation),
                    HostingParticipant(env.participant2.id, Confirmation),
                  ),
                  Some(
                    SigningKeysWithThreshold.tryCreate(
                      NonEmpty.mk(Seq, signingKey),
                      PositiveInt.one,
                    )
                  ),
                ),
                testedProtocolVersion,
              ).toByteString,
              Seq.empty,
            )
          ),
          multiHashSignatures = Seq.empty,
          identityProviderId = context.identityProviderId,
        )
      )
}
