// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Confirmation
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyTransaction,
}

import scala.concurrent.Future

final class AllocateExternalPartyAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with HasExecutionContext {

  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "PartyManagementService#AllocateExternalParty"

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
                  Seq(HostingParticipant(env.participant1.id, Confirmation)),
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
