// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.daml.ledger.api.v2.interactive.interactive_submission_service as iss
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.interactive.ExternalPartyUtils
import io.scalaland.chimney.dsl.*

import scala.concurrent.{ExecutionContext, Future}

final class AllocateExternalPartyAuthIT
    extends AdminOrIDPAdminServiceCallAuthTests
    with ExternalPartyUtils
    with HasExecutionContext {

  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "PartyManagementService#AllocateExternalParty"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] = {
    val (onboardingTransactions, _) =
      generateExternalPartyOnboardingTransactions("alice", Seq(env.participant1.id))

    stub(PartyManagementServiceGrpc.stub(channel), context.token)
      .allocateExternalParty(
        AllocateExternalPartyRequest(
          synchronizerId = env.synchronizer1Id.toProtoPrimitive,
          onboardingTransactions = onboardingTransactions.transactionsWithSingleSignature.map {
            case (transaction, signatures) =>
              AllocateExternalPartyRequest.SignedTransaction(
                transaction.getCryptographicEvidence,
                signatures.map(_.toProtoV30.transformInto[iss.Signature]),
              )
          },
          multiHashSignatures = onboardingTransactions.multiTransactionSignatures.map(
            _.toProtoV30.transformInto[iss.Signature]
          ),
          identityProviderId = context.identityProviderId,
        )
      )
  }

  override implicit def externalPartyExecutionContext: ExecutionContext = parallelExecutionContext
}
