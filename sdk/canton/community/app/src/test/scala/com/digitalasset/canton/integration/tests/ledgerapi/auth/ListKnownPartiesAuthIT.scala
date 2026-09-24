// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.admin.party_management_service.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.integration.TestConsoleEnvironment
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer

import scala.concurrent.Future

final class ListKnownPartiesAuthIT extends AdminOrIDPAdminServiceCallAuthTests {
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.H2](loggerFactory))

  override def serviceCallName: String = "PartyManagementService#ListKnownParties"

  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    stub(PartyManagementServiceGrpc.stub(channel), context.token)
      .listKnownParties(
        ListKnownPartiesRequest(
          pageToken = "",
          pageSize = 0,
          identityProviderId = context.identityProviderId,
        )
      )

}
