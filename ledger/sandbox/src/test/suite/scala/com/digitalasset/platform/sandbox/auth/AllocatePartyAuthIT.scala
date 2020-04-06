// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

import com.daml.ledger.api.v1.admin.party_management_service._

import scala.concurrent.Future

final class AllocatePartyAuthIT extends AdminServiceCallAuthTests {

  override def serviceCallName: String = "PartyManagementService#AllocateParty"

  override def serviceCallWithToken(token: Option[String]): Future[Any] =
    stub(PartyManagementServiceGrpc.stub(channel), token).allocateParty(AllocatePartyRequest())

}
