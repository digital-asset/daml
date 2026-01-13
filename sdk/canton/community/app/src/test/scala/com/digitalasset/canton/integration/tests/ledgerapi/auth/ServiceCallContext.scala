// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.transaction_filter.{EventFormat, UpdateFormat}

final case class ServiceCallContext(
    token: Option[String] = None,
    userIdOverride: Option[String] = None,
    identityProviderId: String = "",
    eventFormat: Option[EventFormat] = None,
    updateFormat: Option[UpdateFormat] = None,
    mainActorId: String = "",
) {

  // empty userId by default -> ledger api will use access token userId in lieu of it
  def userId: String = userIdOverride.getOrElse("")
}
