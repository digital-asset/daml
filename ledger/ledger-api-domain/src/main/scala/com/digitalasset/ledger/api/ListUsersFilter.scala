// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api

import com.daml.ledger.api.domain.IdentityProviderId

sealed trait ListUsersFilter

object ListUsersFilter {
  final case class ByIdentityProviderId(id: IdentityProviderId.Id) extends ListUsersFilter
  final case object Wildcard extends ListUsersFilter
}
