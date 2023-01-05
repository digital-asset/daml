// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.auth

case class ServiceCallContext(
    token: Option[String] = None,
    includeApplicationId: Boolean = true,
    identityProviderId: String = "",
) {
  def applicationId(providedApplicationId: String): String =
    if (includeApplicationId) providedApplicationId else ""
}
