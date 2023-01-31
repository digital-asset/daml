// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.config

case class JwtAudience(enabled: Boolean, audience: Option[String]) {
  def targetAudience: Option[String] = if (enabled) {
    audience
  } else
    None

}
