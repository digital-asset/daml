// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.version.ProtocolVersion

trait ProtocolConfig {
  def devVersionSupport: Boolean
  def betaVersionSupport: Boolean
  def dontWarnOnDeprecatedPV: Boolean
  def initialProtocolVersion: ProtocolVersion
}
