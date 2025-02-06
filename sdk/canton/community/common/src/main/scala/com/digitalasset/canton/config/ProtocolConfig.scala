// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

trait ProtocolConfig {
  def sessionSigningKeys: SessionSigningKeysConfig
  def alphaVersionSupport: Boolean
  def betaVersionSupport: Boolean
  def dontWarnOnDeprecatedPV: Boolean
}
