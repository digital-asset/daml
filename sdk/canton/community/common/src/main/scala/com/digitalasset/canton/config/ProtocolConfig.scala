// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

trait ProtocolConfig {
  def devVersionSupport: Boolean
  def dontWarnOnDeprecatedPV: Boolean
}
