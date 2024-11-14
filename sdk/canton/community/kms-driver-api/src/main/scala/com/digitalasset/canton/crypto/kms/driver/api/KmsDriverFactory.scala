// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.crypto.kms.driver.api

import com.digitalasset.canton.driver.api.DriverFactory

trait KmsDriverFactory extends DriverFactory {
  override type Driver <: KmsDriver
}
