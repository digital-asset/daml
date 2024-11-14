// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.crypto.kms.driver.api.v1

import com.digitalasset.canton.crypto.kms.driver.api
import com.digitalasset.canton.driver.api.v1

trait KmsDriverFactory extends api.KmsDriverFactory with v1.DriverFactory {

  override val version: Int = 1

  override type Driver <: KmsDriver

}
