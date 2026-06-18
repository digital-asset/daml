// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.api.v1

import com.digitalasset.canton.crypto.kms.driver.api
import com.digitalasset.canton.driver.api.v1

trait KmsDriverFactory extends api.KmsDriverFactory with v1.DriverFactory {

  override val version: Int = 1

  override type Driver <: KmsDriver

}
