// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms.driver.api

import com.digitalasset.canton.driver.api.DriverFactory

trait KmsDriverFactory extends DriverFactory {
  override type Driver <: KmsDriver
}
