// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.digitalasset.canton.config.KmsConfig

class GcpKmsCryptoTest extends KmsCryptoTest with PredefinedGcpKmsKeys {
  override val kmsConfig = Some(KmsConfig.Gcp.defaultTestConfig)
}
