// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.digitalasset.canton.config.KmsConfig
import com.digitalasset.canton.crypto.kms.Kms
import com.digitalasset.canton.crypto.kms.gcp.GcpKms

class GcpKmsCryptoTest extends KmsCryptoTest with PredefinedGcpKmsKeys {
  override protected def kmsConfig: Option[KmsConfig.Gcp] = Some(KmsConfig.Gcp.defaultTestConfig)
  override protected def supportedSchemes: Kms.SupportedSchemes = GcpKms
}
