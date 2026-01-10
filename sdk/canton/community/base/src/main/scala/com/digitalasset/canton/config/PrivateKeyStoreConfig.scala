// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Stores the configuration for a private store
  *
  * @param encryption
  *   flags that the store is encrypted with a specific encryption approach. The default value is
  *   None which means unencrypted.
  */
final case class PrivateKeyStoreConfig(
    encryption: Option[EncryptedPrivateStoreConfig] = None
)
