// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import org.scalatest.flatspec.AnyFlatSpec

final class StorageBackendSpecH2
    extends AnyFlatSpec
    with StorageBackendProviderH2
    with StorageBackendSuite
