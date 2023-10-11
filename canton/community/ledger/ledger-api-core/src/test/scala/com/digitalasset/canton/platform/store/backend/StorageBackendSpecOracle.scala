// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import org.scalatest.flatspec.AnyFlatSpec

final class StorageBackendSpecOracle
    extends AnyFlatSpec
    with StorageBackendProviderOracle
    with StorageBackendSuite
