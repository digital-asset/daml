// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import org.scalatest.flatspec.AnyFlatSpec

final class StorageBackendH2Spec
    extends AnyFlatSpec
    with StorageBackendProviderH2
    with StorageBackendSuite
