// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import org.scalatest.flatspec.AsyncFlatSpec

// We factorize the 3 test to spin Canton only once.

class CodegenTest extends AsyncFlatSpec with LedgerTest with InterfacesTest with StakeholdersTest
