// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import magnolify.scalacheck.auto.*
import org.scalacheck.Arbitrary

object GeneratorsError {
  implicit val damlErrorCategoryArb: Arbitrary[com.daml.error.ErrorCategory] = genArbitrary
}
