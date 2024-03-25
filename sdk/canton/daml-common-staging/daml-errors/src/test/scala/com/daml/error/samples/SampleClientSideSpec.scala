// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.error.samples

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SampleClientSideSpec extends AnyFlatSpec with Matchers {

  it should "run successfully" in {
    SampleClientSide.example()
  }

}
