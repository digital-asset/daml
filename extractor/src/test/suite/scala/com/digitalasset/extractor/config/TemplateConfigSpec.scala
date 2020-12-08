// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor.config

import com.daml.scalatest.FlatSpecCheckLaws
import com.daml.extractor.config.Generators.arbTemplateConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.scalacheck.ScalazProperties

class TemplateConfigSpec extends AnyFlatSpec with Matchers with FlatSpecCheckLaws {

  behavior of TemplateConfig.getClass.getSimpleName

  checkLaws(ScalazProperties.order.laws[TemplateConfig])
}
