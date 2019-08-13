// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.config

import com.digitalasset.daml.lf.data.FlatSpecCheckLaws
import com.digitalasset.extractor.config.Generators.arbTemplateConfig
import org.scalatest.{FlatSpec, Matchers}
import scalaz.scalacheck.ScalazProperties

class TemplateConfigSpec extends FlatSpec with Matchers with FlatSpecCheckLaws {

  behavior of TemplateConfig.getClass.getSimpleName

  checkLaws(ScalazProperties.order.laws[TemplateConfig])
}
