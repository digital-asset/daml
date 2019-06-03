// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor.config

import com.digitalasset.extractor.config.Generators.arbTemplateConfig
import org.scalacheck.Properties
import org.scalatest.prop.Checkers
import org.scalatest.{FlatSpec, Matchers}
import scalaz.scalacheck.ScalazProperties

class TemplateConfigSpec extends FlatSpec with Matchers with Checkers {

  behavior of TemplateConfig.getClass.getSimpleName

  checkLaws(ScalazProperties.order.laws[TemplateConfig])

  private def checkLaws(props: Properties): Unit =
    props.properties foreach { case (s, p) => it should s in check(p) }
}
