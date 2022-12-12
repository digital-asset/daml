// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api.opentelemetry

import com.daml.metrics.api.MetricsContext
import io.opentelemetry.api.common.AttributeKey
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.MapHasAsScala

class OpenTelemetryFactorySpec extends AnyWordSpec with Matchers {

  "merging metrics contexts" should {

    "add all labels as attributes" in {
      val mergedAttributes = AttributesHelper.multiContextAsAttributes(
        MetricsContext(
          Map(
            "key1" -> "value"
          )
        ),
        MetricsContext(
          Map(
            "key2" -> "value"
          )
        ),
      )
      mergedAttributes.asMap().asScala should contain theSameElementsAs Map(
        AttributeKey.stringKey("key1") -> "value",
        AttributeKey.stringKey("key2") -> "value",
      )
    }

    "merge contexts with duplicate label keys" in {
      val mergedAttributes = AttributesHelper.multiContextAsAttributes(
        MetricsContext(
          Map(
            "key1" -> "value1"
          )
        ),
        MetricsContext(
          Map(
            "key1" -> "value2",
            "key2" -> "value",
          )
        ),
      )
      mergedAttributes.asMap().asScala should contain theSameElementsAs Map(
        AttributeKey.stringKey("key1") -> "value2",
        AttributeKey.stringKey("key2") -> "value",
      )
    }

  }

}
