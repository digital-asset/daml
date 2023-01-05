// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen.backend.java.inner

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.MDC

final class TrackLineageSpec extends AnyFlatSpec with Matchers {

  behavior of "TrackLineage.of"

  it should "work as expected in the plain case" in {
    TrackLineage.of("someEntity", "someName") {
      MDC.get("entityType") shouldEqual "someEntity"
      MDC.get("entityName") shouldEqual "someName"
    }
  }

  it should "work as expected in the nested case" in {
    TrackLineage.of("someEntity", "someName") {
      TrackLineage.of("someNestedEntity", "someNestedName") {
        MDC.get("entityType") shouldEqual "someNestedEntity"
        MDC.get("entityName") shouldEqual "someName.someNestedName"
      }
    }
  }

  it should "correctly pop items when exiting scopes" in {
    TrackLineage.of("someEntity", "someName") {
      MDC.get("entityType") shouldEqual "someEntity"
      MDC.get("entityName") shouldEqual "someName"
      TrackLineage.of("someNestedEntity", "someNestedName") {
        MDC.get("entityType") shouldEqual "someNestedEntity"
        MDC.get("entityName") shouldEqual "someName.someNestedName"
      }
      MDC.get("entityType") shouldEqual "someEntity"
      MDC.get("entityName") shouldEqual "someName"
    }
    MDC.get("entityType") shouldBe null
    MDC.get("entityName") shouldBe null
  }

  it should "correctly track lineages on a per-thread basis" in {
    val t1 = new Thread(() => {
      for (_ <- 1 to 10000) {
        TrackLineage.of("someEntity1", "someName1") {
          MDC.get("entityType") shouldEqual "someEntity1"
          MDC.get("entityName") shouldEqual "someName1"
        }
        Thread.`yield`()
        MDC.get("entityType") shouldBe null
        MDC.get("entityName") shouldBe null
      }
    })
    val t2 = new Thread(() => {
      for (_ <- 1 to 10000) {
        TrackLineage.of("someEntity2", "someName2") {
          MDC.get("entityType") shouldEqual "someEntity2"
          MDC.get("entityName") shouldEqual "someName2"
        }
        Thread.`yield`()
        MDC.get("entityType") shouldBe null
        MDC.get("entityName") shouldBe null
      }
    })
    t1.start()
    t2.start()
    t1.join()
    t2.join()
  }

  it should "work correctly in the face of exceptions" in {
    TrackLineage.of("someEntity", "someName") {
      try {
        TrackLineage.of("someNestedEntity", "someNestedName") {
          throw new RuntimeException
        }
      } catch {
        case _: Exception => // expected, do nothing
          MDC.get("entityType") shouldEqual "someEntity"
          MDC.get("entityName") shouldEqual "someName"
      }
    }
  }

}
