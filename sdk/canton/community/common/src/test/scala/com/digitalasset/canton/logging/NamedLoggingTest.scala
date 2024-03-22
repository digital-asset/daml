// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging

import com.digitalasset.canton.BaseTest
import com.typesafe.scalalogging.Logger
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ListMap

class NamedLoggingTest extends AnyWordSpec with BaseTest {
  "NamedLoggerFactory" can {
    "append from root" should {
      "not add separator " in {
        NamedLoggerFactory.root.appendUnnamedKey("ignored key", "abc").name shouldBe "abc"
      }
    }
    "append from parent with name" should {
      "add a separator" in {
        NamedLoggerFactory
          .unnamedKey("ignored", "parent")
          .appendUnnamedKey("unnamed", "abc")
          .name shouldBe "parent/abc"
      }
    }
    "name simple key value pair" should {
      "add a key value separator" in {
        NamedLoggerFactory("key", "value").name shouldBe "key=value"
      }
    }
    "append structured property from parent with name" should {
      "add separators" in {
        NamedLoggerFactory("component", "abc")
          .appendUnnamedKey("ignored descendant", "child")
          .name shouldBe "component=abc/child"
      }
    }
    "append structured property from parent with property" should {
      "add second property" in {
        NamedLoggerFactory("parent", "parentId").append("component", "abc").properties shouldBe Map(
          "component" -> "abc",
          "parent" -> "parentId",
        )
      }
    }
    "append of duplicate key" should {
      "fail in regular append" in {
        assertThrows[IllegalArgumentException](
          NamedLoggerFactory("duplicate-key", "a").append("duplicate-key", "b")
        )
      }
      "fail in unnamed append" in {
        assertThrows[IllegalArgumentException](
          NamedLoggerFactory("duplicate-key", "a").appendUnnamedKey("duplicate-key", "b")
        )
      }
    }
  }

  "NamedLogging" can {
    "create logger" should {
      "use name if available" in {
        val sut = InstanceWithNamedLogging("abc")
        sut.loggerFactory.createLoggerFullName.value should endWith("NamedLogging$1:abc")
      }
      "not add name if unavailable" in {
        val sut = InstanceWithNamedLogging("")
        sut.loggerFactory.createLoggerFullName.value should endWith("NamedLogging$1")
      }
    }

    class InstanceWithNamedLogging(val loggerFactory: MockNamedLoggerFactory) extends NamedLogging {
      // eagerly create a logger instance
      private val _logger = logger
    }

    object InstanceWithNamedLogging {
      def apply(name: String) =
        new InstanceWithNamedLogging(new MockNamedLoggerFactory(name, ListMap.empty))
    }

    class MockNamedLoggerFactory(val name: String, val properties: ListMap[String, String])
        extends NamedLoggerFactory {
      @SuppressWarnings(Array("org.wartremover.warts.Var"))
      var createLoggerFullName: Option[String] = None

      override def appendUnnamedKey(key: String, value: String): NamedLoggerFactory = ???
      override def append(key: String, value: String): NamedLoggerFactory = ???
      override private[logging] def getLogger(fullName: String): Logger = {
        createLoggerFullName = Some(fullName)
        Logger(mock[org.slf4j.Logger])
      }
    }
  }
}
