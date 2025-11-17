// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser
import scalapb.GeneratedMessage

class ProtobufToByteStringTest extends AnyWordSpec with Matchers {
  import ProtobufToByteStringTest.*

  private def assertErrors(result: WartTestTraverser.Result, expectedErrors: Int): Assertion = {
    result.errors.length shouldBe expectedErrors
    result.errors.foreach {
      _ should include(ProtobufToByteString.message)
    }
    succeed
  }

  "ProtobufToByteString" should {

    "detect calls to toByteString on generated protobuf messages" in {
      val result = WartTestTraverser(ProtobufToByteString) {
        val x = ??? : MyGeneratedMessage
        x.toByteString
        ()
      }
      assertErrors(result, 1)
    }

    "allow calls to toByteString on other classes" in {
      val result = WartTestTraverser(ProtobufToByteString) {
        val x = new NotAProtobufMessage
        x.toByteString
        ()
      }
      assertErrors(result, 0)
    }

    "can detect renamed calls to toByteString on generated protobuf messages (fails on Scala 2)" in {
      val result = WartTestTraverser(ProtobufToByteString) {
        val x = ??? : MyGeneratedMessage
        import x.toByteString as foo
        foo
        ()
      }
      if (ScalaVersion.isScala3) assertErrors(result, 1)
      else assertErrors(result, 0) // Limitations on Scala 2
    }
  }
}

object ProtobufToByteStringTest {
  private trait MyGeneratedMessage extends GeneratedMessage
  private class NotAProtobufMessage {
    def toByteString: ByteString = ???
  }
}
