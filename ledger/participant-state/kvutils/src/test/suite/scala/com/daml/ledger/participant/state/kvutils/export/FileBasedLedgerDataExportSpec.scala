// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.export

import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.time.Instant

import com.daml.ledger.participant.state.v1
import com.google.protobuf.ByteString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class FileBasedLedgerDataExportSpec extends WordSpec with Matchers with MockitoSugar {
  "addParentChild" should {
    "add entry to correlation ID mapping" in {
      val instance = new FileBasedLedgerDataExporter(mock[DataOutputStream])
      instance.addParentChild("parent", "child")

      instance.correlationIdMapping should contain("child" -> "parent")
    }
  }

  "addToWriteSet" should {
    "append to existing data" in {
      val instance = new FileBasedLedgerDataExporter(mock[DataOutputStream])
      instance.addParentChild("parent", "child")
      instance.addToWriteSet("child", Seq(keyValuePairOf("a", "b")))
      instance.addToWriteSet("child", Seq(keyValuePairOf("c", "d")))

      instance.bufferedKeyValueDataPerCorrelationId should contain(
        "parent" ->
          Seq(keyValuePairOf("a", "b"), keyValuePairOf("c", "d")))
    }
  }

  "finishedProcessing" should {
    "remove all data such as submission info, write-set and child correlation IDs" in {
      val dataOutputStream = new DataOutputStream(new ByteArrayOutputStream())
      val instance = new FileBasedLedgerDataExporter(dataOutputStream)
      instance.addSubmission(
        ByteString.copyFromUtf8("an envelope"),
        "parent",
        Instant.now(),
        v1.ParticipantId.assertFromString("id"))
      instance.addParentChild("parent", "parent")
      instance.addToWriteSet("parent", Seq(keyValuePairOf("a", "b")))

      instance.finishedProcessing("parent")

      instance.inProgressSubmissions shouldBe empty
      instance.bufferedKeyValueDataPerCorrelationId shouldBe empty
      instance.correlationIdMapping shouldBe empty
    }
  }

  private def keyValuePairOf(key: String, value: String): (ByteString, ByteString) =
    ByteString.copyFromUtf8(key) -> ByteString.copyFromUtf8(value)
}
