// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.Paths

import com.daml.ledger.on.filesystem.posix.StateKeysSpec._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlContractId,
  DamlLogEntryId,
  DamlStateKey,
}
import com.google.protobuf.ByteString
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class StateKeysSpec extends WordSpec with Matchers {
  private val root = Paths.get("this", "is", "the", "root")

  "resolving a state key" should {
    "resolve a short key" in {
      val resolved = StateKeys.resolveStateKey(root, stateKey(contractEntryId = "Hello"))

      resolved should be(
        Paths.get(
          "this",
          "is",
          "the",
          "root",
          "12090a070a0548656c6c6f",
        ),
      )
    }

    "resolve a long key in multiple chunks" in {
      val resolved = StateKeys.resolveStateKey(
        root,
        stateKey(contractEntryId =
          "It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness",
        ),
      )

      resolved should be(
        Paths.get(
          "this",
          "is",
          "the",
          "root",
          "12700a6e0a6c49742077617320746865",
          "2062657374206f662074696d65732c20",
          "6974207761732074686520776f727374",
          "206f662074696d65732c206974207761",
          "732074686520616765206f6620776973",
          "646f6d2c206974207761732074686520",
          "616765206f6620666f6f6c6973686e65",
          "7373",
        ),
      )
    }

    "does not generate paths causing collisions due to one being a prefix of the other" in {
      // this works because strings have a length prefix,
      // which means strings of different lengths cannot collide
      val aSegments = StateKeys
        .resolveStateKey(
          root,
          stateKey(contractEntryId = "It was the best of times, it was the worst"),
        )
        .iterator()
        .asScala
        .toVector
      val bSegments = StateKeys
        .resolveStateKey(
          root,
          stateKey(contractEntryId =
            "It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness",
          ),
        )
        .iterator()
        .asScala
        .toVector

      aSegments.last.toString should have length StateKeys.NameChunkSize.toLong
      aSegments.take(bSegments.length) should not be bSegments.take(aSegments.length)
    }
  }
}

object StateKeysSpec {
  private def stateKey(contractEntryId: String): DamlStateKey =
    DamlStateKey
      .newBuilder()
      .setContractId(
        DamlContractId
          .newBuilder()
          .setEntryId(
            DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8(contractEntryId)),
          ),
      )
      .build()
}
