// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.util.MessageRecorderTest.{Data, Data2}
import com.digitalasset.canton.{BaseTestWordSpec, HasTempDirectory}

import java.nio.file.Path

class MessageRecorderTest extends BaseTestWordSpec with HasTempDirectory {

  val testData: Seq[Data] = (0 until 3) map Data

  val recordFile: Path = tempDirectory.resolve("recorded-test-data")

  val recorder = new MessageRecorder(DefaultProcessingTimeouts.testing, loggerFactory)

  "A message recorder" can {
    "record data" in {
      recorder.startRecording(recordFile)
      testData.foreach(m => recorder.record(m))
      recorder.stopRecording()
    }

    "read recorded data" in {
      val readData = MessageRecorder.load[Data](recordFile, logger)
      readData shouldBe testData
    }

    "catch type errors" in {
      a[ClassCastException] shouldBe thrownBy { MessageRecorder.load[Data2](recordFile, logger) }
    }
  }
}

object MessageRecorderTest {
  final case class Data(i: Int)
  final case class Data2(s: String)
}
