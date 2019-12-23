// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{Files, Path}
import java.time.Clock

import com.daml.ledger.on.filesystem.posix.DeleteFiles.deleteFiles
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext}

class FileSystemLedgerReaderWriterIntegrationSpec extends ParticipantStateIntegrationSpecBase {

  var directory: Path = _

  implicit val ec: ExecutionContext = ExecutionContext.global

  override def beforeEach(): Unit = {
    directory = Files.createTempDirectory(getClass.getSimpleName)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (directory != null) {
      deleteFiles(directory)
    }
  }

  override def participantStateFactory(
      participantId: ParticipantId,
      ledgerId: LedgerString,
  ): ReadService with WriteService = {
    val readerWriter =
      Await.result(FileSystemLedgerReaderWriter(ledgerId, participantId, directory), 1.second)
    new KeyValueParticipantState(readerWriter, readerWriter)
  }

  override def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}
