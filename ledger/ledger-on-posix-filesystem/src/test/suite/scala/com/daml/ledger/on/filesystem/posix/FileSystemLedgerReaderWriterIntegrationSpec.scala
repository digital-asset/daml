// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{Files, Path}
import java.time.Clock

import com.daml.ledger.on.filesystem.posix.DeleteFiles.deleteFiles
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase.ParticipantState
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.LedgerString
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.resources.ResourceOwner

import scala.concurrent.ExecutionContext

class FileSystemLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase(
      "File system-based participant state implementation",
    ) {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  private var directory: Path = _

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
  ): ResourceOwner[ParticipantState] = {
    FileSystemLedgerReaderWriter
      .owner(ledgerId, participantId, directory)
      .map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter))
  }

  override def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())
}
