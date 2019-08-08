package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntry
import org.scalatest.{Matchers, WordSpec}
import scala.collection.JavaConverters._

class KVUtilsPackageSpec extends WordSpec with Matchers {
  import TestHelpers._

  "packages" should {

    "be able to submit simple package" in {
      val subm = KeyValueSubmission.archivesToSubmission(
        submissionId = "subm-id",
        archives = List(simpleArchive),
        sourceDescription = "description",
        participantId = "0"
      )
      val (logEntry, newState) = KeyValueCommitting.processSubmission(
        engine = null,
        entryId = mkEntryId(0),
        recordTime = theRecordTime,
        defaultConfig = theDefaultConfig,
        submission = subm,
        participantId = mkParticipantId(0),
        inputState = subm.getInputDamlStateList.asScala.map(_ -> None).toMap
      )
      logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.PACKAGE_UPLOAD_ENTRY
    }
  }

}
