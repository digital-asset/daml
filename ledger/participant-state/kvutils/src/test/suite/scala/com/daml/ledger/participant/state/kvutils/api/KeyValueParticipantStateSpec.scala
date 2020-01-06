package com.daml.ledger.participant.state.kvutils.api

import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class KeyValueParticipantStateSpec
    extends WordSpec
    with Matchers
    with MockitoSugar
    with AkkaBeforeAndAfterAll {
  "participant state" should {
    "close reader and writer" in {
      val reader = mock[LedgerReader]
      val writer = mock[LedgerWriter]
      val instance = new KeyValueParticipantState(reader, writer)
      instance.close()

      verify(reader, times(1)).close()
      verify(writer, times(1)).close()
    }

    "close reader and writer only once if they are the same instance" in {
      val readerWriter = mock[LedgerReader](withSettings().extraInterfaces(classOf[LedgerWriter]))
      val instance =
        new KeyValueParticipantState(readerWriter, readerWriter.asInstanceOf[LedgerWriter])
      instance.close()

      verify(readerWriter, times(1)).close()
    }
  }
}
