package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlPartyAllocation,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.validator.TestHelper.{invalidEnvelope, makePartySubmission}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.{AsyncWordSpec, Matchers}
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future

class RawToDamlLedgerStateReaderAdapterSpec extends AsyncWordSpec with Matchers with MockitoSugar {
  "readState" should {
    "read the right key and deserialize it" in {
      val expectedKey = DefaultStateKeySerializationStrategy.serializeStateKey(aDamlStateKey())
      val expectedValue = DamlStateValue.newBuilder
        .setParty(DamlPartyAllocation.newBuilder.setDisplayName("aParty"))
        .build
      val mockReader = mock[LedgerStateReader]
      when(mockReader.read(any[Seq[LedgerStateOperations.Key]]()))
        .thenReturn(Future.successful(Seq(Some(Envelope.enclose(expectedValue)))))
      val instance =
        new RawToDamlLedgerStateReaderAdapter(mockReader, DefaultStateKeySerializationStrategy)

      instance.readState(Seq(aDamlStateKey())).map { actual =>
        verify(mockReader, times(1)).read(Seq(expectedKey))
        actual shouldBe Seq(Some(expectedValue))
      }
    }

    "throw in case of an invalid envelope returned from underlying reader" in {
      val mockReader = mock[LedgerStateReader]
      when(mockReader.read(any[Seq[LedgerStateOperations.Key]]()))
        .thenReturn(Future.successful(Seq(Some(invalidEnvelope))))
      val instance =
        new RawToDamlLedgerStateReaderAdapter(mockReader, DefaultStateKeySerializationStrategy)

      instance.readState(Seq(aDamlStateKey())).failed.map { actual =>
        actual.getLocalizedMessage should include("failed")
      }
    }

    "throw in case an enveloped value other than a DamlStateValue is returned from underlying reader" in {
      val notADamlStateValue = makePartySubmission("aParty")
      val mockReader = mock[LedgerStateReader]
      when(mockReader.read(any[Seq[LedgerStateOperations.Key]]()))
        .thenReturn(Future.successful(Seq(Some(Envelope.enclose(notADamlStateValue)))))
      val instance =
        new RawToDamlLedgerStateReaderAdapter(mockReader, DefaultStateKeySerializationStrategy)

      instance.readState(Seq(aDamlStateKey())).failed.map { actual =>
        actual.getLocalizedMessage should include("failed")
      }
    }
  }

  private def aDamlStateKey(): DamlStateKey =
    DamlStateKey.newBuilder
      .setContractId("aContractId")
      .build
}
