package com.daml.ledger.participant.state.kvutils.tools.reindexfromstate

import com.daml.ledger.on.memory.InMemoryState.MutableState
import com.daml.ledger.participant.state.kvutils
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.v1.Update.{ConfigurationChanged, PartyAddedToParticipant, PublicPackageUpload}
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, SubmissionId, Update}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp

import scala.collection.mutable.ListBuffer

class StateReindexer(state: MutableState) {
  def generateUpdates(): Iterable[Update] = {
    val result = new ListBuffer[Update]
    for ((key, value) <- state) {
      val damlStateKey = DamlStateKey.parseFrom(key.bytes)
      for {
        damlStateValue <- kvutils.Envelope.openStateValue(value).toOption
        update <- processStateItem(damlStateKey, damlStateValue)
      } {
        result.append(update)
      }
    }
    result
  }

  private def processStateItem(damlStateKey: DamlStateKey, damlStateValue: DamlStateValue): Option[Update] = {
    damlStateKey.getKeyCase match {
      case DamlStateKey.KeyCase.PACKAGE_ID =>
        Some(PublicPackageUpload(
          List(damlStateValue.getArchive),
          None,
          recordTime(),
          None
        ))

      case DamlStateKey.KeyCase.CONFIGURATION =>
        val configurationEntry = damlStateValue.getConfigurationEntry
        Configuration.decode(configurationEntry.getConfiguration).toOption.map { config =>
          ConfigurationChanged(
            recordTime(),
            SubmissionId.assertFromString(configurationEntry.getSubmissionId),
            ParticipantId.assertFromString(configurationEntry.getParticipantId),
            config,
          )
        }

      case DamlStateKey.KeyCase.PARTY =>
        val party = damlStateValue.getParty
        Some(PartyAddedToParticipant(
          Ref.Party.assertFromString(damlStateKey.getParty),
          party.getDisplayName,
          ParticipantId.assertFromString(party.getParticipantId),
          recordTime(),
          None,
        ))

      case DamlStateKey.KeyCase.CONTRACT_KEY =>
        None

      case DamlStateKey.KeyCase.CONTRACT_ID =>
        //        val contract = damlStateValue.getContractState
        //        val contractInstance = contract.getContractInstance
        //        val contractKey = contract.getContractKey
        //        // Look up contract key.
        //        val contractKeyValue =
        //        val createTransaction = TransactionAccepted(
        //          optSubmitterInfo = None,
        //          transactionMeta = ??,
        //          transaction = ??,
        //          recordTime(),
        //          divulgedContracts = List.empty,
        //          blindingInfo = None,
        //        )
        None

      case DamlStateKey.KeyCase.COMMAND_DEDUP | DamlStateKey.KeyCase.SUBMISSION_DEDUP =>
        None

      case DamlStateKey.KeyCase.KEY_NOT_SET =>
        None
    }
  }

  private def recordTime(): Timestamp = Timestamp.Epoch
}
