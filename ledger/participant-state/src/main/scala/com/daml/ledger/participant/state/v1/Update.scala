package com.daml.ledger.participant.state.v1

import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf.DamlLf

/** Participant state update. */
sealed trait Update {

  /** Short one-line description of what the state update is about. */
  def description: String
}

object Update {
  final case class StateInit(ledgerId: LedgerId) extends Update {
    override def description: String = s"Initialize with ledgerId=$ledgerId"
  }

  final case class Heartbeat(recordTime: Timestamp) extends Update {
    override def description: String = s"Heartbeat: $recordTime"
  }
  final case class ConfigurationChanged(newConfiguration: Configuration)
      extends Update {
    override def description: String =
      s"Configuration changed to: $newConfiguration"
  }
  final case class PartyAddedToParticipant(party: Party) extends Update {
    override def description: String = s"Add party '$party' to participant"
  }

  final case class PackageUploaded(optSubmitterInfo: Option[SubmitterInfo],
                                   archive: DamlLf.Archive)
      extends Update {
    override def description: String = s"Upload package ${archive.getHash}"
  }

  final case class TransactionAccepted(
      optSubmitterInfo: Option[SubmitterInfo],
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      transactionId: String,
      recordTime: Timestamp,
      referencedContracts: List[
        (Value.AbsoluteContractId, AbsoluteContractInst)]
  ) extends Update {
    override def description: String = s"Accept transaction $transactionId"
  }

  final case class CommandRejected(
      optSubmitterInfo: Option[SubmitterInfo],
      reason: RejectionReason,
  ) extends Update {
    override def description: String = {
      val commandId = optSubmitterInfo.map(_.commandId).getOrElse("???")
      s"Reject command $commandId: $reason"
    }
  }
}
