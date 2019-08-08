package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.backport.TimeModel
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.digitalasset.daml.lf.archive.testing.Encode
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml_lf.DamlLf
import com.google.protobuf.ByteString

object TestHelpers {

  val simplePackage: Ast.Package =
    p"""
      module Simple {
       record SimpleTemplate = { owner: Party } ;
       template (this : SimpleTemplate) =  {
          precondition True,
          signatories Cons @Party [owner] (Nil @Party),
          observers Nil @Unit,
          agreement "Agreement",
          choices {
            choice Consume : Unit by Cons @Party [owner] (Nil @Party) to upure @Unit ()
          }
        } ;
      }
    """
  val selfPackageId = Ref.PackageId.assertFromString("self")
  val simpleArchive: DamlLf.Archive =
    Encode.encodeArchive(selfPackageId -> simplePackage, defaultParserParameters.languageVersion)

  val theRecordTime: Timestamp = Timestamp.Epoch
  val theDefaultConfig = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault,
    authorizedParticipantId = None,
    openWorld = true,
  )

  def mkEntryId(n: Int): DamlLogEntryId = {
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(n.toString))
      .build
  }

  def mkParticipantId(n: Int): ParticipantId =
    Ref.LedgerString.assertFromString(s"participant-$n")

}
