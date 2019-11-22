// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, TimeModelImpl}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.archive.testing.Encode
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf_dev.DamlLf
import com.google.protobuf.ByteString

object TestHelpers {

  val simplePackage: Ast.Package =
    p"""
      module Simple {
       record @serializable SimpleTemplate = { owner: Party } ;
       template (this : SimpleTemplate) =  {
          precondition True,
          signatories Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party),
          observers Nil @Party,
          agreement "",
          choices {
            choice Consume (x: Unit) : Unit by Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party) to upure @Unit ()
          },
          key @Party (Simple:SimpleTemplate {owner} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party))
        } ;
      }
    """
  val simpleArchive: DamlLf.Archive =
    Encode.encodeArchive(
      defaultParserParameters.defaultPackageId -> simplePackage,
      defaultParserParameters.languageVersion)
  val simplePackageId = Ref.PackageId.assertFromString(simpleArchive.getHash)
  val simpleDecodedPackage =
    Decode.decodeArchive(simpleArchive)._2

  val badArchive: DamlLf.Archive =
    DamlLf.Archive.newBuilder
      .setHash("blablabla")
      .build

  val simpleConsumeChoiceid: Ref.ChoiceName =
    Ref.ChoiceName.assertFromString("Consume")

  def mkSimpleTemplateArg(party: String): Value[Value.AbsoluteContractId] =
    Value.ValueRecord(
      Some(simpleTemplateId),
      ImmArray(
        Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(
          Ref.Party.assertFromString(party)))
    )

  val simpleTemplateId: Ref.Identifier =
    Ref.Identifier(
      simplePackageId,
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("Simple"),
        Ref.DottedName.assertFromString("SimpleTemplate")
      )
    )

  val theRecordTime: Timestamp = Timestamp.Epoch
  val theDefaultConfig = Configuration(
    generation = 0,
    timeModel = TimeModelImpl.reasonableDefault
  )

  def mkEntryId(n: Int): DamlLogEntryId = {
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(n.toString))
      .build
  }

  def mkParticipantId(n: Int): ParticipantId =
    Ref.LedgerString.assertFromString(s"participant-$n")

  def randomString: String = UUID.randomUUID().toString

}
