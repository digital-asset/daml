// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, TimeModel}
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.archive.testing.Encode
import com.digitalasset.daml.lf.data.Ref.{IdString, QualifiedName}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.testing.parser.Implicits._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf_dev.DamlLf
import com.google.protobuf.ByteString

object TestHelpers {

  def damlPackageWithContractData(additionalContractDataType: String): Ast.Package =
    p"""
      module DA.Types {
        record @serializable Tuple2 (a: *) (b: *) = { x1: a, x2: b } ;
      }

      module Simple {
       record @serializable SimpleTemplate = { owner: Party, observer: Party, contractData: $additionalContractDataType } ;
       variant @serializable SimpleVariant = SV: Party ;
       template (this : SimpleTemplate) =  {
          precondition True,
          signatories Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party),
          observers Cons @Party [Simple:SimpleTemplate {observer} this] (Nil @Party),
          agreement "",
          choices {
            choice Consume (x: Unit) : Unit by Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party) to upure @Unit ()
          },
          key @Party (Simple:SimpleTemplate {owner} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party))
        } ;
      }
    """

  def archiveWithContractData(additionalContractDataType: String): DamlLf.Archive = {
    val damlPackage = damlPackageWithContractData(additionalContractDataType)
    Encode.encodeArchive(
      defaultParserParameters.defaultPackageId -> damlPackage,
      defaultParserParameters.languageVersion)
  }

  def packageIdWithContractData(additionalContractDataType: String): IdString.PackageId = {
    val arc = archiveWithContractData(additionalContractDataType)
    Ref.PackageId.assertFromString(arc.getHash)
  }

  def typeConstructorId(ty: String, typeConstructor: String): Ref.Identifier =
    Ref.Identifier(packageIdWithContractData(ty), QualifiedName.assertFromString(typeConstructor))

  def typeConstructorId(ty: String): Ref.Identifier = typeConstructorId(ty, ty)

  def name(v: String): Ref.Name = Ref.Name.assertFromString(v)

  def party(v: String): Ref.Party = Ref.Party.assertFromString(v)

  def decodedPackageWithContractData(additionalContractDataType: String): Ast.Package = {
    val arc = archiveWithContractData(additionalContractDataType)
    Decode.decodeArchive(arc)._2
  }

  val badArchive: DamlLf.Archive =
    DamlLf.Archive.newBuilder
      .setHash("blablabla")
      .build

  val simpleConsumeChoiceid: Ref.ChoiceName =
    Ref.ChoiceName.assertFromString("Consume")

  def mkTemplateArg(
      owner: String,
      observer: String,
      additionalContractDataType: String,
      additionalContractValue: Value[Value.AbsoluteContractId]): Value[Value.AbsoluteContractId] = {
    val tId = templateIdWith(additionalContractDataType)
    Value.ValueRecord(
      Some(tId),
      ImmArray(
        Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(
          Ref.Party.assertFromString(owner)),
        Some(Ref.Name.assertFromString("observer")) -> Value.ValueParty(
          Ref.Party.assertFromString(observer)),
        Some(Ref.Name.assertFromString("contractData")) -> additionalContractValue
      )
    )
  }

  def templateIdWith(additionalContractDataType: String): Ref.Identifier = {
    val pId = packageIdWithContractData(additionalContractDataType)
    val qualifiedName = Ref.QualifiedName(
      Ref.ModuleName.assertFromString("Simple"),
      Ref.DottedName.assertFromString("SimpleTemplate")
    )
    Ref.Identifier(
      pId,
      qualifiedName
    )
  }

  val theRecordTime: Timestamp = Timestamp.Epoch
  val theDefaultConfig = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault
  )

  def mkEntryId(n: Int): DamlLogEntryId = {
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(n.toString))
      .build
  }

  def mkParticipantId(n: Int): ParticipantId =
    Ref.ParticipantId.assertFromString(s"participant-$n")

  def randomLedgerString: Ref.LedgerString =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)
}
