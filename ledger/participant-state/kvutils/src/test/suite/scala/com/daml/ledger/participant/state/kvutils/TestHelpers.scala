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

  def damlPackage(additionalContractDataTy: String): Ast.Package =
    p"""
      module DA.Types {
        record @serializable Tuple2 (a: *) (b: *) = { x1: a, x2: b } ;
      }

      module Simple {
       record @serializable SimpleTemplate = { owner: Party, contractData: $additionalContractDataTy } ;
       variant @serializable SimpleVariant = SV: Party ;
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

  def archive(additionalContractDataTy: String): DamlLf.Archive = {
    val damlP = damlPackage(additionalContractDataTy)
    Encode.encodeArchive(
      defaultParserParameters.defaultPackageId -> damlP,
      defaultParserParameters.languageVersion)
  }

  def packageId(additionalContractDataTy: String): IdString.PackageId = {
    val arc = archive(additionalContractDataTy)
    Ref.PackageId.assertFromString(arc.getHash)
  }

  def typeConstructorId(ty: String, tyCon: String): Ref.Identifier =
    Ref.Identifier(packageId(ty), QualifiedName.assertFromString(tyCon))

  def typeConstructorId(ty: String): Ref.Identifier = typeConstructorId(ty, ty)

  def name(v: String): Ref.Name = Ref.Name.assertFromString(v)

  def party(v: String): Ref.Party = Ref.Party.assertFromString(v)

  def decodedPackage(additionalContractDataTy: String): Ast.Package = {
    val arc = archive(additionalContractDataTy)
    Decode.decodeArchive(arc)._2
  }

  val badArchive: DamlLf.Archive =
    DamlLf.Archive.newBuilder
      .setHash("blablabla")
      .build

  val simpleConsumeChoiceid: Ref.ChoiceName =
    Ref.ChoiceName.assertFromString("Consume")

  def mkTemplateArg(owner: String, value: Value[Value.AbsoluteContractId], additionalContractDataTy: String): Value[Value.AbsoluteContractId] = {
    val tId = templateId(additionalContractDataTy)
    Value.ValueRecord(
      Some(tId),
      ImmArray(
        Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(
          Ref.Party.assertFromString(owner)),
        Some(Ref.Name.assertFromString("contractData")) -> value)
    )
  }

  def templateId(additionalContractDataTy: String): Ref.Identifier = {
    val pId = packageId(additionalContractDataTy)
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
    Ref.LedgerString.assertFromString(s"participant-$n")

  def randomLedgerString: Ref.LedgerString =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)
}
