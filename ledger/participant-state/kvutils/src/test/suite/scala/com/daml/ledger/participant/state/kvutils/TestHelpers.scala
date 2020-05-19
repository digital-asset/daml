// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration
import java.util.UUID

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, TimeModel}
import com.daml.lf.archive.Decode
import com.daml.lf.archive.testing.Encode
import com.daml.lf.data.Ref.{IdString, QualifiedName}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.value.Value
import com.daml.daml_lf_dev.DamlLf
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
            choice Consume (self) (x: Unit) : Unit by Cons @Party [Simple:SimpleTemplate {owner} this] (Nil @Party) to upure @Unit ()
          },
          key @Party (Simple:SimpleTemplate {owner} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party))
        } ;
      }
    """

  def archiveWithContractData(additionalContractDataType: String): DamlLf.Archive = {
    val metadata = if (LanguageVersion.ordering
        .gteq(defaultParserParameters.languageVersion, LanguageVersion.Features.packageMetadata)) {
      Some(
        Ast.PackageMetadata(
          Ref.PackageName.assertFromString("kvutils-tests"),
          Ref.PackageVersion.assertFromString("1.0.0")))
    } else None
    val pkg = damlPackageWithContractData(additionalContractDataType).copy(metadata = metadata)

    Encode.encodeArchive(
      defaultParserParameters.defaultPackageId -> pkg,
      defaultParserParameters.languageVersion)
  }

  def packageIdWithContractData(additionalContractDataType: String): IdString.PackageId =
    Ref.PackageId.assertFromString(archiveWithContractData(additionalContractDataType).getHash)

  def typeConstructorId(ty: String, typeConstructor: String): Ref.Identifier =
    Ref.Identifier(packageIdWithContractData(ty), QualifiedName.assertFromString(typeConstructor))

  def typeConstructorId(ty: String): Ref.Identifier = typeConstructorId(ty, ty)

  def name(value: String): Ref.Name = Ref.Name.assertFromString(value)

  def party(value: String): Ref.Party = Ref.Party.assertFromString(value)

  def decodedPackageWithContractData(additionalContractDataType: String): Ast.Package =
    Decode.decodeArchive(archiveWithContractData(additionalContractDataType))._2

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
      additionalContractValue: Value[Value.AbsoluteContractId]): Value[Value.AbsoluteContractId] =
    Value.ValueRecord(
      Some(templateIdWith(additionalContractDataType)),
      ImmArray(
        Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(
          Ref.Party.assertFromString(owner)),
        Some(Ref.Name.assertFromString("observer")) -> Value.ValueParty(
          Ref.Party.assertFromString(observer)),
        Some(Ref.Name.assertFromString("contractData")) -> additionalContractValue
      )
    )

  def templateIdWith(additionalContractDataType: String): Ref.Identifier =
    Ref.Identifier(
      packageIdWithContractData(additionalContractDataType),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString("Simple"),
        Ref.DottedName.assertFromString("SimpleTemplate")
      )
    )

  val theRecordTime: Timestamp = Timestamp.Epoch
  val theDefaultConfig = Configuration(
    generation = 0,
    timeModel = TimeModel.reasonableDefault,
    maxDeduplicationTime = Duration.ofDays(1),
  )

  def mkEntryId(n: Int): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(n.toString))
      .build

  def mkParticipantId(n: Int): ParticipantId =
    Ref.ParticipantId.assertFromString(s"participant-$n")

  def randomLedgerString: Ref.LedgerString =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)

}
