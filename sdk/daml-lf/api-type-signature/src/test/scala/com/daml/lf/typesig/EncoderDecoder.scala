// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.typesig

import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.{Numeric, Ref}
import com.daml.lf.typesig.PackageSignature.TypeDecl
import com.daml.nonempty.NonEmpty
import io.circe.Decoder.Result
import io.circe._
import io.circe.generic.semiauto._

import scala.annotation.{nowarn, unused}
import scala.collection.immutable

object EncoderDecoder {

  private def stringEncoder[T]: Encoder[T] = Encoder.encodeString.contramap[T](_.toString)
  private def stringDecoder[T](d: String => Either[String, T]): Decoder[T] =
    Decoder.decodeString.emap(d)

  implicit def immArraySeqEncoder[A](implicit e: Encoder[A]): Encoder[ImmArraySeq[A]] =
    Encoder.encodeList[A].contramap(_.toList)

  implicit def immArraySeqDecoder[A](implicit d: Decoder[A]): Decoder[ImmArraySeq[A]] =
    Decoder.decodeList[A].map(l => ImmArraySeq.from(l))

  // String encoders

  lazy implicit val packageIdEncoder: Encoder[Ref.PackageId] = stringEncoder[Ref.PackageId]
  lazy implicit val packageIdDecoder: Decoder[Ref.PackageId] = stringDecoder(
    Ref.PackageId.fromString
  )

  lazy implicit val packageNameEncoder: Encoder[Ref.PackageName] = stringEncoder[Ref.PackageName]
  lazy implicit val packageNameDecoder: Decoder[Ref.PackageName] = stringDecoder(
    Ref.PackageName.fromString
  )

  lazy implicit val refNameEncoder: Encoder[Ref.Name] = stringEncoder[Ref.Name]
  lazy implicit val refNameDecoder: Decoder[Ref.Name] = stringDecoder(Ref.Name.fromString)

  lazy implicit val identifierEncoder: Encoder[Ref.Identifier] = stringEncoder[Ref.Identifier]
  lazy implicit val identifierDecoder: Decoder[Ref.Identifier] = stringDecoder(
    Ref.Identifier.fromString
  )

  lazy implicit val scaleEncoder: Encoder[Numeric.Scale] = Encoder.encodeInt.contramap(t => t.toInt)
  lazy implicit val scaleDecoder: Decoder[Numeric.Scale] =
    Decoder.decodeInt.emap(Numeric.Scale.fromInt)

  lazy implicit val qualifiedNameEncoder: Encoder[Ref.QualifiedName] =
    stringEncoder[Ref.QualifiedName]
  lazy implicit val qualifiedNameDecoder: Decoder[Ref.QualifiedName] = stringDecoder(
    Ref.QualifiedName.fromString
  )

  lazy implicit val packageVersionEncoder: Encoder[Ref.PackageVersion] =
    stringEncoder[Ref.PackageVersion]
  lazy implicit val packageVersionDecoder: Decoder[Ref.PackageVersion] = stringDecoder(
    Ref.PackageVersion.fromString
  )

  lazy implicit val packageMetadataEncoder: Encoder[PackageMetadata] =
    deriveEncoder[PackageMetadata]
  lazy implicit val packageMetadataDecoder: Decoder[PackageMetadata] =
    deriveDecoder[PackageMetadata]

  // Key Encoders

  lazy implicit val refNameKeyEncoder: KeyEncoder[Ref.Name] =
    KeyEncoder.encodeKeyString.contramap(_.toString)
  lazy implicit val refNameKeyDecoder: KeyDecoder[Ref.Name] =
    KeyDecoder.decodeKeyString.map(Ref.Name.assertFromString)

  lazy implicit val qualifiedNameKeyEncoder: KeyEncoder[Ref.QualifiedName] =
    KeyEncoder.encodeKeyString.contramap(_.toString)
  lazy implicit val qualifiedNameKeyDecoder: KeyDecoder[Ref.QualifiedName] =
    KeyDecoder.decodeKeyString.map(Ref.QualifiedName.assertFromString)

  lazy implicit val refTypeConNameKeyEncoder: KeyEncoder[Ref.TypeConName] =
    KeyEncoder.encodeKeyString.contramap(_.toString)
  lazy implicit val refTypeConNameKeyDecoder: KeyDecoder[Ref.TypeConName] =
    KeyDecoder.decodeKeyString.map(Ref.TypeConName.assertFromString)

  private val noIdentifier = "none" // Cannot be an identifier as does not contain a colon

  implicit def optionalKeyEncoder: KeyEncoder[Option[Ref.Identifier]] =
    KeyEncoder.encodeKeyString.contramap({
      case Some(i) => i.toString
      case None => noIdentifier
    })

  implicit def optionalKeyDecoder: KeyDecoder[Option[Ref.Identifier]] =
    KeyDecoder.decodeKeyString.map {
      case `noIdentifier` => None
      case s => Some(Ref.Identifier.assertFromString(s))
    }

  // Derived encoders

  lazy implicit val typeVarEncoder: Encoder[TypeVar] = deriveEncoder[TypeVar]
  lazy implicit val typeVarDecoder: Decoder[TypeVar] = deriveDecoder[TypeVar]

//  lazy implicit val primTypeEncoder: Encoder[PrimType] = deriveEncoder[PrimType]
//  lazy implicit val primTypeDecoder: Decoder[PrimType] = deriveDecoder[PrimType]

  lazy implicit val primTypeEncoder: Encoder[PrimType] = Encoder.encodeString.contramap(_.toString)
  lazy implicit val primTypeDecoder: Decoder[PrimType] = Decoder.decodeString.emap({
    // Untyped
    case "PrimTypeBool" => Right(PrimTypeBool)
    case "PrimTypeInt64" => Right(PrimTypeInt64)
    case "PrimTypeText" => Right(PrimTypeText)
    case "PrimTypeDate" => Right(PrimTypeDate)
    case "PrimTypeTimestamp" => Right(PrimTypeTimestamp)
    case "PrimTypeParty" => Right(PrimTypeParty)
    case "PrimTypeUnit" => Right(PrimTypeUnit)
    // Typed
    case "PrimTypeContractId" => Right(PrimTypeContractId)
    case "PrimTypeList" => Right(PrimTypeList)
    case "PrimTypeOptional" => Right(PrimTypeOptional)
    case "PrimTypeTextMap" => Right(PrimTypeTextMap)
    case "PrimTypeGenMap" => Right(PrimTypeGenMap)
    case other => Left(s"Did not expect PrimType $other")
  })

  lazy implicit val typePrimEncoder: Encoder[TypePrim] = deriveEncoder[TypePrim]
  lazy implicit val typePrimDecoder: Decoder[TypePrim] = deriveDecoder[TypePrim]

  lazy implicit val typeConNameEncoder: Encoder[TypeConName] =
    identifierEncoder.contramap(_.identifier)
  lazy implicit val typeConNameDecoder: Decoder[TypeConName] = identifierDecoder.map(TypeConName)

  lazy implicit val typeConEncoder: Encoder[TypeCon] = deriveEncoder[TypeCon]
  lazy implicit val typeConDecoder: Decoder[TypeCon] = deriveDecoder[TypeCon]

  lazy implicit val typeNumericEncoder: Encoder[TypeNumeric] = deriveEncoder[TypeNumeric]
  lazy implicit val typeNumericDecoder: Decoder[TypeNumeric] = deriveDecoder[TypeNumeric]

  lazy implicit val typeEncoder: Encoder[Type] = deriveEncoder[Type]
  lazy implicit val typeDecoder: Decoder[Type] = deriveDecoder[Type]

  implicit def dataTypeEncoder[RF, VF](implicit
      @unused e1: Encoder[RF],
      @unused e2: Encoder[VF],
  ): Encoder[DataType[RF, VF]] =
    deriveEncoder[DataType[RF, VF]]

  implicit def dataTypeDecoder[RF, VF](implicit
      @unused e1: Decoder[RF],
      @unused e2: Decoder[VF],
  ): Decoder[DataType[RF, VF]] =
    deriveDecoder[DataType[RF, VF]]

  implicit def defDataTypeEncoder[RF, VF](implicit
      @unused e1: Encoder[RF],
      @unused e2: Encoder[VF],
  ): Encoder[DefDataType[RF, VF]] =
    deriveEncoder[DefDataType[RF, VF]]

  implicit def defDataTypeDecoder[RF, VF](implicit
      @unused e1: Decoder[RF],
      @unused e2: Decoder[VF],
  ): Decoder[DefDataType[RF, VF]] =
    deriveDecoder[DefDataType[RF, VF]]

  implicit def templateChoiceEncoder[T](implicit
      @unused e: Encoder[T]
  ): Encoder[TemplateChoice[T]] =
    deriveEncoder[TemplateChoice[T]]

  implicit def templateChoiceDecoder[T](implicit
      @unused e: Decoder[T]
  ): Decoder[TemplateChoice[T]] =
    deriveDecoder[TemplateChoice[T]]

  implicit def nonEmptyEncoder[T <: immutable.Iterable[_]](implicit
      e: Encoder[T]
  ): Encoder[NonEmpty[T]] = new Encoder[NonEmpty[T]] {
    override def apply(a: NonEmpty[T]): Json = e(a.forgetNE)
  }

  @nowarn
  implicit def nonEmptyDecoder[T <: immutable.Iterable[_]](implicit
      d: Decoder[T]
  ): Decoder[NonEmpty[T]] = new Decoder[NonEmpty[T]] {
    override def apply(c: HCursor): Result[NonEmpty[T]] = d(c).flatMap {
      case t: T with immutable.Iterable[_] =>
        NonEmpty.from(t).toRight(DecodingFailure(s"Empty iterable", List.empty))
      case other => Left(DecodingFailure(s"Did not expect $other", List.empty))
    }
  }

  implicit def templateChoicesEncoder[T](implicit
      @unused e: Encoder[T]
  ): Encoder[TemplateChoices[T]] =
    deriveEncoder[TemplateChoices[T]]

  implicit def templateChoicesDecoder[T](implicit
      @unused e: Decoder[T]
  ): Decoder[TemplateChoices[T]] =
    deriveDecoder[TemplateChoices[T]]

  implicit def defTemplateEncoder[T](implicit @unused e: Encoder[T]): Encoder[DefTemplate[T]] =
    deriveEncoder[DefTemplate[T]]

  implicit def defTemplateDecoder[T](implicit @unused e: Decoder[T]): Decoder[DefTemplate[T]] =
    deriveDecoder[DefTemplate[T]]

  implicit def defInterfaceEncoder[T](implicit @unused e: Encoder[T]): Encoder[DefInterface[T]] =
    deriveEncoder[DefInterface[T]]

  implicit def defInterfaceDecoder[T](implicit @unused e: Decoder[T]): Decoder[DefInterface[T]] =
    deriveDecoder[DefInterface[T]]

  implicit def recordEncoder[T](implicit @unused e: Encoder[T]): Encoder[Record[T]] =
    deriveEncoder[Record[T]]

  implicit def recordDecoder[T](implicit @unused e: Decoder[T]): Decoder[Record[T]] =
    deriveDecoder[Record[T]]

  implicit def variantEncoder[T](implicit @unused e: Encoder[T]): Encoder[Variant[T]] =
    deriveEncoder[Variant[T]]

  implicit def variantDecoder[T](implicit @unused e: Decoder[T]): Decoder[Variant[T]] =
    deriveDecoder[Variant[T]]

  lazy implicit val typeDeclEncoder: Encoder[TypeDecl] = deriveEncoder[TypeDecl]
  lazy implicit val typeDeclDecoder: Decoder[TypeDecl] = deriveDecoder[TypeDecl]

  lazy implicit val packageSignatureEncoder: Encoder[PackageSignature] =
    deriveEncoder[PackageSignature]
  lazy implicit val packageSignatureDecoder: Decoder[PackageSignature] =
    deriveDecoder[PackageSignature]

}
