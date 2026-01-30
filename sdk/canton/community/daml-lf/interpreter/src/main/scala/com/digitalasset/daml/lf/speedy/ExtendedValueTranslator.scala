// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.interpretation.Error.Upgrade.TranslationFailed
import com.digitalasset.daml.lf.language.LookupError
import com.digitalasset.daml.lf.value.GenValue
import com.digitalasset.daml.lf.value.Value._

import scala.collection.immutable.ArraySeq

// Used only to translate unnormalized extended GenValues
// Does not perform any type checking or serializability checks

private[lf] final class ExtendedValueTranslator(
    pkgInterface: language.PackageInterface
) {

  @throws[TranslationFailed.Error]
  private[this] def handleLookup[X](either: Either[LookupError, X]): X = either match {
    case Right(v) => v
    case Left(error) =>
      throw TranslationFailed.LookupError(error)
  }

  private[this] def toText(str: String): Text =
    Text.fromString(str) match {
      case Right(value) => value
      case Left(err) => throw TranslationFailed.MalformedText(err)
    }

  @throws[TranslationFailed.Error]
  def unsafeTranslateCid(cid: ContractId): SValue.SContractId =
    cid match {
      case cid: ContractId.V1 if cid.suffix.isEmpty =>
        throw TranslationFailed.NonSuffixedV1ContractId(cid)
      case cid: ContractId.V2 if cid.suffix.isEmpty =>
        throw TranslationFailed.NonSuffixedV2ContractId(cid)
      case _ => SValue.SContractId(cid)
    }

  // For efficiency reasons we do not produce here the monad Result[SValue] but rather throw
  // exceptions in case of error or package missing.
  @throws[TranslationFailed.Error]
  def unsafeTranslateExtendedValue(
      value: GenValue[GenValue.Extension[SValue.SPAP]]
  ): SValue =
    value match {
      case GenValue.Blob(spap) => spap
      case GenValue.Any(ty, v) => SValue.SAny(ty, unsafeTranslateExtendedValue(v))
      case GenValue.TypeRep(ty) => SValue.STypeRep(ty)
      case ValueRecord(Some(tycon), content) =>
        val names = handleLookup(pkgInterface.lookupDataRecord(tycon)).dataRecord.fields.map(_._1)
        val namedContent = content
          .map {
            case (Some(name), v) => (name, v)
            case _ =>
              throw TranslationFailed.InvalidExtendedValue(s"Missing field labels for $tycon")
          }
          .toSeq
          .toMap
        val values = names.map(name => unsafeTranslateExtendedValue(namedContent(name)))
        SValue.SRecord(tycon, names, values.toList.to(ArraySeq))
      case ValueRecord(None, fields) =>
        throw TranslationFailed.InvalidExtendedValue(s"Missing record tycon with fields: $fields")
      case ValueVariant(Some(tycon), variant, content) =>
        val rank = handleLookup(pkgInterface.lookupVariantConstructor(tycon, variant)).rank
        SValue.SVariant(tycon, variant, rank, unsafeTranslateExtendedValue(content))
      case ValueVariant(None, _, _) =>
        throw TranslationFailed.InvalidExtendedValue("Missing variant tycon")
      case ValueList(content) => SValue.SList(content.map(unsafeTranslateExtendedValue(_)))
      case ValueOptional(content) => SValue.SOptional(content.map(unsafeTranslateExtendedValue(_)))
      case ValueTextMap(entries) =>
        if (entries.isEmpty) {
          SValue.SValue.EmptyTextMap
        } else {
          SValue.SMap(
            isTextMap = true,
            entries = entries.toImmArray.toSeq.view.map { case (k, v) =>
              SValue.SText(toText(k)) -> unsafeTranslateExtendedValue(v)
            }.toList,
          )
        }

      case ValueGenMap(entries) =>
        if (entries.isEmpty) {
          SValue.SValue.EmptyGenMap
        } else {
          // we need to evaluate sentries before calling [[SValue.SMap.fromStrictlyOrderedEntries]] because it can
          // throw RuntimeExceptions unrelated to the ordering of keys, e.g. translation errors. Therefore, sentries
          // cannot be a lazy view.
          val sentries = entries.toSeq.map { case (k, v) =>
            unsafeTranslateExtendedValue(k) -> unsafeTranslateExtendedValue(v)
          }
          try {
            SValue.SMap.fromStrictlyOrderedEntries(
              isTextMap = false,
              entries = sentries,
            )
          } catch {
            case _: RuntimeException =>
              throw TranslationFailed.InvalidExtendedValue(
                "Unexpected non-strictly ordered GenMap value"
              )
          }
        }
      case ValueEnum(Some(tycon), value) =>
        val rank = handleLookup(pkgInterface.lookupEnumConstructor(tycon, value))
        SValue.SEnum(tycon, value, rank)
      case ValueEnum(None, _) => throw TranslationFailed.InvalidExtendedValue("Missing enum tycon")
      case ValueContractId(cid) => SValue.SContractId(cid)
      case ValueInt64(n) => SValue.SInt64(n)
      case ValueNumeric(n) => SValue.SNumeric(n)
      case ValueText(t) => SValue.SText(t)
      case ValueTimestamp(timestamp) => SValue.STimestamp(timestamp)
      case ValueDate(date) => SValue.SDate(date)
      case ValueParty(party) => SValue.SParty(party)
      case ValueBool(b) => SValue.SBool(b)
      case ValueUnit => SValue.SUnit
    }

  def translateExtendedValue(
      value: GenValue[GenValue.Extension[SValue.SPAP]]
  ): Either[TranslationFailed.Error, SValue] =
    try {
      Right(unsafeTranslateExtendedValue(value))
    } catch {
      case e: TranslationFailed.Error =>
        Left(e)
    }
}
