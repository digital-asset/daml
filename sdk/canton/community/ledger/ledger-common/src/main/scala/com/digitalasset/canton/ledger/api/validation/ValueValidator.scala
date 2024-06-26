// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.{ContextualizedErrorLogger, NoLogging}
import com.daml.ledger.api.v2.value.Value.Sum
import com.daml.ledger.api.v2.value as api
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.value.Value.{ContractId, ValueUnit}
import com.digitalasset.daml.lf.value.Value as Lf
import io.grpc.StatusRuntimeException
import scalaz.std.either.*
import scalaz.syntax.bifunctor.*

abstract class ValueValidator {

  import ValidationErrors.*

  protected def validateNumeric(s: String): Option[Numeric]

  private[validation] def validateRecordFields(
      recordFields: Seq[api.RecordField]
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ImmArray[(Option[Ref.Name], domain.Value)]] =
    recordFields
      .foldLeft[Either[StatusRuntimeException, BackStack[(Option[Ref.Name], domain.Value)]]](
        Right(BackStack.empty)
      )((acc, rf) => {
        for {
          fields <- acc
          v <- requirePresence(rf.value, "value")
          value <- validateValue(v)
          label <- if (rf.label.isEmpty) Right(None) else requireIdentifier(rf.label).map(Some(_))
        } yield fields :+ label -> value
      })
      .map(_.toImmArray)

  def validateRecord(rec: api.Record)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Lf.ValueRecord] =
    for {
      recId <- validateOptionalIdentifier(rec.recordId)
      fields <- validateRecordFields(rec.fields)
    } yield Lf.ValueRecord(recId, fields)

  def validateValue(v0: api.Value)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.Value] = v0.sum match {
    case Sum.ContractId(cId) =>
      ContractId
        .fromString(cId)
        .bimap(invalidArgument, Lf.ValueContractId(_))
    case Sum.Numeric(value) =>
      validateNumeric(value) match {
        case Some(numeric) =>
          Right(Lf.ValueNumeric(numeric))
        case None =>
          Left(invalidArgument(s"""Could not read Numeric string "$value""""))
      }
    case Sum.Party(party) =>
      Ref.Party
        .fromString(party)
        .left
        .map(invalidArgument)
        .map(Lf.ValueParty)
    case Sum.Bool(b) => Right(Lf.ValueBool(b))
    case Sum.Timestamp(micros) =>
      Time.Timestamp
        .fromLong(micros)
        .left
        .map(invalidArgument)
        .map(Lf.ValueTimestamp)
    case Sum.Date(days) =>
      Time.Date
        .fromDaysSinceEpoch(days)
        .left
        .map(invalidArgument)
        .map(Lf.ValueDate)
    case Sum.Text(text) => Right(Lf.ValueText(text))
    case Sum.Int64(value) => Right(Lf.ValueInt64(value))
    case Sum.Record(rec) =>
      validateRecord(rec)
    case Sum.Variant(api.Variant(variantId, constructor, value)) =>
      for {
        validatedVariantId <- validateOptionalIdentifier(variantId)
        validatedConstructor <- requireName(constructor, "constructor")
        v <- requirePresence(value, "value")
        validatedValue <- validateValue(v)
      } yield Lf.ValueVariant(validatedVariantId, validatedConstructor, validatedValue)
    case Sum.Enum(api.Enum(enumId, value)) =>
      for {
        validatedEnumId <- validateOptionalIdentifier(enumId)
        validatedValue <- requireName(value, "value")
      } yield Lf.ValueEnum(validatedEnumId, validatedValue)
    case Sum.List(api.List(elems)) =>
      elems
        .foldLeft[Either[StatusRuntimeException, BackStack[domain.Value]]](Right(BackStack.empty))(
          (valuesE, v) =>
            for {
              values <- valuesE
              validatedValue <- validateValue(v)
            } yield values :+ validatedValue
        )
        .map(elements => Lf.ValueList(elements.toFrontStack))
    case _: Sum.Unit => Right(ValueUnit)
    case Sum.Optional(o) =>
      o.value.fold[Either[StatusRuntimeException, domain.Value]](Right(Lf.ValueNone))(
        validateValue(_).map(v => Lf.ValueOptional(Some(v)))
      )
    case Sum.TextMap(textMap0) =>
      val map = textMap0.entries
        .foldLeft[Either[StatusRuntimeException, FrontStack[(String, domain.Value)]]](
          Right(FrontStack.empty)
        ) { case (acc, api.TextMap.Entry(key, value0)) =>
          for {
            tail <- acc
            v <- requirePresence(value0, "value")
            validatedValue <- validateValue(v)
          } yield (key -> validatedValue) +: tail
        }
      for {
        entries <- map
        map <- SortedLookupList
          .fromImmArray(entries.toImmArray)
          .left
          .map(invalidArgument)
      } yield Lf.ValueTextMap(map)

    case Sum.GenMap(genMap0) =>
      val genMap = genMap0.entries
        .foldLeft[Either[StatusRuntimeException, BackStack[(domain.Value, domain.Value)]]](
          Right(BackStack.empty)
        ) { case (acc, api.GenMap.Entry(key0, value0)) =>
          for {
            stack <- acc
            key <- requirePresence(key0, "key")
            value <- requirePresence(value0, "value")
            validatedKey <- validateValue(key)
            validatedValue <- validateValue(value)
          } yield stack :+ (validatedKey -> validatedValue)
        }
      genMap.map(entries => Lf.ValueGenMap(entries.toImmArray))

    case Sum.Empty => Left(missingField("value"))
  }

  private[validation] def validateOptionalIdentifier(
      variantIdO: Option[api.Identifier]
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[Ref.Identifier]] =
    variantIdO.map(validateIdentifier(_).map(Some.apply)).getOrElse(Right(None))

  def requireIdentifier(s: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Name] =
    Ref.Name.fromString(s).left.map(invalidArgument)

  def requireNonEmptyParsedId[T](parser: String => Either[String, T])(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, T] =
    if (s.isEmpty)
      Left(missingField(fieldName))
    else
      parser(s).left.map(invalidField(fieldName, _))

  def requireName(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Name] =
    requireNonEmptyParsedId(Ref.Name.fromString)(s, fieldName)

  def requirePackageId(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.PackageId] =
    requireNonEmptyParsedId(Ref.PackageId.fromString)(s, fieldName)

  def requireDottedName(
      s: String,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.DottedName] =
    Ref.DottedName.fromString(s).left.map(invalidField(fieldName, _))

  def requirePresence[T](option: Option[T], fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, T] =
    option.fold[Either[StatusRuntimeException, T]](
      Left(missingField(fieldName))
    )(Right(_))

  def validateIdentifier(identifier: api.Identifier)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.Identifier] =
    for {
      qualifiedName <- validateTemplateQualifiedName(identifier.moduleName, identifier.entityName)
      packageId <- requirePackageId(identifier.packageId, "package_id")
    } yield Ref.Identifier(packageId, qualifiedName)

  def validateTemplateQualifiedName(moduleName: String, entityName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Ref.QualifiedName] =
    for {
      mn <- requireDottedName(moduleName, "module_name")
      en <- requireDottedName(entityName, "entity_name")
    } yield Ref.QualifiedName(mn, en)

}

// Standard version of the Validator use by the ledger API
object ValueValidator extends ValueValidator {

  private[this] val validNumericPattern =
    """[+-]?\d{1,38}(\.\d{0,37})?""".r.pattern

  protected override def validateNumeric(s: String): Option[Numeric] =
    if (validNumericPattern.matcher(s).matches())
      Numeric.fromUnscaledBigDecimal(new java.math.BigDecimal(s)).toOption
    else
      None

}

// Version of the ValueValidator that is stricter for syntax for Numeric but preserves their precision.
// Use by canton's Repair service
object StricterValueValidator extends ValueValidator {
  protected override def validateNumeric(s: String): Option[Numeric] =
    Numeric.fromString(s).toOption
}

object NoLoggingValueValidator {

  def validateRecord(rec: api.Record): Either[StatusRuntimeException, Lf.ValueRecord] =
    ValueValidator.validateRecord(rec)(NoLogging)

  def validateValue(v0: api.Value): Either[StatusRuntimeException, Lf] =
    ValueValidator.validateValue(v0)(NoLogging)

}
