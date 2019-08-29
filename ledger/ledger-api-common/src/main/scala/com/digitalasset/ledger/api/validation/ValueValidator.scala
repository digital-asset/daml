// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.validation

import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ValueUnit}
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.v1.value.Value.Sum
import com.digitalasset.ledger.api.v1.value.{
  Identifier,
  RecordField,
  Value,
  Enum => ApiEnum,
  List => ApiList,
  Map => ApiMap,
  Record => ApiRecord,
  Variant => ApiVariant
}
import com.digitalasset.daml.lf.value.{Value => Lf}
import com.digitalasset.platform.server.api.validation.ErrorFactories._
import com.digitalasset.platform.server.api.validation.FieldValidations.{requirePresence, _}
import io.grpc.StatusRuntimeException

object ValueValidator {

  private[validation] def validateRecordFields(recordFields: Seq[RecordField])
    : Either[StatusRuntimeException, ImmArray[(Option[Ref.Name], domain.Value)]] =
    recordFields
      .foldLeft[Either[StatusRuntimeException, BackStack[(Option[Ref.Name], domain.Value)]]](
        Right(BackStack.empty))((acc, rf) => {
        for {
          fields <- acc
          v <- requirePresence(rf.value, "value")
          value <- validateValue(v)
          label <- if (rf.label.isEmpty) Right(None) else requireIdentifier(rf.label).map(Some(_))
        } yield fields :+ label -> value
      })
      .map(_.toImmArray)

  def validateRecord(
      rec: ApiRecord): Either[StatusRuntimeException, Lf.ValueRecord[AbsoluteContractId]] =
    for {
      recId <- validateOptionalIdentifier(rec.recordId)
      fields <- validateRecordFields(rec.fields)
    } yield Lf.ValueRecord(recId, fields)

  private val validNumericString =
    """[+-]?\d{1,38}(\.\d{0,38})?""".r.pattern

  def validateValue(value: Value): Either[StatusRuntimeException, domain.Value] = value.sum match {
    case Sum.ContractId(cId) =>
      Ref.ContractIdString
        .fromString(cId)
        .left
        .map(invalidArgument)
        .map(coid => Lf.ValueContractId(Lf.AbsoluteContractId(coid)))
    case Sum.Numeric(value) =>
      def err = invalidArgument(s"""Could not read Numeric string "$value"""")
      if (validNumericString.matcher(value).matches())
        Numeric
          .fromUnscaledBigDecimal(new java.math.BigDecimal(value))
          .left map (_ => err) map Lf.ValueNumeric
      else
        Left(err)

    case Sum.Party(party) =>
      Ref.Party.fromString(party).left.map(invalidArgument).map(Lf.ValueParty)
    case Sum.Bool(b) => Right(Lf.ValueBool(b))
    case Sum.Timestamp(micros) =>
      Time.Timestamp.fromLong(micros).left.map(invalidArgument).map(Lf.ValueTimestamp)
    case Sum.Date(days) =>
      Time.Date.fromDaysSinceEpoch(days).left.map(invalidArgument).map(Lf.ValueDate)
    case Sum.Text(text) => Right(Lf.ValueText(text))
    case Sum.Int64(value) => Right(Lf.ValueInt64(value))
    case Sum.Record(rec) =>
      validateRecord(rec)
    case Sum.Variant(ApiVariant(variantId, constructor, value)) =>
      for {
        validatedVariantId <- validateOptionalIdentifier(variantId)
        validatedConstructor <- requireName(constructor, "constructor")
        v <- requirePresence(value, "value")
        validatedValue <- validateValue(v)
      } yield Lf.ValueVariant(validatedVariantId, validatedConstructor, validatedValue)
    case Sum.Enum(ApiEnum(enumId, value)) =>
      for {
        validatedEnumId <- validateOptionalIdentifier(enumId)
        validatedValue <- requireName(value, "value")
      } yield Lf.ValueEnum(validatedEnumId, validatedValue)
    case Sum.List(ApiList(elems)) =>
      elems
        .foldLeft[Either[StatusRuntimeException, BackStack[domain.Value]]](Right(BackStack.empty))(
          (valuesE, v) =>
            for {
              values <- valuesE
              validatedValue <- validateValue(v)
            } yield values :+ validatedValue)
        .map(elements => Lf.ValueList(FrontStack(elements.toImmArray)))
    case _: Sum.Unit => Right(ValueUnit)
    case Sum.Optional(o) =>
      o.value.fold[Either[StatusRuntimeException, domain.Value]](Right(Lf.ValueOptional(None)))(
        validateValue(_).map(v => Lf.ValueOptional(Some(v))))
    case Sum.Map(m) =>
      val entries = m.entries
        .foldLeft[Either[StatusRuntimeException, FrontStack[(String, domain.Value)]]](
          Right(FrontStack.empty)) {
          case (acc, ApiMap.Entry(key, value0)) =>
            for {
              tail <- acc
              v <- requirePresence(value0, "value")
              validatedValue <- validateValue(v)
            } yield (key -> validatedValue) +: tail
        }

      for {
        list <- entries
        map <- SortedLookupList.fromImmArray(list.toImmArray).left.map(invalidArgument)
      } yield Lf.ValueMap(map)

    case Sum.Empty => Left(missingField("value"))
  }

  private[validation] def validateOptionalIdentifier(
      variantIdO: Option[Identifier]): Either[StatusRuntimeException, Option[Ref.Identifier]] =
    variantIdO.map(validateIdentifier(_).map(Some.apply)).getOrElse(Right(None))

}
