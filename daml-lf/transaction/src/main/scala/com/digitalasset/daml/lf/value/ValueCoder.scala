// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.value

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.transaction.VersionTimeline.SpecifiedVersion
import com.digitalasset.daml.lf.transaction.VersionTimeline.Implicits._
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.value.{ValueOuterClass => proto}
import com.google.protobuf

import scala.collection.JavaConverters._
import scalaz.std.either._
import scalaz.std.option._
import scalaz.syntax.traverse._

/**
  * Utilities to serialize and de-serialize Values
  * as they form part of transactions, nodes and contract instances
  */
object ValueCoder {
  import Value.MAXIMUM_NESTING

  /**
    * Error type for signalling errors occuring during decoding serialized values
    * @param errorMessage description
    */
  final case class DecodeError(errorMessage: String)

  object DecodeError extends (String => DecodeError) {
    private[lf] def apply(version: SpecifiedVersion, isTooOldFor: String): DecodeError =
      DecodeError(s"${version.showsVersion} is too old to support $isTooOldFor")
  }

  /**
    * Error type for signalling errors occuring during encoding values
    * @param errorMessage description
    */
  final case class EncodeError(errorMessage: String)

  object EncodeError extends (String => EncodeError) {
    private[lf] def apply(version: SpecifiedVersion, isTooOldFor: String): EncodeError =
      EncodeError(s"${version.showsVersion} is too old to support $isTooOldFor")
  }

  final case class EncodeCid[-Cid](asString: Cid => String, asStruct: Cid => (String, Boolean)) {
    def contramap[Cid2](f: Cid2 => Cid): EncodeCid[Cid2] =
      EncodeCid(f andThen asString, f andThen asStruct)
  }

  final case class DecodeCid[+Cid](
      fromString: String => Either[DecodeError, Cid],
      fromStruct: (String, Boolean) => Either[DecodeError, Cid],
  ) {
    def map[Cid2](f: Cid => Cid2): DecodeCid[Cid2] =
      DecodeCid(fromString andThen (_ map f), (s, b) => fromStruct(s, b) map f)
  }

  /**
    * Simple encoding to wire of identifiers
    * @param id identifier value
    * @return wire format identifier
    */
  def encodeIdentifier(id: Identifier): proto.Identifier = {
    val builder = proto.Identifier.newBuilder().setPackageId(id.packageId)
    builder.addAllModuleName((id.qualifiedName.module.segments.toSeq: Seq[String]).asJava)
    builder.addAllName((id.qualifiedName.name.segments.toSeq: Seq[String]).asJava)
    builder.build()
  }

  /**
    * Simple encoding to wire of identifiers
    * @param id identifier value
    * @param requireVersion force encoding version
    * @return value spec version encoded to, and wire format identifier
    * @note result _1 = requireVersion if defined
    */
  def encodeIdentifier(
      id: Identifier,
      requireVersion: Option[ValueVersion],
  ): (ValueVersion, proto.Identifier) =
    // NB SC: if ValueVersion("1") is no longer a good default in *every* case,
    // you must fold over NodeFetches' identifiers in TransactionVersions.assignVersion
    // because transaction version 1 only permits value-version-1 identifiers in
    // fetch position
    (requireVersion getOrElse ValueVersion("1"), encodeIdentifier(id))

  /**
    * Decode identifier from wire format
    * @param id proto identifier
    * @return identifier
    */
  def decodeIdentifier(id: proto.Identifier): Either[DecodeError, Identifier] =
    for {
      pkgId <- PackageId
        .fromString(id.getPackageId)
        .left
        .map(err => DecodeError(s"Invalid package id '${id.getPackageId}': $err"))

      moduleSegments = id.getModuleNameList.asScala
      module <- ModuleName
        .fromSegments(id.getModuleNameList.asScala)
        .left
        .map(err => DecodeError(s"Invalid module segments $moduleSegments: $err"))

      nameSegments = id.getNameList.asScala
      name <- DottedName
        .fromSegments(nameSegments)
        .left
        .map(err => DecodeError(s"Invalid name segments $nameSegments: $err"))

    } yield Identifier(pkgId, QualifiedName(module, name))

  /** Codecs for ContractId that don't break the method chain style
    * of builders.
    */
  private[lf] implicit final class codecContractId[A](private val self: A) extends AnyVal {
    def setContractIdOrStruct[Cid, Z](
        encodeCid: EncodeCid[Cid],
        version: SpecifiedVersion,
        contractId: Cid,
    )(stringly: (A, String) => Z, structly: (A, proto.ContractId) => Z): Z =
      if (useOldStringField(version)) stringly(self, encodeCid.asString(contractId))
      else {
        val (encCid, encRel) = encodeCid.asStruct(contractId)
        structly(
          self,
          proto.ContractId.newBuilder().setContractId(encCid).setRelative(encRel).build(),
        )
      }

    import proto.ContractId.{getDefaultInstance => placeholderContractId}

    private[this] def requireUnset(v: SpecifiedVersion, s: String): Either[DecodeError, Unit] =
      if (s == "") Right(())
      else Left(DecodeError(s"${v.showsVersion} is too new to use string contract IDs"))

    private[this] def requireUnset(
        v: SpecifiedVersion,
        c: proto.ContractId,
    ): Either[DecodeError, Unit] =
      if (c == null || c == placeholderContractId) Right(())
      else Left(DecodeError(v, isTooOldFor = "message ContractId"))

    private[this] def decodeContractIdStruct(
        p: proto.ContractId,
    ): Either[DecodeError, (String, Boolean)] =
      for {
        cid <- if (p.getContractId.isEmpty) Left(DecodeError("Missing required field contract_id"))
        else Right(p.getContractId)
      } yield (cid, p.getRelative)

    def decodeContractIdOrStruct[Cid, Z](
        decodeCid: DecodeCid[Cid],
        version: SpecifiedVersion,
    )(stringly: A => String, structly: A => proto.ContractId): Either[DecodeError, Cid] = {
      val stringForm = stringly(self)
      val structForm = structly(self)
      if (useOldStringField(version))
        requireUnset(version, structForm) flatMap (
            _ =>
              if (stringForm == "")
                Left(DecodeError("Missing required field contract_id"))
              else decodeCid.fromString(stringForm),
          )
      else
        requireUnset(version, stringForm) flatMap (
            _ =>
              decodeContractIdStruct(structForm) flatMap decodeCid.fromStruct.tupled,
          )
    }

    def decodeOptionalContractIdOrStruct[Cid, Z](
        decodeCid: DecodeCid[Cid],
        version: SpecifiedVersion,
    )(stringly: A => String, structly: A => proto.ContractId): Either[DecodeError, Option[Cid]] = {
      val stringForm = stringly(self)
      val structForm = structly(self)
      if (useOldStringField(version))
        requireUnset(version, structForm) flatMap (
            _ =>
              if (stringForm == "") Right(None)
              else decodeCid.fromString(stringForm) map (Some(_)),
          )
      else
        requireUnset(version, stringForm) flatMap (
            _ =>
              Option(structForm) filter (_ != placeholderContractId) traverseU (
                  sf =>
                    decodeContractIdStruct(sf) flatMap decodeCid.fromStruct.tupled,
                ),
          )
    }
  }

  private def decodeVersion(vs: String): Either[DecodeError, ValueVersion] =
    ValueVersions
      .isAcceptedVersion(vs)
      .fold[Either[DecodeError, ValueVersion]](Left(DecodeError(s"Unsupported value version $vs")))(
        v => Right(v),
      )

  /**
    * Reads a serialized protobuf versioned value,
    * checks if the value version is currently supported and
    * converts the value to the type usable by engine/interpreter.
    *
    * Supported value versions configured in [[ValueVersions]].
    *
    * @param protoValue0 the value to be read
    * @param decodeCid a function to decode stringified contract ids
    * @tparam Cid ContractId type as ContractId (allowing RelativeContractIds) or AbsoluteContractId
    *             see [[com.digitalasset.daml.lf.value.Value]] and [[com.digitalasset.daml.lf.value.Value.ContractId]]
    * @return either error or [VersionedValue]
    */
  def decodeVersionedValue[Cid](
      decodeCid: DecodeCid[Cid],
      protoValue0: proto.VersionedValue,
  ): Either[DecodeError, VersionedValue[Cid]] =
    for {
      version <- decodeVersion(protoValue0.getVersion)
      value <- decodeValue(decodeCid, version, protoValue0.getValue)
    } yield VersionedValue(version, value)

  def decodeValue[Cid](
      decodeCid: DecodeCid[Cid],
      protoValue0: proto.VersionedValue,
  ): Either[DecodeError, Value[Cid]] =
    decodeVersionedValue(decodeCid, protoValue0) map (_.value)

  /**
    * Serializes [[Value]] to protobuf, library decides which [[ValueVersion]] to assign.
    * See [[ValueVersions.assignVersion]].
    *
    * @param value value to be written
    * @param encodeCid a function to stringify contractIds (it's better to be invertible)
    * @tparam Cid ContractId type as ContractId (allowing RelativeContractIds) or AbsoluteContractId
    *             see [[com.digitalasset.daml.lf.value.Value]] and [[com.digitalasset.daml.lf.value.Value.ContractId]]
    * @return protocol buffer serialized values
    */
  def encodeVersionedValue[Cid](
      encodeCid: EncodeCid[Cid],
      value: Value[Cid],
  ): Either[EncodeError, proto.VersionedValue] =
    ValueVersions
      .assignVersion(value)
      .fold(
        err => Left(EncodeError(err)),
        version => encodeVersionedValueWithCustomVersion(encodeCid, VersionedValue(version, value)),
      )

  /**
    * Serializes [[VersionedValue]] to protobuf, caller provides the [[ValueVersion]].
    *
    * @param versionedValue value to be written
    * @param encodeCid a function to stringify contractIds (it's better to be invertible)
    * @tparam Cid ContractId type as ContractId (allowing RelativeContractIds) or AbsoluteContractId
    *             see [[com.digitalasset.daml.lf.value.Value]] and [[com.digitalasset.daml.lf.value.Value.ContractId]]
    * @return protocol buffer serialized values
    */
  def encodeVersionedValueWithCustomVersion[Cid](
      encodeCid: EncodeCid[Cid],
      versionedValue: VersionedValue[Cid],
  ): Either[EncodeError, proto.VersionedValue] =
    for {
      value <- encodeValue(encodeCid, versionedValue.version, versionedValue.value)
    } yield {
      val builder = proto.VersionedValue.newBuilder()
      builder.setVersion(versionedValue.version.protoValue).setValue(value).build()
    }

  /**
    * Method to read a serialized protobuf value
    * to engine/interpreter usable Value type
    *
    * @param protoValue0 the value to be read
    * @param decodeCid a function to decode stringified contract ids
    * @tparam Cid ContractId type as ContractId (allowing RelativeContractIds) or AbsoluteContractId
    *             see [[com.digitalasset.daml.lf.value.Value]] and [[com.digitalasset.daml.lf.value.Value.ContractId]]
    * @return either error or Value
    */
  def decodeValue[Cid](
      decodeCid: DecodeCid[Cid],
      valueVersion: ValueVersion,
      protoValue0: proto.Value,
  ): Either[DecodeError, Value[Cid]] = {
    case class Err(msg: String) extends Throwable(null, null, true, false)

    def identifier(s: String): Name =
      Name
        .fromString(s)
        .fold(
          err => throw Err(s"error decoding variant constructor: $err"),
          identity,
        )

    def assertSince(minVersion: ValueVersion, description: => String) =
      if (valueVersion precedes minVersion)
        throw Err(s"$description is not supported by value version $valueVersion")

    def go(nesting: Int, protoValue: proto.Value): Value[Cid] = {
      if (nesting > MAXIMUM_NESTING) {
        throw Err(
          s"Provided proto value to decode exceeds maximum nesting level of $MAXIMUM_NESTING",
        )
      } else {
        val newNesting = nesting + 1

        protoValue.getSumCase match {
          case proto.Value.SumCase.BOOL =>
            ValueBool(protoValue.getBool)
          case proto.Value.SumCase.UNIT =>
            ValueUnit
          case proto.Value.SumCase.NUMERIC =>
            val d =
              if (useLegacyDecimal(valueVersion))
                Decimal.fromString(protoValue.getNumeric)
              else
                Numeric.fromString(protoValue.getNumeric)
            d.fold(e => throw Err("error decoding decimal: " + e), ValueNumeric)
          case proto.Value.SumCase.INT64 =>
            ValueInt64(protoValue.getInt64)
          case proto.Value.SumCase.TEXT =>
            ValueText(protoValue.getText)
          case proto.Value.SumCase.DATE =>
            val d = Time.Date.fromDaysSinceEpoch(protoValue.getDate)
            d.fold(e => throw Err("error decoding date: " + e), ValueDate)
          case proto.Value.SumCase.TIMESTAMP =>
            val t = Time.Timestamp.fromLong(protoValue.getTimestamp)
            t.fold(e => throw Err("error decoding timestamp: " + e), ValueTimestamp)
          case proto.Value.SumCase.PARTY =>
            val party = Party.fromString(protoValue.getParty)
            party.fold(e => throw Err("error decoding party: " + e), ValueParty)
          case proto.Value.SumCase.CONTRACT_ID | proto.Value.SumCase.CONTRACT_ID_STRUCT =>
            val cid = protoValue.decodeContractIdOrStruct(decodeCid, valueVersion)(
              _.getContractId,
              _.getContractIdStruct,
            )
            cid.fold(err => throw Err(err.errorMessage), ValueContractId(_))
          case proto.Value.SumCase.LIST =>
            ValueList(
              FrontStack(
                ImmArray(protoValue.getList.getElementsList.asScala.map(go(newNesting, _))),
              ),
            )

          case proto.Value.SumCase.VARIANT =>
            val variant = protoValue.getVariant
            val id =
              if (variant.getVariantId == ValueOuterClass.Identifier.getDefaultInstance) None
              else
                decodeIdentifier(variant.getVariantId).fold(
                  { err =>
                    throw Err(err.errorMessage)
                  }, { id =>
                    Some(id)
                  },
                )
            ValueVariant(id, identifier(variant.getConstructor), go(newNesting, variant.getValue))

          case proto.Value.SumCase.ENUM =>
            assertSince(ValueVersions.minEnum, "Value.SumCase.ENUM")
            val enum = protoValue.getEnum
            val id =
              if (enum.getEnumId == ValueOuterClass.Identifier.getDefaultInstance) None
              else
                decodeIdentifier(enum.getEnumId).fold(
                  { err =>
                    throw Err(err.errorMessage)
                  }, { id =>
                    Some(id)
                  },
                )
            ValueEnum(id, identifier(enum.getValue))

          case proto.Value.SumCase.RECORD =>
            val record = protoValue.getRecord
            val id =
              if (record.getRecordId == ValueOuterClass.Identifier.getDefaultInstance) None
              else
                decodeIdentifier(record.getRecordId).fold(
                  { err =>
                    throw Err(err.errorMessage)
                  }, { id =>
                    Some(id)
                  },
                )
            ValueRecord(
              id,
              ImmArray(protoValue.getRecord.getFieldsList.asScala.map(fld => {
                val lbl = if (fld.getLabel.isEmpty) None else Option(identifier(fld.getLabel))
                (lbl, go(newNesting, fld.getValue))
              })),
            )

          case proto.Value.SumCase.OPTIONAL =>
            assertSince(ValueVersions.minOptional, "Value.SumCase.OPTIONAL")
            val option = protoValue.getOptional
            val mbV =
              if (option.getValue == ValueOuterClass.Value.getDefaultInstance) None
              else Some(go(newNesting, option.getValue))
            ValueOptional(mbV)

          case proto.Value.SumCase.MAP =>
            assertSince(ValueVersions.minMap, "Value.SumCase.MAP")
            val entries = ImmArray(
              protoValue.getMap.getEntriesList.asScala.map(entry =>
                entry.getKey -> go(newNesting, entry.getValue),
              ),
            )

            val map = SortedLookupList
              .fromImmArray(entries)
              .fold(
                err => throw Err(err),
                identity,
              )
            ValueTextMap(map)

          case proto.Value.SumCase.GEN_MAP =>
            assertSince(ValueVersions.minGenMap, "Value.SumCase.MAP")
            val genMap = protoValue.getGenMap.getEntriesList.asScala.map(entry =>
              go(newNesting, entry.getKey) -> go(newNesting, entry.getValue),
            )
            ValueGenMap(ImmArray(genMap))

          case proto.Value.SumCase.SUM_NOT_SET =>
            throw Err(s"Value not set")
        }
      }
    }

    try {
      Right(go(0, protoValue0))
    } catch {
      case Err(msg) => Left(DecodeError(msg))
    }
  }

  /**
    * Serialize a Value to protobuf
    *
    * @param v0 value to be written
    * @param encodeCid a function to stringify contractIds (it's better to be invertible)
    * @param valueVersion version of value specification to encode to, or fail
    * @tparam Cid ContractId type as ContractId (allowing RelativeContractIds) or AbsoluteContractId
    *             see [[com.digitalasset.daml.lf.value.Value]] and [[com.digitalasset.daml.lf.value.Value.ContractId]]
    * @return protocol buffer serialized values
    */
  def encodeValue[Cid](
      encodeCid: EncodeCid[Cid],
      valueVersion: ValueVersion,
      v0: Value[Cid],
  ): Either[EncodeError, proto.Value] = {
    case class Err(msg: String) extends Throwable(null, null, true, false)

    def go(nesting: Int, v: Value[Cid]): proto.Value = {
      if (nesting > MAXIMUM_NESTING) {
        throw Err(
          s"Provided DAML-LF value to encode exceeds maximum nesting level of $MAXIMUM_NESTING",
        )
      } else {
        val newNesting = nesting + 1

        val builder = proto.Value.newBuilder()
        v match {
          case ValueUnit =>
            builder.setUnit(protobuf.Empty.newBuilder()).build()
          case ValueBool(b) =>
            builder.setBool(b).build()
          case ValueInt64(i) =>
            builder.setInt64(i).build()
          case ValueNumeric(d) =>
            if (useLegacyDecimal(valueVersion))
              builder.setNumeric(Numeric.toUnscaledString(d)).build()
            else
              builder.setNumeric(Numeric.toString(d)).build()
          case ValueText(t) =>
            builder.setText(t).build()
          case ValueParty(p) =>
            builder.setParty(p).build()
          case ValueDate(d) =>
            builder.setDate(d.days).build()
          case ValueTimestamp(t) =>
            builder.setTimestamp(t.micros).build()
          case ValueContractId(coid) =>
            builder
              .setContractIdOrStruct(encodeCid, valueVersion, coid)(
                _.setContractId(_),
                _.setContractIdStruct(_),
              )
              .build()

          case ValueList(elems) =>
            val listBuilder = proto.List.newBuilder()
            elems.foreach(elem => {
              listBuilder.addElements(go(newNesting, elem))
              ()
            })
            builder.setList(listBuilder).build()

          case ValueRecord(id, fields) =>
            val protoFields = fields
              .map(f => {
                val b = proto.RecordField
                  .newBuilder()
                  .setValue(go(newNesting, f._2))
                f._1.map(b.setLabel)
                b.build()
              })
              .toSeq
              .asJava
            val recordBuilder = proto.Record.newBuilder().addAllFields(protoFields)
            id.foreach(i => recordBuilder.setRecordId(encodeIdentifier(i)))
            builder
              .setRecord(recordBuilder)
              .build()

          case ValueVariant(id, con, arg) =>
            val protoVar = proto.Variant
              .newBuilder()
              .setConstructor(con)
              .setValue(go(newNesting, arg))
            id.foreach(i => protoVar.setVariantId(encodeIdentifier(i)))
            builder.setVariant(protoVar).build()

          case ValueEnum(id, value) =>
            val protoEnum = proto.Enum
              .newBuilder()
              .setValue(value)
            id.foreach(i => protoEnum.setEnumId(encodeIdentifier(i)))
            builder.setEnum(protoEnum).build()

          case ValueOptional(mbV) =>
            val protoOption = proto.Optional.newBuilder()
            mbV.foreach(v => protoOption.setValue(go(newNesting, v)))
            builder.setOptional(protoOption).build()

          case ValueTextMap(map) =>
            val protoMap = proto.Map.newBuilder()
            map.toImmArray.foreach {
              case (key, value) =>
                protoMap.addEntries(
                  proto.Map.Entry
                    .newBuilder()
                    .setKey(key)
                    .setValue(go(newNesting, value)),
                )
                ()
            }
            builder.setMap(protoMap).build()

          case ValueGenMap(entries) =>
            val protoMap = proto.GenMap.newBuilder()
            entries.foreach {
              case (key, value) =>
                protoMap.addEntries(
                  proto.GenMap.Entry
                    .newBuilder()
                    .setKey(go(newNesting, key))
                    .setValue(go(newNesting, value)),
                )
                ()
            }
            builder.setGenMap(protoMap).build()

          case ValueStruct(fields) =>
            throw Err(s"Trying to serialize struct, which are not serializable. Fields: $fields")
        }
      }
    }

    try {
      Right(go(0, v0))
    } catch {
      case Err(msg) => Left(EncodeError(msg))
    }
  }

  // The codomain and domain of the below functions are subject to change
  // without warning or type change; they are stable with respect to
  // each other and nothing else.  As such, they are unsafe for
  // general usage

  private[value] def valueToBytes[Cid](
      encodeCid: EncodeCid[Cid],
      v: Value[Cid],
  ): Either[EncodeError, Array[Byte]] = {
    encodeVersionedValue(encodeCid, v).map(_.toByteArray)
  }

  private[value] def valueFromBytes[Cid](
      decodeCid: DecodeCid[Cid],
      bytes: Array[Byte],
  ): Either[DecodeError, Value[Cid]] = {
    decodeValue(decodeCid, proto.VersionedValue.parseFrom(bytes))
  }

  private[this] def useOldStringField(sv: SpecifiedVersion): Boolean =
    sv precedes ValueVersions.minContractIdStruct

  private[this] def useLegacyDecimal(sv: SpecifiedVersion): Boolean =
    sv precedes ValueVersions.minNumeric
}
