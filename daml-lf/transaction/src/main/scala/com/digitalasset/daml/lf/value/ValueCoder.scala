// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.transaction.TransactionVersion
import com.daml.lf.value.Value._
import com.daml.lf.value.{ValueOuterClass => proto}
import com.google.protobuf

import scala.Ordering.Implicits.infixOrderingOps
import scala.collection.JavaConverters._

/**
  * Utilities to serialize and de-serialize Values
  * as they form part of transactions, nodes and contract instances
  */
object ValueCoder {
  import Value.MAXIMUM_NESTING

  /**
    * Error type for signalling errors occurring during decoding serialized values
    * @param errorMessage description
    */
  final case class DecodeError(errorMessage: String)

  object DecodeError extends (String => DecodeError) {
    private[lf] def apply(version: TransactionVersion, isTooOldFor: String): DecodeError =
      DecodeError(s"transaction version ${version.protoValue} is too old to support $isTooOldFor")
  }

  /**
    * Error type for signalling errors occurring during encoding values
    * @param errorMessage description
    */
  final case class EncodeError(errorMessage: String)

  object EncodeError extends (String => EncodeError) {
    private[lf] def apply(version: TransactionVersion, isTooOldFor: String): EncodeError =
      EncodeError(s"transaction version ${version.protoValue} is too old to support $isTooOldFor")
  }

  abstract class EncodeCid[-Cid] private[lf] {
    private[lf] def encode(contractId: Cid): proto.ContractId
  }

  @deprecated("use CidEncoder", since = "1.1.2")
  val AbsCidDecoder = CidEncoder

  object CidEncoder extends EncodeCid[ContractId] {
    private[lf] def encode(cid: ContractId): proto.ContractId =
      proto.ContractId.newBuilder.setRelative(false).setContractId(cid.coid).build
  }

  abstract class DecodeCid[Cid] private[lf] {
    def decodeOptional(
        structForm: proto.ContractId,
    ): Either[DecodeError, Option[Cid]]

    final def decode(
        structForm: proto.ContractId,
    ): Either[DecodeError, Cid] =
      decodeOptional(structForm).flatMap {
        case Some(cid) => Right(cid)
        case None => Left(DecodeError("Missing required field contract_id"))
      }
  }

  val CidDecoder: DecodeCid[ContractId] = new DecodeCid[ContractId] {

    private def stringToCidString(s: String): Either[DecodeError, Value.ContractId] =
      Value.ContractId
        .fromString(s)
        .left
        .map(_ => //
          DecodeError(s"""cannot parse contractId "$s""""))

    override def decodeOptional(
        structForm: proto.ContractId,
    ): Either[DecodeError, Option[ContractId]] =
      if (structForm.getContractId.isEmpty)
        Right(None)
      else
        stringToCidString(structForm.getContractId).map(Some(_))
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

  // For backward compatibility reasons, V10 is encoded as "6" when used inside a
  // proto.VersionedValue
  private[this] def encodeValueVersion(version: TransactionVersion): String =
    if (version == TransactionVersion.V10) {
      "6"
    } else {
      version.protoValue
    }

  private[this] def decodeValueVersion(vs: String): Either[DecodeError, TransactionVersion] =
    vs match {
      case "6" => Right(TransactionVersion.V10)
      case "10" => Left(DecodeError("Unsupported value version 10"))
      case _ => TransactionVersion.fromString(vs).left.map(DecodeError)
    }

  /**
    * Reads a serialized protobuf versioned value,
    * checks if the value version is currently supported and
    * converts the value to the type usable by engine/interpreter.
    *
    * @param protoValue0 the value to be read
    * @param decodeCid a function to decode stringified contract ids
    * @tparam Cid ContractId type
    * @return either error or [VersionedValue]
    */
  def decodeVersionedValue[Cid](
      decodeCid: DecodeCid[Cid],
      protoValue0: proto.VersionedValue,
  ): Either[DecodeError, VersionedValue[Cid]] =
    for {
      version <- decodeValueVersion(protoValue0.getVersion)
      value <- decodeValue(decodeCid, version, protoValue0.getValue)
    } yield VersionedValue(version, value)

  def decodeValue[Cid](
      decodeCid: DecodeCid[Cid],
      protoValue0: proto.VersionedValue,
  ): Either[DecodeError, Value[Cid]] =
    decodeVersionedValue(decodeCid, protoValue0) map (_.value)

  /**
    * Method to read a serialized protobuf value
    * to engine/interpreter usable Value type
    *
    * @param protoValue0 the value to be read
    * @param decodeCid a function to decode stringified contract ids
    * @tparam Cid ContractId type
    * @return either error or Value
    */
  def decodeValue[Cid](
      decodeCid: DecodeCid[Cid],
      version: TransactionVersion,
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

    def assertSince(minVersion: TransactionVersion, description: => String) =
      if (version < minVersion)
        throw Err(s"$description is not supported by value version $version")

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
            Numeric
              .fromString(protoValue.getNumeric)
              .fold(e => throw Err("error decoding decimal: " + e), ValueNumeric)
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
            val cid = decodeCid.decode(protoValue.getContractIdStruct)
            cid.fold(
              e => throw Err("error decoding contractId: " + e.errorMessage),
              ValueContractId(_))
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
            val option = protoValue.getOptional
            val mbV =
              if (option.getValue == ValueOuterClass.Value.getDefaultInstance) None
              else Some(go(newNesting, option.getValue))
            ValueOptional(mbV)

          case proto.Value.SumCase.MAP =>
            val entries = ImmArray(
              protoValue.getMap.getEntriesList.asScala.map(entry =>
                entry.getKey -> go(newNesting, entry.getValue)),
            )

            val map = SortedLookupList
              .fromImmArray(entries)
              .fold(
                err => throw Err(err),
                identity,
              )
            ValueTextMap(map)

          case proto.Value.SumCase.GEN_MAP =>
            assertSince(TransactionVersion.minGenMap, "Value.SumCase.MAP")
            val genMap = protoValue.getGenMap.getEntriesList.asScala.map(entry =>
              go(newNesting, entry.getKey) -> go(newNesting, entry.getValue))
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
    * Serializes [[VersionedValue]] to protobuf.
    *
    * @param versionedValue value to be written
    * @param encodeCid a function to stringify contractIds (it's better to be invertible)
    * @tparam Cid ContractId type
    * @return protocol buffer serialized values
    */
  def encodeVersionedValue[Cid](
      encodeCid: EncodeCid[Cid],
      versionedValue: VersionedValue[Cid],
  ): Either[EncodeError, proto.VersionedValue] =
    encodeVersionedValue(encodeCid, versionedValue.version, versionedValue.value)

  def encodeVersionedValue[Cid](
      encodeCid: EncodeCid[Cid],
      version: TransactionVersion,
      value: Value[Cid],
  ): Either[EncodeError, proto.VersionedValue] =
    for {
      protoValue <- encodeValue(encodeCid, version, value)
    } yield {
      val builder = proto.VersionedValue.newBuilder()
      builder.setVersion(encodeValueVersion(version)).setValue(protoValue).build()
    }

  /**
    * Serialize a Value to protobuf
    *
    * @param v0 value to be written
    * @param encodeCid a function to stringify contractIds (it's better to be invertible)
    * @param valueVersion version of value specification to encode to, or fail
    * @tparam Cid ContractId type
    * @return protocol buffer serialized values
    */
  def encodeValue[Cid](
      encodeCid: EncodeCid[Cid],
      valueVersion: TransactionVersion,
      v0: Value[Cid],
  ): Either[EncodeError, proto.Value] = {
    case class Err(msg: String) extends Throwable(null, null, true, false)

    def assertSince(minVersion: TransactionVersion, description: => String) =
      if (valueVersion < minVersion)
        throw Err(s"$description is not supported by value version $valueVersion")

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
            builder.setContractIdStruct(encodeCid.encode(coid)).build()
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
            assertSince(TransactionVersion.minGenMap, "Value.SumCase.MAP")
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

        }
      }
    }

    try {
      Right(go(0, v0))
    } catch {
      case Err(msg) => Left(EncodeError(msg))
    }
  }

  private[value] def valueFromBytes[Cid](
      decodeCid: DecodeCid[Cid],
      bytes: Array[Byte],
  ): Either[DecodeError, Value[Cid]] = {
    decodeValue(decodeCid, proto.VersionedValue.parseFrom(bytes))
  }
}
