// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package value

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.transaction.{TransactionVersion, Versioned}
import com.daml.lf.value.Value._
import com.daml.lf.value.{ValueOuterClass => proto}
import com.daml.scalautil.Statement.discard
import com.google.protobuf
import com.google.protobuf.{ByteString, CodedInputStream}

import scala.Ordering.Implicits.infixOrderingOps
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Utilities to serialize and de-serialize Values
  * as they form part of transactions, nodes and contract instances
  */
object ValueCoder {
  import Value.MAXIMUM_NESTING

  /** Error type for signalling errors occurring during decoding serialized values
    * @param errorMessage description
    */
  final case class DecodeError(errorMessage: String)

  object DecodeError extends (String => DecodeError) {
    private[lf] def apply(version: TransactionVersion, isTooOldFor: String): DecodeError =
      DecodeError(s"transaction version ${version.protoValue} is too old to support $isTooOldFor")
  }

  /** Error type for signalling errors occurring during encoding values
    * @param errorMessage description
    */
  final case class EncodeError(errorMessage: String)

  object EncodeError extends (String => EncodeError) {
    private[lf] def apply(version: TransactionVersion, isTooOldFor: String): EncodeError =
      EncodeError(s"transaction version ${version.protoValue} is too old to support $isTooOldFor")
  }

  abstract class EncodeCid private[lf] {
    private[lf] def encode(contractId: ContractId): proto.ContractId
  }

  object CidEncoder extends EncodeCid {
    private[lf] def encode(cid: ContractId): proto.ContractId =
      proto.ContractId.newBuilder.setContractId(cid.coid).build
  }

  abstract class DecodeCid private[lf] {
    def decodeOptional(
        structForm: proto.ContractId
    ): Either[DecodeError, Option[Value.ContractId]]

    final def decode(
        structForm: proto.ContractId
    ): Either[DecodeError, Value.ContractId] =
      decodeOptional(structForm).flatMap {
        case Some(cid) => Right(cid)
        case None => Left(DecodeError("Missing required field contract_id"))
      }
  }

  val CidDecoder: DecodeCid = new DecodeCid {

    private def stringToCidString(s: String): Either[DecodeError, Value.ContractId] =
      Value.ContractId
        .fromString(s)
        .left
        .map(_ => //
          DecodeError(s"""cannot parse contractId "$s"""")
        )

    override def decodeOptional(
        structForm: proto.ContractId
    ): Either[DecodeError, Option[ContractId]] =
      if (structForm.getContractId.isEmpty)
        Right(None)
      else
        stringToCidString(structForm.getContractId).map(Some(_))
  }

  val NoCidDecoder: DecodeCid = new DecodeCid {
    override def decodeOptional(structForm: ValueOuterClass.ContractId) = Right(None)
  }

  /** Simple encoding to wire of identifiers
    * @param id identifier value
    * @return wire format identifier
    */
  def encodeIdentifier(id: Identifier): proto.Identifier = {
    proto.Identifier
      .newBuilder()
      .setPackageId(id.packageId)
      .addAllModuleName((id.qualifiedName.module.segments.toSeq: Seq[String]).asJava)
      .addAllName((id.qualifiedName.name.segments.toSeq: Seq[String]).asJava)
      .build()
  }

  /** Decode identifier from wire format
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
  private[lf] def encodeValueVersion(version: TransactionVersion): String =
    if (version == TransactionVersion.V10) {
      "6"
    } else {
      version.protoValue
    }

  private[this] def decodeValueVersion(vs: String): Either[DecodeError, TransactionVersion] =
    vs match {
      case "6" => Right(TransactionVersion.V10)
      case TransactionVersion.V10.protoValue => Left(DecodeError("Unsupported value version 10"))
      case _ => TransactionVersion.fromString(vs).left.map(DecodeError)
    }

  /** Reads a serialized protobuf versioned value,
    * checks if the value version is currently supported and
    * converts the value to the type usable by engine/interpreter.
    *
    * @param protoValue0 the value to be read
    * @param decodeCid a function to decode stringified contract ids
    * @tparam Cid ContractId type
    * @return either error or [VersionedValue]
    */
  def decodeVersionedValue(
      decodeCid: DecodeCid,
      protoValue0: proto.VersionedValue,
  ): Either[DecodeError, VersionedValue] =
    for {
      version <- decodeValueVersion(protoValue0.getVersion)
      value <- decodeValue(decodeCid, version, protoValue0.getValue)
    } yield Versioned(version, value)

  // We need (3 * MAXIMUM_NESTING + 1) as record and maps use:
  // - 3 nested messages for the cases of non empty records/maps)
  // - 2 nested messages for the cases of empty records/maps
  // Note the number of recursions is one less than the number of nested messages.
  private[this] val MAXIMUM_PROTO_RECURSION_LIMIT = 3 * MAXIMUM_NESTING + 1

  def decodeValue(
      decodeCid: DecodeCid,
      protoValue0: proto.VersionedValue,
  ): Either[DecodeError, Value] =
    decodeVersionedValue(decodeCid, protoValue0) map (_.unversioned)

  private[this] def parseValue(bytes: ByteString): Either[DecodeError, proto.Value] =
    Try {
      val cis = CodedInputStream.newInstance(bytes.asReadOnlyByteBuffer())
      discard(cis.setRecursionLimit(MAXIMUM_PROTO_RECURSION_LIMIT))
      proto.Value.parseFrom(cis)
    } match {
      case Failure(exception: Error) =>
        Left(DecodeError("cannot parse proto Value: " + exception.getMessage))
      case Failure(throwable) =>
        throw throwable
      case Success(value) =>
        Right(value)
    }

  def decodeValue(
      decodeCid: DecodeCid,
      version: TransactionVersion,
      bytes: ByteString,
  ): Either[DecodeError, Value] =
    parseValue(bytes).flatMap(decodeValue(decodeCid, version, _))

  /** Method to read a serialized protobuf value
    * to engine/interpreter usable Value type
    *
    * @param protoValue0 the value to be read
    * @param decodeCid a function to decode stringified contract ids
    * @tparam Cid ContractId type
    * @return either error or Value
    */
  private[this] def decodeValue(
      decodeCid: DecodeCid,
      version: TransactionVersion,
      protoValue0: proto.Value,
  ): Either[DecodeError, Value] = {
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
        throw Err(s"$description is not supported by transaction version $version")

    def assertUntil(minVersion: TransactionVersion, description: => String) =
      if (version >= minVersion)
        throw Err(s"$description is not supported by transaction version $version")

    def go(nesting: Int, protoValue: proto.Value): Value = {
      if (nesting > MAXIMUM_NESTING) {
        throw Err(
          s"Provided proto value to decode exceeds maximum nesting level of $MAXIMUM_NESTING"
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
          case proto.Value.SumCase.CONTRACT_ID_STRUCT =>
            val cid = decodeCid.decode(protoValue.getContractIdStruct)
            cid.fold(
              e => throw Err("error decoding contractId: " + e.errorMessage),
              ValueContractId(_),
            )
          case proto.Value.SumCase.LIST =>
            ValueList(
              protoValue.getList.getElementsList.asScala.map(go(newNesting, _)).to(FrontStack)
            )

          case proto.Value.SumCase.VARIANT =>
            val variant = protoValue.getVariant
            val id =
              if (variant.getVariantId == ValueOuterClass.Identifier.getDefaultInstance) {
                None
              } else {
                assertUntil(
                  TransactionVersion.minTypeErasure,
                  "variant_id field in message Variant",
                )
                decodeIdentifier(variant.getVariantId)
                  .fold((err => throw Err(err.errorMessage)), Some(_))
              }
            ValueVariant(id, identifier(variant.getConstructor), go(newNesting, variant.getValue))

          case proto.Value.SumCase.ENUM =>
            val enumeration = protoValue.getEnum
            val id =
              if (enumeration.getEnumId == ValueOuterClass.Identifier.getDefaultInstance) None
              else {
                assertUntil(TransactionVersion.minTypeErasure, "enum_id field in message Enum")
                decodeIdentifier(enumeration.getEnumId).fold(
                  { err =>
                    throw Err(err.errorMessage)
                  },
                  { id =>
                    Some(id)
                  },
                )
              }
            ValueEnum(id, identifier(enumeration.getValue))

          case proto.Value.SumCase.RECORD =>
            val record = protoValue.getRecord
            val id =
              if (record.getRecordId == ValueOuterClass.Identifier.getDefaultInstance) None
              else {
                assertUntil(TransactionVersion.minTypeErasure, "record_id field in message Record")
                decodeIdentifier(record.getRecordId)
                  .fold((err => throw Err(err.errorMessage)), Some(_))
              }
            ValueRecord(
              id,
              protoValue.getRecord.getFieldsList.asScala.view
                .map { fld =>
                  val lbl =
                    if (fld.getLabel.isEmpty) {
                      None
                    } else {
                      assertUntil(
                        TransactionVersion.minTypeErasure,
                        "label field in message RecordField",
                      )
                      Option(identifier(fld.getLabel))
                    }
                  (lbl, go(newNesting, fld.getValue))
                }
                .to(ImmArray),
            )

          case proto.Value.SumCase.OPTIONAL =>
            val option = protoValue.getOptional
            val mbV =
              if (option.getValue == ValueOuterClass.Value.getDefaultInstance) None
              else Some(go(newNesting, option.getValue))
            ValueOptional(mbV)

          case proto.Value.SumCase.MAP =>
            val entries =
              protoValue.getMap.getEntriesList.asScala.view
                .map(entry => entry.getKey -> go(newNesting, entry.getValue))
                .to(ImmArray)
            val map = SortedLookupList
              .fromImmArray(entries)
              .fold(
                err => throw Err(err),
                identity,
              )
            ValueTextMap(map)

          case proto.Value.SumCase.GEN_MAP =>
            assertSince(TransactionVersion.minGenMap, "Value.SumCase.MAP")
            val genMap = protoValue.getGenMap.getEntriesList.asScala.view
              .map(entry => go(newNesting, entry.getKey) -> go(newNesting, entry.getValue))
              .to(ImmArray)
            ValueGenMap(genMap)

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

  /** Serializes [[VersionedValue]] to protobuf.
    *
    * @param versionedValue value to be written
    * @param encodeCid a function to stringify contractIds (it's better to be invertible)
    * @tparam Cid ContractId type
    * @return protocol buffer serialized values
    */
  def encodeVersionedValue(
      encodeCid: EncodeCid,
      versionedValue: VersionedValue,
  ): Either[EncodeError, proto.VersionedValue] =
    encodeVersionedValue(encodeCid, versionedValue.version, versionedValue.unversioned)

  def encodeVersionedValue(
      encodeCid: EncodeCid,
      version: TransactionVersion,
      value: Value,
  ): Either[EncodeError, proto.VersionedValue] =
    for {
      bytes <- encodeValue(encodeCid, version, value)
    } yield {
      val builder = proto.VersionedValue.newBuilder()
      builder.setVersion(encodeValueVersion(version)).setValue(bytes).build()
    }

  /** Serialize a Value to protobuf
    *
    * @param v0 value to be written
    * @param encodeCid a function to stringify contractIds (it's better to be invertible)
    * @param valueVersion version of value specification to encode to, or fail
    * @tparam Cid ContractId type
    * @return protocol buffer serialized values
    */
  def encodeValue(
      encodeCid: EncodeCid,
      valueVersion: TransactionVersion,
      v0: Value,
  ): Either[EncodeError, ByteString] = {
    case class Err(msg: String) extends Throwable(null, null, true, false)

    def assertSince(minVersion: TransactionVersion, description: => String) =
      if (valueVersion < minVersion)
        throw Err(s"$description is not supported by value version $valueVersion")

    def go(nesting: Int, v: Value): proto.Value = {
      if (nesting > MAXIMUM_NESTING) {
        throw Err(
          s"Provided Daml-LF value to encode exceeds maximum nesting level of $MAXIMUM_NESTING"
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
              discard(listBuilder.addElements(go(newNesting, elem)))
            })
            builder.setList(listBuilder).build()

          case ValueRecord(id, fields) =>
            val recordBuilder = proto.Record.newBuilder()
            fields.foreach { case (fieldName, field) =>
              val b = proto.RecordField.newBuilder()
              if (valueVersion < TransactionVersion.minTypeErasure)
                discard(fieldName.map(b.setLabel))
              discard(b.setValue(go(newNesting, field)))
              discard(recordBuilder.addFields(b))
            }
            if (valueVersion < TransactionVersion.minTypeErasure)
              id.foreach(i => recordBuilder.setRecordId(encodeIdentifier(i)))
            builder.setRecord(recordBuilder).build()

          case ValueVariant(id, con, arg) =>
            val protoVar = proto.Variant
              .newBuilder()
              .setConstructor(con)
              .setValue(go(newNesting, arg))
            if (valueVersion < TransactionVersion.minTypeErasure)
              id.foreach(i => protoVar.setVariantId(encodeIdentifier(i)))
            builder.setVariant(protoVar).build()

          case ValueEnum(id, value) =>
            val protoEnum = proto.Enum
              .newBuilder()
              .setValue(value)
            if (valueVersion < TransactionVersion.minTypeErasure)
              id.foreach(i => protoEnum.setEnumId(encodeIdentifier(i)))
            builder.setEnum(protoEnum).build()

          case ValueOptional(mbV) =>
            val protoOption = proto.Optional.newBuilder()
            mbV.foreach(v => protoOption.setValue(go(newNesting, v)))
            builder.setOptional(protoOption).build()

          case ValueTextMap(map) =>
            val protoMap = proto.Map.newBuilder()
            map.toImmArray.foreach { case (key, value) =>
              discard(
                protoMap.addEntries(
                  proto.Map.Entry
                    .newBuilder()
                    .setKey(key)
                    .setValue(go(newNesting, value))
                )
              )
            }
            builder.setMap(protoMap).build()

          case ValueGenMap(entries) =>
            assertSince(TransactionVersion.minGenMap, "Value.SumCase.MAP")
            val protoMap = proto.GenMap.newBuilder()
            entries.foreach { case (key, value) =>
              discard(
                protoMap.addEntries(
                  proto.GenMap.Entry
                    .newBuilder()
                    .setKey(go(newNesting, key))
                    .setValue(go(newNesting, value))
                )
              )
            }
            builder.setGenMap(protoMap).build()

        }
      }
    }

    try {
      SafeProto.toByteString(go(0, v0)).left.map(EncodeError)
    } catch {
      case Err(msg) => Left(EncodeError(msg))
    }
  }

  private[value] def valueFromBytes(
      decodeCid: DecodeCid,
      bytes: Array[Byte],
  ): Either[DecodeError, Value] = {
    decodeValue(decodeCid, proto.VersionedValue.parseFrom(bytes))
  }

}
