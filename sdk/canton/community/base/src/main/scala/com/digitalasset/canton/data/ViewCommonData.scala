// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewConfirmationParameters.InvalidViewConfirmationParameters
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** Information concerning every '''member''' involved in processing the underlying view.
  */
final case class ViewCommonData private (
    viewConfirmationParameters: ViewConfirmationParameters,
    salt: Salt,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[ViewCommonData.type],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ViewCommonData](hashOps)
    // The class needs to implement ProtocolVersionedMemoizedEvidence, because we want that serialize always yields the same ByteString.
    // This is to ensure that different participants compute the same hash after receiving a ViewCommonData over the network.
    // (Recall that serialization is in general not guaranteed to be deterministic.)
    with ProtocolVersionedMemoizedEvidence
    // The class implements `HasProtocolVersionedWrapper` because we serialize it to an anonymous binary format and need to encode
    // the version of the serialized Protobuf message
    with HasProtocolVersionedWrapper[ViewCommonData] {

  // The toProto... methods are deliberately protected, as they could otherwise be abused to bypass memoization.
  //
  // If another serializable class contains a ViewCommonData, it has to include it as a ByteString
  // (and not as "message ViewCommonData") in its ProtoBuf representation.

  @transient override protected lazy val companionObj: ViewCommonData.type = ViewCommonData

  // Ensures the invariants related to default values hold
  validateInstance().valueOr(err => throw InvalidViewConfirmationParameters(err))

  // We use named parameters, because then the code remains correct even when the ProtoBuf code generator
  // changes the order of parameters.
  def toProtoV30: v30.ViewCommonData = {
    val informees = viewConfirmationParameters.informees.toSeq
    v30.ViewCommonData(
      informees = informees,
      quorums = viewConfirmationParameters.quorums.map(
        _.tryToProtoV30(informees)
      ),
      salt = Some(salt.toProtoV30),
    )
  }

  // When serializing the class to an anonymous binary format, we serialize it to an UntypedVersionedMessage version of the
  // corresponding Protobuf message
  override protected[this] def toByteStringUnmemoized: ByteString = toByteString

  override val hashPurpose: HashPurpose = HashPurpose.ViewCommonData

  override protected def pretty: Pretty[ViewCommonData] = prettyOfClass(
    param("view confirmation parameters", _.viewConfirmationParameters),
    param("salt", _.salt),
  )

  @VisibleForTesting
  def copy(
      viewConfirmationParameters: ViewConfirmationParameters = this.viewConfirmationParameters,
      salt: Salt = this.salt,
  ): ViewCommonData =
    ViewCommonData(viewConfirmationParameters, salt)(
      hashOps,
      representativeProtocolVersion,
      None,
    )
}

object ViewCommonData
    extends VersioningCompanionContextMemoization[
      ViewCommonData,
      HashOps,
    ] {
  override val name: String = "ViewCommonData"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.ViewCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )

  /** Creates a fresh [[ViewCommonData]]. */
  // The "create" method has the following advantages over the auto-generated "apply" method:
  // - The parameter lists have been flipped to facilitate curried usages.
  // - The deserializedFrom field cannot be set; so it cannot be set incorrectly.
  //
  // The method is called "create" instead of "apply"
  // to not confuse the Idea compiler by overloading "apply".
  // (This is not a problem with this particular class, but it has been a problem with other classes.)
  def create(hashOps: HashOps)(
      viewConfirmationParameters: ViewConfirmationParameters,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): Either[InvalidViewConfirmationParameters, ViewCommonData] =
    Either
      .catchOnly[InvalidViewConfirmationParameters] {
        // The deserializedFrom field is set to "None" as this is for creating "fresh" instances.
        new ViewCommonData(viewConfirmationParameters, salt)(
          hashOps,
          protocolVersionRepresentativeFor(protocolVersion),
          None,
        )
      }

  def tryCreate(hashOps: HashOps)(
      viewConfirmationParameters: ViewConfirmationParameters,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): ViewCommonData =
    create(hashOps)(viewConfirmationParameters, salt, protocolVersion)
      .valueOr(err => throw err)

  private def fromProtoV30(
      hashOps: HashOps,
      viewCommonDataP: v30.ViewCommonData,
  )(bytes: ByteString): ParsingResult[ViewCommonData] =
    for {
      informees <- viewCommonDataP.informees.traverse(informee =>
        ProtoConverter.parseLfPartyId(informee, "informees")
      )
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV30, "salt", viewCommonDataP.salt)
        .leftMap(_.inField("salt"))
      quorums <- viewCommonDataP.quorums.traverse(Quorum.fromProtoV30(_, informees))
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      viewConfirmationParameters <- ViewConfirmationParameters.create(informees.toSet, quorums)
    } yield new ViewCommonData(viewConfirmationParameters, salt)(
      hashOps,
      rpv,
      Some(bytes),
    )
}

/** Stores the necessary information necessary to confirm a view.
  *
  * @param informees
  *   list of all members ids that must be informed of this view.
  * @param quorums
  *   multiple lists of confirmers => threshold (i.e., a quorum) that needs to be met for the view
  *   to be approved. We make sure that the parties listed in the quorums are informees of the view
  *   during deserialization.
  */
final case class ViewConfirmationParameters private (
    informees: Set[LfPartyId],
    quorums: Seq[Quorum],
) extends PrettyPrinting
    with NoCopy {

  override protected def pretty: Pretty[ViewConfirmationParameters] = prettyOfClass(
    param("informees", _.informees),
    param("quorums", _.quorums),
  )

  lazy val confirmers: Set[LfPartyId] = quorums.flatMap(_.confirmers.keys).toSet
}

object ViewConfirmationParameters {

  /** Indicates an attempt to create an invalid [[ViewConfirmationParameters]]. */
  final case class InvalidViewConfirmationParameters(message: String)
      extends RuntimeException(message)

  /** Creates a [[ViewConfirmationParameters]] with a single quorum consisting of all confirming
    * parties and a given threshold.
    *
    * Informees are parties involved in the view and can have a weight indicating their confirmation
    * role:
    *   - Non-confirming informees have a weight of 0.
    *   - Confirming informees have a positive weight (with a recommended value of 1).
    *
    * The `threshold` parameter determines the minimum number of confirmers required to approve the
    * view. Currently, only thresholds equal to the total weight of all confirmers are supported
    * (i.e., threshold == sum of weights of all confirming informees).
    */
  def create(
      informees: Map[LfPartyId, NonNegativeInt],
      threshold: NonNegativeInt,
  ): ViewConfirmationParameters =
    ViewConfirmationParameters(
      informees.keySet,
      Seq(
        Quorum(
          informees
            .filter { case (_, weight) => weight.unwrap > 0 }
            .map { case (partyId, weight) => partyId -> PositiveInt.tryCreate(weight.unwrap) },
          threshold,
        )
      ),
    )

  /** There can be multiple quorums/threshold. Therefore, we need to make sure those quorums
    * confirmers are present in the list of informees.
    */
  def create(
      informees: Set[LfPartyId],
      quorums: Seq[Quorum],
  ): Either[InvariantViolation, ViewConfirmationParameters] = {
    val allConfirmers = quorums.flatMap(_.confirmers.keys)
    val notAnInformee = allConfirmers.filterNot(informees.contains)
    Either.cond(
      notAnInformee.isEmpty,
      ViewConfirmationParameters(informees, quorums),
      InvariantViolation(
        field = None,
        error = s"confirming parties $notAnInformee are not in the list of informees",
      ),
    )
  }

  def tryCreate(
      informees: Set[LfPartyId],
      quorums: Seq[Quorum],
  ): ViewConfirmationParameters =
    create(informees, quorums).valueOr(err => throw InvalidViewConfirmationParameters(err.toString))

  /** Extracts all confirming parties' distinct IDs from the list of quorums */
  def confirmersIdsFromQuorums(quorums: Seq[Quorum]): Set[LfPartyId] =
    quorums.flatMap(_.confirmers.keySet).toSet

}
