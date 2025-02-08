// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.v30 as adminProto
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier, store}
import com.digitalasset.canton.{ProtoDeserializationError, topology}

sealed trait TopologyStoreId extends Product with Serializable {
  def toProtoV30: adminProto.StoreId

  def filterString: String

  private[canton] def toInternal: topology.store.TopologyStoreId
}

object TopologyStoreId {

  def tryFromString(store: String): TopologyStoreId = store.toLowerCase() match {
    case "authorized" => Authorized
    case TopologyStoreId.Temporary.MatchesPattern() =>
      TopologyStoreId.Temporary.fromFilterString(store)
    case otherwise => Synchronizer(SynchronizerId.tryFromString(store))
  }

  def fromProtoV30(
      store: adminProto.StoreId,
      fieldName: String,
  ): ParsingResult[TopologyStoreId] =
    store.store match {
      case adminProto.StoreId.Store.Empty => Left(ProtoDeserializationError.FieldNotSet(fieldName))
      case adminProto.StoreId.Store.Authorized(_) => Right(TopologyStoreId.Authorized)
      case adminProto.StoreId.Store.Temporary(temporary) =>
        String185
          .fromProtoPrimitive(temporary.name, fieldName)
          .map(TopologyStoreId.Temporary(_))
      case adminProto.StoreId.Store.Synchronizer(synchronizer) =>
        SynchronizerId
          .fromProtoPrimitive(synchronizer.id, fieldName)
          .map(TopologyStoreId.Synchronizer(_))
    }

  final case class Synchronizer(id: SynchronizerId) extends TopologyStoreId {
    override def toProtoV30: adminProto.StoreId =
      adminProto.StoreId(
        adminProto.StoreId.Store.Synchronizer(adminProto.StoreId.Synchronizer(id.toProtoPrimitive))
      )

    override def filterString: String = id.toProtoPrimitive

    override private[canton] def toInternal: store.TopologyStoreId.SynchronizerStore =
      store.TopologyStoreId.SynchronizerStore(id)
  }

  final case class Temporary(name: String185) extends TopologyStoreId {
    override def toProtoV30: adminProto.StoreId =
      adminProto.StoreId(
        adminProto.StoreId.Store.Temporary(adminProto.StoreId.Temporary(name.unwrap))
      )

    override def filterString: String = s"${Temporary.prefix}${name.unwrap}${Temporary.suffix}"

    override private[canton] def toInternal: store.TopologyStoreId.TemporaryStore =
      store.TopologyStoreId.TemporaryStore.tryFromName(name.unwrap)
  }

  object Temporary {
    val marker = "temp"
    val prefix = s"$marker${UniqueIdentifier.delimiter}"
    val suffix = s"${UniqueIdentifier.delimiter}$marker"

    object MatchesPattern {
      private val Regex = raw"$prefix(.*)$suffix".r
      def unapply(s: String): Boolean = Regex.matches(s)
    }

    def fromFilterString(s: String): Temporary = Temporary(
      String185.tryCreate(s.stripPrefix(prefix).stripSuffix(suffix))
    )

    def fromProtoV30(
        storeId: adminProto.StoreId.Temporary
    ): ParsingResult[Temporary] =
      ProtoConverter.parseLengthLimitedString(String185, storeId.name).map(Temporary(_))

  }

  case object Authorized extends TopologyStoreId {
    override def toProtoV30: adminProto.StoreId =
      adminProto.StoreId(adminProto.StoreId.Store.Authorized(adminProto.StoreId.Authorized()))

    override def filterString: String = "Authorized"

    override private[canton] def toInternal: store.TopologyStoreId.AuthorizedStore =
      store.TopologyStoreId.AuthorizedStore
  }
}
