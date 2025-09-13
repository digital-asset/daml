// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.admin.grpc

import cats.syntax.bifunctor.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.v30 as adminProto
import com.digitalasset.canton.topology.{PhysicalSynchronizerId, SynchronizerId, store}

import scala.language.implicitConversions

sealed trait TopologyStoreId extends Product with Serializable {
  def toProtoV30: adminProto.StoreId

  private[canton] def toInternal(
      lookup: PSIdLookup
  ): Either[SynchronizerId, store.TopologyStoreId]
}

private[canton] trait PSIdLookup {
  def activePSIdFor(synchronizerId: SynchronizerId): Option[PhysicalSynchronizerId]
}

object TopologyStoreId {

  def fromInternal(internalStore: store.TopologyStoreId): TopologyStoreId = internalStore match {
    case store.TopologyStoreId.SynchronizerStore(synchronizerId) =>
      Synchronizer(synchronizerId)
    case store.TopologyStoreId.AuthorizedStore => TopologyStoreId.Authorized
    case store.TopologyStoreId.TemporaryStore(name) => TopologyStoreId.Temporary(name)
  }

  implicit def synchronizerIdIsTopologyStoreId(
      synchronizer: com.digitalasset.canton.topology.Synchronizer
  ): TopologyStoreId.Synchronizer = synchronizer match {
    case id: SynchronizerId => TopologyStoreId.Synchronizer(id)
    case psid: PhysicalSynchronizerId => TopologyStoreId.Synchronizer(psid)
  }

  implicit def physicalSynchronizerIdIsTopologyStoreId(
      synchronizerId: PhysicalSynchronizerId
  ): TopologyStoreId.Synchronizer =
    TopologyStoreId.Synchronizer(synchronizerId)

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
      case adminProto.StoreId.Store.Synchronizer(
            adminProto.Synchronizer(
              adminProto.Synchronizer.Kind.Id(logicalSynchronizerId)
            )
          ) =>
        SynchronizerId
          .fromProtoPrimitive(logicalSynchronizerId, fieldName)
          .map(id => TopologyStoreId.Synchronizer(id))

      case adminProto.StoreId.Store.Synchronizer(
            adminProto.Synchronizer(
              adminProto.Synchronizer.Kind.PhysicalId(physicalSynchronizerId)
            )
          ) =>
        PhysicalSynchronizerId
          .fromProtoPrimitive(physicalSynchronizerId, fieldName)
          .map(id => TopologyStoreId.Synchronizer(id))

      case adminProto.StoreId.Store.Synchronizer(
            adminProto.Synchronizer(
              adminProto.Synchronizer.Kind.Empty
            )
          ) =>
        Left(ProtoDeserializationError.FieldNotSet(fieldName))
    }

  final case class Synchronizer private (id: Either[SynchronizerId, PhysicalSynchronizerId])
      extends TopologyStoreId {

    override def toProtoV30: adminProto.StoreId =
      adminProto.StoreId(
        adminProto.StoreId.Store.Synchronizer(
          adminProto.Synchronizer(
            id.bimap(
              logical => adminProto.Synchronizer.Kind.Id(logical.toProtoPrimitive),
              physical => adminProto.Synchronizer.Kind.PhysicalId(physical.toProtoPrimitive),
            ).merge
          )
        )
      )

    override private[canton] def toInternal(
        lookup: PSIdLookup
    ): Either[SynchronizerId, store.TopologyStoreId.SynchronizerStore] =
      id.fold(
        logical =>
          lookup
            .activePSIdFor(logical)
            .map(store.TopologyStoreId.SynchronizerStore(_))
            .toRight(logical),
        physical => Right(store.TopologyStoreId.SynchronizerStore(physical)),
      )

    private[canton] def logicalSynchronizerId: SynchronizerId = id.fold(identity, _.logical)
  }

  object Synchronizer {
    def apply(synchronizerId: SynchronizerId): Synchronizer = Synchronizer(Left(synchronizerId))
    def apply(physicalSynchronizerId: PhysicalSynchronizerId): Synchronizer =
      Synchronizer(Right(physicalSynchronizerId))

  }

  final case class Temporary(name: String185) extends TopologyStoreId {
    override def toProtoV30: adminProto.StoreId =
      adminProto.StoreId(
        adminProto.StoreId.Store.Temporary(adminProto.StoreId.Temporary(name.unwrap))
      )

    override private[canton] def toInternal(
        lookup: PSIdLookup
    ): Either[SynchronizerId, store.TopologyStoreId.TemporaryStore] =
      Right(store.TopologyStoreId.TemporaryStore(name))
  }

  object Temporary {
    def tryCreate(name: String): Temporary = Temporary(String185.tryCreate(name))

    def fromProtoV30(
        storeId: adminProto.StoreId.Temporary
    ): ParsingResult[Temporary] =
      ProtoConverter.parseLengthLimitedString(String185, storeId.name).map(Temporary(_))

  }

  case object Authorized extends TopologyStoreId {
    override def toProtoV30: adminProto.StoreId =
      adminProto.StoreId(adminProto.StoreId.Store.Authorized(adminProto.StoreId.Authorized()))

    override private[canton] def toInternal(
        lookup: PSIdLookup
    ): Either[SynchronizerId, store.TopologyStoreId.AuthorizedStore] =
      Right(store.TopologyStoreId.AuthorizedStore)
  }
}
