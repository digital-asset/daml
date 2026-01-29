// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.Mutex
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party
import com.google.common.annotations.VisibleForTesting

/** This StringInterning implementation is interning in a transparent way everything it sees. This
  * is only for test purposes.
  */
@SuppressWarnings(Array("org.wartremover.warts.OptionPartial", "org.wartremover.warts.Var"))
@VisibleForTesting
class MockStringInterning extends StringInterning {
  @volatile private var idToString: Map[Int, String] = Map.empty
  @volatile private var stringToId: Map[String, Int] = Map.empty
  @volatile private var autoIntern: Boolean = true
  @volatile private var lastId: Int = 0
  private val lock = new Mutex()

  private val rawStringInterning: StringInterningAccessor[String] =
    new StringInterningAccessor[String] {
      override def internalize(t: String): Int = tryInternalize(t).get

      override def tryInternalize(t: String): Option[Int] = (lock.exclusive {
        stringToId.get(t) match {
          case Some(id) => Some(id)
          case None if autoIntern =>
            lastId += 1
            idToString = idToString + (lastId -> t)
            stringToId = stringToId + (t -> lastId)
            Some(lastId)
          case None => None
        }
      })

      override def externalize(id: Int): String = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[String] = idToString.get(id)

    }

  override val templateId: StringInterningDomain[Ref.NameTypeConRef] =
    new StringInterningDomain[Ref.NameTypeConRef] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Ref.NameTypeConRef): Int = tryInternalize(t).get

      override def tryInternalize(t: Ref.NameTypeConRef): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.NameTypeConRef = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Ref.NameTypeConRef] =
        rawStringInterning.tryExternalize(id).map(Ref.NameTypeConRef.assertFromString)
    }

  override val packageId: StringInterningDomain[Ref.PackageId] =
    new StringInterningDomain[Ref.PackageId] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Ref.PackageId): Int = tryInternalize(t).get

      override def tryInternalize(t: Ref.PackageId): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.PackageId = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Ref.PackageId] =
        rawStringInterning.tryExternalize(id).map(Ref.PackageId.assertFromString)
    }

  override def party: StringInterningDomain[Party] =
    new StringInterningDomain[Party] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Party): Int = tryInternalize(t).get

      override def tryInternalize(t: Party): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Party = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Party] =
        rawStringInterning.tryExternalize(id).map(Party.assertFromString)
    }

  override val synchronizerId: StringInterningDomain[SynchronizerId] =
    new StringInterningDomain[SynchronizerId] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: SynchronizerId): Int = tryInternalize(t).get

      override def tryInternalize(t: SynchronizerId): Option[Int] =
        rawStringInterning.tryInternalize(t.toProtoPrimitive)

      override def externalize(id: Int): SynchronizerId = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[SynchronizerId] =
        rawStringInterning.tryExternalize(id).map(SynchronizerId.tryFromString)
    }

  override val userId: StringInterningDomain[Ref.UserId] =
    new StringInterningDomain[Ref.UserId] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Ref.UserId): Int = tryInternalize(t).get

      override def tryInternalize(t: Ref.UserId): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.UserId = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Ref.UserId] =
        rawStringInterning.tryExternalize(id).map(Ref.UserId.assertFromString)
    }

  override val participantId: StringInterningDomain[Ref.ParticipantId] =
    new StringInterningDomain[Ref.ParticipantId] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Ref.ParticipantId): Int = tryInternalize(t).get

      override def tryInternalize(t: Ref.ParticipantId): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.ParticipantId = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Ref.ParticipantId] =
        rawStringInterning.tryExternalize(id).map(Ref.ParticipantId.assertFromString)
    }

  override val choiceName: StringInterningDomain[Ref.ChoiceName] =
    new StringInterningDomain[Ref.ChoiceName] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Ref.ChoiceName): Int = tryInternalize(t).get

      override def tryInternalize(t: Ref.ChoiceName): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.ChoiceName = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Ref.ChoiceName] =
        rawStringInterning.tryExternalize(id).map(Ref.ChoiceName.assertFromString)
    }

  override val interfaceId: StringInterningDomain[Ref.Identifier] =
    new StringInterningDomain[Ref.Identifier] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Ref.Identifier): Int = tryInternalize(t).get

      override def tryInternalize(t: Ref.Identifier): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.Identifier = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Ref.Identifier] =
        rawStringInterning.tryExternalize(id).map(Ref.Identifier.assertFromString)
    }

  private[store] def reset(): Unit = (lock.exclusive {
    idToString = Map.empty
    stringToId = Map.empty
    autoIntern = true
    lastId = 0
  })

  private[store] def setAutoIntern(newAutoIntern: Boolean): Unit = (lock.exclusive {
    autoIntern = newAutoIntern
  })
}
