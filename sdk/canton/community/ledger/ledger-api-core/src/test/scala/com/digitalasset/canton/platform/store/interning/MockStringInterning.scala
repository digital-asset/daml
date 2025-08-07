// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party

import scala.concurrent.blocking

/** This StringInterning implementation is interning in a transparent way everything it sees. This
  * is only for test purposes.
  */
@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
class MockStringInterning extends StringInterning {
  private var idToString: Map[Int, String] = Map.empty
  private var stringToId: Map[String, Int] = Map.empty
  private var lastId: Int = 0

  private val rawStringInterning: StringInterningAccessor[String] =
    new StringInterningAccessor[String] {
      override def internalize(t: String): Int = tryInternalize(t).get

      override def tryInternalize(t: String): Option[Int] = blocking(synchronized {
        stringToId.get(t) match {
          case Some(id) => Some(id)
          case None =>
            lastId += 1
            idToString = idToString + (lastId -> t)
            stringToId = stringToId + (t -> lastId)
            Some(lastId)
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

  private[store] def reset(): Unit = blocking(synchronized {
    idToString = Map.empty
    stringToId = Map.empty
    lastId = 0
  })
}
