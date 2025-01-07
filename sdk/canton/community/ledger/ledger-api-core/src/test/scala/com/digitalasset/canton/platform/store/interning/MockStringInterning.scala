// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.Party

import scala.concurrent.blocking

/** This StringInterning implementation is interning in a transparent way everything it sees.
  * This is only for test purposes.
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

  override val templateId: StringInterningDomain[Ref.Identifier] =
    new StringInterningDomain[Ref.Identifier] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Ref.Identifier): Int = tryInternalize(t).get

      override def tryInternalize(t: Ref.Identifier): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.Identifier = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Ref.Identifier] =
        rawStringInterning.tryExternalize(id).map(Ref.Identifier.assertFromString)
    }

  override val packageName: StringInterningDomain[Ref.PackageName] =
    new StringInterningDomain[Ref.PackageName] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Ref.PackageName): Int = tryInternalize(t).get

      override def tryInternalize(t: Ref.PackageName): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.PackageName = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Ref.PackageName] =
        rawStringInterning.tryExternalize(id).map(Ref.PackageName.assertFromString)
    }

  override val packageVersion: StringInterningDomain[Ref.PackageVersion] =
    new StringInterningDomain[Ref.PackageVersion] {
      override val unsafe: StringInterningAccessor[String] = rawStringInterning

      override def internalize(t: Ref.PackageVersion): Int = tryInternalize(t).get

      override def tryInternalize(t: Ref.PackageVersion): Option[Int] =
        rawStringInterning.tryInternalize(t.toString)

      override def externalize(id: Int): Ref.PackageVersion = tryExternalize(id).get

      override def tryExternalize(id: Int): Option[Ref.PackageVersion] =
        rawStringInterning.tryExternalize(id).map(Ref.PackageVersion.assertFromString)
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
