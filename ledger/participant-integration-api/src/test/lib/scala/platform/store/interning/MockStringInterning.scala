// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.interning

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.Party

/** This StringInterning implementation is interning in a transparent way everything it sees.
  * This is only for test purposes.
  */
class MockStringInterning extends StringInterning {
  private var idToString: Map[Int, String] = Map.empty
  private var stringToId: Map[String, Int] = Map.empty
  private var lastId: Int = 0

  private val rawStringInterning: StringInterningAccessor[String] =
    new StringInterningAccessor[String] {
      override def internalize(t: String): Int = tryInternalize(t).get

      override def tryInternalize(t: String): Option[Int] = synchronized {
        stringToId.get(t) match {
          case Some(id) => Some(id)
          case None =>
            lastId += 1
            idToString = idToString + (lastId -> t)
            stringToId = stringToId + (t -> lastId)
            Some(lastId)
        }
      }

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

  private[store] def reset(): Unit = synchronized {
    idToString = Map.empty
    stringToId = Map.empty
    lastId = 0
  }
}
