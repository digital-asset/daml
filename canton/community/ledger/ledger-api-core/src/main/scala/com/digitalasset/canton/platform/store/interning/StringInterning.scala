// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.interning

import com.digitalasset.canton.platform.{Identifier, Party}
import com.digitalasset.canton.topology.DomainId

/** The facade for all supported string-interning domains
  *
  * @note The accessors defined in this interface are thread-safe and can
  *       be used concurrently with [[StringInterningView.internize]] and [[StringInterningView.update]].
  */
trait StringInterning {
  def templateId: StringInterningDomain[Identifier]
  def party: StringInterningDomain[Party]
  def domainId: StringInterningDomain[DomainId]
}

/** Composes a StringInterningAccessor for the domain-string type and an unsafe StringInterningAccessor for raw strings
  *
  * @tparam T is the type of the string-related domain object which is interned
  */
trait StringInterningDomain[T] extends StringInterningAccessor[T] {
  def unsafe: StringInterningAccessor[String]
}

object StringInterningDomain {
  private[interning] def prefixing[T](
      prefix: String,
      prefixedAccessor: StringInterningAccessor[String],
      to: String => T,
      from: T => String,
  ): StringInterningDomain[T] =
    new StringInterningDomain[T] {
      override val unsafe: StringInterningAccessor[String] = new StringInterningAccessor[String] {
        override def internalize(t: String): Int = prefixedAccessor.internalize(prefix + t)

        override def tryInternalize(t: String): Option[Int] =
          prefixedAccessor.tryInternalize(prefix + t)

        override def externalize(id: Int): String =
          prefixedAccessor.externalize(id).substring(prefix.length)

        override def tryExternalize(id: Int): Option[String] =
          prefixedAccessor.tryExternalize(id).map(_.substring(prefix.length))
      }

      override def internalize(t: T): Int = unsafe.internalize(from(t))

      override def tryInternalize(t: T): Option[Int] = unsafe.tryInternalize(from(t))

      override def externalize(id: Int): T = to(unsafe.externalize(id))

      override def tryExternalize(id: Int): Option[T] = unsafe.tryExternalize(id).map(to)
    }
}

/** The main interface for using string-interning.
  * Client code can use this to map between interned id-s and string-domain objects back and forth
  *
  * @tparam T is the type of the string-related domain object which is interned
  */
trait StringInterningAccessor[T] {

  /** Get the interned id
    *
    * @param t the value
    * @return the integer id, throws exception if id not found
    */
  def internalize(t: T): Int

  /** Optionally get the interned id
    * @param t the value
    * @return some integer id, or none if not found
    */
  def tryInternalize(t: T): Option[Int]

  /** Get the value for an id
    *
    * @param id integer id
    * @return the value, throws exception if no value found
    */
  def externalize(id: Int): T

  /** Optionally get the value for an id
    *
    * @param id integer id
    * @return some value, or none if not found
    */
  def tryExternalize(id: Int): Option[T]
}
