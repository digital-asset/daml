// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.nonempty

import com.digitalasset.canton.logging.pretty.Pretty
import pureconfig.{ConfigReader, ConfigWriter}

import scala.collection.immutable
import scala.reflect.ClassTag

/** Additional methods for [[com.daml.nonempty.NonEmpty]].
  *
  * Cats instances for [[com.daml.nonempty.NonEmpty]] must be imported explicitly as
  * `import `[[com.daml.nonempty.catsinstances]]`._` when necessary.
  */
object NonEmptyUtil {
  def fromUnsafe[A](xs: A with immutable.Iterable[_]): NonEmpty[A] =
    NonEmpty.from(xs).getOrElse(throw new NoSuchElementException)

  object instances {

    /** This instance is exposed as [[com.digitalasset.canton.logging.pretty.PrettyInstances.prettyNonempty]].
      * It lives only here because `NonEmptyColl.Instance.subst` is private to the `nonempty` package
      */
    def prettyNonEmpty[A](implicit F: Pretty[A]): Pretty[NonEmpty[A]] = {
      type K[T[_]] = Pretty[T[A]]
      NonEmptyColl.Instance.subst[K](F)
    }

    implicit def nonEmptyPureConfigReader[C <: scala.collection.immutable.Iterable[_]](implicit
        reader: ConfigReader[C],
        ct: ClassTag[C],
    ): ConfigReader[NonEmpty[C]] =
      reader.emap(c => NonEmpty.from(c).toRight(EmptyCollectionFound(ct.toString)))

    implicit def nonEmptyPureConfigWriter[C](implicit
        writer: ConfigWriter[C]
    ): ConfigWriter[NonEmpty[C]] =
      writer.contramap(_.forgetNE)
  }

  /** A failure representing an unexpected empty collection
    *
    * @param typ
    *   the type that was attempted to be converted to from an empty string
    */
  final case class EmptyCollectionFound(typ: String) extends pureconfig.error.FailureReason {
    override def description = s"Empty collection found when trying to convert to NonEmpty[$typ]."
  }
}
