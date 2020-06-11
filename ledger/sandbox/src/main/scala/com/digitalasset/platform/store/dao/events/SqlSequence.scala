package com.daml.platform.store.dao.events

import com.daml.platform.store.SimpleSqlAsVectorOf._
import anorm.{ResultSetParser, Row, RowParser, SimpleSql}
import scalaz.{-\/, Free, Functor, \/-}

import java.sql.Connection

object SqlSequence {

  /** A sequence of `SimpleSql`s, terminating in A. */
  type T[A] = Free[Elt, A]

  // this representation is just trampolined Reader, but exposing that
  // would be unsound because it is _not_ distributive, and anyway
  // we may want to make the representation more explicit for more complex
  // analysis (e.g. applying polymorphic transforms to all contained SimpleSqls...)
  final class Elt[+A] private[SqlSequence] (private[SqlSequence] val run: Connection => A)

  object Elt {
    implicit final class syntax[A](private val self: T[A]) extends AnyVal {
      def executeSql(implicit conn: Connection): A = {
        @annotation.tailrec
        def go(self: T[A]): A =
          self.resume match {
            case -\/(elt) => go(elt.run(conn))
            case \/-(a) => a
          }
        go(self)
      }
    }

    implicit val covariant: Functor[Elt] = new Functor[Elt] {
      override def map[A, B](fa: Elt[A])(f: A => B) = new Elt(run = fa.run andThen f)
    }
  }

  def apply[A](s: SimpleSql[_], p: ResultSetParser[A]): T[A] =
    Free liftF new Elt(implicit conn => s as p)

  def vector[A](s: SimpleSql[Row], p: RowParser[A]): T[Vector[A]] =
    Free liftF new Elt(implicit conn => s asVectorOf p)

  def point[A](a: A): T[A] = Free point a
}
