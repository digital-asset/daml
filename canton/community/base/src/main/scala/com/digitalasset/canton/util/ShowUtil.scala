// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Show
import cats.Show.Shown
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedString,
  LengthLimitedStringWrapper,
}
import com.digitalasset.canton.logging.pretty.Pretty

import scala.annotation.tailrec

/** Utility class for clients who want to '''make use''' of pretty printing.
  * Import this as follows:
  * <pre>
  * import com.digitalasset.canton.util.ShowUtil._
  * </pre>
  * In some cases, an import at the top of the file will not make the `show` interpolator available.
  * To work around this, you need to import this INSIDE of the using class.
  *
  * To enforce pretty printing, the `show` interpolator should be used for creating strings.
  * That is, `show"s\$myComplexObject"` will result in a compile error,
  * if pretty printing is not implemented for `myComplexObject`.
  * In contrast, `s"\$myComplexObject"` will fall back to the default (non-pretty) toString implementation,
  * if pretty printing is not implemented for `myComplexObject`.
  * Even if pretty printing is implemented for the type `T` of `myComplexObject`,
  * `s"\$myComplexObject"` will not use it, if the compiler fails to infer `T: Pretty`.
  */
object ShowUtil extends ShowUtil {
  val NL: Shown = Shown(System.lineSeparator())
  val HashLength = 12
}

trait ShowUtil extends cats.syntax.ShowSyntax {
  import ShowUtil.*

  /** Enables the syntax `show"This object is pretty: \$myPrettyType"`.
    */
  implicit def showPretty[T: Pretty]: Show[T] = {
    import Pretty.PrettyOps
    Show.show(_.toPrettyString())
  }

  /** Enables syntax like
    * `show"This is a string: \${myString.doubleQuoted}"`
    * and
    * `show"This is a hash: \${myHash.readableHash}"`.
    */
  abstract class StringOperators(s: String) {

    /** Use this to quote names. (E.g. Domain 'myDomain')
      */
    def singleQuoted: Shown = Shown("'" + s + "'")

    /** Use this to quote string constants, to separate them from the embedding sentence.
      * (E.g. the request failed with "index out of bounds".)
      */
    def doubleQuoted: Shown = Shown("\"" + s + "\"")

    def unquoted: Shown = Shown(s)

    def readableHash: Shown =
      Shown(if (s.length <= HashLength) s else s.take(HashLength) + "...")

    def limit(maxLength: Int): Shown =
      Shown(if (s.length <= maxLength) s else s.take(maxLength) + "...")

    def readableLoggerName(maxLength: Int): Shown = {
      @tailrec
      def go(result: String): String = {
        if (result.length <= maxLength) {
          result
        } else {
          val newResult = result.replaceFirst("^(([a-z]\\.)*[a-z])[a-zA-Z0-9-]*", "$1")
          if (newResult == result) {
            result
          } else {
            go(newResult)
          }
        }
      }

      Shown(go(s))
    }
  }

  implicit class ShowStringSyntax(s: String) extends StringOperators(s)
  implicit class ShowLengthLimitedStringSyntax(s: LengthLimitedString)
      extends StringOperators(s.str)
  implicit class ShowLengthLimitedStringWrapperSyntax(s: LengthLimitedStringWrapper)
      extends StringOperators(s.unwrap)

  /** Enables the syntax `show"\${myEither.showMerged}"`.
    */
  implicit class ShowEitherSyntax[L: Show, R: Show](e: Either[L, R]) {

    /** Prints the (left or right) value of an either without indicating whether it is left or right.
      */
    def showMerged: Shown = Shown(e.fold(_.show, _.show))
  }

  implicit class ShowOptionSyntax[T: Show](o: Option[T]) {
    def showValueOrNone: Shown =
      Shown(o match {
        case Some(value) => value.show
        case None => "None"
      })

    def showValue: Shown =
      Shown(o match {
        case Some(value) => value.show
        case None => ""
      })
  }

  /** Enables syntax like `show"Found several elements: \${myCollection.mkShow()}"`.
    */
  implicit class ShowTraversableSyntax[T: Show](trav: Iterable[T]) {

    /** Like `IterableOnce.mkString(String)` with the difference that
      * individual elements are mapped to strings by using `show` is used instead of `toString`.
      */
    def mkShow(sep: String = ", "): Shown = Shown(trav.map(_.show).mkString(sep))

    /** Like `TraversableOnce.mkString(String, String, String)` with the difference that
      * individual elements are mapped to strings by using `show` is used instead of `toString`.
      */
    def mkShow(start: String, sep: String, end: String): Shown = Shown(
      trav.map(_.show).mkString(start, sep, end)
    )

    def limit(n: Int): Iterable[Shown] = {
      val (prefix, remainder) = trav.splitAt(n)
      val ellipsis = if (remainder.isEmpty) Seq.empty else Seq(Shown("..."))
      prefix.map(e => e: Shown) ++ ellipsis
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Product"))
  implicit class ShowProductSyntax(any: Product) {
    def showWithAdHocPrettyInstance: Shown = {
      implicit val prettyAny: Pretty[Product] = Pretty.adHocPrettyInstance
      Shown(any.show)
    }
  }

  implicit class ShowAnyRefSyntax(anyRef: AnyRef) {
    def showType: Shown = Shown(anyRef.getClass.getSimpleName)
  }
}
