// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import scalaz.Id.Id

import scala.util.Try
import scala.{concurrent => sc}

/** A compatible layer for `scala.concurrent` with extra type parameters to
  * control `ExecutionContext`s.  Deliberately uses the same names as the
  * equivalent concepts in `scala.concurrent`.
  *
  * The trouble with [[sc.ExecutionContext]] is that it is used incoherently.
  * This leads to the problems described in
  * https://failex.blogspot.com/2020/05/global-typeclass-coherence-principles-3.html
  * .  The extension layer in this package adds a phantom type parameter to
  * `ExecutionContext` and related types, so that types can be used to
  * discriminate between ExecutionContexts at compile-time, and so Futures can
  * declare which ExecutionContext their operations are in.
  *
  * For Scala 2.12, you must pass `-Xsource:2.13` to scalac for methods and
  * conversions to be automatically found.  You must also
  * `import scalaz.syntax.bind._` or similar for Future methods like `map`,
  * `flatMap`, and so on.
  *
  * There are no constraints on the `EC` type variable; you need only declare
  * types you wish to use for it that are sufficient for describing the domains
  * in which you want ExecutionContexts to be discriminated.  These types will
  * never be instantiated, so you can simply declare that they exist.  They can
  * be totally separate, or have subtyping relationships; any subtyping
  * relationships they have will be reflected in equivalent subtyping
  * relationships between the resulting `ExecutionContext`s; if you declare
  * `sealed trait Elephant extends Animal`, then automatically
  * `ExecutionContext[Elephant] <: ExecutionContext[Animal]` with scalac
  * preferring the former when available (because it is "more specific").  They
  * can even be singleton types, so you might use `x.type` to suggest that the
  * context is associated with the exact value of the `x` variable.
  *
  * If you want to, say, refer to both [[sc.Future]] and [[concurrent.Future]]
  * in the same file, we recommend importing *the containing package* with an
  * alias rather than renaming each individual class you import.  For example,
  *
  * {{{
  *   import com.daml.concurrent._
  *   import scala.{concurrent => sc}
  *   // OR
  *   import scala.concurrent._
  *   import com.daml.{concurrent => dc}
  * }}}
  *
  * The exact name isn't important, but you should pick a short one that is
  * sufficiently suggestive for you.
  *
  * You should always be able to remove the substring `Of.Instance.T` from any
  * inferred type; we strongly suggest doing this for clarity.
  *
  * Demonstrations of the typing behavior can be found in FutureSpec and
  * ExecutionContextSpec.  This library has no interesting runtime
  * characteristics; you should think of it as exactly like `scala.concurrent`
  * in that regard.
  */
package object concurrent {

  /** Like [[scala.concurrent.Future]] but with an extra type parameter indicating
    * which [[ExecutionContext]] should be used for `map`, `flatMap` and other
    * operations.
    */
  type Future[-EC, +A] = FutureOf.Instance.T[EC, A]

  /** A subtype of [[sc.ExecutionContext]], more specific as `P` gets more
    * specific.
    */
  type ExecutionContext[+P] = ExecutionContextOf.Instance.T[P]
}

// keeping the companions with the same-named type aliases in same file
package concurrent {

  import java.util.concurrent.ExecutorService
  import scala.annotation.nowarn

  object Future {

    /** {{{
      *  Future[MyECTag] { expr }
      * }}}
      *
      * returns `Future[MyECTag, E]` where `E` is `expr`'s inferred type and
      * `ExecutionContext[MyECTag]` is required implicitly.
      */
    def apply[EC]: apply[EC] =
      new apply(())

    @nowarn("msg=dubious usage of method hashCode with unit value")
    final class apply[EC](private val ignore: Unit) extends AnyVal {
      def apply[A](body: => A)(implicit ec: ExecutionContext[EC]): Future[EC, A] =
        sc.Future(body)(ec)
    }

    def fromTry[EC, A](result: Try[A]): Future[EC, A] =
      sc.Future.fromTry(result)
  }

  object ExecutionContext {

    /** Explicitly tag an [[sc.ExecutionContext]], or replace the tag on an
      * [[ExecutionContext]].
      */
    def apply[EC](ec: sc.ExecutionContext): ExecutionContext[EC] =
      ExecutionContextOf.Instance.subst[Id, EC](ec)

    def fromExecutorService[EC](e: ExecutorService): ExecutionContext[EC] =
      apply(sc.ExecutionContext.fromExecutorService(e))

    val parasitic: ExecutionContext[Nothing] =
      ExecutionContext(sc.ExecutionContext.parasitic)
  }
}
