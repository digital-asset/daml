// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.timestamps

import java.time.{Duration => JavaDuration}
import java.time.{Instant => JavaTimestamp}
import scala.concurrent.duration.{FiniteDuration => ScalaDuration}

// The import of the syntax is separate from the import of the instances.  It
// happens that because the two instances that used to be here are in TConv's
// companion, they are in non-orphan space and therefore don't need to be
// imported.  To achieve this effect for other TConv instances, you must put
// them in the companion of either of the relevant TConv instance's type
// parameters.
object AsScalaAsJava {
  // There are two sensible places to put the typeclass constraint: on the
  // implicit class, or on the method.
  //
  // Using the former, you can avoid extending the method namespace of arbitrary
  // values.  In these examples, that lets these asScala and asJava names
  // coexist with the ones from JavaConverters, rather than conflicting with
  // them; that's because the implicit conversion phase fails, so the methods
  // here are never considered unless the TConv instance is present.  The
  // drawback is that you can't use AnyVal subclasses.
  //
  // We even exploit the above lookup properties _in this very file_ by having
  // two asJava variations; it sort of calls out, even, why it is better to use
  // more globalizable method names.
  //
  // The latter is appropriate when you want the method name to more or less
  // "always mean the same thing"; that is, you don't want it to mean
  // arbitrarily different things based on the receiver type.  It also lets you
  // use AnyVal subclasses.  This is usually preferable unless you have a reason
  // *not* to reserve a method name.
  //
  // There is a hybrid approach: you can ask for the constraint at implicit
  // conversion time, *and then again* at method lookup time.  This requires you
  // to handwrite all your implicit conversions, and adds an extra implicit
  // lookup during compile-time at every call. Nevertheless, it can be used if
  // you want the semantics of the former approach, but it is critical that you
  // avoid the allocation of the implicit class intermediary. -SC
  implicit final class ToScalaDurationConversions[D](duration: D)(implicit
      cv: TConv[D, ScalaDuration]
  ) {
    def asScala: ScalaDuration = cv(duration)
  }

  implicit final class ToJavaDurationConversions[D](duration: D)(implicit
      cv: TConv[D, JavaDuration]
  ) {
    def asJava: JavaDuration = cv(duration)
  }

  implicit final class ToJavaTimestampConversions[D](duration: D)(implicit
      cv: TConv[D, JavaTimestamp]
  ) {
    def asJava: JavaTimestamp = cv(duration)
  }
}
