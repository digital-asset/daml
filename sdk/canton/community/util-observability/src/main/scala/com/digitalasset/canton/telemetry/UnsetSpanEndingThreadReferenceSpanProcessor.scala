// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.telemetry

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.context.Context
import io.opentelemetry.sdk.trace.{ReadWriteSpan, ReadableSpan, SpanProcessor}

import java.lang.reflect.Field
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/** This SpanProcessor unsets the field SdkSpan.spanEndingThread in the onEnd method, after it's not
  * used anymore by the SdkSpan class. The field is only relevant for the `Ending` phase of the
  * SdkSpan. However, it holds on to the thread long past the ending of the span, and therefore a
  * reference to the SdkSpan doesn't allow the thread to be garbage collected.
  */
class UnsetSpanEndingThreadReferenceSpanProcessor(override val loggerFactory: NamedLoggerFactory)
    extends SpanProcessor
    with NamedLogging {

  import UnsetSpanEndingThreadReferenceSpanProcessor.*
  // Signal that this span processor wants `onEnd` to be called.
  override def isEndRequired: Boolean = true

  // if there is an error, we only want to log it once, to avoid flooding the logs.
  @VisibleForTesting
  private[canton] val hasErrorBeenLogged = new AtomicBoolean(false)

  override def onEnd(span: ReadableSpan): Unit =
    // only try to unset the field if the span is an SdkSpan
    if (sdkSpanClass.isInstance(span)) {
      // try null-ing the spanEndingThread field. It's not useful anymore when onEnd is called.
      unsetSpanEndingThreadField(span)
    }

  // This method is only visible, so that we can call it directly with a dummy span
  // to test whether the logging behavior works as expected.
  @SuppressWarnings(
    Array("org.wartremover.warts.Null", "com.digitalasset.canton.RequireBlocking")
  )
  @VisibleForTesting
  private[canton] def unsetSpanEndingThreadField(span: ReadableSpan): Unit =
    try {
      // get the lock field value
      val lock = lockField.get(span)
      // acquire the lock
      lock.synchronized {
        // unset the field
        spanEndingThreadField.set(span, null)
      }
    } catch {
      case NonFatal(ex) =>
        if (!hasErrorBeenLogged.getAndSet(true)) {
          noTracingLogger.error(s"Error unsetting the spanEndingThread field for span $span", ex)
        }
    }

  override def isStartRequired: Boolean = false
  override def onStart(parentContext: Context, span: ReadWriteSpan): Unit = ()
}

object UnsetSpanEndingThreadReferenceSpanProcessor {
  // The SdkSpan class is not publicly accessible
  @VisibleForTesting
  val sdkSpanClass: Class[?] = Class.forName("io.opentelemetry.sdk.trace.SdkSpan")

  // The declared field spanEndingThread
  @VisibleForTesting
  val spanEndingThreadField: Field = {
    val f = sdkSpanClass.getDeclaredField("spanEndingThread")
    f.setAccessible(true)
    f
  }
  // The field `spanEndingThread` is guarded by the field `lock`,
  // so let's be a good citizen and only change it while holding the lock
  private val lockField = {
    val f = sdkSpanClass.getDeclaredField("lock")
    f.setAccessible(true)
    f
  }
}
