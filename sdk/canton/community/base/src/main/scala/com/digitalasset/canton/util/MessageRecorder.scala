// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext

import java.io.*
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.concurrent.blocking
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/** Persists data for replay tests.
  */
class MessageRecorder(
    override protected val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends FlagCloseable
    with NamedLogging {

  val streamRef: AtomicReference[Option[ObjectOutputStream]] = new AtomicReference(None)

  def startRecording(destination: Path)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Start recording to $destination")

    sys.addShutdownHook {
      // This is important to not lose messages, if SIGTERM is sent to Canton.
      if (streamRef.get().isDefined) {
        stopRecording()
      }
    }.discard

    val stream = new ObjectOutputStream(
      new BufferedOutputStream(new FileOutputStream(destination.toFile))
    )

    val started = streamRef.compareAndSet(None, Some(stream))

    if (started) {
      logger.debug(s"Started recording to $destination")
    } else {
      stream.close()
      ErrorUtil.internalError(new IllegalStateException("Already recording."))
    }
  }

  /** Serializes and saves the provided message to the output stream.
    * This method is synchronized as the write operations on the underlying [[java.io.ObjectOutputStream]] are not thread safe.
    */
  def record(message: Serializable): Unit = blocking(synchronized {
    streamRef.get().foreach(_.writeObject(message))
  })

  def stopRecording()(implicit traceContext: TraceContext): Unit = {
    logger.debug("Stopping recording...")
    streamRef.getAndSet(None) match {
      case Some(stream) =>
        blocking(synchronized { stream.close() })
      case None =>
        logger.info("Recorder has not been recording.")
    }
  }

  override def onClosed(): Unit = stopRecording()(TraceContext.empty)
}

object MessageRecorder {

  private val loader: ClassLoader = Thread.currentThread().getContextClassLoader

  /** Yields a list containing all messages stored at `source`.
    * Be aware that the method loads all messages into memory. This is tailored to performance testing,
    * because it allows for loading messages before starting performance measurements.
    *
    * @throws java.lang.ClassCastException if a message is not of type `T`
    */
  def load[T <: Serializable](source: Path, logger: TracedLogger)(implicit
      classTag: ClassTag[T],
      traceContext: TraceContext,
  ): List[T] = {
    ResourceUtil.withResource(new BufferedInputStream(new FileInputStream(source.toFile))) {
      rawStream =>
        ResourceUtil.withResource(new ObjectInputStream(rawStream) {
          // Workaround for known bugs in the deserialization framework.
          // https://github.com/scala/bug/issues/9777
          // https://bugs.openjdk.java.net/browse/JDK-8024931
          override def resolveClass(desc: ObjectStreamClass): Class[_] =
            Class.forName(desc.getName, false, loader)
        }) { stream =>
          val builder = List.newBuilder[T]

          @SuppressWarnings(Array("org.wartremover.warts.Return"))
          @tailrec def go(): Unit =
            if (rawStream.available() > 0) {
              Try(stream.readObject()) match {
                case Success(classTag(message)) => builder += message
                case Success(_) =>
                  throw new ClassCastException(s"Unable to cast message to type $classTag.")
                case Failure(_: EOFException) =>
                  // This can occur if Canton is killed during recording. Just stop reading.
                  logger.info(
                    s"Unexpected EOF while reading messages from $source. Discarding rest of input..."
                  )
                  return
                case Failure(e) =>
                  logger.error(s"An unexpected exception occurred while reading from $source", e)
                  val numSuccessful = builder.result().size
                  logger.error(s"Number of successfully read messages is $numSuccessful.")
                  throw e
              }
              go()
            } else {}
          go()

          builder.result()
        }
    }
  }
}
