// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import better.files.*
import better.files.File.newTemporaryFile
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.grpc.ByteStringStreamObserverWithContext
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.TryUtil.ForFailedOps
import com.digitalasset.canton.version.{
  HasRepresentativeProtocolVersion,
  HasVersionedMessageCompanion,
  VersioningCompanion,
}
import com.google.protobuf.ByteString
import io.grpc.Context
import io.grpc.stub.StreamObserver
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Source, Source as PekkoSource}

import java.io.{
  BufferedInputStream,
  ByteArrayInputStream,
  ByteArrayOutputStream,
  InputStream,
  OutputStream,
  PipedInputStream,
  PipedOutputStream,
}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success, Try}

object GrpcStreamingUtils {
  private final val defaultChunkSize: Int =
    1024 * 1024 * 2 // 2MB - This is half of the default max message size of gRPC

  def streamFromClient[Req, Resp, C](
      extractChunkBytes: Req => ByteString,
      extractContext: Req => C,
      processFullRequest: (ByteString, C) => Future[Resp],
      responseObserver: StreamObserver[Resp],
      processingTimeout: Duration = DefaultProcessingTimeouts.unbounded.duration,
  )(implicit ec: ExecutionContext): StreamObserver[Req] = {

    val observer =
      new ByteStringStreamObserverWithContext[Req, C](extractChunkBytes, extractContext) {
        override def onCompleted(): Unit = {
          super.onCompleted()
          val responseF = this.result.flatMap {
            case Some((byteString, context)) =>
              processFullRequest(byteString, context)
            case None =>
              Future.failed(new NoSuchElementException("No elements were received in stream"))
          }

          Try(Await.result(responseF, processingTimeout)) match {
            case Failure(exception) => responseObserver.onError(exception)
            case Success(response) =>
              responseObserver.onNext(response)
              responseObserver.onCompleted()
          }
        }
      }

    observer
  }

  /** Streams requests of gzipped bytes from the client in multiple stages, allowing efficient
    * memory usage.
    *
    *   1. The first request can contain some context `RequestContext`, which is extracted with
    *      `contextFromFirstElement` and passed along with the source.
    *   1. The gzipped bytes from each request are extracted with `getGzippedBytes`.
    *   1. Messages from the decompressed InputStream can be extracted via `parseMessage`.
    *   1. The source, which the caller uses for further processing, continually parses new messages
    *      from the decompressed input stream.
    *
    * @param responseObserver
    *   the response observer to signal errors or completion to the client
    * @param responseIfNoRequests
    *   the response if no requests were actually submitted.
    * @param getGzippedBytes
    *   the extractor method to get a `ByteString` from the request `Req`
    * @param parseMessage
    *   should return `None` if there is no more input to read, `Some(Right(_))` for a successful
    *   parse, and `Some(Left(_))` if an error occurred during parsing. Any parsing errors bubbled
    *   up and the processing is aborted.
    * @param contextFromFirstRequest
    *   extract context from the first request
    * @param action
    *   the main processing pipeline
    */
  def streamGzippedChunksFromClient[Req, Resp, RequestContext, ParsedMessage](
      responseObserver: StreamObserver[Resp],
      responseIfNoRequests: Try[Resp],
      getGzippedBytes: Req => ByteString,
      parseMessage: InputStream => Option[ParsingResult[ParsedMessage]],
  )(
      contextFromFirstRequest: Req => Try[RequestContext]
  )(action: (RequestContext, Source[ParsedMessage, NotUsed]) => FutureUnlessShutdown[Resp])(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
  ): StreamObserver[Req] = {
    // for extracting the context on the first request and creating the pekko source
    val isFirst = new AtomicBoolean(true)
    // this Piped*Stream setup connects the incoming requests containing the gzipped bytes
    // with the pekko parsing source
    val output = new PipedOutputStream()
    val input = new PipedInputStream(output)

    // when reporting the error, also close the output stream, since there will be no more data to be processed
    def reportError(err: Throwable): Unit = {
      responseObserver.onError(err)
      output.close()
    }

    // blocking write the request bytes to the output stream
    def writeRequestBytes(request: Req): Unit = Try {
      // gRPC backpressure works by "blocking" the onNext call.
      blocking(getGzippedBytes(request).writeTo(output))
    }.forFailed(reportError)

    new StreamObserver[Req] {
      override def onNext(value: Req): Unit =
        if (isFirst.getAndSet(false)) {
          // only extract the context from the first request
          contextFromFirstRequest(value) match {
            case Failure(exception) =>
              reportError(exception)

            case Success(context) =>
              // hold a lazy reference, because the constructor of GZIPInputStream immediately executes a blocking read
              lazy val gunzip = new GzipCompressorInputStream(input)
              // construct the source by repeatedly reading from the gunzip input stream until there's no more data.
              // we don't really have a "state", so we use Unit.
              val parsedMessagesSource = PekkoSource.unfold(())(_ =>
                blocking(parseMessage(gunzip))
                  .map {
                    case Left(err) =>
                      // throw the parsing error to cancel the source
                      throw ProtoDeserializationError.ProtoDeserializationFailure
                        .Wrap(err)
                        .asGrpcError
                    case Right(value) =>
                      () -> value
                  }
              )
              // the main processing pipeline is also only triggered after the first request
              val resultFUS = action(context, parsedMessagesSource)
              GrpcStreamingUtils.futureUnlessShutdownToStreamObserver(
                resultFUS.thereafter { _ =>
                  // after the main processing pipeline is finished for whatever reason, we can safely close the input
                  input.close()
                },
                responseObserver,
              )
              writeRequestBytes(value)
          }
        } else writeRequestBytes(value)

      override def onError(t: Throwable): Unit =
        reportError(t)

      override def onCompleted(): Unit =
        if (isFirst.get()) {
          // If no request has been received (which means that the main processing pipeline has not been triggered),
          // complete the stream according to `responseIfNoRequests` and close the Piped*Streams.
          GrpcStreamingUtils.futureUnlessShutdownToStreamObserver(
            FutureUnlessShutdown.fromTry(responseIfNoRequests.thereafter { _ =>
              input.close()
              output.close()
            }),
            responseObserver,
          )
        } else {
          // if any requests were processed, simply close the output pipe. the final response will be triggered by the
          // processing pipeline
          output.close()
        }
    }
  }

  /** Stream the provided bytestring to the server. To avoid loading the whole byte string into
    * memory, the other variant of `streamToServer` can be used.
    *
    * @param load
    *   Loader (endpoint of the service)
    * @param requestBuilder
    *   Builds a request from an array of bytes
    * @param byteString
    *   The byte string to be streamed.
    */
  def streamToServer[Req, Resp](
      load: StreamObserver[Resp] => StreamObserver[Req],
      requestBuilder: Array[Byte] => Req,
      byteString: ByteString,
  ): Future[Resp] = {
    val it = byteString.toByteArray.grouped(defaultChunkSize)
    def readNextChunk(): Option[Either[Throwable, Req]] =
      it.nextOption().map(bytes => Right(requestBuilder(bytes)))

    streamToServer(load, _ => readNextChunk())
  }

  /** Stream data to the server
    * @param load
    *   Loader (endpoint of the service)
    * @param readNextChunk
    *   Method to read the data to be streamed. Streaming happens as long as the method returns
    *   Some(Right()).
    *   - If Some(Left()) is encountered, the stream is terminated with an error.
    *   - When None is encountered, the stream is completed.
    */
  def streamToServer[Req, Resp](
      load: StreamObserver[Resp] => StreamObserver[Req],
      readNextChunk: Unit => Option[Either[Throwable, Req]],
  ): Future[Resp] = {
    val requestComplete = Promise[Resp]()
    val ref = new AtomicReference[Option[Resp]](None)

    val responseObserver = new StreamObserver[Resp] {
      override def onNext(value: Resp): Unit =
        ref.set(Some(value))

      override def onError(t: Throwable): Unit = requestComplete.failure(t)

      override def onCompleted(): Unit =
        ref.get() match {
          case Some(response) => requestComplete.success(response)
          case None =>
            requestComplete.failure(
              io.grpc.Status.CANCELLED
                .withDescription("Server completed the request before providing a response")
                .asRuntimeException()
            )
        }
    }
    val requestObserver = load(responseObserver)

    @tailrec
    def read(): Unit =
      readNextChunk(()) match {
        case Some(Right(nextRequest)) =>
          blocking(requestObserver.onNext(nextRequest))
          read()

        case Some(Left(error)) =>
          requestObserver.onError(error)

        case None =>
          requestObserver.onCompleted()
      }

    read()
    requestComplete.future
  }

  def streamToClient[T](
      responseF: OutputStream => Future[Unit],
      responseObserver: StreamObserver[T],
      fromByteString: FromByteString[T],
      processingTimeout: Duration = DefaultProcessingTimeouts.unbounded.duration,
      chunkSizeO: Option[Int] = None,
  )(implicit ec: ExecutionContext): Unit = {
    val context = io.grpc.Context
      .current()
      .withCancellation()

    val outputStream = new ByteArrayOutputStream()
    context.run { () =>
      val processingResult = responseF(outputStream).map { _ =>
        val chunkSize = chunkSizeO.getOrElse(defaultChunkSize)
        val inputStream = new ByteArrayInputStream(outputStream.toByteArray)
        streamResponseChunks(context, responseObserver)(
          new BufferedInputStream(inputStream),
          chunkSize,
          fromByteString,
        )
      }
      finishStream(context, responseObserver)(processingResult, processingTimeout)
    }
  }

  def streamToClientFromFile[T](
      responseF: File => Future[Unit],
      responseObserver: StreamObserver[T],
      fromByteString: FromByteString[T],
      processingTimeout: Duration = DefaultProcessingTimeouts.unbounded.duration,
      chunkSizeO: Option[Int] = None,
  )(implicit ec: ExecutionContext): Unit = {
    val file = newTemporaryFile()

    val context = io.grpc.Context
      .current()
      .withCancellation()

    context.run { () =>
      val processingResult = responseF(file).map { _ =>
        val chunkSize = chunkSizeO.getOrElse(defaultChunkSize)
        streamResponseChunks(context, responseObserver)(
          file.newInputStream.buffered(chunkSize),
          chunkSize,
          fromByteString,
        )
      }
      finishStream(context, responseObserver)(processingResult, processingTimeout)
    }
  }

  /** Deserializes versioned message instances from a given stream.
    *
    * IMPORTANT: Expects data in the input stream that has been serialized with
    * [[com.digitalasset.canton.version.HasProtocolVersionedWrapper#writeDelimitedTo]]! Otherwise,
    * you'll get weird deserialization behaviour without errors, or you'll observe misaligned
    * message fields and message truncation errors result from having used
    * [[scalapb.GeneratedMessage#writeDelimitedTo]] directly.
    *
    * @return
    *   either an error, or a list of versioned message instances
    */
  def parseDelimitedFromTrusted[ValueClass <: HasRepresentativeProtocolVersion](
      stream: InputStream,
      objectType: VersioningCompanion[ValueClass],
  ): Either[String, List[ValueClass]] =
    parseDelimitedFromTrustedInternal(stream, objectType.parseDelimitedFromTrusted)

  /** Deserializes versioned message instances from a given stream.
    *
    * IMPORTANT: Expects data in the input stream that has been serialized with
    * [[com.digitalasset.canton.version.HasVersionedWrapper#writeDelimitedTo]]! Otherwise, you'll
    * get weird deserialization behaviour without errors, or you'll observe misaligned message
    * fields and message truncation errors result from having used
    * [[scalapb.GeneratedMessage#writeDelimitedTo]] directly.
    *
    * @return
    *   either an error, or a list of versioned message instances
    */
  def parseDelimitedFromTrusted[ValueClass](
      stream: InputStream,
      objectType: HasVersionedMessageCompanion[ValueClass],
  ): Either[String, List[ValueClass]] =
    parseDelimitedFromTrustedInternal(stream, objectType.parseDelimitedFromTrusted)

  private def parseDelimitedFromTrustedInternal[ValueClass](
      stream: InputStream,
      parser: InputStream => Option[ParsingResult[ValueClass]],
  ): Either[String, List[ValueClass]] = {
    // Assume we can load all parsed messages into memory
    @tailrec
    def read(acc: List[ValueClass]): Either[String, List[ValueClass]] =
      parser(stream) match {
        case Some(parsed) =>
          parsed match {
            case Left(parseError) =>
              Left(parseError.message)
            case Right(value) =>
              // Prepend for efficiency!
              read(value :: acc)
          }
        case None =>
          Right(acc.reverse)
      }
    read(Nil)
  }

  private def streamResponseChunks[T](
      context: Context.CancellableContext,
      responseObserver: StreamObserver[T],
  )(
      inputStream: InputStream,
      chunkSize: Int,
      fromByteString: FromByteString[T],
  ): Unit =
    inputStream.autoClosed { s =>
      Iterator
        .continually(s.readNBytes(chunkSize))
        // Before pushing new chunks to the stream, keep checking that the context has not been cancelled
        // This avoids the server reading the entire dump file for nothing if the client has already cancelled
        .takeWhile(_.nonEmpty && !context.isCancelled)
        .foreach { byteArray =>
          val chunk: ByteString = ByteString.copyFrom(byteArray)
          responseObserver.onNext(fromByteString.toT(chunk))
        }
    }

  private def finishStream[T](
      context: Context.CancellableContext,
      responseObserver: StreamObserver[T],
  )(f: Future[Unit], timeout: Duration): Unit =
    Try(Await.result(f, timeout)) match {
      case Failure(exception) =>
        responseObserver.onError(exception)
        context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
        ()
      case Success(_) =>
        if (!context.isCancelled) responseObserver.onCompleted()
        else {
          context.cancel(new io.grpc.StatusRuntimeException(io.grpc.Status.CANCELLED))
          ()
        }
    }

  def futureUnlessShutdownToStreamObserver[A](
      fus: FutureUnlessShutdown[A],
      observer: StreamObserver[A],
  )(implicit ec: ExecutionContext, elc: ErrorLoggingContext): Unit = fus.unwrap.onComplete {
    case Failure(exception) => observer.onError(exception)
    case Success(Outcome(value)) =>
      observer.onNext(value)
      observer.onCompleted()
    case Success(AbortedDueToShutdown) =>
      observer.onError(GrpcErrors.AbortedDueToShutdown.Error().asGrpcError)
  }
}

// Define a type class for converting ByteString to the generic type T
trait FromByteString[T] {
  def toT(chunk: ByteString): T
}
