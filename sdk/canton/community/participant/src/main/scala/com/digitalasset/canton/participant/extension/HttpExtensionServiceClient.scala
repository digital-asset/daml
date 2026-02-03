// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.ExtensionServiceConfig
import com.digitalasset.canton.tracing.TraceContext

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.UUID
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Random, Try}

/** HTTP client implementation for extension services with retry logic,
  * JWT authentication, and TLS support.
  *
  * @param extensionId The extension identifier (key from config map)
  * @param config Configuration for this extension service
  * @param sharedHttpClient Shared HTTP client with connection pooling
  * @param loggerFactory Logger factory
  */
class HttpExtensionServiceClient(
    override val extensionId: String,
    config: ExtensionServiceConfig,
    sharedHttpClient: HttpClient,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ExtensionServiceClient
    with NamedLogging {

  // Construct the endpoint URL
  private val scheme = if (config.useTls) "https" else "http"
  private val endpoint: URI = URI.create(s"$scheme://${config.host}:${config.port}/api/v1/external-call")

  // Load JWT token from config or file
  private lazy val jwtToken: Option[String] = {
    config.jwt.orElse {
      config.jwtFile.flatMap { path =>
        Try {
          new String(Files.readAllBytes(path)).trim
        }.toOption
      }
    }
  }

  // Declared function config hashes for validation
  private val declaredConfigHashes: Map[String, String] =
    config.declaredFunctions.map(f => f.functionId -> f.configHash).toMap

  override def getDeclaredConfigHash(functionId: String): Option[String] =
    declaredConfigHashes.get(functionId)

  override def call(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): FutureUnlessShutdown[Either[ExtensionCallError, String]] = {
    FutureUnlessShutdown.outcomeF {
      Future {
        blocking {
          callWithRetry(functionId, configHash, input, mode)
        }
      }
    }
  }

  override def validateConfiguration()(implicit tc: TraceContext): FutureUnlessShutdown[ExtensionValidationResult] = {
    FutureUnlessShutdown.outcomeF {
      Future {
        blocking {
          // Try to make a simple health check call
          // For now, we just verify we can establish a connection
          try {
            val requestId = generateRequestId()
            val requestBuilder = HttpRequest
              .newBuilder()
              .uri(endpoint)
              .timeout(Duration.ofMillis(config.connectTimeout.toMillis))
              .header("Content-Type", "application/octet-stream")
              .header("X-Daml-External-Function-Id", "_health")
              .header("X-Daml-External-Config-Hash", "")
              .header("X-Daml-External-Mode", "validation")
              .header(config.requestIdHeader, requestId)

            jwtToken.foreach { token =>
              requestBuilder.header("Authorization", s"Bearer $token")
            }

            val req = requestBuilder
              .POST(HttpRequest.BodyPublishers.ofString(""))
              .build()

            // We don't really care about the response, just that we can connect
            val resp = sharedHttpClient.send(req, HttpResponse.BodyHandlers.ofString())

            // Any response (even 4xx) means the service is reachable
            if (resp.statusCode() >= 200 && resp.statusCode() < 600) {
              ExtensionValidationResult.Valid
            } else {
              ExtensionValidationResult.Invalid(Seq(s"Unexpected response code: ${resp.statusCode()}"))
            }
          } catch {
            case e: java.net.ConnectException =>
              ExtensionValidationResult.Invalid(Seq(s"Cannot connect to extension service: ${e.getMessage}"))
            case e: java.net.http.HttpTimeoutException =>
              ExtensionValidationResult.Invalid(Seq(s"Connection timeout: ${e.getMessage}"))
            case e: Exception =>
              ExtensionValidationResult.Invalid(Seq(s"Validation failed: ${e.getMessage}"))
          }
        }
      }
    }
  }

  /** Generate a unique request ID for tracking */
  private def generateRequestId(): String = UUID.randomUUID().toString

  /** Make an HTTP call with retry logic */
  private def callWithRetry(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): Either[ExtensionCallError, String] = {
    val deadlineMs = System.currentTimeMillis() + config.maxTotalTimeout.toMillis

    def loop(attempt: Int, lastError: Option[ExtensionCallError]): Either[ExtensionCallError, String] = {
      val nowMs = System.currentTimeMillis()
      if (nowMs >= deadlineMs) {
        val finalError = lastError.getOrElse(
          ExtensionCallError(504, "Total timeout exceeded", None)
        )
        logger.error(
          s"External call to extension '$extensionId' exceeded maximum total timeout of ${config.maxTotalTimeout} after $attempt attempts: ${finalError.message}"
        )
        Left(finalError)
      } else if (attempt > config.maxRetries.value) {
        val finalError = lastError.getOrElse(
          ExtensionCallError(500, "Max retries exceeded", None)
        )
        logger.error(
          s"External call to extension '$extensionId' failed after ${config.maxRetries} retries: ${finalError.message} (status=${finalError.statusCode})"
        )
        Left(finalError)
      } else {
        val result = singleCall(functionId, configHash, input, mode)

        result match {
          case Right(response) =>
            if (attempt > 0) {
              logger.info(s"External call to extension '$extensionId' succeeded after $attempt retries")
            }
            Right(response)

          case Left(error) if shouldRetry(error) && attempt < config.maxRetries.value =>
            val remainingTimeMs = deadlineMs - System.currentTimeMillis()
            val delay = calculateBackoff(attempt + 1, error.retryAfter, remainingTimeMs)

            if (delay >= remainingTimeMs) {
              logger.warn(
                s"External call to extension '$extensionId' failed (attempt ${attempt + 1}/${config.maxRetries}): ${error.message} (status=${error.statusCode}). Cannot retry: insufficient time remaining (${remainingTimeMs}ms)"
              )
              Left(error)
            } else {
              logger.warn(
                s"External call to extension '$extensionId' failed (attempt ${attempt + 1}/${config.maxRetries}): ${error.message} (status=${error.statusCode}). Retrying in ${delay}ms"
              )

              try {
                Thread.sleep(delay)
                loop(attempt + 1, Some(error))
              } catch {
                case _: InterruptedException =>
                  Thread.currentThread().interrupt()
                  Left(error)
              }
            }

          case Left(error) =>
            logger.error(
              s"External call to extension '$extensionId' failed with non-retryable error: ${error.message} (status=${error.statusCode}, requestId=${error.requestId.getOrElse("none")})"
            )
            Left(error)
        }
      }
    }

    loop(0, None)
  }

  /** Make a single HTTP call without retry logic */
  private def singleCall(
      functionId: String,
      configHash: String,
      input: String,
      mode: String,
  )(implicit tc: TraceContext): Either[ExtensionCallError, String] = {
    val requestId = generateRequestId()

    try {
      val requestBuilder = HttpRequest
        .newBuilder()
        .uri(endpoint)
        .timeout(Duration.ofMillis(config.requestTimeout.toMillis))
        .header("Content-Type", "application/octet-stream")
        .header("X-Daml-External-Function-Id", functionId)
        .header("X-Daml-External-Config-Hash", configHash)
        .header("X-Daml-External-Mode", mode)
        .header(config.requestIdHeader, requestId)

      jwtToken.foreach { token =>
        requestBuilder.header("Authorization", s"Bearer $token")
      }

      val req = requestBuilder
        .POST(HttpRequest.BodyPublishers.ofString(input))
        .build()

      logger.debug(
        s"Making external call to extension '$extensionId': functionId=$functionId, mode=$mode, requestId=$requestId"
      )

      val resp = sharedHttpClient.send(req, HttpResponse.BodyHandlers.ofString())

      resp.statusCode() match {
        case 200 =>
          logger.debug(s"External call to extension '$extensionId' succeeded: requestId=$requestId")
          Right(resp.body())

        case 400 =>
          Left(parseErrorResponse(resp, requestId, "Bad Request"))

        case 401 =>
          Left(parseErrorResponse(resp, requestId, "Unauthorized - check JWT token"))

        case 403 =>
          Left(parseErrorResponse(resp, requestId, "Forbidden - insufficient permissions"))

        case 404 =>
          Left(parseErrorResponse(resp, requestId, "Function not found"))

        case 408 =>
          Left(parseErrorResponse(resp, requestId, "Request timeout"))

        case 429 =>
          Left(parseErrorResponseWithRetry(resp, requestId, "Rate limit exceeded"))

        case 500 =>
          Left(parseErrorResponse(resp, requestId, "Internal server error"))

        case 502 =>
          Left(parseErrorResponse(resp, requestId, "Bad gateway"))

        case 503 =>
          Left(parseErrorResponseWithRetry(resp, requestId, "Service unavailable"))

        case 504 =>
          Left(parseErrorResponse(resp, requestId, "Gateway timeout"))

        case code =>
          Left(parseErrorResponse(resp, requestId, s"HTTP $code"))
      }
    } catch {
      case e: java.net.http.HttpTimeoutException =>
        logger.warn(s"External call to extension '$extensionId' timed out: requestId=$requestId")
        Left(ExtensionCallError(408, s"Request timeout: ${e.getMessage}", Some(requestId)))

      case e: java.net.ConnectException =>
        logger.error(s"External call to extension '$extensionId' connection failed: requestId=$requestId, error=${e.getMessage}")
        Left(ExtensionCallError(503, s"Connection failed: ${e.getMessage}", Some(requestId)))

      case e: java.io.IOException =>
        logger.error(s"External call to extension '$extensionId' I/O error: requestId=$requestId, error=${e.getMessage}")
        Left(ExtensionCallError(503, s"I/O error: ${e.getMessage}", Some(requestId)))

      case e: Exception =>
        logger.error(s"External call to extension '$extensionId' unexpected error: requestId=$requestId, error=${e.getMessage}")
        Left(ExtensionCallError(500, s"Unexpected error: ${e.getMessage}", Some(requestId)))
    }
  }

  private def parseErrorResponse(
      resp: HttpResponse[String],
      requestId: String,
      defaultMessage: String,
  ): ExtensionCallError = {
    val body = resp.body()
    val message = if (body != null && body.nonEmpty && body.length < 500) {
      s"$defaultMessage: $body"
    } else {
      defaultMessage
    }
    ExtensionCallError(resp.statusCode(), message, Some(requestId))
  }

  private def parseErrorResponseWithRetry(
      resp: HttpResponse[String],
      requestId: String,
      defaultMessage: String,
  ): ExtensionCallErrorWithRetry = {
    val retryAfter = Try {
      val opt = resp.headers().firstValue("Retry-After")
      if (opt.isPresent) Some(opt.get()) else None
    }.toOption.flatten.flatMap(s => Try(s.toInt).toOption)

    val body = resp.body()
    val message = if (body != null && body.nonEmpty && body.length < 500) {
      s"$defaultMessage: $body"
    } else {
      defaultMessage
    }

    ExtensionCallErrorWithRetry(resp.statusCode(), message, Some(requestId), retryAfter)
  }

  private def shouldRetry(error: ExtensionCallError): Boolean = {
    error.statusCode match {
      case 408 | 429 | 500 | 502 | 503 | 504 => true
      case _ => false
    }
  }

  private def calculateBackoff(attempt: Int, retryAfter: Option[Int], remainingTimeMs: Long): Long = {
    val connectTimeoutMs = config.connectTimeout.toMillis
    val requestTimeoutMs = config.requestTimeout.toMillis
    val minTimeForNextRequest = connectTimeoutMs + requestTimeoutMs
    val availableForBackoff = (remainingTimeMs - minTimeForNextRequest).max(0L)
    val retryInitialDelayMs = config.retryInitialDelay.toMillis
    val retryMaxDelayMs = config.retryMaxDelay.toMillis

    val baseDelay = retryAfter match {
      case Some(seconds) =>
        (seconds * 1000L).min(retryMaxDelayMs).min(availableForBackoff)

      case None =>
        val exponentialDelay = retryInitialDelayMs * Math.pow(2, (attempt - 1).toDouble).toLong
        val jitter = (Random.nextDouble() * 0.3 * exponentialDelay).toLong
        (exponentialDelay + jitter).min(retryMaxDelayMs).min(availableForBackoff)
    }

    baseDelay
  }

  private implicit class ExtensionCallErrorOps(error: ExtensionCallError) {
    def retryAfter: Option[Int] = error match {
      case e: ExtensionCallErrorWithRetry => e.retryAfterSeconds
      case _ => None
    }
  }
}

/** Extension call error with retry-after header support */
final case class ExtensionCallErrorWithRetry(
    override val statusCode: Int,
    override val message: String,
    override val requestId: Option[String],
    retryAfterSeconds: Option[Int],
) extends ExtensionCallError(statusCode, message, requestId)

object HttpExtensionServiceClient {

  /** Create an insecure SSL context for development (trusts all certificates) */
  def createInsecureSSLContext(): SSLContext = {
    val trustAllCerts = Array[TrustManager](new X509TrustManager {
      def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      def getAcceptedIssuers(): Array[X509Certificate] = Array.empty
    })

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(null, trustAllCerts, new SecureRandom())
    sslContext
  }
}
