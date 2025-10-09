// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.daml.logging.{ContextualizedLogger, LoggingContext}

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.URI
import java.nio.file.{Files, Paths}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.UUID
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}
import scala.util.{Random, Try}

/** Production-ready HTTP client for DAML external calls with full observability,
  * authentication, TLS support, and retry logic.
  */
private[speedy] object ExternalHttpClient {

  private implicit def loggingContext: LoggingContext = LoggingContext.ForTesting

  private val logger = ContextualizedLogger.get(getClass)

  /** Error information from external call failures */
  final case class ExternalCallError(
      statusCode: Int,
      message: String,
      requestId: Option[String],
      retryAfter: Option[Int], // Seconds to wait before retry (from Retry-After header)
  )

  // Configuration: Endpoint URL (full URL including path)
  private lazy val endpointUrl: String = {
    sys.env
      .get("DAML_EXTERNAL_CALL_ENDPOINT")
      .orElse(sys.props.get("daml.external.call.endpoint"))
      .getOrElse("http://127.0.0.1:1606/api/v1/external-call")
  }

  private lazy val endpoint: URI = URI.create(endpointUrl)

  // Configuration: JWT Token
  private lazy val jwtToken: Option[String] = {
    sys.env
      .get("DAML_EXTERNAL_CALL_JWT_TOKEN")
      .orElse(sys.props.get("daml.external.call.jwt.token"))
      .orElse {
        sys.env
          .get("DAML_EXTERNAL_CALL_JWT_TOKEN_FILE")
          .orElse(sys.props.get("daml.external.call.jwt.token.file"))
          .flatMap { path =>
            Try {
              new String(Files.readAllBytes(Paths.get(path))).trim
            }.toOption
          }
      }
  }

  // Configuration: TLS Insecure Mode (WARNING: only for development)
  private lazy val tlsInsecure: Boolean = {
    sys.env
      .get("DAML_EXTERNAL_CALL_TLS_INSECURE")
      .orElse(sys.props.get("daml.external.call.tls.insecure"))
      .exists { s => s.trim.toLowerCase == "true" || s == "1" }
  }

  // Configuration: Connect Timeout
  private lazy val connectTimeoutMs: Long = {
    sys.env
      .get("DAML_EXTERNAL_CALL_CONNECT_TIMEOUT_MS")
      .orElse(sys.props.get("daml.external.call.connect.timeout.ms"))
      .flatMap(s => Try(s.toLong).toOption)
      .getOrElse(500L)
  }

  // Configuration: Request Timeout
  // Default is 8s per request to allow multiple retry attempts within transaction timeout
  private lazy val requestTimeoutMs: Long = {
    sys.env
      .get("DAML_EXTERNAL_CALL_REQUEST_TIMEOUT_MS")
      .orElse(sys.props.get("daml.external.call.request.timeout.ms"))
      .flatMap(s => Try(s.toLong).toOption)
      .getOrElse(8000L) // 8s default - allows ~3 attempts with backoff within 25s total timeout
  }

  // Configuration: Maximum Total Timeout
  // Absolute deadline for the entire retry sequence (including all retries and backoffs)
  // Default is 25s (slightly less than typical 30s transaction timeout) to allow clean failure
  private lazy val maxTotalTimeoutMs: Long = {
    sys.env
      .get("DAML_EXTERNAL_CALL_MAX_TOTAL_TIMEOUT_MS")
      .orElse(sys.props.get("daml.external.call.max.total.timeout.ms"))
      .flatMap(s => Try(s.toLong).toOption)
      .getOrElse(25000L) // 25s default - ensures clean failure before transaction timeout
  }

  // Configuration: Max Retries
  private lazy val maxRetries: Int = {
    sys.env
      .get("DAML_EXTERNAL_CALL_MAX_RETRIES")
      .orElse(sys.props.get("daml.external.call.max.retries"))
      .flatMap(s => Try(s.toInt).toOption)
      .getOrElse(3)
  }

  // Configuration: Retry Initial Delay
  private lazy val retryInitialDelayMs: Long = {
    sys.env
      .get("DAML_EXTERNAL_CALL_RETRY_INITIAL_DELAY_MS")
      .orElse(sys.props.get("daml.external.call.retry.initial.delay.ms"))
      .flatMap(s => Try(s.toLong).toOption)
      .getOrElse(1000L)
  }

  // Configuration: Retry Max Delay
  private lazy val retryMaxDelayMs: Long = {
    sys.env
      .get("DAML_EXTERNAL_CALL_RETRY_MAX_DELAY_MS")
      .orElse(sys.props.get("daml.external.call.retry.max.delay.ms"))
      .flatMap(s => Try(s.toLong).toOption)
      .getOrElse(10000L)
  }

  // Configuration: Request ID Header Name
  private lazy val requestIdHeader: String = {
    sys.env
      .get("DAML_EXTERNAL_CALL_REQUEST_ID_HEADER")
      .orElse(sys.props.get("daml.external.call.request.id.header"))
      .getOrElse("X-Request-Id")
  }

  // Configuration: Echo Mode (for testing)
  private lazy val echoEnabled: Boolean = {
    val v = sys.env
      .get("DAML_EXTERNAL_CALL_ECHO")
      .orElse(sys.props.get("daml.external.call.echo"))
    v.exists { s =>
      val t = s.trim.toLowerCase(java.util.Locale.ROOT)
      t == "1" || t == "true" || t == "yes"
    }
  }

  // HTTP Client with TLS configuration
  private lazy val client: HttpClient = {
    val builder = HttpClient
      .newBuilder()
      .version(HttpClient.Version.HTTP_1_1)
      .connectTimeout(Duration.ofMillis(connectTimeoutMs))

    val _ = if (tlsInsecure) {
      logger.warn(
        "WARNING: TLS certificate validation is DISABLED. This should only be used in development!"
      )
      builder.sslContext(createInsecureSSLContext())
    } else {
      // TODO: Custom truststore support could be added here if needed
      // For now, use default SSL context which validates certificates normally
      ()
    }

    builder.build()
  }

  /** Creates an SSL context that trusts all certificates (INSECURE - dev only) */
  private def createInsecureSSLContext(): SSLContext = {
    val trustAllCerts = Array[TrustManager](new X509TrustManager {
      def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}
      def getAcceptedIssuers(): Array[X509Certificate] = Array.empty
    })

    val sslContext = SSLContext.getInstance("TLS")
    sslContext.init(null, trustAllCerts, new SecureRandom())
    sslContext
  }

  /** Generate a unique request ID for tracking */
  private def generateRequestId(): String = UUID.randomUUID().toString

  /** Make an HTTP call with retry logic
    *
    * @param functionId External function identifier
    * @param configHexLower Configuration hash (lowercase hex)
    * @param inputHexLower Input data (lowercase hex)
    * @param mode Execution mode (submission/validation/pure)
    * @return Either error information or successful response body
    */
  def callWithRetry(
      functionId: String,
      configHexLower: String,
      inputHexLower: String,
      mode: String,
  ): Either[ExternalCallError, String] = {
    if (echoEnabled) {
      logger.debug(s"Echo mode enabled, returning input as output")
      Right(inputHexLower)
    } else {
      // Initialize absolute deadline at the start to ensure total time doesn't exceed transaction limits
      val deadlineMs = System.currentTimeMillis() + maxTotalTimeoutMs

      def loop(attempt: Int, lastError: Option[ExternalCallError]): Either[ExternalCallError, String] = {
        // Check if we've exceeded the absolute deadline
        val nowMs = System.currentTimeMillis()
        if (nowMs >= deadlineMs) {
          val finalError = lastError.getOrElse(
            ExternalCallError(504, "Total timeout exceeded", None, None)
          )
          logger.error(
            s"External call exceeded maximum total timeout of ${maxTotalTimeoutMs}ms after $attempt attempts: ${finalError.message}"
          )
          Left(finalError)
        } else if (attempt > maxRetries) {
          val finalError = lastError.getOrElse(
            ExternalCallError(500, "Max retries exceeded", None, None)
          )
          logger.error(
            s"External call failed after $maxRetries retries: ${finalError.message} (status=${finalError.statusCode})"
          )
          Left(finalError)
        } else {
          val result = call(functionId, configHexLower, inputHexLower, mode)

          result match {
            case Right(response) =>
              if (attempt > 0) {
                logger.info(s"External call succeeded after $attempt retries")
              }
              Right(response)

            case Left(error) if shouldRetry(error) && attempt < maxRetries =>
              val remainingTimeMs = deadlineMs - System.currentTimeMillis()
              val delay = calculateBackoff(attempt + 1, error.retryAfter, remainingTimeMs)
              
              // If the calculated delay would exceed the deadline, fail immediately
              if (delay >= remainingTimeMs) {
                logger.warn(
                  s"External call failed (attempt ${attempt + 1}/${maxRetries}): ${error.message} (status=${error.statusCode}). Cannot retry: insufficient time remaining (${remainingTimeMs}ms)"
                )
                Left(error)
              } else {
                logger.warn(
                  s"External call failed (attempt ${attempt + 1}/${maxRetries}): ${error.message} (status=${error.statusCode}). Retrying in ${delay}ms (${remainingTimeMs - delay}ms remaining)"
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
                s"External call failed with non-retryable error: ${error.message} (status=${error.statusCode}, requestId=${error.requestId.getOrElse("none")})"
              )
              Left(error)
          }
        }
      }

      loop(0, None)
    }
  }

  /** Make a single HTTP call without retry logic */
  private def call(
      functionId: String,
      configHexLower: String,
      inputHexLower: String,
      mode: String,
  ): Either[ExternalCallError, String] = {
    val requestId = generateRequestId()

    try {
      val requestBuilder = HttpRequest
        .newBuilder()
        .uri(endpoint)
        .timeout(Duration.ofMillis(requestTimeoutMs))
        .header("Content-Type", "application/octet-stream")
        .header("X-Daml-External-Function-Id", functionId)
        .header("X-Daml-External-Config-Hash", configHexLower)
        .header("X-Daml-External-Mode", mode)
        .header(requestIdHeader, requestId)

      // Add JWT token if configured
      jwtToken.foreach { token =>
        requestBuilder.header("Authorization", s"Bearer $token")
      }

      val req = requestBuilder
        .POST(HttpRequest.BodyPublishers.ofString(inputHexLower))
        .build()

      logger.debug(
        s"Making external call: functionId=$functionId, mode=$mode, requestId=$requestId"
      )

      val resp = client.send(req, HttpResponse.BodyHandlers.ofString())

      resp.statusCode() match {
        case 200 =>
          logger.debug(s"External call succeeded: requestId=$requestId")
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
        logger.warn(s"External call timed out: requestId=$requestId")
        Left(ExternalCallError(408, s"Request timeout: ${e.getMessage}", Some(requestId), None))

      case e: java.net.ConnectException =>
        logger.error(s"External call connection failed: requestId=$requestId, error=${e.getMessage}")
        Left(ExternalCallError(503, s"Connection failed: ${e.getMessage}", Some(requestId), None))

      case e: java.io.IOException =>
        logger.error(s"External call I/O error: requestId=$requestId, error=${e.getMessage}")
        Left(ExternalCallError(503, s"I/O error: ${e.getMessage}", Some(requestId), None))

      case e: Exception =>
        logger.error(s"External call unexpected error: requestId=$requestId, error=${e.getMessage}")
        Left(ExternalCallError(500, s"Unexpected error: ${e.getMessage}", Some(requestId), None))
    }
  }

  /** Parse error response (simple text-based, no JSON) */
  private def parseErrorResponse(
      resp: HttpResponse[String],
      requestId: String,
      defaultMessage: String,
  ): ExternalCallError = {
    val body = resp.body()
    val message = if (body != null && body.nonEmpty && body.length < 500) {
      s"$defaultMessage: $body"
    } else {
      defaultMessage
    }
    ExternalCallError(resp.statusCode(), message, Some(requestId), None)
  }

  /** Parse error response with Retry-After header support */
  private def parseErrorResponseWithRetry(
      resp: HttpResponse[String],
      requestId: String,
      defaultMessage: String,
  ): ExternalCallError = {
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

    ExternalCallError(resp.statusCode(), message, Some(requestId), retryAfter)
  }

  /** Determine if an error should trigger a retry */
  private def shouldRetry(error: ExternalCallError): Boolean = {
    error.statusCode match {
      case 408 | 429 | 500 | 502 | 503 | 504 => true
      case _ => false
    }
  }

  /** Calculate backoff delay with exponential backoff and jitter
    *
    * @param attempt Current retry attempt (1-based)
    * @param retryAfter Optional Retry-After header value in seconds
    * @param remainingTimeMs Remaining time until deadline in milliseconds
    * @return Delay in milliseconds (guaranteed to be less than remainingTimeMs)
    */
  private def calculateBackoff(attempt: Int, retryAfter: Option[Int], remainingTimeMs: Long): Long = {
    // Reserve minimum time for the next request attempt (connect + request timeout)
    val minTimeForNextRequest = connectTimeoutMs + requestTimeoutMs
    val availableForBackoff = (remainingTimeMs - minTimeForNextRequest).max(0L)

    val baseDelay = retryAfter match {
      case Some(seconds) =>
        // Respect Retry-After header but cap at max delay and available time
        (seconds * 1000L).min(retryMaxDelayMs).min(availableForBackoff)

      case None =>
        // Exponential backoff with jitter
        val exponentialDelay = retryInitialDelayMs * Math.pow(2, (attempt - 1).toDouble).toLong
        val jitter = (Random.nextDouble() * 0.3 * exponentialDelay).toLong // 0-30% jitter
        (exponentialDelay + jitter).min(retryMaxDelayMs).min(availableForBackoff)
    }

    baseDelay
  }
}

