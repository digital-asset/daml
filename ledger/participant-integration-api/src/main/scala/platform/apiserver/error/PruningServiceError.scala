package com.daml.platform.apiserver.error

sealed trait PruningServiceError extends BaseError
object PruningServiceError extends PruningServiceErrorGroup {
  @Explanation("""Pruning has failed because of an internal server error.""")
  @Resolution("Identify the error in the server log.")
  object InternalServerError
    extends ErrorCode(id = "INTERNAL_PRUNING_ERROR", ErrorCategory.SystemInternalAssumptionViolated) {
    case class Error(reason: String)(implicit val loggingContext: ErrorLoggingContext)
      extends BaseError.Impl(
        cause = "Internal error such as the inability to write to the database"
      )
        with PruningServiceError
  }

}
