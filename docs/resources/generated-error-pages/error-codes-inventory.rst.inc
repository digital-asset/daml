1. KvErrors
===================================================================================================================

Errors that are specific to ledgers based on the KV architecture: Daml Sandbox and VMBC.


1.1. KvErrors / Consistency
===================================================================================================================

Errors that highlight transaction consistency issues in the committer context.



.. _error_code_INCONSISTENT_INPUT:

INCONSISTENT_INPUT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: At least one input has been altered by a concurrent transaction submission.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: The correct resolution depends on the business flow, for example it may be possible to proceed without an archived contract as an input, or the transaction submission may be retried to load the up-to-date value of a contract key.




.. _error_code_VALIDATION_FAILURE:

VALIDATION_FAILURE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: Validation of a transaction submission failed using on-ledger data.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Either some input contracts have been pruned or the participant is misbehaving.



1.2. KvErrors / Internal
===================================================================================================================

Errors that arise from an internal system misbehavior.



.. _error_code_INVALID_PARTICIPANT_STATE:

INVALID_PARTICIPANT_STATE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: An invalid participant state has been detected.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.




.. _error_code_MISSING_INPUT_STATE:

MISSING_INPUT_STATE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The participant didn't provide a necessary transaction submission input.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.




.. _error_code_REJECTION_REASON_NOT_SET:

REJECTION_REASON_NOT_SET
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: A rejection reason has not been set.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.




.. _error_code_SUBMISSION_FAILED:

SUBMISSION_FAILED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: An unexpected error occurred while submitting a command to the ledger.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.



1.3. KvErrors / Resources
===================================================================================================================

Errors that relate to system resources.



.. _error_code_RESOURCE_EXHAUSTED:

RESOURCE_EXHAUSTED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Deprecation**: Replaced by RESOURCE_OVERLOADED. Since: 2.3.0
    
    **Explanation**: A system resource has been exhausted.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: Retry the transaction submission or provide the details to the participant operator.




.. _error_code_RESOURCE_OVERLOADED:

RESOURCE_OVERLOADED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: A system resource is overloaded.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: Retry the transaction submission or provide the details to the participant operator.



1.4. KvErrors / Time
===================================================================================================================

Errors that relate to the Daml concepts of time.



.. _error_code_CAUSAL_MONOTONICITY_VIOLATED:

CAUSAL_MONOTONICITY_VIOLATED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: At least one input contract's ledger time is later than that of the submitted transaction.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Retry the transaction submission.




.. _error_code_INVALID_RECORD_TIME:

INVALID_RECORD_TIME
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The record time is not within bounds for reasons other than deduplication, such as excessive latency. Excessive clock skew between the participant and the committer or a time model that is too restrictive may also produce this rejection.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Retry the submission or contact the participant operator.




.. _error_code_RECORD_TIME_OUT_OF_BOUNDS:

RECORD_TIME_OUT_OF_BOUNDS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The record time is not within bounds for reasons such as excessive latency, excessive clock skew between the participant and the committer or a time model that is too restrictive.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: Retry the submission or contact the participant operator.




.. _error_code_RECORD_TIME_OUT_OF_RANGE:

RECORD_TIME_OUT_OF_RANGE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The record time is not within bounds for reasons other than deduplication, such as excessive latency. Excessive clock skew between the participant and the committer or a time model that is too restrictive may also produce this rejection.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Retry the transaction submission or contact the participant operator.



2. ParticipantErrorGroup
===================================================================================================================




2.1. ParticipantErrorGroup / IndexErrors
===================================================================================================================

Errors raised by the Participant Index persistence layer.


2.1.1. ParticipantErrorGroup / IndexErrors / DatabaseErrors
===================================================================================================================





.. _error_code_INDEX_DB_INVALID_RESULT_SET:

INDEX_DB_INVALID_RESULT_SET
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if the result set returned by a query against the Index database is invalid.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.




.. _error_code_INDEX_DB_SQL_NON_TRANSIENT_ERROR:

INDEX_DB_SQL_NON_TRANSIENT_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a non-transient error arises when executing a query against the index database.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact the participant operator.




.. _error_code_INDEX_DB_SQL_TRANSIENT_ERROR:

INDEX_DB_SQL_TRANSIENT_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a transient error arises when executing a query against the index database.

    **Category**: TransientServerFailure

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status UNAVAILABLE including a detailed error message

    **Resolution**: Re-submit the request.



2.2. ParticipantErrorGroup / LedgerApiErrors
===================================================================================================================

Errors raised by or forwarded by the Ledger API.



.. _error_code_HEAP_MEMORY_OVER_LIMIT:

HEAP_MEMORY_OVER_LIMIT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error happens when the JVM heap memory pool exceeds a pre-configured limit.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: The following actions can be taken: 1. Review the historical use of heap space by inspecting the metric given in the message. 2. Review the current heap space limits configured in the rate limiting configuration. 3. Try to space out requests that are likely to require a large amount of memory to process.




.. _error_code_LEDGER_API_INTERNAL_ERROR:

LEDGER_API_INTERNAL_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if there was an unexpected error in the Ledger API.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.




.. _error_code_MAXIMUM_NUMBER_OF_STREAMS:

MAXIMUM_NUMBER_OF_STREAMS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error happens when the number of concurrent gRPC streaming requests exceeds the configured limit.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: The following actions can be taken: 1. Review the historical need for concurrent streaming by inspecting the metric given in the message. 2. Review the maximum streams limit configured in the rate limiting configuration. 3. Try to space out streaming requests such that they do not need to run in parallel with each other.




.. _error_code_PARTICIPANT_BACKPRESSURE:

PARTICIPANT_BACKPRESSURE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs when a participant rejects a command due to excessive load. Load can be caused by the following factors: 1. when commands are submitted to the participant through its Ledger API, 2. when the participant receives requests from other participants through a connected domain.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: Wait a bit and retry, preferably with some backoff factor. If possible, ask other participants to send fewer requests; the domain operator can enforce this by imposing a rate limit.




.. _error_code_REQUEST_TIME_OUT:

REQUEST_TIME_OUT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a request processing status is not known and a time-out is reached.

    **Category**: DeadlineExceededRequestStateUnknown

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status DEADLINE_EXCEEDED including a detailed error message

    **Resolution**: Retry for transient problems. If non-transient contact the operator as the time-out limit might be too short.




.. _error_code_SERVER_IS_SHUTTING_DOWN:

SERVER_IS_SHUTTING_DOWN
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when the participant server is shutting down.

    **Category**: TransientServerFailure

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status UNAVAILABLE including a detailed error message

    **Resolution**: Contact the participant operator.




.. _error_code_SERVICE_NOT_RUNNING:

SERVICE_NOT_RUNNING
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when the requested service has already been closed.

    **Category**: TransientServerFailure

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status UNAVAILABLE including a detailed error message

    **Resolution**: Retry re-submitting the request. If the error persists, contact the participant operator.




.. _error_code_THREADPOOL_OVERLOADED:

THREADPOOL_OVERLOADED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This happens when the rate of submitted gRPC requests requires more CPU or database power than is available.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: The following actions can be taken: Here the 'queue size' for the threadpool = 'submitted tasks' - 'completed tasks' - 'running tasks' 1. Review the historical 'queue size' growth by inspecting the metric given in the message. 2. Review the maximum 'queue size' limits configured in the rate limiting configuration. 3. Try to space out requests that are likely to require a lot of CPU or database power.       




.. _error_code_UNSUPPORTED_OPERATION:

UNSUPPORTED_OPERATION
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error category is used to signal that an unimplemented code-path has been triggered by a client or participant operator request.

    **Category**: InternalUnsupportedOperation

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status UNIMPLEMENTED without any details due to security reasons

    **Resolution**: This error is caused by a participant node misconfiguration or by an implementation bug. Resolution requires participant operator intervention.



2.2.1. ParticipantErrorGroup / LedgerApiErrors / AdminServices
===================================================================================================================

Errors raised by Ledger API admin services.



.. _error_code_CONFIGURATION_ENTRY_REJECTED:

CONFIGURATION_ENTRY_REJECTED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a new configuration is rejected.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Fetch newest configuration and/or retry.




.. _error_code_INTERNALLY_INVALID_KEY:

INTERNALLY_INVALID_KEY
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: A cryptographic key used by the configured system is not valid

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.




.. _error_code_PACKAGE_UPLOAD_REJECTED:

PACKAGE_UPLOAD_REJECTED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a package upload is rejected.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Refer to the detailed message of the received error.



2.2.1.1. ParticipantErrorGroup / LedgerApiErrors / AdminServices / UserManagementServiceErrors
===================================================================================================================





.. _error_code_TOO_MANY_USER_RIGHTS:

TOO_MANY_USER_RIGHTS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: A user can have only a limited number of user rights. There was an attempt to create a user with too many rights or grant too many rights to a user.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Retry with a smaller number of rights or delete some of the already existing rights of this user. Contact the participant operator if the limit is too low.




.. _error_code_USER_ALREADY_EXISTS:

USER_ALREADY_EXISTS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: There already exists a user with the same user-id.

    **Category**: InvalidGivenCurrentSystemStateResourceExists

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ALREADY_EXISTS including a detailed error message

    **Resolution**: Check that you are connecting to the right participant node and the user-id is spelled correctly, or use the user that already exists.




.. _error_code_USER_NOT_FOUND:

USER_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The user referred to by the request was not found.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: Check that you are connecting to the right participant node and the user-id is spelled correctly, if yes, create the user.



2.2.2. ParticipantErrorGroup / LedgerApiErrors / AuthorizationChecks
===================================================================================================================

Authentication and authorization errors.



.. _error_code_INTERNAL_AUTHORIZATION_ERROR:

INTERNAL_AUTHORIZATION_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: An internal system authorization error occurred.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact the participant operator.




.. _error_code_PERMISSION_DENIED:

PERMISSION_DENIED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given if the supplied authorization token is not sufficient for the intended command. The exact reason is logged on the participant, but not given to the user for security reasons.

    **Category**: InsufficientPermission

    **Conveyance**: This error is logged with log-level WARN on the server side. This error is exposed on the API with grpc-status PERMISSION_DENIED without any details due to security reasons

    **Resolution**: Inspect your command and your token or ask your participant operator for an explanation why this command failed.




.. _error_code_STALE_STREAM_AUTHORIZATION:

STALE_STREAM_AUTHORIZATION
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The stream was aborted because the authenticated user's rights changed, and the user might thus no longer be authorized to this stream. 

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: The application should automatically retry fetching the stream. It will either succeed, or fail with an explicit denial of authentication or permission.




.. _error_code_UNAUTHENTICATED:

UNAUTHENTICATED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given if the submitted command does not contain a JWT token on a participant enforcing JWT authentication.

    **Category**: AuthInterceptorInvalidAuthenticationCredentials

    **Conveyance**: This error is logged with log-level WARN on the server side. This error is exposed on the API with grpc-status UNAUTHENTICATED without any details due to security reasons

    **Resolution**: Ask your participant operator to provide you with an appropriate JWT token.



2.2.3. ParticipantErrorGroup / LedgerApiErrors / CommandExecution
===================================================================================================================

Errors raised during the command execution phase of the command submission evaluation.



.. _error_code_FAILED_TO_DETERMINE_LEDGER_TIME:

FAILED_TO_DETERMINE_LEDGER_TIME
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if the participant fails to determine the max ledger time of the used contracts. Most likely, this means that one of the contracts is not active anymore which can happen under contention. It can also happen with contract keys. 

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: Retry the transaction submission.



2.2.3.1. ParticipantErrorGroup / LedgerApiErrors / CommandExecution / Interpreter
===================================================================================================================

Errors raised during the command interpretation phase of the command submission evaluation.



.. _error_code_CONTRACT_NOT_ACTIVE:

CONTRACT_NOT_ACTIVE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if an exercise or fetch happens on a transaction-locally consumed contract.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: This error indicates an application error.




.. _error_code_DAML_AUTHORIZATION_ERROR:

DAML_AUTHORIZATION_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a Daml transaction fails due to an authorization error. An authorization means that the Daml transaction computed a different set of required submitters than you have provided during the submission as `actAs` parties.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: This error type occurs if there is an application error.




.. _error_code_DAML_INTERPRETATION_ERROR:

DAML_INTERPRETATION_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a Daml transaction fails during interpretation.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: This error type occurs if there is an application error.




.. _error_code_DAML_INTERPRETER_INVALID_ARGUMENT:

DAML_INTERPRETER_INVALID_ARGUMENT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a Daml transaction fails during interpretation due to an invalid argument.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: This error type occurs if there is an application error.



2.2.3.1.1. ParticipantErrorGroup / LedgerApiErrors / CommandExecution / Interpreter / LookupErrors
===================================================================================================================

Errors raised in lookups during the command interpretation phase.



.. _error_code_CONTRACT_KEY_NOT_FOUND:

CONTRACT_KEY_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if the Daml engine interpreter cannot resolve a contract key to an active contract. This can be caused by either the contract key not being known to the participant, or not being known to the submitting parties or the contract representing an already archived key.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: This error type occurs if there is contention on a contract.



2.2.3.2. ParticipantErrorGroup / LedgerApiErrors / CommandExecution / Package
===================================================================================================================

Command execution errors raised due to invalid packages.



.. _error_code_ALLOWED_LANGUAGE_VERSIONS:

ALLOWED_LANGUAGE_VERSIONS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the uploaded DAR is based on an unsupported language version.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Use a DAR compiled with a language version that this participant supports.




.. _error_code_PACKAGE_VALIDATION_FAILED:

PACKAGE_VALIDATION_FAILED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a package referred to by a command fails validation. This should not happen as packages are validated when being uploaded.

    **Category**: MaliciousOrFaultyBehaviour

    **Conveyance**: This error is logged with log-level WARN on the server side. This error is exposed on the API with grpc-status UNKNOWN without any details due to security reasons

    **Resolution**: Contact support.



2.2.3.3. ParticipantErrorGroup / LedgerApiErrors / CommandExecution / Preprocessing
===================================================================================================================

Errors raised during command conversion to the internal data representation.



.. _error_code_COMMAND_PREPROCESSING_FAILED:

COMMAND_PREPROCESSING_FAILED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if a command fails during interpreter pre-processing.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect error details and correct your application.



2.2.4. ParticipantErrorGroup / LedgerApiErrors / ConsistencyErrors
===================================================================================================================

Potential consistency errors raised due to race conditions during command submission or returned as submission rejections by the backing ledger.



.. _error_code_CONTRACT_NOT_FOUND:

CONTRACT_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if the Daml engine can not find a referenced contract. This can be caused by either the contract not being known to the participant, or not being known to the submitting parties or already being archived.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: This error type occurs if there is contention on a contract.




.. _error_code_DISCLOSED_CONTRACT_INVALID:

DISCLOSED_CONTRACT_INVALID
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if one of the disclosed contracts' payload or its metadata does not match the actual payload or metadata of the contract.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Use a valid disclosed contract payload and metadata and re-submit the command.




.. _error_code_DUPLICATE_COMMAND:

DUPLICATE_COMMAND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: A command with the given command id has already been successfully processed.

    **Category**: InvalidGivenCurrentSystemStateResourceExists

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ALREADY_EXISTS including a detailed error message

    **Resolution**: The correct resolution depends on the use case. If the error received pertains to a submission retried due to a timeout, do nothing, as the previous command has already been accepted. If the intent is to submit a new command, re-submit using a distinct command id. 




.. _error_code_DUPLICATE_CONTRACT_KEY:

DUPLICATE_CONTRACT_KEY
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error signals that within the transaction we got to a point where two contracts with the same key were active.

    **Category**: InvalidGivenCurrentSystemStateResourceExists

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ALREADY_EXISTS including a detailed error message

    **Resolution**: This error indicates an application error.




.. _error_code_INCONSISTENT:

INCONSISTENT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: At least one input has been altered by a concurrent transaction submission.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: The correct resolution depends on the business flow, for example it may be possible to proceed without an archived contract as an input, or the transaction submission may be retried to load the up-to-date value of a contract key.




.. _error_code_INCONSISTENT_CONTRACTS:

INCONSISTENT_CONTRACTS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: An input contract has been archived by a concurrent transaction submission.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: The correct resolution depends on the business flow, for example it may be possible to proceed without the archived contract as an input, or a different contract could be used.




.. _error_code_INCONSISTENT_CONTRACT_KEY:

INCONSISTENT_CONTRACT_KEY
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: An input contract key was re-assigned to a different contract by a concurrent transaction submission.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Retry the transaction submission.




.. _error_code_INVALID_LEDGER_TIME:

INVALID_LEDGER_TIME
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The ledger time of the submission violated some constraint on the ledger time.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Retry the transaction submission.




.. _error_code_SUBMISSION_ALREADY_IN_FLIGHT:

SUBMISSION_ALREADY_IN_FLIGHT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: Another command submission with the same change ID (application ID, command ID, actAs) is already being processed.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: Listen to the command completion stream until a completion for the in-flight command submission is published. Alternatively, resubmit the command. If the in-flight submission has finished successfully by then, this will return more detailed information about the earlier one. If the in-flight submission has failed by then, the resubmission will attempt to record the new transaction on the ledger. 



2.2.5. ParticipantErrorGroup / LedgerApiErrors / PackageServiceError
===================================================================================================================

Errors raised by the Package Management Service on package uploads.



.. _error_code_DAR_NOT_SELF_CONSISTENT:

DAR_NOT_SELF_CONSISTENT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the uploaded Dar is broken because it is missing internal dependencies.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Contact the supplier of the Dar.




.. _error_code_DAR_VALIDATION_ERROR:

DAR_VALIDATION_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the validation of the uploaded dar failed.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect the error message and contact support.




.. _error_code_PACKAGE_SERVICE_INTERNAL_ERROR:

PACKAGE_SERVICE_INTERNAL_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates an internal issue within the package service.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Inspect the error message and contact support.



2.2.5.1. ParticipantErrorGroup / LedgerApiErrors / PackageServiceError / Reading
===================================================================================================================

Package parsing errors raised during package upload.



.. _error_code_DAR_PARSE_ERROR:

DAR_PARSE_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the content of the Dar file could not be parsed successfully.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect the error message and contact support.




.. _error_code_INVALID_DAR:

INVALID_DAR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the supplied dar file was invalid.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect the error message for details and contact support.




.. _error_code_INVALID_DAR_FILE_NAME:

INVALID_DAR_FILE_NAME
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the supplied dar file name did not meet the requirements to be stored in the persistence store.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect error message for details and change the file name accordingly




.. _error_code_INVALID_LEGACY_DAR:

INVALID_LEGACY_DAR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the supplied zipped dar is an unsupported legacy Dar.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Please use a more recent dar version.




.. _error_code_INVALID_ZIP_ENTRY:

INVALID_ZIP_ENTRY
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the supplied zipped dar file was invalid.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect the error message for details and contact support.




.. _error_code_ZIP_BOMB:

ZIP_BOMB
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error indicates that the supplied zipped dar is regarded as zip-bomb.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect the dar and contact support.



2.2.6. ParticipantErrorGroup / LedgerApiErrors / RequestValidation
===================================================================================================================

Validation errors raised when evaluating requests in the Ledger API.



.. _error_code_INVALID_ARGUMENT:

INVALID_ARGUMENT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error is emitted when a submitted ledger API command contains an invalid argument.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect the reason given and correct your application.




.. _error_code_INVALID_DEDUPLICATION_PERIOD:

INVALID_DEDUPLICATION_PERIOD
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error is emitted when a submitted ledger API command specifies an invalid deduplication period.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Inspect the error message, adjust the value of the deduplication period or ask the participant operator to increase the maximum deduplication period.




.. _error_code_INVALID_FIELD:

INVALID_FIELD
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error is emitted when a submitted ledger API command contains a field value that cannot be understood.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect the reason given and correct your application.




.. _error_code_LEDGER_ID_MISMATCH:

LEDGER_ID_MISMATCH
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: Every ledger API command contains a ledger-id which is verified against the running ledger.           This error indicates that the provided ledger-id does not match the expected one.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: Ensure that your application is correctly configured to use the correct ledger.




.. _error_code_MISSING_FIELD:

MISSING_FIELD
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error is emitted when a mandatory field is not set in a submitted ledger API command.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Inspect the reason given and correct your application.




.. _error_code_NON_HEXADECIMAL_OFFSET:

NON_HEXADECIMAL_OFFSET
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The supplied offset could not be converted to a binary offset.

    **Category**: InvalidIndependentOfSystemState

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status INVALID_ARGUMENT including a detailed error message

    **Resolution**: Ensure the offset is specified as a hexadecimal string.




.. _error_code_OFFSET_AFTER_LEDGER_END:

OFFSET_AFTER_LEDGER_END
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a read request uses an offset beyond the current ledger end.

    **Category**: InvalidGivenCurrentSystemStateSeekAfterEnd

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status OUT_OF_RANGE including a detailed error message

    **Resolution**: Use an offset that is before the ledger end.




.. _error_code_OFFSET_OUT_OF_RANGE:

OFFSET_OUT_OF_RANGE
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a read request uses an offset invalid in the requests' context.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Inspect the error message and use a valid offset.




.. _error_code_PARTICIPANT_PRUNED_DATA_ACCESSED:

PARTICIPANT_PRUNED_DATA_ACCESSED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a read request tries to access pruned data.

    **Category**: InvalidGivenCurrentSystemStateOther

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status FAILED_PRECONDITION including a detailed error message

    **Resolution**: Use an offset that is after the pruning offset.



2.2.6.1. ParticipantErrorGroup / LedgerApiErrors / RequestValidation / NotFound
===================================================================================================================





.. _error_code_LEDGER_CONFIGURATION_NOT_FOUND:

LEDGER_CONFIGURATION_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The ledger configuration could not be retrieved. This could happen due to incomplete initialization of the participant or due to an internal system error.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: Contact the participant operator.




.. _error_code_PACKAGE_NOT_FOUND:

PACKAGE_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a read request tries to access a package which does not exist on the ledger.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: Use a package id pertaining to a package existing on the ledger.




.. _error_code_TRANSACTION_NOT_FOUND:

TRANSACTION_NOT_FOUND
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The transaction does not exist or the requesting set of parties are not authorized to fetch it.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: Check the transaction id and verify that the requested transaction is visible to the requesting parties.



2.2.7. ParticipantErrorGroup / LedgerApiErrors / WriteServiceRejections
===================================================================================================================

Generic submission rejection errors returned by the backing ledger's write service.



.. _error_code_DISPUTED:

DISPUTED
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Deprecation**: Corresponds to transaction submission rejections that are not produced anymore.
    
    **Explanation**: An invalid transaction submission was not detected by the participant.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.




.. _error_code_OUT_OF_QUOTA:

OUT_OF_QUOTA
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Deprecation**: Corresponds to transaction submission rejections that are not produced anymore.
    
    **Explanation**: The Participant node did not have sufficient resource quota to submit the transaction.

    **Category**: ContentionOnSharedResources

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status ABORTED including a detailed error message

    **Resolution**: Inspect the error message and retry after after correcting the underlying issue.




.. _error_code_PARTY_NOT_KNOWN_ON_LEDGER:

PARTY_NOT_KNOWN_ON_LEDGER
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: One or more informee parties have not been allocated.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: Check that all the informee party identifiers are correct, allocate all the informee parties, request their allocation or wait for them to be allocated before retrying the transaction submission.




.. _error_code_SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT:

SUBMITTER_CANNOT_ACT_VIA_PARTICIPANT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: A submitting party is not authorized to act through the participant.

    **Category**: InsufficientPermission

    **Conveyance**: This error is logged with log-level WARN on the server side. This error is exposed on the API with grpc-status PERMISSION_DENIED without any details due to security reasons

    **Resolution**: Contact the participant operator or re-submit with an authorized party.




.. _error_code_SUBMITTING_PARTY_NOT_KNOWN_ON_LEDGER:

SUBMITTING_PARTY_NOT_KNOWN_ON_LEDGER
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The submitting party has not been allocated.

    **Category**: InvalidGivenCurrentSystemStateResourceMissing

    **Conveyance**: This error is logged with log-level INFO on the server side. This error is exposed on the API with grpc-status NOT_FOUND including a detailed error message

    **Resolution**: Check that the party identifier is correct, allocate the submitting party, request its allocation or wait for it to be allocated before retrying the transaction submission.



2.2.7.1. ParticipantErrorGroup / LedgerApiErrors / WriteServiceRejections / Internal
===================================================================================================================

Errors that arise from an internal system misbehavior.



.. _error_code_INTERNALLY_DUPLICATE_KEYS:

INTERNALLY_DUPLICATE_KEYS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The participant didn't detect an attempt by the transaction submission to use the same key for two active contracts.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.




.. _error_code_INTERNALLY_INCONSISTENT_KEYS:

INTERNALLY_INCONSISTENT_KEYS
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: The participant didn't detect an inconsistent key usage in the transaction. Within the transaction, an exercise, fetch or lookupByKey failed because the mapping of `key -> contract ID` was inconsistent with earlier actions.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. This error is exposed on the API with grpc-status INTERNAL without any details due to security reasons

    **Resolution**: Contact support.

