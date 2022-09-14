TransientServerFailure
========================================================================================================================
    **Category id**: 1

    **gRPC status code**: UNAVAILABLE

    **Default log level**: INFO

    **Description**: One of the services required to process the request was not available.

    **Resolution**: Expectation: transient failure that should be handled by retrying the request with appropriate backoff.

    **Retry strategy**: Retry quickly in load balancer.


ContentionOnSharedResources
========================================================================================================================
    **Category id**: 2

    **gRPC status code**: ABORTED

    **Default log level**: INFO

    **Description**: The request could not be processed due to shared processing resources (e.g. locks or rate limits that replenish quickly) being occupied. If the resource is known (i.e. locked contract), it will be included as a resource info. (Not known resource contentions are e.g. overloaded networks where we just observe timeouts, but can’t pin-point the cause).

    **Resolution**: Expectation: this is processing-flow level contention that should be handled by retrying the request with appropriate backoff.

    **Retry strategy**: Retry quickly (indefinitely or limited), but do not retry in load balancer.


DeadlineExceededRequestStateUnknown
========================================================================================================================
    **Category id**: 3

    **gRPC status code**: DEADLINE_EXCEEDED

    **Default log level**: INFO

    **Description**: The request might not have been processed, as its deadline expired before its completion was signalled. Note that for requests that change the state of the system, this error may be returned even if the request has completed successfully. Note that known and well-defined timeouts are signalled as [[ContentionOnSharedResources]], while this category indicates that the state of the request is unknown.

    **Resolution**: Expectation: the deadline might have been exceeded due to transient resource congestion or due to a timeout in the request processing pipeline being too low. The transient errors might be solved by the application retrying. The non-transient errors will require operator intervention to change the timeouts.

    **Retry strategy**: Retry for a limited number of times with deduplication.


SystemInternalAssumptionViolated
========================================================================================================================
    **Category id**: 4

    **gRPC status code**: INTERNAL

    **Default log level**: ERROR

    **Description**: Request processing failed due to a violation of system internal invariants.  This error is exposed on the API with grpc-status INTERNAL without any details for security reasons

    **Resolution**: Expectation: this is due to a bug in the implementation or data corruption in the systems databases. Resolution will require operator intervention, and potentially vendor support.

    **Retry strategy**: Retry after operator intervention.


MaliciousOrFaultyBehaviour
========================================================================================================================
    **Category id**: 5

    **gRPC status code**: UNKNOWN

    **Default log level**: WARN

    **Description**: Request processing failed due to unrecoverable data loss or corruption (e.g. detected via checksums). This error is exposed on the API with grpc-status UNKNOWN without any details for security reasons

    **Resolution**: Expectation: this can be a severe issue that requires operator attention or intervention, and potentially vendor support.

    **Retry strategy**: Retry after operator intervention.


AuthInterceptorInvalidAuthenticationCredentials
========================================================================================================================
    **Category id**: 6

    **gRPC status code**: UNAUTHENTICATED

    **Default log level**: WARN

    **Description**: The request does not have valid authentication credentials for the operation. This error is exposed on the API with grpc-status UNAUTHENTICATED without any details for security reasons

    **Resolution**: Expectation: this is an application bug, application misconfiguration or ledger-level misconfiguration. Resolution requires application and/or ledger operator intervention.

    **Retry strategy**: Retry after application operator intervention.


InsufficientPermission
========================================================================================================================
    **Category id**: 7

    **gRPC status code**: PERMISSION_DENIED

    **Default log level**: WARN

    **Description**: The caller does not have permission to execute the specified operation. This error is exposed on the API with grpc-status PERMISSION_DENIED without any details for security reasons

    **Resolution**: Expectation: this is an application bug or application misconfiguration. Resolution requires application operator intervention.

    **Retry strategy**: Retry after application operator intervention.


SecurityAlert
========================================================================================================================
    **Category id**: 15

    **gRPC status code**: INVALID_ARGUMENT

    **Default log level**: WARN

    **Description**: A potential attack has been detected.  This error is exposed on the API with grpc-status INVALID_ARGUMENT without any details for security reasons.

    **Resolution**: Expectation: this can be a severe issue that requires operator attention or intervention, and potentially vendor support.

    **Retry strategy**: Errors in this category are non-retryable.


InvalidIndependentOfSystemState
========================================================================================================================
    **Category id**: 8

    **gRPC status code**: INVALID_ARGUMENT

    **Default log level**: INFO

    **Description**: The request is invalid independent of the state of the system.

    **Resolution**: Expectation: this is an application bug or ledger-level misconfiguration (e.g. request size limits). Resolution requires application and/or ledger operator intervention.

    **Retry strategy**: Retry after application operator intervention.


InvalidGivenCurrentSystemStateOther
========================================================================================================================
    **Category id**: 9

    **gRPC status code**: FAILED_PRECONDITION

    **Default log level**: INFO

    **Description**: The mutable state of the system does not satisfy the preconditions required to execute the request. We consider the whole Daml ledger including ledger config, parties, packages, users and command deduplication to be mutable system state. Thus all Daml interpretation errors are reported as this error or one of its specializations.

    **Resolution**: ALREADY_EXISTS and NOT_FOUND are special cases for the existence and non-existence of well-defined entities within the system state; e.g., a .dalf package, contracts ids, contract keys, or a transaction at an offset. OUT_OF_RANGE is a special case for reading past a range. Violations of the Daml ledger model always result in these kinds of errors. Expectation: this is due to application-level bugs, misconfiguration or contention on application-visible resources; and might be resolved by retrying later, or after changing the state of the system. Handling these errors requires an application-specific strategy and/or operator intervention.

    **Retry strategy**: Retry after application operator intervention.


InvalidGivenCurrentSystemStateResourceExists
========================================================================================================================
    **Category id**: 10

    **gRPC status code**: ALREADY_EXISTS

    **Default log level**: INFO

    **Description**: Special type of InvalidGivenCurrentSystemState referring to a well-defined resource.

    **Resolution**: Same as [[InvalidGivenCurrentSystemStateOther]].

    **Retry strategy**: Inspect resource failure and retry after resource failure has been resolved (depends on type of resource and application).


InvalidGivenCurrentSystemStateResourceMissing
========================================================================================================================
    **Category id**: 11

    **gRPC status code**: NOT_FOUND

    **Default log level**: INFO

    **Description**: Special type of InvalidGivenCurrentSystemState referring to a well-defined resource.

    **Resolution**: Same as [[InvalidGivenCurrentSystemStateOther]].

    **Retry strategy**: Inspect resource failure and retry after resource failure has been resolved (depends on type of resource and application).


InvalidGivenCurrentSystemStateSeekAfterEnd
========================================================================================================================
    **Category id**: 12

    **gRPC status code**: OUT_OF_RANGE

    **Default log level**: INFO

    **Description**: This error is only used by the Ledger API server in connection with invalid offsets.

    **Resolution**: Expectation: this error is only used by the Ledger API server in connection with invalid offsets.

    **Retry strategy**: Retry after application operator intervention.


BackgroundProcessDegradationWarning
========================================================================================================================
    **Category id**: 13

    **gRPC status code**: N/A

    **Default log level**: WARN

    **Description**: This error category is used internally to signal to the system operator an internal degradation.

    **Resolution**: Inspect details of the specific error for more information.

    **Retry strategy**: Not an API error, therefore not retryable.


InternalUnsupportedOperation
========================================================================================================================
    **Category id**: 14

    **gRPC status code**: UNIMPLEMENTED

    **Default log level**: ERROR

    **Description**: This error category is used to signal that an unimplemented code-path has been triggered by a client or participant operator request. This error is exposed on the API with grpc-status INTERNAL without any details for security reasons

    **Resolution**: This error is caused by a ledger-level misconfiguration or by an implementation bug. Resolution requires participant operator intervention.

    **Retry strategy**: Errors in this category are non-retryable.