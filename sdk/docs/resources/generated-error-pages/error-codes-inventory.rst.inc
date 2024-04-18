1. ParticipantErrorGroup
===================================================================================================================




1.1. ParticipantErrorGroup / CommonErrors
===================================================================================================================

Common errors raised in Daml services and components.



.. _error_code_REQUEST_TIME_OUT:

REQUEST_TIME_OUT
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when a request processing status is not known and a time-out is reached.

    **Category**: DeadlineExceededRequestStateUnknown

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status DEADLINE_EXCEEDED including a detailed error message.

    **Resolution**: Retry for transient problems. If non-transient contact the operator as the time-out limit might be too short.




.. _error_code_SERVER_IS_SHUTTING_DOWN:

SERVER_IS_SHUTTING_DOWN
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when the participant server is shutting down.

    **Category**: TransientServerFailure

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status UNAVAILABLE including a detailed error message.

    **Resolution**: Contact the participant operator.




.. _error_code_SERVICE_INTERNAL_ERROR:

SERVICE_INTERNAL_ERROR
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error occurs if one of the services encountered an unexpected exception.

    **Category**: SystemInternalAssumptionViolated

    **Conveyance**: This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status INTERNAL without any details for security reasons.

    **Resolution**: Contact support.




.. _error_code_SERVICE_NOT_RUNNING:

SERVICE_NOT_RUNNING
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This rejection is given when the requested service has already been closed.

    **Category**: TransientServerFailure

    **Conveyance**: This error is logged with log-level INFO on the server side and exposed on the API with grpc-status UNAVAILABLE including a detailed error message.

    **Resolution**: Retry re-submitting the request. If the error persists, contact the participant operator.




.. _error_code_UNSUPPORTED_OPERATION:

UNSUPPORTED_OPERATION
---------------------------------------------------------------------------------------------------------------------------------------
    
    **Explanation**: This error category is used to signal that an unimplemented code-path has been triggered by a client or participant operator request.

    **Category**: InternalUnsupportedOperation

    **Conveyance**: This error is logged with log-level ERROR on the server side. It is exposed on the API with grpc-status UNIMPLEMENTED without any details for security reasons.

    **Resolution**: This error is caused by a participant node misconfiguration or by an implementation bug. Resolution requires participant operator intervention.

