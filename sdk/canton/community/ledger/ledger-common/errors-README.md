# Table of Contents

1. [Overview](#overview)
1. [Error codes](#error-codes)
1. [Error categories](#error-categories)
1. [Error groups](#error-groups)

## Overview

Error codes are typically returned as part of gRPC calls.

The users mentioned later in this document might be participant operators, application developers or application users.

## Error codes

Base class: `com.daml.error.ErrorCode`

### Error code definition

Example:
```

object SomeErrorGroup extends ErrorGroup() {

    @Explanation("This explanation becomes part of the official documentation.")
    @Resolution("This resolution becomes part of the official documentation.")
    object MyNewErrorCode extends ErrorCode(id = "<THIS_IS_AN_ERROR_CODE_ID>",
                                            category = ErrorCategory.InvalidIndependentOfSystemState){

        case class Reject(reason: String)
                         (implicit loggingContext: ContextualizedErrorLogger)
          extends LoggingTransactionErrorImpl(cause = reason)

    }
}

```
In the example above `MyNewErrorCode` is a new error code which has its own unique error code `id`
and belongs to some error `category`.
`Reject` is a supplementary class to create a concrete instance of this error code.

Things to note:
1. Each error code should be placed within some error group in order to render it in the correct section in the
   official Daml documentation.
1. The descriptions from `Exaplantion` and `Resolution` annotations are used to automatically generate sections
   of the official Daml documentation. Make sure they are clear and helpful to the users.
1. Error code ids and category assignment are communicated to the user and we consider them part of the public API.
1. You should consider creating a user migration guide for example when you:
    - add, remove or change an error code id,
    - change the set of error code ids that is being returned from a particular gRPC endpoint,
    - change the category an error code belongs to.

When adding a new error code make sure to add a unit test where you assert on the contents of the error as delivered to the users.


## Usage

Typically you will want to immediately convert an error code into an instance of `io.grpc.StatusRuntimeException`:

```
MyNewErrorCode.ErrorCode("Msg").asGrpcError: StatusRuntimeException
```

In practice you will also need to provide some additional implicit values. Look for concrete examples in the codebase.



## Error categories

Base class: `com.daml.error.ErrorCategory`.

You must not add, remove or change existing error categories unless you have good reasons to do so.
They are a part of the public API.

Any such change is likely a breaking change especially that we encourage the users
to base their error handling logic on error categories rather than bare gRPC status codes or more fine-grained
error codes.

Error categories, similar to error codes, use Scala annotations to provide descriptions that are used to automatically
generate sections of the official Daml documentation. Make sure they are clear and helpful to the users.

NOTE: Currently we have a unique mapping from error categories to gRPC status codes.
This is incidental and might change in the future.

## Error groups

Base class: `com.daml.error.ErrorGroup.ErrorGroup`.

Error groups are NOT part of the public API.
They only influence how we render error codes sections in the official Daml documentation.


## Handling errors on the client side

See `com.daml.error.samples.SampleUserSide`.

## Error definitions

The error definitions defined in this target (see `com.digitalasset.canton.ledger.error.CommonErrors`)
are generic errors that can be used across various Daml components (e.g. Ledger API test tool, Ledger API client, Ledger API server etc.).
