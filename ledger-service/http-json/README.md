# HTTP JSON Service

See "HTTP JSON API Service" on docs.daml.com for usage information.

Documentation can also be found in the RST format:
- [HTTP JSON API Service](/docs/source/json-api/index.rst)
- [Daml-LF JSON Encoding](/docs/source/json-api/lf-value-specification.rst)
- [Search Query Language](/docs/source/json-api/search-query-language.rst)
- [Metrics of the HTTP JSON API](/docs/source/json-api/metrics.rst)

## Development guidelines

The original endpoints for the JSON API followed some guidelines that
should be kept in mind when considering extensions and changes to the
API.

1. The JSON API is here to get basic ACS-using applications off the
   ground quickly, with a shallow development learning curve.  It is not
   meant to support every ledger use case or every size of application.
   Always keep in mind that JSON API is an ordinary ledger client; there
   is nothing that it can do that any application written against the
   gRPC API could not do, and in a way that is more optimized for
   whatever advanced application someone might have in mind.

2. JSON API is not a REST API.  That's one reason why REST isn't in the
   name, and that we never refer to it as REST anywhere in the
   documentation.  It uses HTTP, and JSON, not REST.  So "it's what REST
   does" is not considered at all in API design.

3. Instead, the most influential external design idea is that of
   minimizing the use of HTTP error codes, in favor of reporting status
   information in the response body, [as linked from the JSON API
   documentation](https://blog.restcase.com/rest-api-error-codes-101/).
   So do not overthink HTTP status; we do report broadly reasonable
   status, but what matters is that the response error body is
   semantically useful.  The sole exception is that, in most cases, gRPC
   errors upstream translate to [the gRPC-recommended HTTP status
   codes](https://cloud.google.com/apis/design/errors#generating_errors);
   this is handled globally within json-api, so you do not have to think
   about this, as long as you do not suppress exceptions from the ledger
   client.

4. Endpoints should be POSTs, and input arguments should be supplied as
   a POST request body, not in the URL.  This allows for obvious
   extension should more arguments be needed, whereas such extensions
   are tricky for URLs.
   
5. "Support the simplest, most common use cases" should always be
   favored heavily over "support anything someone might want to do with
   this endpoint".

6. As such, when considering how to present the arguments of an
   endpoint, don't include an argument simply because it is also present
   in a gRPC endpoint; consider whether it is actually important enough
   for the use cases described above.

7. Returning extra data is not free; it is a permanent cognitive burden
   on every new user of the endpoint.  As such, if a piece of
   information will not be used by many users of the endpoint, it should
   probably be stripped away.  Do not merely pass through everything
   that an associated gRPC endpoint happens to return.

8. It's preferable for a JSON API endpoint to _add simplifying value_.
   The best example of this is for template IDs; the JSON API lets you
   play fast-and-loose with package IDs _because it helps you get off
   the ground quickly_ as described in (1), not because this should be
   done for all applications.  A more complicated way that it adds value
   is with the query language.  The easiest way to _add simplifying
   value_ is to exclude rarely-needed arguments and results as mentioned
   in the above points.

9. The query language leans heavily on (1).  It is meant to efficiently
   support a basic set of application queries, not be all things to all
   querying types.  Supporting a broader set of query features means
   supporting that basic set worse in some ways, be it by obscuring the
   core feature set from newcomers or by penalizing performance of those
   core queries.  As such, consider whether a query feature will be
   broadly useful for the types of applications listed in (1), among
   other things, before committing to supporting it.
