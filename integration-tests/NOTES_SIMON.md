# Scratchpad

## Shell commands to dabble with user management

bazel run //ledger/sandbox:sandbox-binary -- \
  --ledgerid "yellow-pages-sandbox" \
  --jdbcurl="jdbc:postgresql://localhost/sandbox_db?user=sandbox_user&password=sandbox_pw" \
  --log-level=INFO \
  --auth-jwt-rs256-crt sandbox.crt

!! do not forget "scope": "daml_ledger_api" in the token

grpcurl list localhost:6865

grpcurl \
    -plaintext \
    -H "Authorization: Bearer `cat participant_admin.token`" \
    localhost:6865 \
    com.daml.ledger.api.v1.LedgerIdentityService.GetLedgerIdentity

grpcurl \
    -plaintext \
    -H "Authorization: Bearer `cat participant_admin.token`" \
    localhost:6865 \
    com.daml.ledger.api.v1.admin.UserManagementService.ListUsers

grpcurl \
    -plaintext \
    -H "Authorization: Bearer `cat participant_admin.token`" \
    -d '{ "user_id": "participant_admin" }' \
    localhost:6865 \
    com.daml.ledger.api.v1.admin.UserManagementService.ListUserRights


grpcurl \
    -plaintext \
    -H "Authorization: Bearer `cat participant_admin.token`" \
    -d '{ "user": { "id": "another_admin" }, "rights": [{"participantAdmin":{}}] }' \
    localhost:6865 \
    com.daml.ledger.api.v1.admin.UserManagementService.CreateUser



## grpcul

grpcurl --help
Usage:
	/nix/store/m7j2dqalsxdih9539nnvkhwam36krq8d-grpcurl-1.8.5/bin/grpcurl [flags] [address] [list|describe] [symbol]

The 'address' is only optional when used with 'list' or 'describe' and a
protoset or proto flag is provided.

If 'list' is indicated, the symbol (if present) should be a fully-qualified
service name. If present, all methods of that service are listed. If not
present, all exposed services are listed, or all services defined in protosets.

If 'describe' is indicated, the descriptor for the given symbol is shown. The
symbol should be a fully-qualified service, enum, or message name. If no symbol
is given then the descriptors for all exposed or known services are shown.

If neither verb is present, the symbol must be a fully-qualified method name in
'service/method' or 'service.method' format. In this case, the request body will
be used to invoke the named method. If no body is given but one is required
(i.e. the method is unary or server-streaming), an empty instance of the
method's request type will be sent.

The address will typically be in the form "host:port" where host can be an IP
address or a hostname and port is a numeric port or service name. If an IPv6
address is given, it must be surrounded by brackets, like "[2001:db8::1]". For
Unix variants, if a -unix=true flag is present, then the address must be the
path to the domain socket.

Available flags:
  -H value
    	Additional headers in 'name: value' format. May specify more than one
    	via multiple flags. These headers will also be included in reflection
    	requests to a server.
  -allow-unknown-fields
    	When true, the request contents, if 'json' format is used, allows
    	unkown fields to be present. They will be ignored when parsing
    	the request.
  -authority string
    	The authoritative name of the remote server. This value is passed as the
    	value of the ":authority" pseudo-header in the HTTP/2 protocol. When TLS
    	is used, this will also be used as the server name when verifying the
    	server's certificate. It defaults to the address that is provided in the
    	positional arguments.
  -cacert string
    	File containing trusted root certificates for verifying the server.
    	Ignored if -insecure is specified.
  -cert string
    	File containing client certificate (public key), to present to the
    	server. Not valid with -plaintext option. Must also provide -key option.
  -connect-timeout float
    	The maximum time, in seconds, to wait for connection to be established.
    	Defaults to 10 seconds.
  -d string
    	Data for request contents. If the value is '@' then the request contents
    	are read from stdin. For calls that accept a stream of requests, the
    	contents should include all such request messages concatenated together
    	(possibly delimited; see -format).
  -emit-defaults
    	Emit default values for JSON-encoded responses.
  -expand-headers
    	If set, headers may use '${NAME}' syntax to reference environment
    	variables. These will be expanded to the actual environment variable
    	value before sending to the server. For example, if there is an
    	environment variable defined like FOO=bar, then a header of
    	'key: ${FOO}' would expand to 'key: bar'. This applies to -H,
    	-rpc-header, and -reflect-header options. No other expansion/escaping is
    	performed. This can be used to supply credentials/secrets without having
    	to put them in command-line arguments.
  -format string
    	The format of request data. The allowed values are 'json' or 'text'. For
    	'json', the input data must be in JSON format. Multiple request values
    	may be concatenated (messages with a JSON representation other than
    	object must be separated by whitespace, such as a newline). For 'text',
    	the input data must be in the protobuf text format, in which case
    	multiple request values must be separated by the "record separator"
    	ASCII character: 0x1E. The stream should not end in a record separator.
    	If it does, it will be interpreted as a final, blank message after the
    	separator. (default "json")
  -format-error
    	When a non-zero status is returned, format the response using the
    	value set by the -format flag .
  -help
    	Print usage instructions and exit.
  -import-path value
    	The path to a directory from which proto sources can be imported, for
    	use with -proto flags. Multiple import paths can be configured by
    	specifying multiple -import-path flags. Paths will be searched in the
    	order given. If no import paths are given, all files (including all
    	imports) must be provided as -proto flags, and grpcurl will attempt to
    	resolve all import statements from the set of file names given.
  -insecure
    	Skip server certificate and domain verification. (NOT SECURE!) Not
    	valid with -plaintext option.
  -keepalive-time float
    	If present, the maximum idle time in seconds, after which a keepalive
    	probe is sent. If the connection remains idle and no keepalive response
    	is received for this same period then the connection is closed and the
    	operation fails.
  -key string
    	File containing client private key, to present to the server. Not valid
    	with -plaintext option. Must also provide -cert option.
  -max-msg-sz int
    	The maximum encoded size of a response message, in bytes, that grpcurl
    	will accept. If not specified, defaults to 4,194,304 (4 megabytes).
  -max-time float
    	The maximum total time the operation can take, in seconds. This is
    	useful for preventing batch jobs that use grpcurl from hanging due to
    	slow or bad network links or due to incorrect stream method usage.
  -msg-template
    	When describing messages, show a template of input data.
  -plaintext
    	Use plain-text HTTP/2 when connecting to server (no TLS).
  -proto value
    	The name of a proto source file. Source files given will be used to
    	determine the RPC schema instead of querying for it from the remote
    	server via the gRPC reflection API. When set: the 'list' action lists
    	the services found in the given files and their imports (vs. those
    	exposed by the remote server), and the 'describe' action describes
    	symbols found in the given files. May specify more than one via multiple
    	-proto flags. Imports will be resolved using the given -import-path
    	flags. Multiple proto files can be specified by specifying multiple
    	-proto flags. It is an error to use both -protoset and -proto flags.
  -protoset value
    	The name of a file containing an encoded FileDescriptorSet. This file's
    	contents will be used to determine the RPC schema instead of querying
    	for it from the remote server via the gRPC reflection API. When set: the
    	'list' action lists the services found in the given descriptors (vs.
    	those exposed by the remote server), and the 'describe' action describes
    	symbols found in the given descriptors. May specify more than one via
    	multiple -protoset flags. It is an error to use both -protoset and
    	-proto flags.
  -protoset-out string
    	The name of a file to be written that will contain a FileDescriptorSet
    	proto. With the list and describe verbs, the listed or described
    	elements and their transitive dependencies will be written to the named
    	file if this option is given. When invoking an RPC and this option is
    	given, the method being invoked and its transitive dependencies will be
    	included in the output file.
  -reflect-header value
    	Additional reflection headers in 'name: value' format. May specify more
    	than one via multiple flags. These headers will *only* be used during
    	reflection requests and will be excluded when invoking the requested RPC
    	method.
  -rpc-header value
    	Additional RPC headers in 'name: value' format. May specify more than
    	one via multiple flags. These headers will *only* be used when invoking
    	the requested RPC method. They are excluded from reflection requests.
  -servername string
    	Override server name when validating TLS certificate. This flag is
    	ignored if -plaintext or -insecure is used.
    	NOTE: Prefer -authority. This flag may be removed in the future. It is
    	an error to use both -authority and -servername (though this will be
    	permitted if they are both set to the same value, to increase backwards
    	compatibility with earlier releases that allowed both to be set).
  -unix
    	Indicates that the server address is the path to a Unix domain socket.
  -use-reflection
    	When true, server reflection will be used to determine the RPC schema.
    	Defaults to true unless a -proto or -protoset option is provided. If
    	-use-reflection is used in combination with a -proto or -protoset flag,
    	the provided descriptor sources will be used in addition to server
    	reflection to resolve messages and extensions.
  -user-agent string
    	If set, the specified value will be added to the User-Agent header set
    	by the grpc-go library.
  -v	Enable verbose output.
  -version
    	Print version.
  -vv
    	Enable very verbose output.



## Sandbox docs -- as of 2021-01-21

Sandbox version 0.0.0
Usage: sandbox [options] [<archive>...]

  <archive>...             Daml archives to load in .dar format. Only Daml-LF v1 Archives are currently supported. Can be mixed in with optional arguments.
  -a, --address <value>    Service host. Defaults to binding on localhost.
  -p, --port <value>       Service port. Defaults to 6865.
  --port-file <value>      File to write the allocated port number to. Used to inform clients in CI about the allocated port.
  --ledgerid <value>       Ledger ID. If missing, a random unique ledger ID will be used.
  --participant-id <value>
                           Participant ID. Defaults to 'sandbox-participant'.
  --dalf                   This argument is present for backwards compatibility. DALF and DAR archives are now identified by their extensions.
  -s, --static-time        Use static time. When not specified, wall-clock-time is used.
  -w, --wall-clock-time    Use wall clock time (UTC). This is the default.
  --no-parity              Legacy flag with no effect.
  --pem <value>            TLS: The pem file to be used as the private key.
  --tls-secrets-url <value>
                           TLS: URL of a secrets service that provides parameters needed to decrypt the private key. Required when private key is encrypted (indicated by '.enc' filename suffix).
  --crt <value>            TLS: The crt file to be used as the cert chain. Required if any other TLS parameters are set.
  --cacrt <value>          TLS: The crt file to be used as the trusted root CA.
  --client-auth <value>    TLS: The client authentication mode. Must be one of none, optional or require. Defaults to required.
  --min-tls-version <value>
                           TLS: Indicates the minimum TLS version to enable. If specified must be either '1.2' or '1.3'.
  --cert-revocation-checking <value>
                           TLS: enable/disable certificate revocation checks with the OCSP. Disabled by default.
  --max-inbound-message-size <value>
                           Max inbound message size in bytes. Defaults to 4194304.
  --maxInboundMessageSize <value>
                           This flag is deprecated -- please use --max-inbound-message-size.
  --jdbcurl <value>        This flag is deprecated -- please use --sql-backend-jdbcurl.
  --log-level <value>      Default logging level to use. Available values are DEBUG, ERROR, TRACE, WARN, INFO. Defaults to INFO.
  --auth-jwt-rs256-crt <value>
                           Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given X509 certificate file (.crt)
  --auth-jwt-es256-crt <value>
                           Enables JWT-based authorization, where the JWT is signed by ECDSA256 with a public key loaded from the given X509 certificate file (.crt)
  --auth-jwt-es512-crt <value>
                           Enables JWT-based authorization, where the JWT is signed by ECDSA512 with a public key loaded from the given X509 certificate file (.crt)
  --auth-jwt-rs256-jwks <value>
                           Enables JWT-based authorization, where the JWT is signed by RSA256 with a public key loaded from the given JWKS URL
  --events-page-size <value>
                           Number of events fetched from the index for every round trip when serving streaming calls. Default is 1000.
  --buffers-prefetching-parallelism <value>
                           Number of events fetched/decoded in parallel for populating the Ledger API internal buffers. Default is 8.
  --acs-id-page-size <value>
                           Number of contract ids fetched from the index for every round trip when serving ACS calls. Default is 20000.
  --acs-id-fetching-parallelism <value>
                           Number of contract id pages fetched in parallel when serving ACS calls. Default is 2.
  --acs-contract-fetching-parallelism <value>
                           Number of event pages fetched in parallel when serving ACS calls. Default is 2.
  --acs-global-parallelism-limit <value>
                           Maximum number of concurrent ACS queries to the index database. Default is 10.
  --acs-id-queue-limit <value>
                           Maximum number of contract ids queued for fetching. Default is 10000000.
  --max-commands-in-flight <value>
                           Maximum number of submitted commands for which the CommandService is waiting to be completed in parallel, for each distinct set of parties, as specified by the `act_as` property of the command. Reaching this limit will cause new submissions to wait in the queue before being submitted. Default is 256.
  --input-buffer-size <value>
                           Maximum number of commands waiting to be submitted for each distinct set of parties, as specified by the `act_as` property of the command. Reaching this limit will cause the server to signal backpressure using the ``RESOURCE_EXHAUSTED`` gRPC status code. Default is 512.
  --max-lf-value-translation-cache-entries <value>
                           The maximum size of the cache used to deserialize Daml-LF values, in number of allowed entries. By default, nothing is cached.
  --max-ledger-time-skew <value>
                           Maximum skew (in seconds) between the ledger time and the record time. Default is 120.
  --enable-append-only-schema
                           Deprecated parameter. The append-only index database with parallel ingestion is now always enabled.
  --enable-compression     Enables application-side compression of Daml-LF values stored in the database using the append-only schema. By default, compression is disabled.
  --use-pre-1.18-error-codes
                           Enables gRPC error code compatibility mode to the pre-1.18 behaviour. This option is deprecated and will be removed in future release versions.
  --user-management-cache-expiry <value>
                           Defaults to 5 seconds. Determines the maximum delay for propagating user management state changes.
  --user-management-max-cache-size <value>
                           Defaults to 100 entries. Determines the maximum in-memory cache size for user management state.
  --metrics-reporter <value>
                           Start a metrics reporter. Must be one of "console", "csv:///PATH", "graphite://HOST[:PORT][/METRIC_PREFIX]", or "prometheus://HOST[:PORT]".
  --metrics-reporting-interval <value>
                           Set metric reporting interval.
  --help                   Print the usage text
  --early-access-unsafe    Enable preview version of the next Daml-LF language. Should not be used in production.
  --contract-id-seeding <value>
                           Set the seeding mode of contract IDs. Possible values are strong, testing-weak, testing-static. "strong" mode is not compatible with development mode. Default is "strong".
  --implicit-party-allocation <value>
                           When referring to a party that doesn't yet exist on the ledger, Sandbox will implicitly allocate that party. You can optionally disable this behavior to bring Sandbox into line with other ledgers.
  --sql-backend-jdbcurl <value>
                           Deprecated: Use the Daml Driver for PostgreSQL if you need persistence.
                           The JDBC connection URL to a Postgres database containing the username and password as well. If present, Sandbox will use the database to persist its data.
  --database-connection-pool-size <value>
                           The number of connections in the database connection pool used for serving ledger API requests.
