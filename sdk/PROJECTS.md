# Contributing to the Daml and Canton Projects

The following projects are of great value but less urgency and have therefore not yet been picked up by a developer. If 
you are up for a challenge and would like to move the project significantly forward, please consider picking one and
contributing to it. 

The list summarizes projects that are part of the Daml ecosystem beyond the core Daml language and SDK repository.

If you are interested in one of these projects and would like to know more about it, please reach out to the maintainers via https://discuss.daml.com/.

## Daml
### Public, Protected, Private Access Modifiers
Right now, all templates and choices are public. We would like to introduce new access modifiers that restrict
the visibility of templates and choices:
- instances of contracts of protected templates cannot be created on the Ledger API.
- instances of contract of private templates can only be created within the defining module.
- protected choices cannot be exercised on the Ledger API.
- private choices can only be exercised within the defining module.

### Separate Daml Script from Daml
Daml script is too tightly entangled with Daml. We would like to completely separate it such that Daml scripts are not a dependency of Canton.

### DAR Dependency Management
The current DAR dependency management is very basic. At compile time, all dependencies of a DAR get packaged by default.
Instead, we would like to separate this such that by default, only "lean" DARs exist, and an additional command to package
all dependencies together into a shipping DAR. On top of this, it would be desirable to establish a Maven like system
to share DAR dependencies (by e.g. using Maven ...)

### DAR Verification Tooling
We would like to extend our DAR tooling to allow users to verify any DAR that they have received. The compilation of DARs
is deterministic, but requires the same versions of the SDK. Therefore, the goal would be to build a tool that can verify
a DAR by determining the right version of the SDK, compiling the DAR and verifying the content.

## Canton
### Core
#### Native Canton Console
The Canton Console is a Scala based application to interact with Canton nodes through their Admin APIs by exposing
a Scala prompt to the user. While the experience should largely resemble a classic shell prompt, it is clear that
this is not fully provided by the current implementation. In addition, the startup time of the JVM make bash scripting
with the Canton console painful. Therefore, we would like to implement a native shell for the Canton Console which can
run from the Command line without the need for the JVM. Many solutions are possible, from Rust to Python.

#### Interactive Submission to Replace Daml Exceptions
The interactive submission service is a feature that allows to first compute transactions before submitting them (optionally
authorized with an external signature). This feature could be extended to allow "extending" a prepared submission, such
that "Exception catching" could be removed from Daml. This would allow to remove the "try-catch"

#### Interning and Encoding Optimization
The hashes shipped in Canton are currently encoded in a verbose way which does not optimize for size. We would like to
change the on-the-wire encoding of hashes (package, template, contract id, parties, domains, participants, mediators, 
sequencers) to be more compact. In some cases, we might even want to intern these values to save space whenever they 
are used repeatedly. Not every interning makes sense, as the view payload is compressed before encryption, but there 
should be quite some gain possible.

### Deployment
Right now, Canton is shipped as JAR with reference configuration files. We would like to provide a more user-friendly
deployment experience by providing Helm charts, standard deployments for cloud environments, and even a standard
brew package for MacOS or deb/rpm for Linux.

## SDK
### Proto Native GRPC Ledger API
The current GRPC Ledger API encodes values using recursive record data structures. This makes the values on the API almost
impossible to use without codegen. We would like to switch to "proto-native" encoding by dynamically generating the
protobuf schema from the Daml-LF schema and let applications use protobuf directly instead of requiring codegen, as
this would support all languages with protobuf support.

### Swagger for JSON API
In order to use the JSON API, developers need to derive the JSON schema from their understanding of the Daml templates
and choices. We would like to auto-generate a Swagger schema from the Daml-LF schema to make it easier to use the JSON API.

### Javascript and Python Automation
Using the JSON and GRPC Ledger API in Javascript and Python is currently cumbersome, as the SDK only officially supports
Java and Typescript, with Typescript support being optimized for UI development. We would like to improve the situation and 
provide decent application and automation programming support for more popular languages such as Javascript and Python.

### Ledger Explorer
The Navigator in Daml 1.x and 2.x allowed to explore the Ledger in a web-interface. This feature is currently missing as the
previous backend was not ported to 3.x. We would like to re-implement the Ledger Explorer and update it with the most recent features.

## Integration
### An ERC20 Token Bridge
While tokens in the Canton world are interoperable in the entire network, they are not interoperable with the Ethereum.
We would like to implement a standard ERC20 token bridge to allow for the transfer of tokens between the Canton network
and public Ethereum. The token bridge could be implemented straight forward as a decentralized party in Daml.

## Splice / Global Synchronizer
### MetaMask Snap for Canton Coin
We would like to create a metamask snap to integrate with Canton coin.