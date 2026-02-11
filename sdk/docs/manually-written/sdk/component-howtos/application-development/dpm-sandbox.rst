.. _component-howtos-application-development-daml-sandbox:

.. _sandbox-manual:

Sandbox
=======

Sandbox is a program for running a Canton ledger with your Daml code.
The ledger uses the simplest topology possible: a single Participant Node connected to a Synchronizer Node.
Use the sandbox when you need access to a Canton ledger running your Daml code and matching the target Participant Node topology is not required.

Install
-------

Install the Sandbox by :externalref:`installing dpm <dpm-install>`.

Configure
---------

To configure the Sandbox, use the command line.

Command line configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

To view all available command line configuration options for Sandbox, run ``dpm sandbox --help`` in your terminal:

.. code-block:: none

      Usage: dpm sandbox [--port ARG] [--admin-api-port ARG]
                          [--sequencer-public-port ARG] [--sequencer-admin-port ARG]
                          [--mediator-admin-port ARG] [--json-api-port ARG]
                          [--json-api-port-file PATH] [--canton-port-file PATH]
                          [--static-time | --wall-clock-time] [--canton-help]
                          [-c|--config FILE] [--port-file PATH] [--dar PATH] [ARG]

      Available options:
        --json-api-port ARG      Port that the HTTP JSON API should listen on, omit to
                                 disable it
        --json-api-port-file PATH
                                 File to write canton json-api port when ready
        --canton-port-file PATH  File to write canton participant ports when ready
        --canton-help            Display the help of the underlying Canton JAR instead
                                 of the Sandbox wrapper. This is only required for
                                 advanced options.
        -c,--config FILE         Set configuration file(s). If several configuration
                                 files assign values to the same key, the last value
                                 is taken.
        --port-file PATH         File to write ledger API port when ready
        --dar PATH               DAR file to upload to sandbox
        --shutdown-stdin-close   Shut down when stdin is closed, disabled by default
        -h,--help                Show this help text

Any unrecognized command-line arguments will be treated as arguments to be
supplied to the underlying Canton JAR. Display these options by running ``dpm sandbox --canton-help``
in your terminal.

Canton configuration
^^^^^^^^^^^^^^^^^^^^

Behind the scenes, Sandbox runs an underlying Canton ledger with a default
configuration file to initialize a participant named ``sandbox``, a sequencer
named ``sequencer1``, and a mediator named ``mediator1``.

Configure the underlying Canton ledger further in one of two ways:

* Specify additional configuration file(s) to apply on top of the default
  configuration using ``--config filepath``. If several configuration files
  assign to the same key, the last value is taken.

* Set the value of a specific key with ``-C key=value``. For example, to
  override the ledger-api port of the sandbox participant, the command would be:

  .. code-block:: none

        dpm sandbox -C canton.participants.sandbox.ledger-api.port=9999

.. _canton-declarative-api:

Canton Declarative API
^^^^^^^^^^^^^^^^^^^^^^
The declarative API allows specifying DARs, Parties and Users desired to be present on the Canton ledger.
The ledger will automatically take care of uploading the DARs, and creating the parties and users.

- upload local DARs

.. code-block:: none

  canton.parameters.enable-alpha-state-via-config = yes
  canton.parameters.state-refresh-interval = 5s
  
  canton.participants.sandbox.alpha-dynamic.dars = [
    { location = "./my-asset.dar" },
  ]

Operate
-------

Start Canton with a single participant:

.. code-block:: none

  $ dpm sandbox
  Starting Canton sandbox.
  Listening at port 6865
  Canton sandbox is ready.

Interacting with Sandbox's ledger
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the sandbox is running, you may interact with it the same way you would for
any Canton instance. For example, you may upload dars to it, or run scripts
against it:

.. code-block:: none

    $ dpm sandbox --dar <path to DAR>
    $ dpm script --ledger-host localhost --port 6865 --dar <path to DAR> --script-name <script name in DAR>

Because ``dpm sandbox`` is a Canton instance, all documentation for using Canton applies.

.. _running-canton-console-against-daml-sandbox:

Connecting to Sandbox's console
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once you have a Sandbox running locally (i.e. after running ``dpm sandbox``)
you may connect to Sandbox remotely by running the ``dpm canton-console``
command in a separate terminal:

.. code-block:: none

    $ daml canton-console
       _____            _
      / ____|          | |
     | |     __ _ _ __ | |_ ___  _ __
     | |    / _` | '_ \| __/ _ \| '_ \
     | |___| (_| | | | | || (_) | | | |
      \_____\__,_|_| |_|\__\___/|_| |_|

      Welcome to Canton!
      Type `help` to get started. `exit` to leave.

    @

You can quit the session by running the ``exit`` command.

Built-in documentation
""""""""""""""""""""""

The Canton console comes with built-in documentation. You
can use the ``help`` command to get online documentation for top-level commands. Many objects in the
console also have built-in help that you can access by invoking the ``help`` method on them.

For example, you can ask for help on the ``health`` object by typing:

.. code-block:: scala

  health.help

Or go more in-depth about specific items within that object, as in the following example:

.. code-block:: scala

  health.help("status")

Interact with the Sandbox
"""""""""""""""""""""""""

One of the objects available in the Canton console represents the Sandbox itself. The object is called
``sandbox`` and you can use it to interact with the Sandbox. For example, you can list the DARs loaded
on the Sandbox by running the following command:

.. code-block:: scala

  sandbox.dars.list()

Among the various features available as part of the console, you can manage parties and packages,
check the health of the Sandbox, perform pruning operations, and more. Consult the built-in documentation mentioned
above and the main documentation for the Canton console to learn about further capabilities.

How it works
""""""""""""

Canton offers a console where you can run administrative or debugging commands.

When you run the Sandbox using ``dpm sandbox``, you are effectively starting an
in-memory instance of Canton with a single sync domain and a single participant.

As such, you can interact with the running Sandbox using the console, just like you would
in a production environment.

For an in-depth guide on how to use this tool against a production, staging or
testing environment, consult the :externalref:`main documentation for the Canton console <canton_console>`.

Testing your Daml contracts
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Sandbox is primarily used as the first step in :brokenref:`testing your Daml contracts in isolation <sdlc-howtos_how-to-test-your-backends_daml-sandbox>`

.. _sandbox-authorization:

Run with authorization
^^^^^^^^^^^^^^^^^^^^^^

By default, Sandbox accepts all valid Ledger API requests without performing any request authorization.

To start Sandbox with authorization using `JWT-based <https://jwt.io/>`__
access tokens as described in the
:ref:`Authorization documentation <authorization>`, create a
config file that specifies the type of
authorization service and the path to the certificate, then supply that config
file to Sandbox with ``dpm sandbox --config auth.conf``.

.. code-block:: none
   :caption: auth.conf

   canton.participants.sandbox.ledger-api.auth-services = [{
       // type can be
       //   jwt-rs-256-crt
       //   jwt-es-256-crt
       //   jwt-es-512-crt
       //   jwt-rs-256-jwks with an additional url
       //   unsafe-jwt-hmac-256 with an additional secret
       type = jwt-rs-256-crt
       certificate = my-certificate.cert
   }]

The settings under ``auth-services`` are described in detail in `API configuration documentation <jwt-authorization>`__

Generate JSON web tokens (JWT)
""""""""""""""""""""""""""""""

To generate access tokens for testing purposes, use the `jwt.io <https://jwt.io/>`__ web site.


Generate RSA keys
"""""""""""""""""

To generate RSA keys for testing purposes, use the following command

.. code-block:: none

  openssl req -nodes -new -x509 -keyout sandbox.key -out sandbox.crt

which generates the following files:

- ``sandbox.key``: the private key in PEM/DER/PKCS#1 format
- ``sandbox.crt``: a self-signed certificate containing the public key, in PEM/DER/X.509 Certificate format

Generate EC keys
""""""""""""""""

To generate keys to be used with ES256 for testing purposes, use the following command

.. code-block:: none

  openssl req -x509 -nodes -days 3650 -newkey ec:<(openssl ecparam -name prime256v1) -keyout ecdsa256.key -out ecdsa256.crt

which generates the following files:

- ``ecdsa256.key``: the private key in PEM/DER/PKCS#1 format
- ``ecdsa256.crt``: a self-signed certificate containing the public key, in PEM/DER/X.509 Certificate format

Similarly, you can use the following command for ES512 keys:

.. code-block:: none

  openssl req -x509 -nodes -days 3650 -newkey ec:<(openssl ecparam -name secp521r1) -keyout ecdsa512.key -out ecdsa512.crt

.. _sandbox-tls:

Run with TLS
^^^^^^^^^^^^

To enable TLS, you need to specify the private key for your server and
the certificate chain. This enables TLS for both the gRPC Ledger API and
the Canton Admin API. When enabling client authentication, you also
need to specify client certificates which can be used by Canton’s
internal processes. Note that the identity of the application
will not be proven by using this method, i.e. the `application_id` field in the request
is not necessarily correlated with the CN (Common Name) in the certificate.
Below, you can see an example config. For more details on TLS, refer to
Canton’s documentation on TLS configuration.


.. code-block:: none
   :caption: tls.conf

   canton.participants.sandbox.ledger-api {
     tls {
       // the certificate to be used by the server
       cert-chain-file = "./tls/ledger-api.crt"
       // private key of the server
       private-key-file = "./tls/ledger-api.pem"
       // trust collection, which means that all client certificates will be verified using the trusted
       // certificates in this store. if omitted, the JVM default trust store is used.
       trust-collection-file = "./tls/root-ca.crt"
       // define whether clients need to authenticate as well (default not)
       client-auth = {
         // none, optional and require are supported
         type = require
         // If clients are required to authenticate as well, we need to provide a client
         // certificate and the key, as Canton has internal processes that need to connect to these
         // APIs. If the server certificate is trusted by the trust-collection, then you can
         // just use the server certificates. Otherwise, you need to create separate ones.
         admin-client {
           cert-chain-file = "./tls/admin-client.crt"
           private-key-file = "./tls/admin-client.pem"
         }
       }
     }
   }

Dev Protocol
^^^^^^^^^^^^

To enable the canton dev protocol:

.. code:: shell

  dpm sandbox --dev

Run using the Oracle JDK
^^^^^^^^^^^^^^^^^^^^^^^^

When using the Oracle JDK, you may likely get an error such as the following when executing transactions:

.. code-block:: none

   java.lang.SecurityException: JCE cannot authenticate the provider BC
           at java.base/javax.crypto.JceSecurity.getInstance(JceSecurity.java:150)
           at java.base/javax.crypto.Mac.getInstance(Mac.java:272)
           at com.digitalasset.canton.crypto.provider.jce.JcePureCrypto.$anonfun$computeHmacWithSecretInternal$1(JcePureCrypto.scala:873)
           (...)

The reason is that the Canton JAR embeds the BouncyCastle provider, and the Oracle JVM requires to verify the signature of third-party security libraries.

In this case, you need to download the `BouncyCastle JAR file <https://mvnrepository.com/artifact/org.bouncycastle/bcprov-jdk18on/1.83>`__ and explicitly provide it on the class path while running the sandbox as follows:

.. code:: shell

   java -cp bcprov-jdk18on-1.83.jar:$HOME/.dpm/cache/components/canton-enterprise/3.5.0/lib/canton-enterprise-3.5.0.jar com.digitalasset.canton.CantonCommunityApp sandbox

replacing `3.5.0` with your appropriate Canton version.


Troubleshoot
------------

Failed to bind to address
^^^^^^^^^^^^^^^^^^^^^^^^^

By default, Sandbox reserves five ports for its Canton services:

* ``6865`` for the participant's Ledger API
* ``6866`` for the participant's Admin API
* ``6867`` for the sequencer's public API
* ``6868`` for the sequencer's admin API
* ``6869`` for the mediator's admin API

The Sandbox will also bind to the port specified in the ``--json-api-port``, if
any.

When one of these ports is already used by an existing process, Sandbox will
emit an error that contains the following text:

.. code-block:: none

   Failed to bind to address /127.0.0.1:<port number>

This is most commonly either caused by an existing process that is already
listening on that port, or if you do not have the permissions to bind to that
address.

On Linux, the ``lsof -n -i`` command lists what processes are already listening
to a port. For example, if an existing Java program is already listening to
6865, ``lsof`` would look as follows:

.. code-block:: none

      $ lsof -n -i
      ...
      java       707977 username       77u  IPv6 67556378      0t0  TCP 127.0.0.1:6865 (LISTEN)
      ...

If killing the existing process isn't an option, or if you don't have the
permission to bind to a given port, you can reconfigure the ports of a given
node using the top-level options described below:

* Use ``--port=<port>`` to override binding to ``6865``
* Use ``--admin-api-port=<port>`` to override binding to ``6866``
* Use ``--sequencer-public-port=<port>`` to override binding to ``6867``
* Use ``--sequencer-admin-port=<port>`` to override binding to ``6868``
* Use ``--mediator-admin-port=<port>`` to override binding to ``6869``
* Use ``--json-api-port`` to change the port to which the JSON API binds.

SDK not installed
^^^^^^^^^^^^^^^^^

If the ``daml.yaml`` file of the project you are currently in specifies a
version of the dpm SDK that is not installed, you may get the following error
message:

.. code-block:: none

      SDK not installed. Cannot run command without SDK.

To fix this, you can:

* Install the SDK as instructed to by the error, or
* Change the SDK version in the project's ``daml.yaml`` file, or
* Change directories to be outside of the project, where the default Daml
  version that is already installed on your system will be used.
