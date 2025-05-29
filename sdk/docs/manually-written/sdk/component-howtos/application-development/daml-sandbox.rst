.. _component-howtos-application-development-daml-sandbox:

.. _sandbox-manual:

Daml Sandbox
============

The Daml Sandbox, or Sandbox for short, is a program that enables rapid application prototyping by running a simple Canton ledger with one participant.

Install
-------

Install the Daml Sandbox by :brokenref:`installing the Daml Assistant <Install section of Daml Assistant>`.

Configure
---------

To configure the Daml Sandbox, we use the command line.

Command line configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^

To view all available command line configuration options for Daml Sandbox, run ``daml sandbox --help`` in your terminal:

.. code-block:: none

      Usage: daml sandbox [--port ARG] [--admin-api-port ARG] 
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
supplied to the underlying Canton JAR. Display these options by running ``daml sandbox --canton-help``
in your terminal.

Canton configuration
^^^^^^^^^^^^^^^^^^^^

Behind the scenes, Daml Sandbox runs an underlying Canton ledger with a default
configuration file to initialize a participant named ``sandbox``, a sequencer
named ``sequencer1``, and a mediator named ``mediator1``.

Configure the underlying Canton ledger further in one of two ways:

* Specify additional configuration file(s) to apply on top of the default
  configuration using ``--config filepath``. If several configuration files
  assign to the same key, the last value is taken.

* Set the value of a specific key with ``-C key=value``. For example, to
  override the ledger-api port of the sandbox participant, the command would be:

  .. code-block:: none

        daml sandbox -C canton.participants.sandbox.ledger-api.port=9999

Operate
-------

There are two ways to start running the Daml Sandbox:

#. Run the Daml Sandbox in isolation with:

   .. code-block:: none

         $ daml sandbox
         Starting Canton sandbox.
         Listening at port 6865
         Canton sandbox is ready.

   This will start Canton with a single participant.

#. Start Sandbox using the ``daml start`` command while in a Daml project. This command will:

   #. Launch the Sandbox via an underlying call to ``daml sandbox``, the command above.
   #. Compile the Daml project to a DAR as specified in the project's ``daml.yaml``.
   #. Upload the resulting DAR to the running Sandbox.
   #. The script specified in the ``init-script`` field in ``daml.yaml`` will be loaded into the ledger.

   .. code-block:: none

         $ daml start
         ...
         Compiling main to a DAR.
         ...
         Created .daml/dist/main-1.0.0.dar
         ...
         Waiting for canton sandbox to start.
         Uploading .daml/dist/main-1.0.0.dar to localhost:6865
         DAR upload succeeded.
         Running the initialization script.
         ...
         Waiting for JSON API to start.
         The Canton sandbox and JSON API are ready to use.

   **Note**: To forward an option to the underlying ``daml sandbox`` call, use
   the ``--sandbox-option`` flag.

   For example, to change the sandbox's ledger-api port, the normal command would be

   .. code-block:: none

         daml sandbox -C canton.participants.sandbox.ledger-api.port=9999

   whereas the Daml Start command would be

   .. code-block:: none

         daml start --sandbox-option -C --sandbox-option canton.participants.sandbox.ledger-api.port=9999

Interacting with Sandbox's ledger
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once the sandbox is running, you may interact with it the same way you would for
any Canton instance. For example, you may upload dars to it, or run scripts
against it:

.. code-block:: none

    $ daml ledger upload-dar --host localhost --port 6865 <path to DAR>
    $ daml script --ledger-host localhost --port 6865 --dar <path to DAR> --script-name <script name in DAR>

Because ``daml sandbox`` is a Canton instance, all documentation for using Canton applies.

Connecting to Sandbox's console
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Once you have a Sandbox running locally (i.e. after running ``daml start`` or ``daml sandbox``)
you may connect to Sandbox remotely by running the ``daml canton-console``
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
**********************

The Canton console comes with built-in documentation. You
can use the ``help`` command to get online documentation for top-level commands. Many objects in the
console also have further built-in help that you can access by invoking the ``help`` method on them.

For example, you can ask for help on the ``health`` object by typing:

.. code-block:: scala

  health.help

Or go more in depth about specific items within that object as in the following example:

.. code-block:: scala

  health.help("status")

Interact with the Sandbox
*************************

One of the objects available in the Canton console represents the Sandbox itself. The object is called
``sandbox`` and you can use it to interact with the Sandbox. For example, you can list the DARs loaded
on the Sandbox by running the following command:

.. code-block:: scala

  sandbox.dars.list()

Among the various features available as part of the console, you can manage parties and packages,
check the health of the Sandbox, perform pruning operations and more. Consult the built-in documentation mentioned
above and the main documentation for the Canton console to learn about further capabilities.

How it works
************

Canton offers a console where you can run administrative or debugging commands.

When you run the Sandbox using ``daml start`` or ``daml sandbox``, you are effectively starting an
in-memory instance of Canton with a single sync domain and a single participant.

As such, you can interact with the running Sandbox using the console, just like you would
in a production environment.

For an in-depth guide on how to use this tool against a production, staging or
testing environment, consult the :externalref:`main documentation for the Canton console <canton_console>`.

.. _running-canton-console-against-daml-sandbox:

Testing your Daml contracts
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Daml Sandbox is primarily used as the first step in :brokenref:`testing your Daml contracts in isolation <sdlc-howtos_how-to-test-your-backends_daml-sandbox>`

.. _sandbox-authorization:

Run with authorization
^^^^^^^^^^^^^^^^^^^^^^

By default, Sandbox accepts all valid Ledger API requests without performing any request authorization.

To start Sandbox with authorization using `JWT-based <https://jwt.io/>`__
access tokens as described in the
:ref:`Authorization documentation <authorization>`, create a
config file that specifies the type of
authorization service and the path to the certificate, then supply that config
file to Sandbox with ``daml sandbox --config auth.conf``.

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

Troubleshoot
------------

Failed to bind to address
^^^^^^^^^^^^^^^^^^^^^^^^^

By default, Daml Sandbox reserves five ports for its Canton services:

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

On Linux, the ``lsof -n -i`` command can help you discover lists what processes
are already listening to a port. For example, if an existing Java program is
already listening to 6865, ``lsof`` would look as follows:

.. code-block:: none

      $ lsof -n -i
      ...
      java       707977 username       77u  IPv6 67556378      0t0  TCP 127.0.0.1:6865 (LISTEN)
      ...

If killing the existing process isn't an option, or if you don't have the
permission to bind to a given port, you can reconfigure the ports of a given
node using the top-level options described in.

* Use ``--port=<port>`` to override binding to ``6865``
* Use ``--admin-api-port=<port>`` to override binding to ``6866``
* Use ``--sequencer-public-port=<port>`` to override binding to ``6867``
* Use ``--sequencer-admin-port=<port>`` to override binding to ``6868``
* Use ``--mediator-admin-port=<port>`` to override binding to ``6869``
* Use ``--json-api-port`` to change the port to which the JSON API binds.

SDK not installed
^^^^^^^^^^^^^^^^^

If the ``daml.yaml`` file of the project you are currently in specifies a
version of the Daml SDK that is not installed, you may get the following error
message:

.. code-block:: none

      SDK not installed. Cannot run command without SDK.

In order to fix this, either

* Install the SDK as instructed to by the error, or
* Change the SDK version in the project's ``daml.yaml`` file, or
* Change directories to be outside of the project, where the default Daml
  version that is already installed on your system will be used.
