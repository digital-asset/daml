.. Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _smart-contract-upgrades:

Smart Contract Upgrade
######################

.. .. toctree::
   :hidden:

Overview
========

What is Smart Contract Upgrade (SCU)?
-------------------------------------

Smart Contract Upgrade (SCU) allows Daml models (packages in DAR files) to be
updated on Canton transparently, provided some guidelines in making the
changes are followed. For example, you can fix an application bug by uploading
the DAR of the fixed package. This feature requires the minimum versions of LF
1.17 and Canton Protocol version 7. This section provides an overview of
the SCU feature, while :ref:`The Smart Contract Upgrade Model in Depth
<upgrade-model-reference>` is a concise, technical description of the feature.

Smart Contract Upgrade (SCU) is a feature for Daml
packages which enable authors to publish new versions of their templates
while maintaining compatibility with prior versions, without any
downtime for package users and existing contracts.

Package authors previously upgraded their packages by either:

-  | Providing a Daml workflow for upgrading contracts to the new version,
     and tell users of the old version to use the workflow to upgrade
     their old templates to the new versions.
   | This requires communication with all package users, splits package users
     across the two versions during the migration, and may incur
     significant network cost. This approach is described
     :ref:`here <upgrade-overview>`.

-  | Uploading the new version of their application code to their participant,
     temporarily stopping workflows relating to the old version, and manually
     upgrading every old template on the participant to the new version by
     directly manipulating Canton’s databases. This method is error-prone and
     requires some downtime for the participant.

Now, with SCU, any contract from the old package is automatically interpreted
as the new version as soon as it is used in a new workflow that requires
it. This feature is well-suited for developing and rolling out incremental
template changes. There are guidelines to ensure upgrade compatibility
between DAR files. The compatibility is checked at compile time, DAR
upload time, and runtime. This is to ensure data backwards upgrade
compatibility and forwards compatibility (subject to the guidelines
being followed) so that DARs can be safely upgraded to new versions. It
also prevents unexpected data loss at clients if a runtime downgrade
occurs (e.g., a client is using template version 1.0.0 while the
participant node has the newer version 1.1.0).

A general guideline for SCU is that additive application changes are allowed
but items cannot be removed. A summary of the allowed changes in templates
are:

-  A template can add new ``Optional`` fields at the end of the list of fields;

-  A ``record`` datatype can add new optional fields at the end of the list of
   fields, and a ``variant`` or ``enum`` datatype can add new constructors at
   the end;

-  The ``ensure`` predicate can be changed and it is reevaluated at interpretation;

-  A choice signature can be changed by adding ``Optional`` parameters at the end;

-  The controller of a choice can be changed;

-  The observer of a choice can be changed

-  The body of a choice can be changed;

-  A new choice can be added to a template;

-  The implementation of an interface instance can be changed;

The following table summarizes the changes supported by SCU. Consult the
sections below for additional information. For application updates
that are not covered by SCU, consult the :ref:`Automating the Upgrade Process
<upgrade-automation>` section, which describes an upgrade tool for
migrating contracts from an old version to a new version.

.. csv-table::
  :file: upgrade-scopes.csv
  :widths: 20, 20, 15, 45
  :header-rows: 1

In this way, package authors can publish new package versions that
improve contract functionality without requiring any
error-prone migrations, without downtime, without requiring any
additional network traffic, and without any extensive communication with
downstream users.

With SCU, packages may now be referenced by either:

-  the package ID

-  the package name.

Package names are used to group versions of a package. Any two packages with
the same package name must have distinct package versions.

**Note:**

-  When defining a Daml package, the ``name`` field of the package's ``daml.yaml`` is now used to
   specify the SCU package name.

The JSON API server is compatible with the smart contract upgrade
feature by:

-  Supporting package names for commands and queries;

-  Allowing use of an optional ``packageIdSelectionPreference`` field to
   specify a preferred package ID to use;

-  Requiring either a package ID or package name to be present to disambiguate
   the partially-qualified form of template/interface IDs.

Previously JSON API had supported partially qualified template IDs,
(i.e. simply ``<module>:<entity>``) as an interactive convenience which
fails if there is more than one package with matching template names.
Since this format was not supported for production use and does not work
with SCU, it is now unavailable.

The Java and TypeScript codegen allow the use of package name and
package ID (if needed).

PQS supports for this feature.  PQS now supports the highly sought after feature of using package-name
(specified in ``daml.yaml``) instead of the more cumbersome package-id. When specifying a
template/interface/choice name, simply substitute any package-id with the
package-name (eg. now ``register:DA.Register:Token``) instead of the old
``deadbeefpackageidhex:DA.Register:Token`` format. This applies to template
filters and SQL queries (eg. via the ``active()`` function). These functions will
always return all versions of a given identifier. Qualified name can be:

- fully qualified, e.g. ``<package-name>:<module-path>:<template-name>``

- partially qualified, e.g. ``<module-path>:<template-name>``

Qualified names cannot be ambiguous.

The PQS Read API now returns the package-name, package-id, and package-version
for each contract instance, making it easy for users to determine and inspect
different versions over time. To reconstruct the old experience (should you
need to) of querying one specific version, use a filter predicate in
the SQL.

.. code-block:: sql

    SELECT * 
      FROM active('mypackage:My.App:MyTemplate') 
      WHERE package_id = 'deadbeefpackageidhex'


Requirements
------------

Note that SCU is only available when the criteria below are met:

-  Canton 2.10.x or above

-  Daml LF Version 1.17 or above

-  Canton Protocol Version 7 or above

There are instructions below on how to configure this setup. The
sections below, unless explicitly stated otherwise, assume that this is
the case.

To prevent unexpected behavior, this feature enforces a unique package name and version for each DAR being
uploaded to a participant node.
This closes a loophole where the participant node allowed multiple DARs with
the same package name and version. For backward compatibility, this
restriction only applies for packages compiled with LF >= 1.17. If LF <
1.15 is used, there can be several packages with the same name and
version but this should be corrected; duplicate package names and versions are no longer supported.

Smart Contract Upgrade Basics
-----------------------------

To upgrade a package the package author modifies their existing
package to add new functionality, such as new fields and choices. When
the new package is uploaded to a participant with the old version, 
the participant ensures that every modification to the model in the
new version is a valid upgrade of the previous version.

To be able to automatically upgrade a contract or datatype, SCU
restricts the kinds of changes that a new package version can introduce
over its prior version.

For example, the simplest kind of data transformation that SCU supports
is adding a field to a template.

Given the following first version of a template:

.. code:: daml

  template IOU
    with
      issuer: Party
      owner: Party
      value: Int
    where
      signatory issuer
      observer owner

You can add a new field for currency:

.. code:: daml

  template IOU
    with
      issuer: Party
      owner: Party
      value: Int
      -- New field:
      currency: Optional String
    where
      signatory issuer
      observer owner

With SCU, any new template fields must be optional - templates from the
old version are automatically upgraded to new versions by setting the
new field to ``None``. This optional field requirement extends to all
records in your package. Conversely, newer contracts with this field set
to ``None`` can be automatically downgraded to previous versions of the
template in workflows that have not yet been updated.

Automatic Data Upgrades and Downgrades
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When extending data in a Daml model, SCU requires the old model to be
representable in the new model. For extending a record, we can
only add nullable (``Optional``) fields, so that old data can be represented
by setting these fields to ``None``. Similar constraints hold for
Variants and Enums, which only allow adding constructors, with some
other restrictions covered in `Continuing to Write Your Upgrades <#continuing-to-write-your-upgrades>`__. This
approach is inspired by
`Protobuf <https://protobuf.dev/programming-guides/proto3/#updating>`__
and Typescript's ability to ignore `excess
fields <https://www.typescriptlang.org/docs/handbook/2/objects.html#excess-property-checks>`__
via ``as``.

Automatic data upgrades occur in the following places:

**Submissions to the Ledger API**

When you submit a command, and provide only a package-name instead of a package-id,
Canton will automatically upgrade (or downgrade) the payloads you give to the most
recent version of the package that is uploaded on the participant. It
will also use the most recent implementation of any choices you exercise
directly through the Ledger API, by automatically upgrading/downgrading the choice argument.
Choice result upgrading/downgrading is handled by Consuming Clients, as discussed later in this section.
This behavior can be influenced by :ref:`the package preference <package-preference>`.

**Updates in a choice body**

When Fetching a contract, the contract payload will be automatically upgraded/downgraded to match
the version expected by the calling choice body, as compiled into the DAR.
When Exercising a choice on a contract, the contract payload will be upgraded/downgraded
to match the version of the choice expected by the calling choice body. This means
that in a choice body, an exercised choice argument or return type is never upgraded/downgraded.

**Consuming Clients (such as Daml Script, TypeScript/Java codegen)**

When clients query the Ledger API for contracts, the returned event
payload format matches the template originally used for generating the
event (creating a contract/exercising a choice). It is the
responsibility of these clients to upgrade/downgrade the payloads they
receive to match what is expected downstream. The same applies to choice
results. Daml Script, as well as TypeScript/Java codegen, does this for you to
match the Ledger API response to the package versions they were run/built from.

Upgrading Across the Stack
--------------------------

These are all the components that interact with SCU,
and how you as a user should be aware that they interacts.

Canton
~~~~~~

When considering the Canton ledger nodes, only the Canton participant
node is aware of smart contract upgrading. The Canton domain nodes are
only concerned with the protocol version which must be at least 7 to allow connected participants to use upgradable Daml packages.

Below, we provide a brief overview of the interactions with the
participant node that have been adapted for supporting the smart
contract upgrading feature starting with Canton 2.10:

-  DAR upload requests go through an additional validation stage to
   check the contained new packages for upgrade-compatibility with
   other packages previously uploaded on the participant.

-  Ledger API command submissions can be automatically or explicitly
   up/downgraded if multiple upgrade-supported (language version >= 1.17) packages exist for the same package-name.

-  Ledger API streaming queries are adapted to support fetching events
   more generically, by package-name.

Code Generation
~~~~~~~~~~~~~~~

The Java and TypeScript CodeGen have been updated to perform upgrades on
retrieved contracts, and now use package-names over package-ids for
commands to the participant.

JSON API Server
~~~~~~~~~~~~~~~

To match the changes to the Ledger API, the JSON API similarly supports
package-name queries and command submission.

PQS & Daml Shell
~~~~~~~~~~~~~~~~

As of 2.10, PQS only supports querying contracts via package-name, 
dropping support for direct package-id queries. See
`the PQS section <#pqs>`__ for more information and a work-around.

Daml Shell builds on top of PQS, so inherits this behavior.

Daml Script
~~~~~~~~~~~

Support for SCU is available in the opt-in LTS version of Daml Script.

This version acts as a drop-in replacement for the previous
Daml Script, and enables support for upgrades on all queries and
command submissions.

We also expose functions for more advanced interactions with
upgrades, as well as to revert to the previous submission behavior.

Daml Compiler
~~~~~~~~~~~~~

The Daml compiler supports the ``upgrades:`` configuration field - every
time ``daml build`` is invoked, it validates the current package for
upgrade compatibility against the package specified in the ``upgrades:``
field.

Validation emits custom error messages for common upgrading mistakes,
and warns the package author when upgrading a package in a potentially
unsafe way. Note however that this validation cannot be complete, as
upgrade validity depends on a participant’s package store. The
participant’s DAR upload checks have the final say on upgrade validity.

Limitations
-----------

To allow SCU to minimize downtime, and multiple versions
of a package to be active at once, we limit the types of
transformations that can be performed on live data. Following are some
data transformations that cannot be made using SCU upgrades:

-  Renaming, removal, or rearrangement of fields in a template

-  Conversion of records to variants and vice versa

-  Moving templates/datatypes to other modules

-  Upgrading interface and exception definitions

-  Adding/removing an interface instance on a template

These restrictions are required to give a simple model of runtime
upgrades, avoiding ambiguity and non-obvious side effects. If you
require any of these types of changes, you may need to consider a
redeployment with downtime, either using the approach suggested in :ref:`Rolling out backwards-incompatible changes <backwards-incompatible-changes>`
or using the existing upgrade procedure described in :ref:`here <upgrade-overview>`.

In this version of SCU, the following functionality has not yet
been implemented, but may be implemented in future releases.

-  Retroactive interface instances are not compatible with SCU upgrades.
   SCU allows instances to be changed in an upgrade. However, a new interface
   instance cannot be added to a template in an upgrade; it requires an offline migration.

-  Daml Script does not support SCU or LF1.17, you must use Daml Script LTS.

There are further limitations with respect to managing the packages on a running ledger:

- Once a version of a package is uploaded to the ledger, it cannot be replaced or removed.
  If a package is uploaded by mistake it can only be overriden by uploading an even newer version.

- As a consequence of the above, if a record is extended by mistake with an optional field,
  that field must be part of all future versions.

- Package versions cannot be deleted, they can only be unvetted. As a good rule of thumb
  the highest version for each package name should be vetted.

- If, for whatever reason, a lower version of a package is vetted and preferred, that
  package version needs to be explicitly mentioned in the command submissions in the
  specific `package_id_selection_preference` field of the grpc `Commands` message.

- Participants that confirm and observe a certain transaction must have vetted the package
  version that is preferred by the submitting participant. If that is not the case, the transaction
  is rejected.

- The contract create events sent as part of the active contracts and transaction streams
  are always represented according to the template definition in the package version used
  at time of their creation.

The Programming Model by Example
================================

Writing Your First Smart Contract Upgrade
-----------------------------------------

Setup
~~~~~

We continue with the example introduced in `Smart Contract Upgrade Basics <#smart-contract-upgrade-basics>`__. Begin by defining the first (old) version
of our package:

.. code:: bash

  > mkdir -p v1/my-pkg
  > cd v1/my-pkg
  > daml init
  > daml version
  SDK versions:
    2.10.0  (project SDK version from daml.yaml)

Running ``daml version`` should print a line showing that 2.10.0 or higher is the "project SDK version from daml.yaml".

Add ``daml-script-lts`` to the list of dependencies in ``v1/my-pkg/daml.yaml``,
as well as ``--target=1.17`` to the ``build-options``:

.. code:: yaml

  ...
  name: my-pkg
  ...
  dependencies:
  - daml-prim
  - daml-stdlib
  - daml-script-lts
  build-options:
  - --target=1.17

**Note:** Throughout this tutorial we ignore some best practices
in favor of simplicity. In particular, we recommend a package undergoing SCU
should contain a version identifier in its name as well, but we omit this - consult the section on :ref:`package naming best
practices <upgrade-package-naming>` to learn more. We also recommend
that you do not depend on ``daml-script-lts`` in any package that is uploaded to
the ledger - more on this :ref:`here <upgrade-dont-upload-daml-script>`.

Then create ``v1/my-pkg/daml/Main.daml``:

.. code:: daml

  module Main where

  import Daml.Script

  template IOU
    with
      issuer: Party
      owner: Party
      value: Int
    where
      signatory issuer
      observer owner
      key issuer : Party
      maintainer key


Running daml build should successfully produce a DAR in
``v1/my-pkg/.daml/dist/my-pkg-1.0.0.dar``:

.. code:: bash

  > daml build
  Running single package build of my-pkg as no multi-package.yaml was found.
  ...
  Compiling my-pkg to a DAR.
  ...
  Created .daml/dist/my-pkg-1.0.0.dar

Now you can create the second (new) version of this package, which
upgrades the first version. Navigate back to the root directory and copy
the v1 package into a v2 directory.

.. code:: bash

  > cd ../..
  > cp -r v1 v2
  > cd v2/my-pkg

Edit the ``daml.yaml`` to update the package version, and add the ``upgrades:``
field pointing to v1:

.. code:: yaml

  version: 1.1.0
  ...
  dependencies:
  - daml-prim
  - daml-stdlib
  - daml-script-lts
  upgrades: ../../v1/my-pkg/.daml/dist/my-pkg-1.0.0.dar
  build-options:
  - --target=1.17

Any changes you make to v2 are now validated as correct upgrades
over v1.


Adding a New Field
~~~~~~~~~~~~~~~~~~

Begin by adding a new ``currency`` field to ``v2/my-pkg/daml/Main.daml``:

.. code:: daml

  ...
  template IOU
    with
      issuer: Party
      owner: Party
      value: Int
      currency: Text -- New field
    where
  ...

Run ``daml build``. An error is emitted:

.. code:: bash

  > daml build
  ...
  error type checking template Main.IOU :
    The upgraded template IOU has added new fields, but those fields are not Optional.
  ERROR: Creation of DAR file failed.

Any new fields added to a template must be optional - old contracts
from the previous version are automatically upgraded by setting new
fields to ``None``.

Fix the ``currency`` field to be optional, and re-run ``daml build``:

.. code:: daml

  ...
      currency: Optional Text -- New field
  ...

.. code:: bash

  > daml build
  ...
  Created .daml/dist/my-pkg-1.0.0.dar

The build may produce warnings about expression changes - this is
covered in the `Continuing to Write Your
Upgrades <#continuing-to-write-your-upgrades>`__ section.

Seeing Upgraded Fields in Contracts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Using the Daml Sandbox, we can see our old contracts automatically
upgrade.

Add a script to make and get IOUs to ``v1/my-pkg/daml/Main.daml``:

.. code:: daml

  module Main where

  import Daml.Script
  ...
  mkIOU : Script Party
  mkIOU = do
    alice <- allocateParty "alice"
    alice `submit` createCmd (IOU alice alice 1)
    pure alice

  getIOU : Party -> Script (Optional (ContractId IOU, IOU))
  getIOU key = queryContractKey @IOU key key

Open ``v2/my-pkg/daml/Main.daml`` and add scripts to make IOUs with and
without a currency field, and a script to get any IOU:

.. code:: daml

  module Main where

  import Daml.Script
  ...
  mkIOU : Script Party
  mkIOU = do
    alice <- allocateParty "alice"
    alice `submit` createCmd (IOU alice alice 1 (Some "USD"))
    pure alice

  mkIOUWithoutCurrency : Script Party
  mkIOUWithoutCurrency = do
    alice <- allocateParty "alice"
    alice `submit` createCmd (IOU alice alice 1 None)
    pure alice

  getIOU : Party -> Script (Optional (ContractId IOU, IOU))
  getIOU key = queryContractKey @IOU key key

Start a new terminal, run ``daml sandbox`` to start a simple ledger in which
to test upgrades.

.. code:: bash

  > daml sandbox
  Starting Canton sandbox.
  Listening at port 6865
  Canton sandbox is ready.

Start another terminal, separately from the terminal in which the
sandbox is running. From inside ``v1/my-pkg``, upload and run the ``mkIOU``
script and place the resulting party for Alice into an output file
``alice-v1``:

.. code:: bash

  > cd v1/my-pkg
  > daml ledger upload-dar --port 6865
  > daml script \
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.0.0.dar \
      --script-name Main:mkIOU \
      --output-file alice-v1
  ...

From inside ``v2/my-pkg``, upload and run the ``getIOU`` script, passing in the
``alice-v1`` file as the script’s input:

.. code:: bash

  > cd ../../v2/my-pkg
  > daml ledger upload-dar --port 6865
  > daml script \
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.1.0.dar \
      --script-name Main:getIOU \
      --output-file /dev/stdout \
      --input-file ../../v1/my-pkg/alice-v1
  ...
  {
    "_1": "...",
    "_2": {
    "issuer": "party-...",
    "owner": "party-...",
    "value": 1,
    "currency": null
    }
  }
  ...

The returned contract has a field ``currency`` which is set to ``null``. When
running the ``getIOU`` script from v1, this field does not appear.

.. code:: bash

  > cd ../../v1/my-pkg
  > daml script \
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.0.0.dar \
      --script-name Main:getIOU \
      --output-file /dev/stdout \
      --input-file alice-v1
  ...
  {
    "_1": "...",
    "_2": {
    "issuer": "party-...",
    "owner": "party-...",
    "value": 1
    }
  }
  ...

Downgrading Contracts
~~~~~~~~~~~~~~~~~~~~~

New contracts cannot be downgraded if they have a value in their
Optional fields. Create a new v2 IOU contract from the ``v2/my-pkg``
directory, with ``USD`` as currency:

.. code:: bash

  > # Create a new v2 IOU contract, with USD as currency
  > cd ../../v2/my-pkg
  > daml script \
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.1.0.dar \
      --script-name Main:mkIOU \
      --output-file alice-v2
  ...

Query it from a v1 script in the ``v1/my-pkg`` directory:

.. code:: bash

  > # Query from v1 package
  > cd ../../v1/my-pkg
  > daml script \
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.0.0.dar \
      --script-name Main:getIOU \
      --output-file /dev/stdout \
      --input-file ../../v2/my-pkg/alice-v2
  ...
  Exception in thread "main" com.daml.lf.engine.script.Script$FailedCmd: Command QueryContractKey failed: Failed to translate create argument:
  ...
  An optional contract field with a value of Some may not be dropped during downgrading.

The error states that the optional field may not be dropped during
downgrading.

Contracts created in a workflow from a v2 package may be used if the
optional, upgraded fields are set to ``None``. For example, create an IOU
with the currency field set to ``None`` using ``mkIOUWithoutCurrency``:

.. code:: bash

  > # Create a new v2 IOU contract, without USD as currency
  > cd ../../v2/my-pkg
  > daml script \
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.1.0.dar \
      --script-name Main:mkIOUWithoutCurrency \
      --output-file alice-v2
  ...

And then query it from v1:

.. code:: bash

  > # Query from v1 package
  > cd ../../v1/my-pkg
  > daml script \
  	--ledger-host localhost --ledger-port 6865 \
  	--dar .daml/dist/my-pkg-1.0.0.dar \
  	--script-name Main:getIOU \
  	--output-file /dev/stdout \
  	--input-file ../../v2/my-pkg/alice-v2
  ...
    "issuer": "party-...",
  	"owner": "party-...",
  	"value": 1
  ...

In this case, the query from v1 succeeded because all upgraded fields
are set to ``None``.

Adding a Choice
~~~~~~~~~~~~~~~

SCU also allows package authors to add new choices - add the
example choice ``Double`` to ``v2/my-pkg/daml/Main.daml``, which archives
the current contract and produces a new one with twice the value.

.. code:: daml

  ...
      maintainer key
      choice Double : ContractId IOU
          controller issuer
          do create this with value = value * 2
  ...

Along with a script to call it.

.. code:: daml

  import DA.Optional (fromSome)

  ...

  doubleIOU : Party -> Script IOU
  doubleIOU alice = do
    oIOU <- getIOU alice
    case oIOU of
      Some (cid, _) -> do
        newCid <- alice `submit` exerciseCmd cid Double
        fromSome <$> queryContractId alice newCid
      None -> fail "Failed to find IOU"

Compiled changes are checked against the previous version and pass:

.. code:: bash

  > daml build
  ...
  Compiling my-pkg to a DAR.
  ...
  Created .daml/dist/my-pkg-1.1.0.dar
  ...

Restart the sandbox and re-upload both v1 and v2:

.. code:: bash

  > cd v1/my-deps
  > daml ledger upload-dar --port 6865
  > # Make a new IOU
  > daml script \
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.0.0.dar \
      --script-name Main:mkIOU \
      --output-file alice-v1
  ...
  > cd ../../v2/my-deps
  > daml ledger upload-dar --port 6865
  ...
  > daml script \
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.1.0.dar \
      --script-name Main:doubleIOU \
      --output-file /dev/stdout \
      --input-file ../../v1/my-pkg/alice-v1
  ...
  	"issuer": "party-...",
  	"owner": "party-...",
  	"value": 2,
  	"currency": null
  ...

Contracts made in v1 can now be exercised with choices introduced in
v2.

Exercising a v1 choice on a v2 contract is also possible if upgraded
fields are set to ``None``, but this requires a different script function -
replace the use of ``exerciseCmd`` with ``exerciseExactCmd`` in the body of
``doubleIOU`` in v1, and restart your sandbox.

.. code:: bash

  > # Replace exerciseCmd with exerciseExactCmd in v1
  > # Do it using your editor, or use `sed`
  > sed -i -E 's/exerciseCmd/exerciseExactCmd/g' \
      v1/my-pkg/daml/Main.daml

Once you’ve restarted your sandbox, create an IOU without a currency in
V2 via ``mkIOUWithoutCurrency``, then run ``doubleIOU`` on it from V1:

.. code:: bash

  > # Create a new v2 IOU contract, without USD as currency
  > cd v2/my-pkg
  > daml ledger upload-dar --port 6865
  > daml script \
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.1.0.dar \
      --script-name Main:mkIOUWithoutCurrency \
      --output-file alice-v2
  > cd ../../v1/my-pkg
  > daml ledger upload-dar --port 6865
  > daml script \
  	--ledger-host localhost --ledger-port 6865 \
  	--dar .daml/dist/my-pkg-1.0.0.dar \
  	--script-name Main:doubleIOU \
  	--output-file /dev/stdout \
  	--input-file ../../v2/my-pkg/alice-v2
  ...
  	"issuer": "party-...",
  	"owner": "party-...",
  	"value": 2
  ...

Existing choices can also be upgraded, as covered in
`Continuing to Write Your Upgrades <#continuing-to-write-your-upgrades>`__.

Deploying Your First Upgrade
----------------------------

Configuring Canton to Support Smart Upgrading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using the feature one must be using Protocol Version 7.

Using Smart Contract Upgrading Enabled Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once you have finished development of your smart contract app, use the
mentioned upgrade-enabled options in daml.yaml to compile and generate
the related DAR. This can be uploaded using the existing gRPC endpoints
without modifications and is immediately available for use.

.. note::

  In 2.10, once a DAR is successfully uploaded, it cannot be
  safely removed from the participant node. Participant operators must
  then ensure that uploaded functionality does not break the intended
  upgrade lineage as newly uploaded DARs affect the upgrading line (i.e.
  all subsequent uploads must be compatible with this one as well).

.. note::

  Upgrade-supported packages stored on the participant must
  lead to unique package-id -> (package-name, package-version) relationships
  since runtime package-name -> package-id
  resolution must be deterministic (see `Ledger API <#ledger-api>`__). For this
  reason, once a LF 1.17+ DAR has been uploaded with its main package
  having a specific package-name/package-version, this relationship cannot
  be overridden. Hence, uploading a DAR with different content for the
  same name/version as an existing DAR on the participant leads to a
  rejection with error code KNOWN_DAR_VERSION.

Validate the DAR Against a Running Participant Node
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Starting with 2.10 you can validate your DAR against the current
participant node state without uploading it to the participant via the
``PackageManagementService.validateDar`` Ledger API endpoint. This allows
participant node operators to first check the DAR before uploading it.

This operation is also available via the Canton Admin API and Console:

.. code::

  participant.dars.validate("dars/CantonExamples.dar")

Upgrading and Package Vetting
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Upgradable packages are also subject to :ref:`package vetting
restrictions <package_vetting>`:
in to be able to use a package in Daml transactions with smart
contract upgrading, it must be vetted by all participants informed about
the transaction. This applies to both the packages used for creating
the contracts and the target packages.

**Note:** Package vetting is enabled by default on DAR upload
operations.

Continuing to Write Your Upgrades
---------------------------------

SCU allows package authors to change many more aspects of their packages
- fields can be extended in templates, choices, and data type
definitions. Choice bodies can be changed, and other expressions such as
key definitions and signatory lists can be changed with caveats.

Setup
~~~~~

Continue the package defined in the `Writing Your First
Upgrade <#writing-your-first-upgrade>`__ section above, but overwrite
the v1 and v2 IOU modules. The v1 IOU module should be overwritten as follows:

.. code:: daml

  module Main where
  
  import Daml.Script
  
  template IOU
    with
      issuer: Party
      owner: Party
      value: Int
    where
      signatory issuer
      observer owner
      key issuer : Party
      maintainer key
  
  mkIOU : Script Party
  mkIOU = do
    alice <- allocateParty "alice"
    alice `submit` createCmd (IOU alice alice 1)
    pure alice
  
  getIOU : Party -> Script (Optional (ContractId IOU, IOU))
  getIOU key = queryContractKey @IOU key key

The v2 IOU module should be overwritten to look like the following:

.. code:: daml

  module Main where
  
  import Daml.Script
  import DA.Optional (fromOptional)
  
  template IOU
    with
      issuer: Party
      owner: Party
      value: Int
      currency: Optional Text
    where
      signatory issuer
      observer owner
      key issuer : Party
      maintainer key
  
  mkIOU : Script Party
  mkIOU = do
    alice <- allocateParty "alice"
    alice `submit` createCmd (IOU alice alice 1 (Some "USD"))
    pure alice
  
  mkIOUWithoutCurrency : Script Party
  mkIOUWithoutCurrency = do
    alice <- allocateParty "alice"
    alice `submit` createCmd (IOU alice alice 1 None)
    pure alice
  
  getIOU : Party -> Script (Optional (ContractId IOU, IOU))
  getIOU key = queryContractKey @IOU key key

All other files should remain the same.

Changing Choices
~~~~~~~~~~~~~~~~

Add the following choice, ``Duplicate``, to both v1 and v2 versions of IOU:

.. code:: daml

      data DuplicateResult = DuplicateResult with
        newCid : ContractId IOU

      choice Duplicate : DuplicateResult
        controller issuer
        do
          cid <- create this with value = value * 2
          return DuplicateResult with newCid = cid

Running ``daml build`` should succeed without errors.

.. code:: bash

  > cd v1/my-pkg
  > daml build
  ...
  Created .daml/dist/my-pkg-1.0.0.dar
  > cd ../../v2/my-pkg
  > daml build
  ...
  Created .daml/dist/my-pkg-1.1.0.dar

Next, upgrade the ``Duplicate`` choice by adding an optional field ``amount``,
and changing the behavior of the choice to default to a multiple of 3. Also
upgrade the ``DuplicateResult`` data type to include the old value.
Replace the definitions of the ``DuplicateResult`` data type and of the
``Duplicate`` choice in v2 only:

.. code:: daml

  ...
  -- Add import to top of module
  import DA.Optional (fromOptional)
  ...
  -- Replace DuplicateResult definition
  data DuplicateResult = DuplicateResult with
    newCid : ContractId IOU
    oldValue : Optional Int -- New optional oldValue field
  ...
     -- Replace Duplicate choice implementation
     choice Duplicate : DuplicateResult
       with
         amount : Optional Int -- New optional amount
       controller issuer
       do
         let amt = fromOptional 3 amount
         cid <- create this with value = value * amt
         return DuplicateResult with
           newCid = cid
           oldValue = Some value
  ...

Add a script called ``duplicateIOU`` in V1:

.. code:: daml

  ...
  duplicateIOU : Party -> Script (Optional (ContractId IOU, IOU))
  duplicateIOU key = do
    mbIOU <- getIOU key
    case mbIOU of
      None -> pure None
      Some (iouCid, _) -> do
        res <- key `submit` exerciseExactCmd iouCid Duplicate
        mbNewIOU <- queryContractId key res.newCid
        case mbNewIOU of
          Some newIOU -> pure (Some (newCid, newIOU))
          None -> pure None

A similar script called ``duplicateIOU`` should be added in V2, supplying an
``amount`` field:

.. code:: daml

  ...
  duplicateIOU : Party -> Script (Optional (ContractId IOU, Int, IOU))
  duplicateIOU key = do
    mbIOU <- getIOU key
    case mbIOU of
      None -> pure None
      Some (iouCid, _) -> do
        res <- key `submit` exerciseExactCmd iouCid Duplicate { amount = Some 4 }
        case res.oldValue of
          None -> pure None -- This should never happen
          Some oldValue -> 
            mbNewIOU <- queryContractId key res.newCid
            case mbNewIOU of
              Some newIOU -> pure (Some (newCid, oldValue, newIOU))
              None -> pure None

Running the v1 ``duplicateIOU`` script with ``exerciseExactCmd`` always runs
the v1 implementation for the ``Duplicate`` choice, and likewise for v2.

Modifying Signatory Definitions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Other definitions can be changed, but warnings are emitted to remind the
developer that the changes can be unsafe and need to be made with care
to preserve necessary invariants.

Signatories and observers are one expression that can be changed. Importantly,
SCU assumes that the new definition does not alter the computed values of the
signatories. The computed value of the observers is allowed to change in one
specific way: observers that are also signatories can be removed. Any other
change to the computed value of the observers (losing a non-signatory observer,
adding an observer) is not allowed.

For example, add a new field of "outside observers" to the v2 IOU
template, and add them to the observer definition.

.. code:: daml

  ...
      -- Add a new outsideObservers field
      outsideObservers: Optional [Party]
    where
      signatory issuer
      -- Add outsideObservers to the observer definition
      observer owner, fromOptional [] outsideObservers
  ...

The new observer definition allows v2 contracts and beyond to add
new observers via the outsideObservers field. However, any existing
contracts default to ``None`` for the ``outsideObservers`` field, so all
existing contracts have the same observer list as before: the
single owner.

In the case where a contract's signatories or observers change in during an 
upgrade/downgrade in a way that doesn't meet the constraints above, the upgrade,
and thus full transaction, fails at runtime.

Modifying Key Expressions
~~~~~~~~~~~~~~~~~~~~~~~~~

Similarly, key expressions can be changed as long as they evaluate to
the same value for existing contracts.

For example, v2 can add a new field "alternative key" to the v2 IOU
template, and use it instead of the default key when present.

.. code:: daml

  ...
      -- Add a new alternativeKey field
      alternativeKey: Optional Party
    where
      key fromOptional issuer alternativeKey
  ...

All old contracts will default to using the ``issuer``, and new contracts
will use the ``alternativeKey`` field.

Upgrading Enums
~~~~~~~~~~~~~~~

Variants and enums can be extended using SCU, either by adding
fields to an existing constructor, or by adding a new constructor to the
end of the list.

Redefine the IOU package, overwriting the v1 and v2 sections similarly
to the previous section. Overwrite the IOU package in both V1 and V2
with the following:

.. code:: daml

  module Main where
  
  import Daml.Script
  
  template IOU
    with
      issuer: Party
      owner: Party
      value: Int
      currency: Currency
    where
      signatory issuer
      observer owner
      key issuer : Party
      maintainer key
  
  data Currency
    = USD
    | GBP
    deriving (Show, Eq, Ord)
  
  mkIOU : Script Party
  mkIOU = do
    alice <- allocateParty "alice"
    alice `submit` createCmd (IOU alice alice 1 USD)
    pure alice
  
  getIOU : Party -> Script (Optional (ContractId IOU, IOU))
  getIOU key = queryContractKey @IOU key key

Instead of using ``Text`` for the currency field, here we use an enum
data-type ``Currency`` with two constructors: ``USD`` and ``GBP``.

Running ``daml build`` should succeed with no errors:

.. code:: bash

  > cd v1/my-pkg
  > daml build
  ...
  Created .daml/dist/my-pkg-1.0.0.dar
  > cd ../../v2/my-pkg
  > daml build
  ...
  Created .daml/dist/my-pkg-1.1.0.dar

When you want to extend our contract to support new currencies, you can
add new entries to the end of our ``Currency`` enum.

.. code:: daml

  ...
  data Currency
    = USD
    | GBP
    | CHF -- Add a new currency type
    deriving (Show, Eq, Ord)
  ...

Upgrades of extended enums from an old version to a new version always succeed. In the case of IOUs, a v1 IOU can always be interpreted
as a v2 IOU because the constructors for its ``currency`` field are a subset
of those in a v2 contract.

For example, create an IOU with USD via v1’s ``mkIOU`` script, and query it
via v2’s ``getIOU`` script:

.. code:: bash

  > cd v1/my-pkg
  > daml script
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.0.0.dar \
      --script-name Main:mkIOU \
      --output-file alice-v1
  ...
  > cd ../../v2/my-pkg
  > daml script
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.1.0.dar \
      --script-name Main:getIOU \
      --output-file /dev/stdout \
      --input-file ../../v1/my-pkg/alice-v1
  ...
      "issuer": "party-...",
      "owner": "party-...",
      "value": 1,
      "currency": "USD"
  ...

Only constructors that are defined in both
v1 and v2 can be downgraded from v2 to v1. Any constructor that does not
exist in the v1 package fails to downgrade with a runtime error. In
the case of our ``IOU``, any ``CHF`` fails to downgrade, so any v2 contracts
with a ``CHF`` currency cannot be used in v1 workflows.

For example, create a contract with ``CHF`` as its ``currency`` field via v2’s
``mkIOU`` script. Attempting to query it via v1’s ``getIOU`` script fails
with a lookup error for the CHF variant.

.. code:: bash

  > cd v2/my-pkg
  > daml script
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.1.0.dar \
      --script-name Main:mkIOU \
      --output-file alice-v2
  ...
  > cd ../../v1/my-pkg
  > daml script
      --ledger-host localhost --ledger-port 6865 \
      --dar .daml/dist/my-pkg-1.0.0.dar \
      --script-name Main:getIOU \
      --output-file /dev/stdout \
      --input-file ../../v2/my-pkg/alice-v2
  ...
  Failed to translate create argument: Lookup(NotFound(DataVariantConstructor(c1543a5c2b7ff03650162e68e03e469d1ecf9f546565d3809cdec2e0255ed902:Main:Currency,CHF),DataEnumConstructor(c1543a5c2b7ff03650162e68e03e469d1ecf9f546565d3809cdec2e0255ed902:Main:Currency,CHF)))
  ...

Upgrading Variants
~~~~~~~~~~~~~~~~~~

Variants, also known as algebraic data types, are very similar to enums
except that they also contain structured data.

For example, the following variant has two constructors, each with
unique fields. Overwrite both v1 and v2 modules with the following
source:

.. code:: daml

  module Main where
  
  data Shape
    = Circle
    | Polygon { sides : Int }

You can extend this variant in two ways. You can add a new constructor,
similarly to enums. Modify the v2 module to add a new ``Line`` constructor
with a ``len`` field:

.. code:: daml

  module Main where
  
  data Shape
    = Circle
    | Polygon { sides : Int }
    | Line { len : Numeric 10 } -- New line constructor

As before, building should succeed.

.. code:: bash

  > cd v1/my-pkg
  > daml build
  ...
  Created .daml/dist/my-pkg-1.0.0.dar
  > cd ../../v2/my-pkg
  > daml build
  ...
  Created .daml/dist/my-pkg-1.1.0.dar

You can also add a new field to a constructor, similarly to templates -
for example, add a ``sideLen`` field to the ``Polygon`` constructor, to specify
the lengths of the sides of the polygon.

.. code:: daml

  data Shape
    = Circle
    | Polygon
        { sides : Int
        , sideLen : Numeric 10 -- New field
        }
    | Line { len : Numeric 10 }

Building this fails because the new ``sideLen`` field is non-optional.

.. code:: bash

  > cd v2/my-pkg
  > daml build
  ...
  error type checking data type Main.Shape:
    The upgraded variant constructor Polygon from variant Shape has added a field.
  ERROR: Creation of DAR file failed.

Making the new ``sideLen`` field optional fixes the error:

.. code:: daml

  ...
        , sideLen : Optional (Numeric 10) -- New field
  ...

.. code:: bash

  > cd v2/my-pkg
  > daml build
  ...
  Created .daml/dist/my-pkg-1.1.0.dar

Limitations in Upgrading Variants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Upgrading variants has some limitations - because the ``Circle``
constructor has been defined without a field in curly braces, it cannot be
upgraded with new fields.

.. code:: daml

  ...
    -- Add a field where no fields existed
    = Circle { radius : Optional (Numeric 10) }
  ...

.. code:: bash

  > cd v2/my-pkg
  > daml build
  ...
  error type checking data type Main.Shape:
    The upgraded data type Shape has changed the type of a variant.
  ERROR: Creation of DAR file failed.

The same applies to variants with unnamed fields. If the v1 definition
of the ``Line`` constructor were as follows, it would also not be able to
upgrade:

.. code:: daml

  ...
    | Line (Numeric 10)

In general, in order to enable future upgrades, it is strongly
recommended that all constructors use named fields, and that all
constructors have at least one field. If a constructor has no fields in
an initial v1 package, one can assign a dummy field.

For example, the correct way to write the v1 ``Circle`` constructor would be
as follows:

.. code:: daml

  ...
    = Circle { dummy : () }
  ...

The subsequent v2 upgrade would succeed:

.. code:: daml

  ...
    = Circle { dummy : (), radius : Optional (Numeric 10) }
  ...

Nested Datatypes
~~~~~~~~~~~~~~~~

If a data type, choice, or template has a field which refers to another
data type, the larger data type can be upgraded if the field’s data
type is upgradeable.

For example, given the data type ``A`` with a field referring to data type
``B``,

.. code:: daml

  data A = A { b : B }
  data B = B { field : Text }

If modifications made to ``B`` are valid for SCU, then ``A`` is also valid.

Dependencies
~~~~~~~~~~~~

Package authors may upgrade the dependencies of a package as well as the
package itself. A new version of a package may add new dependencies, and
must have all the (non-:ref:`utility-package <upgrades-utility-package>`)
dependencies of the old version. If these dependencies are used in ways that are
checked for upgrades, each existing dependency must be either
unchanged from the old DAR or an upgrade of its previous version.

For example, given a dependencies folder containing v1, v2, and v3
of a dependency package ``dep``:

.. code:: bash

  > ls ./dependencies
  dep-1.0.0.dar
  dep-2.0.0.dar
  dep-3.0.0.dar

Then assume a version ``1.0.0`` of a package ``main`` that depends on a datatype
from version ``2.0.0`` of ``dep``:

.. code:: daml

  module Main where

  import qualified Dep

  data MyData = MyData
    { depData : Dep.AdditionalData
    }

.. code:: yaml

  ...
  dependencies:
  - daml-prim
  - daml-stdlib
  - daml3-script
  data-dependencies:
  - dependencies/dep-2.0.0.dar
  ...

Because a package with a newer version may upgrade any dependency to a newer
version (or keep the version the same), version ``2.0.0`` of the ``main``
package may keep its dependencies the same, or it may upgrade ``dep`` to
``3.0.0``:

.. code:: yaml

  ...
  dependencies:
  - daml-prim
  - daml-stdlib
  - daml3-script
  data-dependencies:
  - dependencies/dep-3.0.0.dar # Can upgrade dep-2.0.0 to dep-3.0.0, or leave it unchanged
  ...

You cannot downgrade a dependency when using that dependency's datatypes. For example, ``main`` may not downgrade ``dep`` to version ``1.0.0``.
The following ``daml.yaml`` would be invalid:

.. code:: yaml

  ...
  dependencies:
  - daml-prim
  - daml-stdlib
  - daml3-script
  data-dependencies:
  - dependencies/dep-1.0.0.dar # Cannot downgrade dep-2.0.0 to dep-1.0.0
  ...

If you try to build this package, the typechecker returns an error on a package ID
mismatch for the Dep:AdditionalData field, because the Dep:AdditionalData
reference in this case has changed to a package that is not a legitimate upgrade
of the original.

.. code:: bash

  > daml build
  ...
  error type checking data type Main.MyData:
  The upgraded data type MyData has changed the types of some of its original fields:
    Field 'depData' changed type from <dep-2.0.0 package ID>:Dep:AdditionalData to <dep-1.0.0 package ID>:Dep:AdditionalData

Upgrading Interface Instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SCU also supports changing Interface instances. First, create a
new package directory ``my-iface``, with ``my-iface/daml.yaml`` and
module ``my-iface/daml/MyIface.daml``:

.. code:: yaml

  sdk-version: 2.10.0
  name: my-iface
  version: 1.0.0
  source: daml/MyIface.daml
  parties:
  - Alice
  - Bob
  dependencies:
  - daml-prim
  - daml-stdlib
  build-options:
  - --target=1.17

.. code:: daml

  module MyIface where
  
  data HasValueView = HasValueView { hasValueView : Int }
  
  interface HasValue where
    viewtype HasValueView
    getValue : Int

And build the module:

.. code:: bash

  > cd my-iface
  > daml build
  ...
  Created .daml/dist/my-iface-1.0.0.dar

Add references to the newly created DAR in both ``v1/my-pkg/daml.yaml`` and
``v2/my-pkg/daml.yaml``:

.. code:: yaml

  ...
  dependencies:
  - daml-prim
  - daml-stdlib
  - daml3-script
  - ../../my-iface/.daml/dist/my-iface-1.0.0.dar
  ...

Overwrite both ``v1/my-pkg/daml/Main.daml`` and ``v2/my-pkg/daml/Main.daml``
with the following:

.. code:: daml

  module Main where
  
  import Daml.Script
  import MyIface
  
  template IOU
    with
      issuer: Party
      owner: Party
      value: Int
    where
      signatory issuer
      observer owner
      key issuer : Party
      maintainer key

      interface instance HasValue for IOU where
        view = HasValueView value
        getValue = value

Interface instances can be changed by an upgrade. For example, v2 can
change the definition of ``getValue`` in the ``HasValue`` instance.

Add a ``quantity`` field to the v2 IOU package, and amend the definition of
``getValue`` to use it:

.. code:: daml

  ...
  import DA.Optional (fromOptional)
  
  template IOU
    with
      issuer: Party
      owner: Party
      value: Int
      quantity: Optional Int -- new quantity field
    where
  ...
      interface instance HasValue for IOU where
        view = HasValueView (value * fromOptional 1 quantity)
        -- Use quantity field to calculate value
        getValue = value * fromOptional 1 quantity

Shut down and relaunch the Daml sandbox, then build and upload the two
DARs. They should both succeed again:

.. code:: bash

  > cd v1/my-pkg
  > daml build
  > daml ledger upload-dar --port 6865
  ...
  Uploading .daml/dist/my-pkg-1.0.0.dar to localhost:6865
  DAR upload succeeded.
  > cd ../../v2/my-pkg
  > daml build
  > daml ledger upload-dar --port 6865
  ...
  Uploading .daml/dist/my-pkg-1.1.0.dar to localhost:6865
  DAR upload succeeded.

Packages with new versions cannot remove an instance that is already
there. For example, the v2 IOU template cannot remove its instance of
``HasValue``. Comment out the interface instance for ``HasValue`` from
``v2/my-pkg/daml/Main.daml`` completely, then restart the sandbox and try to
reupload the two versions:

.. code:: bash

  > cd v1/my-pkg
  > daml build
  > daml ledger upload-dar --port 6865
  ...
  Uploading .daml/dist/my-pkg-1.0.0.dar to localhost:6865
  DAR upload succeeded.
  > cd ../../v2/my-pkg
  > daml build
  > daml ledger upload-dar --port 6865
  ...
  Uploading .daml/dist/my-pkg-2.0.0.dar to localhost:6865
  upload-dar did not succeed: ... Implementation of interface ...:MyIface:HasValue by template IOU appears in package that is being upgraded, but does not appear in this package.

Packages with new versions cannot add an interface instance to an existing
template either. For example, restore the instance deleted in the previous step
and remove the ``HasValue`` interface from ``v2/my-pkg/daml/Main.daml`` instead.
Then restart the sandbox and try to reupload the two versions.

.. code:: bash

  > cd v1/my-pkg
  > daml build
  > daml ledger upload-dar --port 6865
  ...
  Uploading .daml/dist/my-pkg-1.0.0.dar to localhost:6865
  DAR upload succeeded.
  > cd ../../v2/my-pkg
  > daml build
  > daml ledger upload-dar --port 6865
  ...
  Uploading .daml/dist/my-pkg-2.0.0.dar to localhost:6865
  upload-dar did not succeed: ... Implementation of ...:MyIface:HasValue by template IOU appears in this package, but does not appear in package that is being upgraded.

Similarly to choices, scripts may invoke interface implementations from
their own version using ``exerciseExactCmd``.

Upgrading Interfaces
~~~~~~~~~~~~~~~~~~~~

Interface instances may be upgraded, but interface definitions cannot be
upgraded. If an interface definition is present in v1 of a package, it must be
removed from all subsequent versions of that package.

Because interfaces definitions may not be defined in subsequent versions, any
package that uses an interface definition from a dependency package can never
upgrade that dependency to a new version.

For this reason, it is :ref:`strongly recommended that interfaces always be defined
in their own packages separately from templates <upgrades-separate-interfaces-and-exceptions>`.

Developer Workflow
------------------

This section contains suggestions on how to set up your development environment
for SCU to help you iterate more quickly on your projects.

Multi-Package Builds
~~~~~~~~~~~~~~~~~~~~

Following the :ref:`best practices <upgrades-best-practices>` outlined below and the :ref:`testing recommendations <upgrades-testing>` leads to a
proliferation of packages in your project.  Use :ref:`Multi-Package builds
<multi-package-build>` to reliably rebuild these packages as you iterate on your
project. Multi-package builds also enable cross-package navigation in Daml
Studio.

The :ref:`Multi-package builds for upgrades <upgrades-multi-package>` section
goes into more detail on how to set up multi-package builds for SCU and how it 
can help with testing.

Working with a Running Canton Instance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With SCU, it is no longer possible to iterate on a package by
uploading it to a running participant after each rebuild. A participant rejects two packages with the same name and version whose content
differs.

There are two ways to work around this:
  - restart the participant after each rebuild
  - change the version name of the package before each rebuild

:ref:`Environment variable interpolation <environment-variable-interpolation>`
in the Daml Assistant can help with the latter. In the ``daml.yaml`` file of each
of your packages, append an environment variable to the name of the package:

.. code:: daml

    name: my-package-v${UNIQUE_BUILD_ID}

Make sure to also append the variable to the name of the DAR file produced by
``my-package`` in the ``daml.yaml`` files that depend on it:

.. code:: daml

  dependencies:
  - daml-prim
  - daml-stdlib
  data-dependencies:
  - ../my-package/.daml/dist/my-package-${UNIQUE_BUILD_ID}.dar

Then, before invoking ``daml build --all``, increment the ``UNIQUE_BUILD_ID``
environment variable. This ensures that the build produces unique DAR files that can be uploaded to the participant without conflict.

Working with the Daml Sandbox
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For the same reason, the :ref:`Daml sandbox <sandbox-manual>` does not support
hot-swapping of SCU-enabled (LF 1.17) packages.

There are two ways to work around this:
  - restart the sandbox after each rebuild
  - change the version name of your packages after each rebuild, as outlined in
    the previous section


The Upgrade Model in Depth - Reference
--------------------------------------

You can find the in-depth upgrading model, which can be used as a reference
for valid upgrades, :ref:`here <upgrade-model-reference>`.

Package Selection in the Ledger API
===================================

Until the introduction of SCU, template IDs in requests to the Ledger API were all of the form
``<package-id>:<module-name>:<template-name>``.
For disambiguation, going forward, we refer to this format as **by-package-id template IDs**.

With SCU, we introduce a more generic template reference of the format
``#<package-name>:<module-name>:<template-name>`` (**by-package-name template ID**)
that scopes templates by package-name, allowing version-agnostic addressing of templates on the Ledger API.
It serves as a reference to all template IDs with the qualified name ``<module-name>:<template-name>``
from all packages with the name ``package-name`` known to the Ledger API server.

`By-package-name template ID` addressing is supported for all upgradable packages (LF >= 1.17)
and is possible on both the write path (command submission) and read path (Ledger API queries).

The `by-package-name template ID` is an API-level concept of the Ledger API.
Internally, Canton uses by-package-id template IDs for all operations.
Therefore, when the new format is used in requests to the Ledger API,
a dynamic resolution is performed as described in the following sections.

.. _package-preference:

Package Preference
------------------

On processing a command submission, there are scenarios where the Ledger API server needs
to resolve `by-package-name template IDs` to `by-package-id template IDs` (see :ref:`dynamic-package-resolution-in-command-submission`).
For this purpose, the **package preference** concept is introduced
as the mapping from package-name to package ID for all known package-names on the participant.

The package preference is needed at each command submission time and is assembled from two sources:

- The **default** package preference of the Ledger API server to which the command is submitted.
  This is the exhaustive mapping from package-name to package ID for all known package-names on the participant.
  If multiple package IDs exist for a package-name, the package ID of the package with the highest
  version is used.

- The package-id selection preference list specified in the submitted command's
  :ref:`package_id_selection_preference <com.daml.ledger.api.v1.Commands.package_id_selection_preference>` in a command submission.
  This is package-id resolution list explicitly provided by the client to
  override the default package preference mentioned above.

   - See :ref:`here <daml-script-package-preference>` for how to provide this in Daml-Script

   -  **Note:** The :ref:`package_id_selection_preference <com.daml.ledger.api.v1.Commands.package_id_selection_preference>`
      must not lead to ambiguous resolutions for package-names,
      meaning that it must not contain two package-ids pointing to
      packages with the same package-name, as otherwise the submission will fail with
      an ``INVALID_ARGUMENT`` error.

.. note::
    **Important**: DAR uploads change the package preference on the participant.
    If a new uploaded DAR contains an upgradable (LF >= 1.17) package with a higher version
    than the existing package preference for the same package name,
    the default preference is updated to the new package ID.
    Essentially, this affects the Daml code version used in command submissions.

.. _dynamic-package-resolution-in-command-submission:

Dynamic Package Resolution in Command Submission
------------------------------------------------

Dynamic package resolution can happen in two cases during command submission:

-  For command submissions that use a `by-package-name template ID`
   in the command’s templateId field (e.g. in a
   create command :ref:`here <com.daml.ledger.api.v1.CreateCommand>`)

-  For command submissions whose Daml interpretation requires the execution of
   interface choices or fetch-by-interface actions.

In these situations, the :ref:`package preference <package-preference>` is used for
selecting the target implementation package for these interface actions.

Dynamic Package Resolution in Ledger API Queries
------------------------------------------------

When subscribing for :ref:`transaction <transaction-trees>`
or :ref:`active contract streams <active-contract-service>`,
users can now use the `by-package-name template ID` format
in the :ref:`template-id request filter field <com.daml.ledger.api.v1.TemplateFilter.template_id>`.
to specify that they’re interested in fetching events for all templates
pertaining to the specified package-name. This template selection set is
dynamic and it widens with each uploaded template/package.

**Note:** The by-package-name query mechanism described here does not
apply to events sourced from non-upgradable templates (coming from
packages with LF < 1.17)

Example
~~~~~~~

Given the following packages with LF 1.17 existing on the participant
node:

-  Package AppV1

   -  package-name: ``app1``

   -  package-version: ``1.0.0``

   -  template-ids: ``pkgId1:mod:T``

-  Package AppV2

   -  package-name: ``app1``

   -  package-version: ``1.1.0``

   -  template-ids: ``pkgId2:mod:T``

If a transaction query is created with a templateId specified as
``#app1:mod:T``, then the events stream will include events from both
template-ids: ``pkgId1:mod:T`` and ``pkgId2:mod:T``

Migrating to SCU
================

SCU is only supported on LF1.17, which in turn is only supported on
Canton Protocol Version 7. Existing deployed contracts
require migration and redeployment to use this feature.
See :ref:`here <upgrade_to_2.10>` for the migration guide to 2.10 and Protocol Version 7

First you must migrate your Daml model to be compatible with
upgrades; see `Best Practices <#best-practices>`__ for what to
change here. Pay particular attention to the case of interfaces and
exceptions, as failure to do so could lead to packages which are
incompatible with SCU and require the use of another full migration (and
downtime).

Next, be aware of the new package-name scoping rules, and
ensure that your package set does not violate them. In short, LF1.17 packages
with the same package name are unified under SCU, so you should ensure that
all of your packages that are not intended to be direct upgrades of each other
have unique package names.
Note also that only one package for each version
can exist within a given package name.
LF1.15 packages are not subject to this restriction, and can exist alongside LF1.17
packages.

Once you have your new DARs, you need to upgrade your Canton and
protocol version together, since 2.10 introduces a new protocol version.
The steps to achieve this are given in the :ref:`Canton Upgrading
manual <one_step_migration>`.

Finally, you can migrate your live data from your previous DARs to the
new LF1.17 DARs, using one of the existing downtime upgrade techniques
listed :ref:`here <upgrades-index>`.

.. _upgrades-best-practices:

Best Practices
==============

To ensure that future upgrades and DAR lifecycling go smoothly, we
recommend the following practices:

.. _upgrades-separate-interfaces-and-exceptions:

Separate Interfaces/Exceptions from Templates
---------------------------------------------

Interface and exception definitions are not upgradable. As such, if you attempt
to redefine an interface or exception in version 2 of a package, even if it is
unchanged, the package does not type check. Removing an interface from the
version 2 package also causes issues, especially if the interface has
choices.

This means that template definitions that exist in the same package as
interfaces and exception definitions are not upgradeable. To avoid this
issue, move interface and exception definitions into a separate package such that subsequent versions of your template package all depend on the same version of the package with interfaces/exceptions.

For example, a single package ``main`` defined as follows would not be able to
upgrade, leaving the template ``T`` non-upgradeable.

.. code:: daml

  module Main where

  interface I where
    ...

  template T with
    ...

.. code:: yaml

  sdk-version: 2.10.0
  name: main
  version: 1.0.0
  source: Main.daml
  dependencies:
  - daml-prim
  - daml-stdlib
  build-options:
  - --target=1.17

The SCU type checker emits an error and refuses to compile this module:

.. code:: text

  error type checking <none>:
    This package defines both interfaces and templates. This may make this package and its dependents not upgradeable.
    
    It is recommended that interfaces are defined in their own package separate from their implementations.
    Downgrade this error to a warning with -Wupgrade-interfaces
    Disable this error entirely with -Wno-upgrade-interfaces

**Note:** It is very strongly recommended that you do not compile interfaces or
exceptions in a package alongside templates. However, you can downgrade this
error to a warning by passing the ``-Wupgrade-interfaces`` flag, or ignore this
error entirely with the ``-Wno-upgrade-interfaces`` flag.

The recommended way to fix this is to split the ``main`` package by redefining
it as two packages, ``helper`` and ``main``:

.. code:: daml

  module Helper where

  interface I where
    ...

.. code:: yaml

  sdk-version: 2.10.0
  name: helper
  version: 1.0.0
  source: Helper.daml
  dependencies:
  - daml-prim
  - daml-stdlib
  build-options:
  - --target=1.17

.. code:: daml

  module Main where

  import qualified Helper

  template T with
    ...

.. code:: yaml

  sdk-version: 2.10.0
  name: main
  version: 1.0.0
  source: Main.daml
  dependencies:
  - daml-prim
  - daml-stdlib
  data-dependencies:
  - <path to helper DAR>
  build-options:
  - --target=1.17

To manage the complexity of working with multiple packages at once, we recommend using :ref:`multi-package builds <multi-package-build>`.

Remove Retroactive Instances
----------------------------

SCU is not compatible with retroactive interface instances, so if you are migrating to SCU from an LF1.15
project that uses retroactive instances, you must move the instances to their respective templates
during the migration.
See `Limitations <#limitations>`__ for more information.

Avoid Contract Metadata Changes
-------------------------------

The signatories, stakeholders, contract key, and ensure clauses of a contract
should be fixed at runtime for a given contract. Changing their definitions in
your Daml code is discouraged and triggers a warning from the SCU typechecker. 

We strongly recommend against altering the type of a key. If changing the type
of a key cannot be avoided, consider using an off-ledger migration instead of
SCU.

.. _upgrade-package-naming:

Breaking Changes via Explicit Package Version
---------------------------------------------

To make a breaking change to your package that
is not upgrade compatible, you can change the name of your package to indicate a
breaking version bump. To enable this, we recommend that your package
name contains a version marker for when a breaking change occurs.

For example, for your first iteration of a package, you would name it
``main-v1``, starting with package version ``1.0.0``. In this case, the ``v1``
is part of the *package name*, not the package version. You could publish
upgrade-compatible versions by changing the ``version:`` field from ``1.0.0`` to
``2.0.0`` to ``3.0.0``. These versions would all be upgrade-compatible with
one another:

.. code:: text

  main-v1-1.0.0
  main-v1-2.0.0
  main-v1-3.0.0

Note how the ``v1`` in all three packages remains stable - this means the
package name has not changed, and ensures that these three packages and their
datatypes are considered by the runtime and the type checker to be upgradeable.

To make a breaking change, publish a new package version with package name ``main-v2``. Because this package has a
different package name from those with ``main-v1``, it is not typechecked
against those packages and its datatypes are not automatically converted.
You would need to manually migrate values from ``main-v1`` packages to
``main-v2`` -- existing downtime upgrade techniques are listed :ref:`here <upgrades-index>`.

Avoid Depending on LF 1.15 Packages
-----------------------------------

Smart contract upgrades were only enabled in LF 1.17. This means that packages
of previous LF versions, namely <= LF 1.15, are not upgradeable. By extension,
their datatypes and templates are not upgradeable.

This means that datatypes in an LF 1.17 package with fields that use datatyps
from an LF 1.15 dependency cannot ever upgrade those datatypes, so that package remains a dependency forever.

As an example, assume we have three packages:
* ``main``, which uses LF 1.17, with module ``Main``
* ``dep1``, which uses LF 1.15, with module ``Dep1``
* ``dep2``, which uses LF 1.17, with module ``Dep2``

.. code:: daml

  module Main where

  import qualified Dep1
  import qualified Dep2

  data MyData = MyData
    { field1: Dep1.D
    , field2: Dep2.D
    , field3: (Dep1.D, Dep2.D)
    }

Because datatype ``Dep1.D`` comes from an LF 1.15 dependency, it cannot be
upgraded, and so ``field1`` and the second element of ``field3`` can never be
changed. However, ``field2`` and the first element of ``field3`` can be upgraded
as new upgraded versions of ``dep2`` become available.

If you compile ``main``, you can expect a warning about the use of an LF 1.15
package's datatype in the definition of ``MyData``.

.. code:: bash

  > daml build
  ...
  warning while type checking data type Main.MyData:
    This package has LF version 1.17, but it depends on a serializable type <dep1 package ID>:Dep1:D from package <dep 1 package ID> (dep1, 1.0.0) which has LF version 1.15.
    
    We do not recommend that >= LF1.17 packages depend on <= LF1.15 datatypes in places that may be serialized to the ledger, because those datatypes are not upgradeable.
    Upgrade this warning to an error -Werror=upgrade-serialized-non-upgradeable-dependency
    Remove this warning with -Wno-upgrade-serialized-non-upgradeable-dependency

**Note:** For added safety, you may upgrade these warnings to errors with
``-Werror=upgrade-serialized-non-upgradeable-dependency``. We recommend against removing these
warnings with ``-Wno-upgrade-serialized-non-upgradeable-dependency``.

.. _upgrade-dont-upload-daml-script:

Avoid Depending on Daml Script Packages
---------------------------------------

We recommend only depending on Daml Script packages such as ``daml-script-lts``
in dedicated packages for running tests written in Daml Script. These packages
should not be part of your model and should not be uploaded to the ledger.

Daml Script packages are not guaranteed to be upgradeable across SDK versions.
If you depend on a Daml Script datatype in a serializable
position (e.g. the field of a template), your package may rely on
a Daml Script package in a way that can neither be removed nor upgraded to the
next SDK version. Your package and any of its SCU upgrades would
be stuck on that SDK version.

For example, suppose you have a ``main`` package that depends on
``daml-script-lts`` from SDK 2.10.0.

.. code:: yaml

  version: 2.10.0
  name: main
  ...
  dependencies:
  - daml-prim
  - daml-stdlib
  - daml-script-lts
  build-options:
  - --target=1.17

.. code:: daml

  module Main where

  import qualified Daml.Script

  data MyData = MyData
    { field1 : Daml.Script.PartyIdHint
    }

Because ``MyData`` is a serializable datatype, any changes to it must be valid
upgrade changes (e.g. adding a field) for the ``main`` package itself to be upgraded. If SDK 2.10.X introduces a change to
``daml-script-lts`` that is not a valid upgrade of the ``daml-script-lts`` in
SDK 2.10.0, then ``field1`` is not upgradeable to the next version of the
SDK, nor can the field be dropped because the field is used in SCU checks.

**Note**: While it is unlikely that an SDK update breaks ``daml-script-lts``
for upgrades, we still strongly recommend against it. In Daml 3.x, Canton may
disallow uploading ``daml-script-lts`` entirely.

At that point, all future development on ``main``, including future upgrades,
would be locked to SDK 2.10.0. To bump the SDK version, ``main`` would have to
be migrated via a manual upgrade tool with downtime -- existing downtime upgrade
techniques are listed :ref:`here <upgrades-index>`.

For this reason, we strongly recommend against using Daml Script as an upgrade
dependency for any package going on the ledger. Whenever building the ``main``
package above, you can expect a warning:

.. code:: bash

  > daml build
  ...
  warning while type checking data type Main.MyData:
    This package depends on a datatype <package ID>:Daml.Script.Stable:PartyIdHint from <package ID> (daml-script-lts-stable, 0.0.0) with LF version 1.17.
    
    We do not recommend using datatypes from Daml Script in >= LF1.17 packages, because those datatypes are not upgradeable.
    Upgrade this warning to an error -Werror=upgrade-serialized-daml-script
    Remove this warning with -Wno-upgrade-serialized-daml-script

**Note:** For added safety, you may upgrade these warnings to errors with
``-Werror=upgrade-serialized-daml-script``. We recommend against removing these
warnings with ``-Wno-upgrade-serialized-daml-script``.

If instead ``main`` depends on a datatype in a non-serializable position such
as the type signature of a function, ``main`` can still be upgraded without
breaking any SCU restrictions. For example:

.. code:: daml

  module Main where

  import qualified Daml.Script

  data MyData = MyData
    { field1 : Text
    }

  myFunction : Daml.Script.PartyIdHint -> Bool
  myFunction _ = True

In this case, when changing SDK from 2.10.0 to 2.10.X, the typechecker
ignores the change to ``Daml.Script.PartyIdHint`` in ``myFunction``, because it
is not in a serializable position. This means ``daml-script-lts`` can be kept
even when it is not a valid upgrade from one version to the next.

**Note:** We still recommend against depending on Daml Script for
ledger-uploaded packages, even in this case with non-serializable positions.

Operational Design Guideline for Upgrading Daml Apps
----------------------------------------------------

When considering upgrading, we regard each Daml application as composed of:

- **On-ledger components**: The Daml code running the on-ledger logic (i.e. the DARs uploaded
  to all participant nodes interacting with the app)

- **Off-ledger components** interacting with the ledger via the Ledger API or other supported
  Canton Ledger API clients (JSON API or PQS).
  These are typically Java or TypeScript services implementing off-ledger business logic.

Zero-Downtime Upgrades
~~~~~~~~~~~~~~~~~~~~~~

Upgrading an application without operational downtime can be achieved by designing the above-mentioned components to allow:

- **Asynchronous rollout**: Operators deploy updated software at their own pace,
  similar to established CI/CD practices in micro-service environments.

- **Synchronous switch-over**: All components switch to using updated Daml code at the same time.

**Designing for asynchronous upgrade roll-outs**

To allow for asynchronous roll-outs, off-ledger components should:

- **use package names in Ledger API requests**:
  App components interacting with the Ledger API should use `by-package-name template IDs`
  instead of `by-package-id template IDs` in all their command submissions and queries.
  This allows:

    - Ledger API server-side version selection of the package preference for command submissions.

    - reading all versions of the templates in queries, even newer versions
      than the one the component was developed against.

- **handle missing optional fields**:
  App components reading from the Ledger API or Ledger API clients
  must be prepared to handle missing optional fields in records,
  including those in the initial package.
  The TypeScript and Java codegens for reading Daml values do so by default.

- **use exhaustive package preference**: on each command submission, the ``package_id_selection_preference`` is set ensuring that:

    - The package ID of every package used in the command and of every package
      used by all possible interface instances is included in the package preference.

    - Within an application, all submissions use the same package preference.

.. note::
    Following these recommendations decouples the application's behavior from the DAR uploads
    (see :ref:`the note regarding Package Preference <package-preference>`),
    as package resolution is fully determined by the explicit
    and exhaustive package preference provided in command submissions.

**Operational steps for upgrading**

Once a new Daml package version of the application is ready, define an operational window
for allowing the asynchronous rollout of the updated components.
During this window, Canton node and off-ledger app operators should:

- Upload the upgraded DARs to the participant nodes

- Roll-out the updated off-ledger components

After the operational window passes, the application can switch over to the upgraded logic as such:

- A switch-over time is decided and communicated to all app clients in advance,
  along with the updated package preference pertaining to the application's upgraded DARs.
  For example, you may encode the switch-over time in a config value set on all components;
  or publish it at a well-known HTTP endpoint from which all components read it.

- After the switch-over time, all Ledger API clients update their package preference
  and use it for subsequent command submissions.

.. note::
    Ledger API clients or participant nodes that do not finish the components rollout before the update switch-over time
    may not be able to participate in the upgraded workflow.

.. _backwards-incompatible-changes:

Rolling out backwards-incompatible changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some changes to a Daml workflow cannot be made backwards-compatible,
such as changing the definition of a template in a breaking way (e.g., extending the observer set).

To handle such changes, you can replace the existing contract with an upgraded one of the target template as follows:

- Introduce the target template as a new template following the :ref:`Breaking changes via Explicit Package Version <upgrade-package-naming>`

- Add a consuming ``OriginalTemplate_UpgradeToNewTemplate`` choice to the existing template by rolling out a new version in a backwards-compatible fashion.

- Where required, provide reference data for the ``upgrade`` choice via additional choice arguments.

- Implement backend automation or a contract migration tool similar to the one described :ref:`here <upgrade-overview>`
  to migrate all old contracts to the new ones by exercising the ``upgrade`` choice on the existing contracts.

.. note::
    Rolling out backwards-incompatible changes as described in this section
    incurs downtime on affected workflows until their contracts have been converted by the automation.
    In order to not disrupt business, such rollouts should be executed during maintenance windows.

.. note::
    Note that this kind of upgrade requires O(number-of-active-contracts) of transactions to roll out.
    Depending on the size of your ACS this can take a long time and consume significant compute and storage resources.
    In contrast, the backwards-compatible upgrades can be rolled out with constant cost, independent of the size of your ACS.

.. _upgrades-testing:

Testing
=======

Standalone Upgradeability Checks
--------------------------------

We recommend using the ``upgrade-check`` tool to perform a standalone check that all of the DARs typecheck against one another correctly as further validation of your upgraded packages.

This tool takes a list of DARs and runs Canton's participant-side upgrade
typechecking without spinning up an instance of Canton. You should pass the
tool the list of DARs constituting your previous model and the list of DARs for
your new model.

For example, assume you have a helper package ``helper`` that does not change,
and two packages ``main`` and ``dep``.

.. code:: text

  main-1.0.0.dar
  dep-1.0.0.dar
  helper-1.0.0.dar

After upgrading your model, you would publish a new DAR ``main-2.0.0.dar`` for ``main``
and a new DAR ``dep-2.0.0.dar`` for ``dep``. We would then recommend running the
upgrade-check tool as follows:

.. code:: bash

  > daml upgrade-check --participant helper-1.0.0.dar dep-1.0.0.dar main-1.0.0.dar dep-2.0.0.dar main-2.0.0.dar
  ...

This runs the same upload validation over these DARs that would be run in
the event of an upload to the ledger, and prints out the same messages and
errors. Because it does not require a ledger to be spun up, the command runs
much more quickly.

We can also check that all of the DARs pass compiler-side checks, but this is
much less likely to indicate an issue because the DARs are typechecked during
compilation.

.. code:: bash

  > daml upgrade-check --compiler helper-1.0.0.dar dep-1.0.0.dar main-1.0.0.dar dep-2.0.0.dar main-2.0.0.dar
  ...

Dry Run Uploading to a Test Environment
---------------------------------------

If you have a test environment with DARs that are not available to you, you may
not be able to supply a complete list of DARs for your previous model to the
standalone ``upgrade-check`` tool.

In this case, we recommend that as a further check for the validity of your
upgraded package, you perform a dry-run upload of your package to a testing
environment, using the ``--dry-run`` flag of the ``daml ledger upload-dar``
command. This also runs the upgrade typechecking, but does not persist your
package to the ledger.

For workflows involving multiple DARs, we recommend more robust testing by
running a Canton sandbox with the same version and environment as your
in-production participant and uploading all the old and new packages that
constitute your Daml app.

Daml Script Testing
-------------------

Daml Script has been used for demonstrative purposes in this document, however
usually the complexities of live upgrades comes with your workflows, not the data
transformations themselves. You can use Daml Script (with Canton) to test some
isolated simple cases, but for thorough tests of you system using SCU, you should
prefer full `Workflow Testing <#workflow-testing>`__.

We recommend placing your Daml Script tests
in a separate package which depends on all versions of your business logic when testing your upgrades with Daml Script. This testing
package should not be uploaded to the ledger if possible, as it depends on the ``daml-script-lts`` package.
This package emits a warning on the participant when uploaded, as it serves no purpose on a participant,
cannot be fully removed (as with any package), and may not be uploadable to the
ledger in future versions (Daml 3). More information about this limitation :ref:`here <upgrade-dont-upload-daml-script>`.

Depending on multiple versions of the same package does however face ambiguity issues with
imports. You can resolve these issues using :ref:`module prefixes <module_prefixes>`:

.. code:: yaml

  name: my-testing-package
  ...
  data-dependencies:
    - my/v1/main-package.dar
    - my/v2/main-package.dar
  module-prefixes:
    main-1.0.0.dar: V11
    main-1.1.0.dar: V12

It is important to verify old workflows are still functional under
new data and new implementation when writing your tests. You also need to verify that any new workflows intended
to be backward compatible can consume old data. You should build your testing structure to
cover this how you see fit, but we give the following recommendation as a starting point:

If your new version only includes choice body or interface instance changes (that is, it is a patch release)

-  | Run your existing test suite for V1 but updated to call V2 choices. This can be
     done with a rewrite, or by passing down a :ref:`package preference <daml-script-package-preference>`
     and calling the test with both the V1 and V2 package ID.

If your new version includes data changes, be that to contract payloads or choices (that is, it is a minor release)

-  | Assuming your data change affects a template payload, write separate setup code for V1 and V2, populating new fields
   | ``setupV1 : Script V1TestData``
   | ``setupV2 : Script V2TestData``
   | These new data types should mostly just hold Contract IDs

-  | Update your existing test suite from V1 to take a :ref:`package preference <daml-script-package-preference>`,
     allowing the V2 implementation without additional fields to choices to be called.
   | ``testV1 : [PackageId] -> V1TestData -> Script ()``

-  | Run the above test suite against V1 data, passing a V1 preference, then a V2 preference.
   | This ensures your changes haven't broken any existing workflows.

-  | Next write tests for your new/modified workflows, using the V2 choice implementations. This does not need a package preference.
   | ``testV2 : V2TestData -> Script ()``

-  | Run these tests against both the V1 setup and the V2 setup, to ensure your new workflows support existing/old templates.
   | In order to do this, you'll need some way to upcast your ``V1TestData``, i.e.
   | ``upcastTestData : V1TestData -> V2TestData``
   | This function should mostly just call ``coerceContractId`` on any contract IDs, and fill in any ``None`` values if needed.

-  | Finally, you can cover any workflows that require the contract data to already be upgraded (new fields populated), these are
     written entirely in V2 without any special considerations.

Multi-package builds for upgrades
--------------------------------------
.. _upgrades-multi-package:

When you are developing upgrades, you may have multiple DARs in scope that need
to be built together. Tracking these DARs and building them in the right order
can be complicated, especially as you develop live and as the project grows.

:ref:`Multi-package builds <multi-package-build>` help
with projects containing multiple DARs, for example, a project using upgrades.

To understand how multi-package builds simplify the
development of a project using upgrades, begin by creating a new Daml project
with the ``upgrades-example`` template.

.. code:: bash

   > daml new upgraded-iou --template upgrades-example
   > cd upgraded-iou
   > tree
   .
   ├── multi-package.yaml
   ├── run-test.sh
   ├── interfaces
   │   ├── daml/...
   │   └── daml.yaml
   ├── main-v1
   │   ├── daml/...
   │   └── daml.yaml
   ├── main-v2
   │   ├── daml/...
   │   └── daml.yaml
   └── test
       ├── daml/...
       └── daml.yaml

The example template contains:

- A package ``upgraded-iou-interfaces``, which defines an interface ``Asset``
  and a viewtype ``Asset.View``.
- The first version (1.0.0) of a package ``upgraded-iou-main``, which defines a
  template ``IOU`` with instance of ``upgraded-iou-interfaces:Main.Asset``.
- The second version (2.0.0) of ``upgraded-iou-main`` which upgrades
  the first. It adds a new ``description`` field to ``IOU``, and uses it (when
  the field is defined) in an upgraded implementation of ``Asset``.
- A testing package ``upgraded-iou-test``, which depends on both
  ``upgraded-iou-main-1.0.0`` and ``upgraded-iou-main-2.0.0``. It defines a
  script which exercises v1.0.0 and v2.0.0 ``IOU``s via their ``Asset``
  interface.
- A script ``run-test.sh``, which runs the main test in ``upgraded-iou-test``.
- A ``multi-package.yaml`` file which lists our four packages.

Without multi-package builds you would test your program like this:

.. code:: bash

   > # Run sandbox in the background, wait until the three lines below are shown
   > daml sandbox &
   Starting Canton sandbox.
   Listening at port 6865
   Canton sandbox is ready.
   > # Build all, run test
   > cd interfaces/; daml build --enable-multi-package=no
   > cd ../main-v1/; daml build --enable-multi-package=no
   > cd ../main-v2/; daml build --enable-multi-package=no
   > cd ../test/; daml build --enable-multi-package=no
   > cd ..
   > ./run-test.sh
   > # Modify v2, run test
   > cd main-v2/
   > ... modify main-v2 ...
   > daml build --enable-multi-package=no
   > cd ../test/; daml build --enable-multi-package=no
   > cd ..
   > # Modify test, run test
   > cd test/
   > daml build --enable-multi-package=no
   > cd ..
   > ./run-test.sh
   ...
   Test output:
   ...
   > kill %1 # Kill backgrounded sandbox process

Forgetting to rebuild packages after changing their source would not cause a
failure - for example, if you modified the source from ``main-v2`` in an
incompatible way but did not recompile it, the ``test`` package would still compile
successfully against the previous DAR for ``main-v2``.

.. code:: bash

   > # Modify main-v2 in an incompatible way
   > cd main-v2/
   > ... add a non-optional field `currency: Text` to template T in main-v2 ...
   > cd ../test/
   > daml build --enable-multi-package=no
   ...
   Created .daml/dist/upgraded-iou-upgrades-template-test-1.0.0.dar
   > # Compiling `test` succeeded even though main-v2 was changed incorrectly

With Daml multi-package builds, all builds automatically rebuild
dependencies if their source has changed:

.. code:: bash

   > cd test/
   > daml build # --enable-multi-package is set to true by default
   ...
   Building /home/dylanthinnes/ex-upgrades-template/main-v2
   ...
   Severity: DsError
   Message: 
   error type checking template Main.IOU :
     The upgraded template IOU has added new fields, but the following new fields are not Optional:
       Field 'currency' with type Text
   ...
   > # Compiling `test` failed as expected because main-v2 was changed incorrectly

The ``./run-test.sh`` script automatically rebuilds all DARs in the package that
need to be rebuilt:

.. code:: bash

   > daml sandbox & # Start sandbox in background
   Starting Canton sandbox.
   Listening at port 6865
   Canton sandbox is ready.
   > ... Fix main-v2 by dropping non-optional `currency` field ...
   > # Re-run test
   > ./run-test.sh
   ...
   Building /home/dylanthinnes/ex-upgrades-template/main-v2
   ...
   > # Modify test, run test
   > ... modify test ...
   > daml build --all
   > ./run-test.sh

Multi-package builds invoked by ``daml build --all`` always recompile stale dependencies and DARs in order. This ensures a
fully up-to-date package environment before running ``./run-test.sh``.

Workflow Testing
----------------

While testing your workflows is application-specific, we still
recommend at least one test for your core workflows that follows this pattern:

1. Start your app using version 2.0 of your DAR, but only upload version 1.0.
2. Initialize the app and start one instance of every core workflow.
3. Upload version 2.0 of your DAR.
4. Switch your backends to start using version 2.0, ideally this should be a flag.
5. Validate that the core workflows are in the same state and advance them to check that they are not stuck.

SCU Support in Daml Tooling
===========================

Codegen
-------

For packages that support SCU (i.e. LF1.17), generated code uses
package names in place of package IDs in template IDs. Retrieved data
from the ledger is subject to the upgrade transformations described
in previous sections.

Concretely, this is implemented as follows:

Java
~~~~

The classes that are generated for each template and interface contain a
``TEMPLATE_ID`` field, which, for upgradable packages, now use a
package name rather than a package ID. To help you determine
the package ID of these packages, we have added a new ``PACKAGE_ID`` field to all
such classes. Upgradable packages also have ``PACKAGE_NAME`` and
``PACKAGE_VERSION`` fields.

If you need to identify a template by the specific package ID of the DAR from
which the code was generated, you can use the ``TEMPLATE_ID_WITH_PACKAGE_ID`` field,
which is on all generated classes and their companion objects. When submitting
commands you may also use the ``packageIdSelectionPreference`` to explicitly specify which package
ID(s) you want the ledger to use to interpret the commands. Note that using these options
prevents your application from seamlessly supporting upgrades.

TypeScript
~~~~~~~~~~

The ``templateId`` field on generated template classes has been updated to
use the package name as the package qualifier for upgrade-compatible
packages. This is used for command submission and queries. However,
note that queries return the package qualifier with the
package ID rather than the package name. Generated modules now also
give the package "reference", which is the package name for upgrade-compatible packages; for other packages it is the package ID.

To perform package ID-qualified commands/queries in an upgrade
compatible package, create a copy of the template object using
the following:

.. code:: typescript

  const MyTemplateWithPackageId = {
    ...pkg.Mod.MyTemplate,
    templateId: `${pkg.packageId}:Mod:MyTemplate`,
  }

.. _json-api-server-1:

JSON API Server
----------------

Template IDs may still be used with a package ID; however,
for packages built as LF 1.17 or greater, the package may also be
identified by name. That is to say, for upgradable packages a template ID can have
the form ``#<package-name>:<module-name>:<template-name>``, and this is
resolved to corresponding templates from all packages which share this
name and are built at 1.17 or above. For packages built at LF 1.15,
the templates are not identifiable via a package name, and a
package ID must be used.

Note: template IDs in query results always use a package ID. This
allows you to distinguish the source of a particular contract. This means
that if you use a template with a package name in the request, you can
no longer expect the template IDs in the result to exactly match the
input template ID.

Package ID selection preference: preferences apply to JSON API where you
can specify your preferred selection of package versions.

PQS
---

To match the package-name changes to the Ledger API, PQS has changed how packages
are selected for queries. All queries that take a Daml identity in the form
``<package-id>:<module-name>:<template-name>`` now take a package name in place
of package ID. Note that this differs from the Ledger API in that the `#` prefix
is not required for PQS, as PQS has dropped direct package ID queries.
Queries for package names return all versions of a given contract, alongside the
package version and package ID for each contract.

.. note::
  If you still need to perform a query with an explicit package ID, you can either use
  a previous version of PQS or add the following filter predicate to your query:
  ``SELECT \* FROM active('my_package:My.App:MyTemplate') WHERE package_id = 'my_package_id'``

Given that PQS uses a document-oriented model for ledger content
(JSONB), extensions to contract payloads are handled simply by returning
the additional data in the blob.

Daml Shell
----------

Daml Shell builds on PQS by providing a shell interface to inspect the
ledger using package name to create an integrated view of all versions of contracts.

Daml-Script
-----------

Daml 2.10 introduces a new version of Daml Script which can be used by
depending on ``daml-script-lts`` in your ``daml.yaml``, as described
in `Writing your first upgrade <#writing-your-first-upgrade>`__. This version of Daml Script
supports upgrades over the Ledger API.

All commands and queries in this version of Daml Script now use
upgrades/downgrades automatically to ensure that they exercise the correct versions
of choices and return correct payloads.

The following additional functionality is available for more advanced
uses of SCU.

**Exact commands**

Each of the four submission commands now has an "exact" variant: ``createExactCmd``, ``exerciseExactCmd``, ``exerciseByKeyExactCmd``, and
``createAndExerciseExactCmd``. These commands force the participant to
use the exact version of the package that your script uses, so you can be certain of the choice code you are
calling. Note that exact and non-exact commands can be mixed in the same
submission.

.. _daml-script-package-preference:

**Daml Script Package Preference**

A submission can specify a :ref:`package preference <package-preference>`
as a list of package IDs:

.. code:: daml

  (actAs alice <> packagePreference [myPackageId]) `submitWithOptions` createCmd ...

Note the use of ``submitWithOptions : SubmitOptions -> Commands a -> Script a``.
You can build ``SubmitOptions`` by combining the ``actAs`` and ``packagePreference`` functions with ``<>``.

The full list of builders for ``SubmitOptions`` is as follows:

.. code:: daml

  -- `p` can be `Party`, `[Party]`, etc.
  actAs : IsParties p => p -> SubmiOptions
  readAs : IsParties p => p -> SubmitOptions

  disclose : Disclosure -> SubmitOptions
  discloseMany : [Disclosure] -> SubmitOptions

  newtype PackageId = PackageId Text
  packagePreference : [PackageId] -> SubmitOptions

A ``PackageId`` can be hard-coded in your script, in which case it must be updated whenever the package changes. Otherwise,
it can be provided using the ``--input-file`` flag of the ``daml script`` command line tool.

The following example demonstrates reading the package ID from a DAR and passing it to a script:

.. code:: bash

  # Path to the dar you want to pass as package preference.
  PACKAGE_DAR=path/to/main/dar.dar
  # Path to the dar containing the Daml script for which you want to pass the package-id
  SCRIPT_DAR=path/to/script/dar.dar
  # Extract the package-id of PACKAGE_DAR's main package.
  daml damlc inspect-dar ${PACKAGE_DAR} --json | jq '.main_package_id' > ./package-id-script-input.json
  # replace --ide-ledger with --ledger-host and --ledger-port for deployed Canton
  daml script --dar ${SCRIPT_DAR} --script-name Main:main --ide-ledger --input-file ./package-id-script-input.json

Following this, your script would look like:

.. code:: daml

  module Main where

  import Daml.Script

  main : Text -> Script ()
  main rawPkgId = do
    let pkgId = PackageId rawPkgId
    alice <- allocateParty "alice"
    -- Commands omitted for brevity
    let submitOptions = actAs alice <> packagePreference [pkgId]
    submitOptions `submitWithOptions` createCmd ...

Daml Studio support
-------------------

Daml Studio runs a reference model of Canton called the IDE Ledger. This
ledger has been updated to support the relevant parts of the Smart Contract
Upgrades feature.
