.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Release notes
#############

This page contains release notes for the SDK.

0.12.0
------

- Change in how values are addressed in Navigator's `frontend-config.js`.
  
  - Old syntax for accessing values: `argument.foo.bar`
  - New syntax: 

    .. code-block:: javascript

        argument.fields.find(f => f.label === "foo").fields.find(f => f.label === "bar").value

        // Or using a helper function
        DamlLfValue.evalPath(argument, ["foo", "bar"])
        DamlLfValue.toJSON(argument).foo.bar

0.11.32
-------

- DAML standard library (breaking change): Removed ``DA.List.split`` function, which was never intended to be
  exposed and doesn't do what the name suggests.
- Java Bindings (breaking change): Removed type parameter for ``DamlList`` and ``DamlOptional`` classes.
  The ``DamlList``, ``DamlOptional``, and ``ContractId`` classes were previously parameterized (i.e ``DamlList[String]``)
  for consistency with the DAML language. The type parameter has been removed as such type information
  is not supported by the underlying Ledger API and and therefore the parameterized type couldn’t be
  checked for correctness.
- Java Bindings (breaking change): For all classes in the package ``com.daml.ledger.javaapi.data``, we shortened
  the names of the conversion methods from long forms like ``fromProtoGeneratedCompletionStreamRequest`` and
  ``toProtoGeneratedCompletionStreamRequest`` to the much shorter ``fromProto`` and ``toProto``.
- Navigator: Add support for Optional and recursive data types.
- Navigator: Improve start up performance for big DAML models.
- Navigator (breaking change): Refactor the GraphQL API. If you're maintaining a modified version of
  the Navigator frontend, you'll need to adapt all your GraphQL queries to the new API.
- Navigator: Fix an issue where it was not possible to enter contract arguments involving contract IDs.
- Navigator: Fix issues where the console could not read some events or commands from its database.
- DAML syntax (breaking change) : For the time being, datatypes with a single data constructor not associated with an argument are not accepted. For example ``data T = T``. To workaround this use ``data T = T {}`` or ``data T = T ()`` (depending on whether you desire ``T`` be interpreted as a product or a sum).

0.11.3
------

*Released on 2019-02-07*

Changes:

- Navigator: Fix display of Date values.
- Extractor: Add first version of Extractor with PostgreSQL support.

0.11.2
------

*Released on 2019-01-31*

Changes:

- Navigator: Add a terminal-based console interface using SQLite as a backend.
- Navigator: Now writes logs to ./navigator.log by default using Logback.
- DAML Studio: Significant performance improvements.
- DAML Studio: New table view for scenario results.
- DAML Standard Library: New type classes.
- Node.js bindings: Documentation updated to use version 0.4.0 and DAML 1.2.

0.11.1
------

*Released on 2019-01-24*

Changes:

- Java Bindings: Fixed Timestamp.fromInstant and Timestamp.toInstant.
- Java Bindings: Added Timestamp.getMicroseconds.

0.11.0
------

*Released on 2019-01-17*

Changes:

- Documentation: :doc:`DAML documentation </daml/reference/index>` and :doc:`examples </examples/examples>` now use DAML 1.2.

  To convert your code to DAML 1.2, see the :doc:`conversion guide </daml//1-2-conversion>`.
- Documentation: Added a comprehensive :doc:`quickstart guide </getting-started/quickstart>` that replaces the old "My first project" example.

	As part of this, removed the My first project, IOU and PvP examples.
- Documentation: Added a :doc:`guide to building applications against a DA ledger </app-dev/app-arch/index>`.
- Documentation: Updated the :doc:`support and feedback page <support>`.

- Ledger API: Version 1.4.0 has support for multi-party subscriptions in the transactions and active contracts services.
- Ledger API: Version 1.4.0 supports the verbose field in the transactions and active contracts services.
- Ledger API: Version 1.4.0 has full support for transaction trees.
- Sandbox: Implements Ledger API version 1.4.0.
- Java Bindings: Examples updated to use version 2.5.2 which implements Ledger API version 1.4.0.

.. - TODO: add changes here
