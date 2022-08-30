.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _daml-ref-interfaces:

Reference: Interfaces
#####################

.. warning::
  This feature is under active development and not officially supported in
  production environments.

In Daml, an interface defines an abstract type together with a behavior
specified by its view type, method signatures, and choices. For a template to
conform to this interface, there must be a corresponding ``interface instance``
definition where all the methods of the interface (including the special ``view``
method) are implemented. This allows decoupling such behavior from its
implementation, so other developers can write applications in terms of the
interface instead of the concrete template.

Configuration
*************
In order to test this experimental feature, you need to enable
the dev mode for both the Daml compiler and Canton.

For the Daml compiler, add the following line to your `daml.yaml` file:
.. code-block:: none

   build-options: [--target=1.dev]

To enable dev mode for Canton, follow the instructions in :ref:`this documentation <how-do-i-enable-unsupported-features>`
to enable experimental features.

Interface Declaration
*********************

An interface declaration is somewhat similar to a template declaration.

Interface Name
--------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_NAME_BEGIN
   :end-before: -- INTERFACE_NAME_END

- This is the name of the interface.
- It's preceded by the keyword ``interface`` and followed by the keyword ``where``.
- It must begin with a capital letter, like any other type name.

Implicit abstract type
----------------------

- Whenever an interface is defined, an abstract type is defined with the same
  name. "Abstract" here means:

  - Values of this type cannot be created using a data constructor. Instead,
    they are constructed by applying the function ``toInterface`` to a template
    value.
  - Values of this type cannot be inspected directly via case analysis.
    Instead, use functions such as ``fromInterface``.
  - See :ref:`daml-ref-interface-functions` for more information on these and
    other functions for interacting with interface values.

- An interface value carries inside it the type and parameters of the template
  value from which it was constructed.
- As for templates, the existence of a local binding ``b`` of type ``I``, where
  ``I`` is an interface does not necessarily imply the existence on the ledger
  of a contract with the template type and parameters used to construct ``b``.
  This can only be assumed if ``b`` the result of a fetch from the ledger within
  the same transaction.

Interface Methods
-----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_METHODS_BEGIN
   :end-before: -- INTERFACE_METHODS_END

- An interface may define any number of methods.
- A method definition consists of the method name and the method type, separated
  by a single colon ``:``. The name of the method must be a valid identifier
  beginning with a lowercase letter or an underscore.
- A method definition introduces a top level function of the same name:

  - If the interface is called ``I``, the method is called ``m``, and the
    method type is ``M`` (which might be a function type), this introduces
    the function ``m : I -> M``:

    .. literalinclude:: ../code-snippets-dev/Interfaces.daml
      :language: daml
      :start-after: -- INTERFACE_METHODS_TOP_LEVEL_BEGIN
      :end-before: -- INTERFACE_METHODS_TOP_LEVEL_END

  - The first argument's type ``I`` means that the function can only be applied
    to values of the interface type ``I`` itself. Methods cannot
    be applied to template values, even if there exists an
    ``interface instance`` of ``I`` for that template. To use an interface
    method on a template value, first convert it using the ``toInterface``
    function.

  - Applying the function to such argument results in a value of type ``M``,
    corresponding to the implementation of ``m`` in the interface instance of
    ``I`` for the underlying template type ``t`` (the type of the template value
    from which the interface value was constructed).

- One special method, ``view``, must be defined for the viewtype.
  (see :ref:`interface-viewtype` below).

.. _interface-viewtype:

Interface View Type
-------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_VIEWTYPE_DATATYPE_BEGIN
   :end-before: -- INTERFACE_VIEWTYPE_DATATYPE_END

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_VIEWTYPE_BEGIN
   :end-before: -- INTERFACE_VIEWTYPE_END

- All interface instances must implement a special ``view`` method which returns
  a value of the type declared by ``viewtype``.
- The type must be a record.
- This type is returned by subscriptions on interfaces.

.. _interface-choices:

Interface Choices
-----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_CHOICES_BEGIN
   :end-before: -- INTERFACE_CHOICES_END

- Interface choices work in a very similar way to template choices. Any contract
  of a template type for which an interface instance exists will grant the
  choice to the controlling party.
- Interface choices can only be exercised on values of the corresponding
  interface type. To exercise an interface choice on a template value, first
  convert it using the ``toInterface`` function.
- Interface methods can be used to define the controller of a choice
  (e.g. ``method1``) as well as the actions that run when the choice is
  *exercised* (e.g. ``method2`` and ``method3``).
- As for template choices, the ``choice`` keyword can be optionally prefixed
  with the ``nonconsuming`` keyword to specify that the contract will not be
  consumed when the choice is exercised. If not specified, the choice will be
  ``consuming``. Note that the ``preconsuming`` and ``postconsuming`` qualifiers
  are not supported on interface choices.
- See :doc:`choices` for full reference information, but note that
  controller-first syntax is not supported for interface choices.

Empty Interfaces
----------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- EMPTY_INTERFACE_BEGIN
   :end-before: -- EMPTY_INTERFACE_END

- It is possible (though not necessarily useful) to define an interface without
  methods, precondition or choices. However, a view type must always be defined,
  though it can be set to unit.

Interface Instances
*******************

For context, a simple template definition:

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- TEMPLATE_HEADER_BEGIN
   :end-before: -- TEMPLATE_HEADER_END

``interface instance`` clause
-----------------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_INSTANCE_IN_TEMPLATE_BEGIN
   :end-before: -- INTERFACE_INSTANCE_IN_TEMPLATE_END

- To make a template an instance of an existing interface, an
  ``interface instance`` clause must be defined in the template declaration.
- The template of the clause must match the enclosing declaration. In other
  words, a template ``T`` declaration can only contain ``interface instance``
  clauses where the template is ``T``.
- The clause must start with the keywords ``interface instance``, followed by
  the name of the interface, then the keyword ``for`` and the name of the
  template, and finally the keyword ``where``, which introduces a block where
  **all** the methods of the interface must be implemented.
- Within the clause, there's an implicit local binding ``this`` referring to the
  contract on which the method is applied, which has the type of the template's
  data record. The template parameters of this contract are also in scope.
- Method implementations can be defined using the same syntax as for top level
  functions, including pattern matches and guards (e.g. ``method3``).
- Each method implementation must return the same type as specified for that
  method in the interface declaration.
- The implementation of the special ``view`` method must return the type
  specified as the ``viewtype`` in the interface declaration.

``interface instance`` clause in the interface
----------------------------------------------

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_INSTANCE_IN_INTERFACE_BEGIN
   :end-before: -- INTERFACE_INSTANCE_IN_INTERFACE_END

- To make an *existing* template an instance of a new interface, the
  ``interface instance`` clause must be defined in the *interface* declaration.
- In this case, the *interface* of the clause must match the enclosing
  declaration. In other words, an interface ``I`` declaration can only contain
  ``interface instance`` clauses where the interface is ``I``.
- All other rules for ``interface instance`` clauses are the same whether the
  enclosing declaration is a template or an interface. In particular, the
  implicit local binding ``this`` always has the type of the *template*'s
  record.

Empty ``interface instance`` clause
-----------------------------------

- If the interface has no methods, the interface instance only needs to
  implement the ``view`` method:

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- EMPTY_INTERFACE_INSTANCE_BEGIN
   :end-before: -- EMPTY_INTERFACE_INSTANCE_END

.. _daml-ref-interface-functions:

Interface Functions
*******************

.. list-table::
   :header-rows: 1

   * - Function
     - Type
     - Instantiated type
     - Notes
   * - ``interfaceTypeRep``
     - ``HasInterfaceTypeRep i => i -> TemplateTypeRep``
     - ``MyInterface -> TemplateTypeRep``
     - The value of the resulting ``TemplateTypeRep`` indicates what template
       was used to construct the interface value.
   * - ``toInterface``
     - ``forall i t. HasToInterface t i => t -> i``
     - ``MyTemplate -> MyInterface``
     - Converts a template value into an interface value.
   * - ``fromInterface``
     - ``HasFromInterface t i => i -> Optional t``
     - ``MyInterface -> Optional MyTemplate``
     - Attempts to convert an interface value back into a template value.
       The result is ``None`` if the expected template type doesn't match the
       underlying template type used to construct the contract.
   * - ``toInterfaceContractId``
     - ``forall i t. HasToInterface t i => ContractId t -> ContractId i``
     - ``ContractId MyTemplate -> ContractId MyInterface``
     - Convert a template contract id into an interface contract id.
   * - ``fromInterfaceContractId``
     - ``forall t i. HasFromInterface t i => ContractId i -> ContractId t``
     - ``ContractId MyInterface -> ContractId MyTemplate``
     - Converts an interface contract id into a template contract id.
       This function does not verify that the given contract id actually points
       to a contract of the resulting type; if that is not the case, a
       subsequent ``fetch``, ``exercise`` or ``archive`` will fail.
       Therefore, this should only be used when the underlying contract is known
       to be of the resulting type, or when the result is immediately used by a
       ``fetch``, ``exercise`` or ``archive`` action and a transaction failure
       is the desired behavior in case of mismatch.
       In all other cases, consider using ``fetchFromInterface`` instead.
   * - ``fetchFromInterface``
     - ``forall t i. (HasFromInterface t i, HasFetch i) => ContractId i -> Update (Optional (ContractId t, t))``
     - ``ContractId MyInterface -> Update (Optional (ContractId MyTemplate, MyTemplate))``
     - Attempts to fetch and convert an interface contract id into a template,
       returning both the converted contract and its contract id if the
       conversion is successful, or ``None`` otherwise.

Required Interfaces
*******************

.. warning::
  This feature is under active development and not officially supported in
  production environments.
  Required interfaces are only available when targeting Daml-LF 1.dev.

.. literalinclude:: ../code-snippets-dev/Interfaces.daml
   :language: daml
   :start-after: -- INTERFACE_REQUIRES_BEGIN
   :end-before: -- INTERFACE_REQUIRES_END

- An interface can depend on other interfaces. These are specified with the
  ``requires`` keyword after the interface name but before the
  ``where`` keyword, separated by commas.
- For an interface declaration to be valid, its list of required interfaces
  must be transitively closed. In other words, an interface ``I`` cannot
  require an interface ``J`` without also explicitly requiring all the
  interfaces required by ``J``. The order, however, is irrelevant.

  For example, given

  .. literalinclude:: ../code-snippets-dev/Interfaces.daml
     :language: daml
     :start-after: -- INTERFACE_TRANSITIVE_REQUIRES_GIVEN_BEGIN
     :end-before: -- INTERFACE_TRANSITIVE_REQUIRES_GIVEN_END

  This declaration for interface ``Square`` would cause a compiler error

  .. literalinclude:: ../code-snippets-dev/Interfaces.daml
     :language: daml
     :start-after: -- INTERFACE_TRANSITIVE_REQUIRES_INCORRECT_BEGIN
     :end-before: -- INTERFACE_TRANSITIVE_REQUIRES_INCORRECT_END

  Explicitly adding ``Shape`` to the required interfaces fixes the error

  .. literalinclude:: ../code-snippets-dev/Interfaces.daml
     :language: daml
     :start-after: -- INTERFACE_TRANSITIVE_REQUIRES_CORRECT_BEGIN
     :end-before: -- INTERFACE_TRANSITIVE_REQUIRES_CORRECT_END

- For a template ``T`` to be a valid ``interface instance`` of an interface
  ``I``, ``T`` must also be an ``interface instance`` of each of the interfaces
  required by ``I``.

Interface Functions
-------------------

.. list-table::
   :header-rows: 1

   * - Function
     - Notes
   * - ``toInterface``
     - Can also be used to convert an interface value to one of its required
       interfaces.
   * - ``fromInterface``
     - Can also be used to convert a value of an interface type to one of its
       requiring interfaces.
   * - ``toInterfaceContractId``
     - Can also be used to convert an interface contract id into a contract id
       of one of its required interfaces.
   * - ``fromInterfaceContractId``
     - Can also be used to convert an interface contract id into a contract id
       of one of its requiring interfaces.
   * - ``fetchFromInterface``
     - Can also be used to fetch and convert an interface contract id into a
       contract and contract id of one of its requiring interfaces.
