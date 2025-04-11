Concepts
--------

.. list-table::
   :widths: 30 70

   * - ``Party``
     - A party represents a person or legal entity (for example a bank). Parties can create contracts and exercise choices and are represented by the ``Party`` data type in Daml.
   * - ``Signatories``, ``observers``, and ``controllers``
     - Signatories, observers, and controllers are parties involved in actions taken on a contract, i.e., actions that are *exercised* on a contract. Signatories, observers, and controllers are therefore represented by the ``Party`` data type. They control who can read, create and archive a contract.
   * - ``Contract``
     - Contracts are created from blueprints called ``templates`` - this is the Daml code you write. Templates include:

         - contract data (e.g., date, description, parties involved etc.)
         - roles (signatory, observer)
         - choices and their respective controllers (*who* gets to do *what*)

       Every contract is a *template instance* stored as a row on the ledger. Contracts are immutable: once they are created on the ledger, the information in the contract cannot be changed. In order to “change” a contract you need to create a new one with the desired contract data.
   * - ``Choice``
     - A choice is something that a party can exercise (take action) on a contract. Choices give you a way to transform the data in a contract: while the contract itself is immutable, you can write a choice that archives the contract and creates a new version of it with the updated data.


       A choice can only be exercised by its controller and contains the ``authorization`` of all of the contract’s signatories as well as of the controller.
   * - ``Ledger``
     - The ledger represents the database where all contracts are recorded. More information on Daml Ledgers can be found `here <https://docs.daml.com/concepts/ledger-model/ledger-structure.html#ledgers>`__.


If you are interested you can find the detailed glossary `here <https://docs.daml.com/concepts/glossary.html>`__.

.. todo:: Update/remove the old urls.

    The old paragraph also has a broken link to the course: https://daml.com/learn/fundamental-concepts/
