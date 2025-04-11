Updates
-------

Updates specify the transactions that will be committed to the ledger. Updates are described within a do block:

.. code-block::

    do
      cid <- create NewContract with field1 = 1
                                     , field2 = "hello world"
      let answer = 42
      exercise cid SomeChoice with choiceArgument = "123"
      return answer

.. list-table::
   :widths: 30 70

   * - ``create``
     - create an instance of the given template on the ledger

       ``create NameOfTemplate with exampleParameters``
   * - ``exercise``
     - exercise a choice on a given contract by contract id

       ``exercise IdOfContract NameOfChoiceContract with choiceArgument1 = value1``
   * - ``exerciseByKey``
     - exercise a choice on a given contract by contract key

       ``exerciseByKey @ContractType contractKey NameOfChoiceOnContract with choiceArgument1 = value1``
   * - ``fetch``
     - fetch the contract data from the ledger by contract id

       ``fetchedContract <- fetch IdOfContract``
   * - ``fetchByKey``
     - fetch the contract id and data from the ledger by contract key

       ``fetchedContract <- fetchByKey @ContractType contractKey``
   * - ``lookupByKey``
     - check whether a contract with the given key exists and if yes, return the contract id

       ``fetchedContractId <- lookupByKey @ContractType contractKey``
   * - ``abort``
     - abort a transaction with an error message, the transaction will not be committed to the ledger

       ``abort errorMessage``
   * - ``assert``
     - assert that a given predicate holds, otherwise fail the transaction

       ``assert (condition == True)``
   * - ``getTime``
     - get the ledger effective time

       ``currentTime <- getTime``
   * - ``return``
     - return a value from a do block

       ``return 42``
   * - ``let``
     - bind a local variable or define a local function within the update do block

       .. code-block::

           let createContract x = create NameOfContract with issuer = x; owner = x

            let answer = 42

   * - ``this``
     - refers to the current contract data that contains this update in a choice

       ``create NewContract with owner = this.owner``
   * - ``forA``
     - run a for loop of actions over a list

       ``forA [alice, bob, charlie] $ \p -> create NewContract with owner = p``
