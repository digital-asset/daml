
.. _module-proposaldsl-55246:

Module ProposalDSL
------------------


Templates
^^^^^^^^^

.. _template-proposaldsl-proposal-62786:

template **Proposal**

  .. list-table::
     :widths: 15 10 30
     :header-rows: 1
  
     * - Field
       - Type
       - Description
     * - asset
       - t
       -
     * - receivers
       - [Party]
       -
     * - name
       - Text
       -

  + **Choice Accept**
  + **Choice External:Archive**

Typeclasses
^^^^^^^^^^^

.. _class-proposaldsl-proposalinstance-15268:

**class (Template t) => ProposalInstance t where**
  
  .. _function-proposaldsl-signatoryproposal-98755:
  
  **signatoryProposal**
    : Proposal t -> [Party]


  
  .. _function-proposaldsl-observerproposal-918:
  
  **observerProposal**
    : Proposal t -> [Party]


  
  .. _function-proposaldsl-ensureproposal-73020:
  
  **ensureProposal**
    : Proposal t -> Bool


  
  .. _function-proposaldsl-agreementproposal-20063:
  
  **agreementProposal**
    : Proposal t -> Text


  
  .. _function-proposaldsl-createproposal-2110:
  
  **createProposal**
    : Proposal t -> Update (ContractId (Proposal t))


  
  .. _function-proposaldsl-fetchproposal-52411:
  
  **fetchProposal**
    : ContractId (Proposal t) -> Update (Proposal t)


  
  .. _function-proposaldsl-archiveproposal-26739:
  
  **archiveProposal**
    : ContractId (Proposal t) -> Update ()


  
  .. _function-proposaldsl-haskeyproposal-15527:
  
  **hasKeyProposal**
    : HasKey (Proposal t)


  
  .. _function-proposaldsl-keyproposal-31568:
  
  **keyProposal**
    : Proposal t -> ([Party], Text)


  
  .. _function-proposaldsl-maintainerproposal-77012:
  
  **maintainerProposal**
    : HasKey (Proposal t) -> ([Party], Text) -> [Party]


  
  .. _function-proposaldsl-fetchbykeyproposal-89026:
  
  **fetchByKeyProposal**
    : ([Party], Text) -> Update (ContractId (Proposal t), Proposal t)


  
  .. _function-proposaldsl-lookupbykeyproposal-43975:
  
  **lookupByKeyProposal**
    : ([Party], Text) -> Update (Optional (ContractId (Proposal t)))


  
  .. _function-proposaldsl-consumptionproposalarchive-38663:
  
  **consumptionProposalArchive**
    : PreConsuming (Proposal t)


  
  .. _function-proposaldsl-controllerproposalarchive-48053:
  
  **controllerProposalArchive**
    : Proposal t -> Archive -> [Party]


  
  .. _function-proposaldsl-actionproposalarchive-64515:
  
  **actionProposalArchive**
    : ContractId (Proposal t) -> Proposal t -> Archive -> Update ()


  
  .. _function-proposaldsl-exerciseproposalarchive-96641:
  
  **exerciseProposalArchive**
    : ContractId (Proposal t) -> Archive -> Update ()


  
  .. _function-proposaldsl-consumptionproposalaccept-70582:
  
  **consumptionProposalAccept**
    : PreConsuming (Proposal t)


  
  .. _function-proposaldsl-controllerproposalaccept-85504:
  
  **controllerProposalAccept**
    : Proposal t -> Accept -> [Party]


  
  .. _function-proposaldsl-actionproposalaccept-24726:
  
  **actionProposalAccept**
    : ContractId (Proposal t) -> Proposal t -> Accept -> Update (ContractId t)


  
  .. _function-proposaldsl-exerciseproposalaccept-1112:
  
  **exerciseProposalAccept**
    : ContractId (Proposal t) -> Accept -> Update (ContractId t)


