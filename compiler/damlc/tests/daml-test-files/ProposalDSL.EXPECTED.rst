
.. _module-proposaldsl-55246:

Module ProposalDSL
------------------


Typeclasses
^^^^^^^^^^^

.. _class-proposaldsl-proposalinstance-15268:

**class (Template t) => ProposalInstance t where**
  
  .. _function-proposaldsl-signatoryproposal-98755:
  
  **signatoryProposal**
    : `Proposal <data-proposaldsl-proposal-5268_>`_ t -> [Party]


  
  .. _function-proposaldsl-observerproposal-918:
  
  **observerProposal**
    : `Proposal <data-proposaldsl-proposal-5268_>`_ t -> [Party]


  
  .. _function-proposaldsl-ensureproposal-73020:
  
  **ensureProposal**
    : `Proposal <data-proposaldsl-proposal-5268_>`_ t -> Bool


  
  .. _function-proposaldsl-agreementproposal-20063:
  
  **agreementProposal**
    : `Proposal <data-proposaldsl-proposal-5268_>`_ t -> Text


  
  .. _function-proposaldsl-createproposal-2110:
  
  **createProposal**
    : `Proposal <data-proposaldsl-proposal-5268_>`_ t -> Update (ContractId (`Proposal <data-proposaldsl-proposal-5268_>`_ t))


  
  .. _function-proposaldsl-fetchproposal-52411:
  
  **fetchProposal**
    : ContractId (`Proposal <data-proposaldsl-proposal-5268_>`_ t) -> Update (`Proposal <data-proposaldsl-proposal-5268_>`_ t)


  
  .. _function-proposaldsl-archiveproposal-26739:
  
  **archiveProposal**
    : ContractId (`Proposal <data-proposaldsl-proposal-5268_>`_ t) -> Update ()


  
  .. _function-proposaldsl-haskeyproposal-15527:
  
  **hasKeyProposal**
    : HasKey (`Proposal <data-proposaldsl-proposal-5268_>`_ t)


  
  .. _function-proposaldsl-keyproposal-31568:
  
  **keyProposal**
    : `Proposal <data-proposaldsl-proposal-5268_>`_ t -> ([Party], Text)


  
  .. _function-proposaldsl-maintainerproposal-77012:
  
  **maintainerProposal**
    : HasKey (`Proposal <data-proposaldsl-proposal-5268_>`_ t) -> ([Party], Text) -> [Party]


  
  .. _function-proposaldsl-fetchbykeyproposal-89026:
  
  **fetchByKeyProposal**
    : ([Party], Text) -> Update (ContractId (`Proposal <data-proposaldsl-proposal-5268_>`_ t), `Proposal <data-proposaldsl-proposal-5268_>`_ t)


  
  .. _function-proposaldsl-lookupbykeyproposal-43975:
  
  **lookupByKeyProposal**
    : ([Party], Text) -> Update (Optional (ContractId (`Proposal <data-proposaldsl-proposal-5268_>`_ t)))


  
  .. _function-proposaldsl-consumptionproposalarchive-38663:
  
  **consumptionProposalArchive**
    : PreConsuming (`Proposal <data-proposaldsl-proposal-5268_>`_ t)


  
  .. _function-proposaldsl-controllerproposalarchive-48053:
  
  **controllerProposalArchive**
    : `Proposal <data-proposaldsl-proposal-5268_>`_ t -> Archive -> [Party]


  
  .. _function-proposaldsl-actionproposalarchive-64515:
  
  **actionProposalArchive**
    : ContractId (`Proposal <data-proposaldsl-proposal-5268_>`_ t) -> `Proposal <data-proposaldsl-proposal-5268_>`_ t -> Archive -> Update ()


  
  .. _function-proposaldsl-exerciseproposalarchive-96641:
  
  **exerciseProposalArchive**
    : ContractId (`Proposal <data-proposaldsl-proposal-5268_>`_ t) -> Archive -> Update ()


  
  .. _function-proposaldsl-consumptionproposalaccept-70582:
  
  **consumptionProposalAccept**
    : PreConsuming (`Proposal <data-proposaldsl-proposal-5268_>`_ t)


  
  .. _function-proposaldsl-controllerproposalaccept-85504:
  
  **controllerProposalAccept**
    : `Proposal <data-proposaldsl-proposal-5268_>`_ t -> `Accept <data-proposaldsl-accept-93712_>`_ -> [Party]


  
  .. _function-proposaldsl-actionproposalaccept-24726:
  
  **actionProposalAccept**
    : ContractId (`Proposal <data-proposaldsl-proposal-5268_>`_ t) -> `Proposal <data-proposaldsl-proposal-5268_>`_ t -> `Accept <data-proposaldsl-accept-93712_>`_ -> Update (ContractId t)


  
  .. _function-proposaldsl-exerciseproposalaccept-1112:
  
  **exerciseProposalAccept**
    : ContractId (`Proposal <data-proposaldsl-proposal-5268_>`_ t) -> `Accept <data-proposaldsl-accept-93712_>`_ -> Update (ContractId t)



Data types
^^^^^^^^^^

.. _data-proposaldsl-accept-93712:

data **Accept**

  
  
  .. _constr-proposaldsl-accept-49965:
  
  **Accept**
  
  

.. _data-proposaldsl-proposal-5268:

data **Proposal t**

  
  
  .. _constr-proposaldsl-proposal-16421:
  
  **Proposal**
  
  
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
