.. _module-proposaliou-96142:

Module ProposalIou
------------------

Templates
^^^^^^^^^

.. _type-proposaliou-iou-51326:

**template** `Iou <type-proposaliou-iou-51326_>`_

  .. list-table::
     :widths: 15 10 30
     :header-rows: 1
  
     * - Field
       - Type
       - Description
     * - issuer
       - Party
       - 
     * - owner
       - Party
       - 
     * - amount
       - `Decimal <https://docs.daml.com/daml/reference/base.html#type-ghc-types-decimal-54602>`_
       - 
  
  + **Choice External\:Archive**
    
  
  + **Choice Burn**
    

Template Instances
^^^^^^^^^^^^^^^^^^

.. _type-proposaliou-proposaliou-81988:

**template instance** `ProposalIou <type-proposaliou-proposaliou-81988_>`_
  \= Proposal `Iou <type-proposaliou-iou-51326_>`_
