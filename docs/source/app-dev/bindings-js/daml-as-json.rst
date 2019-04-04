.. Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

.. _bindings-js-daml-as-json:

DAML as JSON
############

The Node.js bindings accept as parameters and return as results plain JavaScript objects.

More specifically, these objects are subset that is fully compatible with the JSON data-interchange format. This means that any request and response to and from the ledger can be easily used with other services that accept JSON as an input format.

The `reference documentation`_ is more specific about the expected shape of objects sent to and received from the ledger, but to give you a sense of how these objects look, the following represents the creation of a ``Pvp`` record.

.. code-block:: javascript

    {
        create: {
            templateId: { packageId: '934023fa9c89e8f89b8a', name: 'Pvp.Pvp' },
            arguments: {
                recordId: { packageId: '934023fa9c89e8f89b8a', name: 'Pvp.Pvp' },
                fields: {
                    buyer        : { party: 'some-buyer' },
                    seller       : { party: 'some-seller' },
                    baseIssuer   : { party: 'some-base-issuer' },
                    baseCurrency : { text: 'CHF' },
                    baseAmount   : { decimal: '1000000.00' },
                    baseIouCid   : { variant: { variantId: { packageId: 'ba777d8d7c88e87f7', name: 'Maybe' }, constructor: 'Just', value: { contractId: '76238b8998a98d98e978f' } } },
                    quoteIssuer  : { party: 'some-quote-issuer' },
                    quoteCurrency: { text: 'USD' },
                    quoteAmount  : { decimal: '1000001.00' },
                    quoteIouCid  : { variant: { variantId: { packageId: 'ba777d8d7c88e87f7', name: 'Maybe' }, constructor: 'Just', value: { contractId: '76238b8998a98d98e978f' } } },
                    settleTime   : { timestamp: 93641099000000000 }
                }
            }
        }
    }

Notice that fields of a template are represented as keys in ``fields``. Each value is then another object, where the key is the type. This is necessary for the ledger to disambiguate between, for example, strings that represent ``text`` and strings that represent a ``decimal``.

.. _reference documentation: ./reference/index.html
