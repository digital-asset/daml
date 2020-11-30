.. Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Querying the Navigator local database
#####################################

You can query contracts, transactions, events, or commands in any way you'd like, by querying the Navigator Console's local database(s) directly. This page explains how you can run queries.

.. note:: Because of the strong DAML privacy model, each party will see a different subset of the ledger data. For this reason, each party has its own local database.

The Navigator database is implemented on top of `SQLite <https://sqlite.org/index.html>`_. SQLite understands most of the standard SQL language. For information on how to compose SELECT statements, see to the SQLite `SELECT syntax specification <https://www.sqlite.org/lang_select.html>`_.

To run queries, use the ``sql`` Navigator Console command. Take a look at the examples below to see how you might use this command.

How the data is structured
**************************
To get full details of the schema, run ``sql_schema``.

Semi-structured data (such as contract arguments or template parameters) are stored in columns of type `JSON <https://www.sqlite.org/json1.html>`_. 

You can compose queries against the content of JSON columns by using the SQLite functions `json_extract <https://www.sqlite.org/json1.html#jex>`_ and `json_tree <https://www.sqlite.org/json1.html#jtree>`_.


Example query using plain SQL
*****************************
Filter on the template id of contracts::

    sql select count (*) from contract where template_id like '%Offer%'

Example queries using JSON functions
************************************
Select JSON fields from a JSON column by specifying the path::

    sql select json_extract(value, '$.argument.landlord') from contract

Filter on the value of a JSON field::

    sql select contract.id, json_tree.fullkey  from contract, json_tree(contract.value) where atom is not null and json_tree.value like '%BANK1%'

Filter on the JSON key and value::

    sql select contract.id from contract, json_tree(contract.value) where atom is not null and json_tree.key = 'landlord' and json_tree.value like '%BANK1%'

Filter on the value of a JSON field for a given path::

    sql select contract.id from contract where json_extract(contract.value, '$.argument.landlord') like '%BANK1%'

Identical query using json_tree::

    sql select contract.id from contract, json_tree(contract.value) where atom is not null and json_tree.fullkey = '$.argument.landlord' and json_tree.value like '%BANK1%'

Filter on the content of an array if the index is specified::

    sql select contract.id from contract where json_extract(contract.value, '$.template.choices[0].name') = 'Accept'

Filter on the content of an array if the index is not specified::

    sql select contract.id from contract, json_tree(contract.value) where atom is not null and json_tree.path like '$.template.choices[%]' and json_tree.value ='Accept'
 
