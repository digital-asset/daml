.. Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

Extending Daml Applications
###########################

.. toctree::
   :hidden:

Consider the following simple Daml model for carbon certificates:

.. literalinclude:: example/carbon-1.0.0/daml/CarbonV1.daml
  :language: daml
  :start-after: -- CARBON_BEGIN
  :end-before: -- CARBON_END

It contains two templates. The above template representing a carbon compensation
certificate. And a second template to create the `CarbonCert` via a
:ref:`Propose-Accept workflow <intro propose accept>`.

Now we want to extend this model to add trust labels for certificates
by third parties.  We don't want to make any changes to the already deployed
model. Changes to a Daml model will result in changed package ID's for the
contained templates. This means that if a Daml model is already deployed, the
modified Daml code will not be able to reference contracts instantiated with the
old package. To avoid this problem, it's best to put extensions in a new
package.

In our example we call the new package `carbon-label` and implement the label
template like

.. literalinclude:: example/carbon-label/daml/CarbonLabel.daml
  :language: daml
  :start-after: -- CARBON_LABEL_BEGIN
  :end-before: -- CARBON_LABEL_END

The `CarbonLabel` template references the `CarbonCert` contract of the
`carbon-1.0.0` packages by contract ID. Hence, we need to import the CarbonV1
module and add the `carbon-1.0.0` to the dependencies in the `daml.yaml` file.
Because we want to be independent of the Daml SDK used for both packages, we
import the `carbon-1.0.0` package as data dependency

.. literalinclude:: example/carbon-label/daml.yaml
  :language: daml
  :start-after: # DAML_YAML_BEGIN
  :end-before: # DAML_YAML_END

Deploying an extension is simple: just upload the new package to the ledger with
the `daml ledger upload-dar` command. In our example the ledger runs on the
localhost:

.. code-block:: bash

  daml ledger upload-dar --ledger-port 6865 --ledger-host localhost ./daml/dist/carbon-label-1.0.0.dar

If instead of just extending a Daml model you want to modify an already deployed
template of your Daml model, you need to perform an upgrade of your Daml
application. This is the content of the next section.
