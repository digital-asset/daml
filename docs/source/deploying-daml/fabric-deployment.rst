.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0
.. _fabric-deployment:

Deploying DAML on Fabric
########################

What We're Doing
================

1. Setting up our DAML App, in this case a project called :code:`create-daml-app`
2. Building and running a local Fabric ledger with DAML support through the :code:`daml-on-fabric` project
3. Deploying our `create-daml-app` code to our Fabric ledger where it will record its state
4. Running a JSON endpoint that automatically creates every endpoint we need for our DAML application
5. Starting up a React UI that will consume these JSON endpoints with no lower level interaction with DAML or Fabric necessary


Building Our DAML App
=====================

1. Let's :code:`daml create-daml-app my-app` to create our example application in the :code:`my-app` directory.
2. Now follow the `build instructions <https://github.com/digital-asset/daml/blob/master/templates/create-daml-app/README.md>`__ in :code:`README.md`.
	a. Build the project up to and including the :code:`yarn build` command, then come back here

Starting up Fabric
==================

Now that we have our DAML app built it needs a place to run, let's grab :code:`daml-on-fabric` and get it running

3. Simply :code:`git clone git@github.com:digital-asset/daml-on-fabric.git` and follow the `instructions <hhttps://github.com/digital-asset/daml-on-fabric>`__ for "Running a local Hyperledger Fabric network"
	b. Make sure for Java, Scala, and SBT that you are using the exact versions specified or otherwise you may encounter build or runtime issues. You can use sdkman.io to easily install these specific versions and manage multiple versions.
4. From the root `daml-on-fabric` directory run :code:`sbt "run --port 6865 --role provision,time,ledger" -J-DfabricConfigFile=config.json` which will let the DAML runtime start talking to our Fabric instance.
	a. Give this process a moment to start up, it is ready once you see output like

	.. literalinclude:: sbt-example-output
		:language: log

Deploying Our DAML App
======================

Alright our Fabric instance is up and running, time to deploy our application and give it a JSON endpoint for our user facing UI

5. From the :code:`my-app` directory run :code:`daml deploy --host localhost --port 6865`. This will deploy your DAR file (ie. DAML application) to your Fabric ledger.
6. From the same directory run :code:`daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575` to automatically create a json api that will connect to your Fabric ledger and serve up a JSON endpoint for your UI to use

Setting up Our Frontend
=======================

Alright our backend is all up and running. Now let's start up our UI

7. Set `REACT_APP_LEDGER_ID=fabric-ledger` so our UI knows the id of the ledger to use
8. Start the ui by running :code:`yarn start` from the :code:`my-app/ui` directory
9. The UI should start up a browser window by itself once ready, if it doesn’t go to `<http://localhost:3000/>`__
	a. It may take a few moments for the website to load as yarn starts the server before the UI is built
10. Login as :code:`Alice`, :code:`Bob`, or :code:`Charlie` and try out :code:`create-daml-app`
	a. Due to some initialization steps the first time you login may take a few seconds, this delay is not present on subsequent logins.

Congratulations, you’ve successfully deployed your first DAML app to a live Fabric ledger!