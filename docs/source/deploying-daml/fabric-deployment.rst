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

1. Let's :code:`git clone git@github.com:digital-asset/create-daml-app.git` the :code:`create-daml-app` repository and follow the `build instructions <https://github.com/digital-asset/create-daml-app>`__.
	a. Build the project up until, and including the :code:`yarn workspaces run build` command, then come back here

Starting up Fabric
==================

Now that we have our DAML app built it needs a place to run, let's grab :code:`daml-on-fabric` and get it running

2. Simply :code:`git clone git@github.com:hacera/daml-on-fabric.git` and follow the `instructions <https://github.com/hacera/daml-on-fabric>`__ for "Running a local Hyperledger Fabric network"
	a. CAVEAT: You won't need to install the Fabric dependencies here as they are handled automatically by docker when running the container. You will need the rest of the dependencies though.
	b. CAVEAT: Make sure for Java, Scala, and SBT that you are using the exact versions specified or otherwise you may encounter build or runtime issues. You can use sdkman.io to easily install these specific versions and manage multiple versions.
	c. CAVEAT: By Java version 1.8 they mean Java 8.
3. From the root `daml-on-fabric` directory run :code:`sbt "run --port 6865 --role provision,time,ledger,explorer"` which will let the DAML runtime start talking to our Fabric instance.
	a. Give this process a moment to start up, it is ready once you see output like

	.. literalinclude:: sbt-example-output
		:language: log

Deploying Our DAML App
======================

Alright our Fabric instance is up and running, time to deploy our application and give it a JSON endpoint for our user facing UI

4. From the :code:`create-daml-app` directory run :code:`daml deploy --host localhost --port 6865`. This will deploy your DAR file (ie. DAML application) to your Fabric ledger.
5. From the same directory run :code:`daml json-api --ledger-host localhost --ledger-port 6865 --http-port 7575` from the same directory to automatically create a json api that will connect to your Fabric ledger and serve up a JSON endpoint for your end user app/ui to use

Setting up Our Frontend
=======================

Alright our backend is all up and running. Now let's start up our UI

6. Start the ui as mentioned in :code:`create-daml-app/README.md` (ie. :code:`cd ui/ && yarn start`)
	a. CAVEAT: Before starting the UI with :code:`yarn start` change the ledgerid from :code:`create-daml-app-sandbox` to :code:`fabric-ledger` in :code:`ui/src/config.ts`
7. The UI should start up a browser window by itself once ready, if it doesn’t go to `<http://localhost:3000/>`__
	a. CAVEAT: It may take 10+ seconds for the website to load as yarn starts the server before the UI is built
8. Login as :code:`Alice`, :code:`Bob`, or :code:`Carol` and try out :code:`create-daml-app`
	a. CAVEAT: Due to some initialization steps the first time you login may take a few seconds, this delay is not present on subsequent logins.

Congratulations, you’ve successfully deployed your first DAML app to a live Fabric ledger!