.. Copyright (c) 2020 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0
.. _sawtooth-deployment:

Deploying DAML on Sawtooth
##########################

What We're Doing
================

1. Setting up our DAML App, in this case a project called :code:`create-daml-app`
2. Building and running a local Sawtooth ledger with DAML support through the :code:`daml-on-sawtooth` project
3. Deploying our `create-daml-app` code to our Sawtooth ledger where it will record its state
4. Running a JSON endpoint that automatically creates every endpoint we need for our DAML application
5. Starting up a React UI that will consume these JSON endpoints with no lower level interaction with DAML or Sawtooth necessary

Building Our DAML App
=====================

1. Let's :code:`git clone git@github.com:digital-asset/create-daml-app.git` the :code:`create-daml-app` repository and follow the `build instructions <https://github.com/digital-asset/create-daml-app>`__.
	a. Build the project up until, and including the :code:`yarn workspaces run build` command, then come back here

Starting up Sawtooth
====================

Now that we have our DAML app built it needs a place to run, let's grab :code:`daml-on-sawtooth` and get it running

2. Simply :code:`git clone git@github.com:blockchaintp/daml-on-sawtooth.git` and follow the `build instructions <https://github.com/blockchaintp/daml-on-sawtooth/>`__ to build and run the Sawtooth instance
	a. CAVEAT: You'll need to refer to the first 3 steps of :code:`README.md` and the "Using the docker based toolchain" section of :code:`BUILD.md`

Deploying Our DAML App
======================

Alright our Sawtooth instance is up and running, time to deploy our application and give it a JSON endpoint for our user facing UI

3. From the :code:`create-daml-app` directory run :code:`daml deploy --host localhost --port 9000`. This will deploy your DAR file (ie. DAML application) to your Sawtooth ledger.
	a. CAVEAT: Deployment may fail with the error :code:`daml-helper: GRPCIOBadStatusCode StatusDeadlineExceeded (StatusDetails {unStatusDetails = "Deadline Exceeded"})`, if so exit your Sawtooth instance with Ctrl+C or CMD+C, run :code:`./docker/run.sh stop`, restart it with :code:`./docker/run.sh start`, and finally retry the :code:`daml deploy` command above.
4. From the same directory run :code:`daml json-api --ledger-host localhost --ledger-port 9000 --http-port 7575 --default-ttl 60s` from the same directory to automatically create a json api that will connect to your Sawtooth ledger and serve up a JSON endpoint for your end user app/ui to use
	a. CAVEAT: Sawtooth requires a higher ttl for transactions than daml’s default which is why we set `default-ttl` to `60s`

Setting up Our Frontend
=======================

Alright our backend is all up and running. Now let's start up our UI

5. Start the ui as mentioned in :code:`create-daml-app/README.md` (ie. :code:`cd ui/ && yarn start`)
	a. CAVEAT: Before starting the UI with :code:`yarn start` change the ledgerid from :code:`create-daml-app-sandbox` to :code:`default-ledgerid` in :code:`ui/src/config.ts`
6. The UI should start up a browser window by itself once ready, if it doesn’t go to `<http://localhost:3000/>`__
	a. CAVEAT: It may take 10+ seconds for the website to load as yarn starts the server before the UI is built
7. Login as :code:`Alice`, :code:`Bob`, or :code:`Carol` and try out :code:`create-daml-app`
	a. CAVEAT: Due to some initialization steps the first time you login may take a few seconds, this delay is not present on subsequent logins.

Congratulations, you’ve successfully deployed your first DAML app to a live Sawtooth ledger!