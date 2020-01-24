.. Copyright (c) 2019 The DAML Authors. All rights reserved.
.. SPDX-License-Identifier: Apache-2.0

DAML roadmap (as of January 2020)
===================================

This page specifies the major features we're planning to add next to the DAML Ecosystem. Plans and timelines are subject to change. If you need any of these features or want to request others, see the :doc:`/support/support` page for how to get in touch.

We aim to land the following changes in the coming three months.

- **Developer Experience (DX)**

  - Improved version of the “Getting Started Guide” (former “Quickstart”) that will include instructions on how to build an end-to-end application with a UI
  - An end-to-end skeleton application that can be installed via the DAML Assistant
  - Improved compatibility between different DAML SDK versions


- **Front-end support**

  - A front-end JavaScript/TypeScript framework including supporting libraries and tools that facilitate interaction with data on a DAML ledger
  - A React library built on top of the above-mentioned framework that binds ledger data to React components

- **New “how-tos” in the docs**

  - How to deploy DAML models to existing Ledgers
  - How to deploy DAML full-stack DAML applications 
  - How to migrate a DAML model to a new version

- **Ledger model improvements**

  - Improved Ledger Time model that works seamlessly in a distributed setting, and contains optional safeguards against duplicate command submission
  - Better privacy controls by separating the disclosure mechanisms for business and validation purposes and adding new mechanisms to share single contracts and delegate read rights 

- **Deployment Options**

  - ALPHA version of DAML on Corda available through partners