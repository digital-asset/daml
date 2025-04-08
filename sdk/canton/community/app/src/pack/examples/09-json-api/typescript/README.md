# JSON API Example - TypeScript

This repository contains code and configurations demonstrating how to interact with the Ledger using the JSON API. It utilizes TypeScript to access the Ledger API from a Node.js client.

## Prerequisites

Before running the project, ensure you have the following installed:

- A Bash-compatible terminal
- [DAML](https://docs.daml.com/getting-started/installation.html)
- Node.js and npm

## Running the Project

To run the project, follow these steps in two separate terminal windows:

### Step 1: Start the Ledger

1. Open the first terminal window.
2. Run the following command to start the Ledger:
   ```sh
   ../run.sh
   ```

### Step 2: Set Up and Execute the TypeScript Client

1. Open the second terminal window.
2. Execute the following commands in sequence:
   ```sh
   npm install
   ```
   Installs the necessary dependencies.
   ```sh
   npm run generate_daml_bindings
   ```
   Generates TypeScript definitions for the DAML model.
   ```sh
   npm run generate_api
   ```
   Generates TypeScript bindings for the JSON API using `openapi.yaml` and `openapi-fetch`.
   ```sh
   npm run compile
   ```
   Compiles the TypeScript code.
   ```sh
   npm run scenario
   ```
   Runs an example scenario.

## Code Structure

The source code for JSON API usage patterns can be found in the `src` folder. Review these files to understand the interaction with the Ledger API.

For further details, refer to the documentation provided in the repository or the official DAML documentation.







