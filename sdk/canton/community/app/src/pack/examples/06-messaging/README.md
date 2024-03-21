# Messaging via the global domain

***
WARNING: The global Canton domain is currently not running. This example does not work at the moment.
You need to start your own Canton domain and set the environment variable canton-examples.domain-url
to the URL of your domain.
***
TODO(#7564) Make this example work again once the global domain is up
***

Participants require a domain to communicate with each other. Digital
Asset is running a generally available global Canton domain
(Canton.Global). Any participant can decide to connect to the global
domain and use it for bilateral communication.

The messaging example provides a simple messaging application via the
global domain.

The example is structured as follows:

```
    .
    |-- message                            Daml model for messages
    |   |- .daml/dist/message-0.0.1.dar    Compiled DAR file
    |   |- daml/Message.daml               Daml source code for messages
    |   |- daml.yaml                       Daml configuration file
    |   |- frontend-config.js              Configuration file for Daml Navigator
    |
    |-- contact                            Daml model for contacts
    |   |- daml/Contact.daml               Incomplete Daml source code for contacts
    |   |- daml/Contact.solution           Example solution for the Daml exercise below
    |   |- daml.yaml                       Daml configuration file
    |   |- frontend-config.js              Configuration file for Daml Navigator
    |
    |-- canton.conf                        Configuration file for one participant
    |-- init.canton                        Initialization script for Canton
```

The files in `message` must not be changed because it defines the
format of messages to be exchanged.  So `message-0.0.1.dar` must be
the same on all participants that want to exchange messages.


Run the application by performing the following steps:

1. Compile the contact model by issuing the command `daml build` in
   the `contact` folder.  This should generate the file
   `contact/.daml/dist/contact-0.0.1.dar`.

2. Start Canton from the `06-messaging` folder with the following command

   ```
        ../../bin/canton -c canton.conf --bootstrap init.canton
   ```

   If you have never connected to the global domain before, you will
   be shown the terms of service for using the global domain. You will
   have to accept it once in order to be able to use it.

   Next, you will be asked for your username in the messaging
   application. Canton usernames may contain only letters, numbers,
   `-` and `_` and may not be longer than 189. Canton will suffix your
   username to make it globally unique. Your suffixed user name will
   be output on the screen.

   You can set the username in the Java system property
   `canton-examples.username` as a command-line argument:

   ```
     ../../bin/canton -c canton.conf --bootstrap init.canton -Dcanton-examples.username=Alice
   ```

3. Start Daml Navigator.

   After step 2, Canton outputs the command that you need to run to
   start Daml Navigator. Run the command in a separate terminal from
   the `contact` folder. Typically, the command looks as follows:

   ```
     daml navigator server localhost 7011 -t wallclock --port 7015 -c ui-backend-participant1.conf
   ```

   This will start the frontend on port 7015.

4. Open a browser and point it to `http://localhost:7015`.
   Login with your chosen username.

5. Find someone else whom you want to send a message.  You can search
   for usernames with the following command in the Canton console:

   ```
     findUser("Alice")
   ```

   This will list all suffixed usernames that contain the string
   `Alice`.  Note that these users need not be currently online.

   Click on the `Message:Message` template in the `Templates` view of
   Navigator to create a new message.  Put your suffixed username as
   `sender` and the recipient's suffixed username as `receiver`.

   Click `Submit` to send the message. A `Message:Message` contract
   should soon be shown in the `Contracts` table as well as under `Sent`.

   The receiver can use the `Reply` choice to send a message back.

   Stop Canton and Navigator after that.

   Note: Canton is configured to run with a file-based database.
   Your username suffix and the messages will be persisted
   on your computer in the file `participant1.mv.db`.
   Delete this file if you want to start afresh.

6. Extend the `Contact` Daml model.  As is, you must specify suffixed
   username of yourself and your contact whenever you send a new
   message.  The `Contact` template in `contact/daml/Contact.daml`
   can store these usernames, but it does not have any choices yet.

   Add a non-consuming choice `Send` to the `Contact` template that
   takes a message as parameter. It shall create a `Message` with
   `myself` as sender, `other` as recipient, and the given message.

   Write a script to test the message sending via a `Contact` contract
   and run the script in Daml studio.

   Compile the extended `Contact` Daml model by running `daml build`
   in the `contact` folder.

7. Restart Canton and Navigator as described in Step 5.
   You will be shown a reminder of your suffixed user name
   instead of being asked for one.

   Create a `Contact` contract for your counterparty.
   Use the `Send` choice on the `Contact` to send a message.

   Since you have modified the `Contact` template, there will be now
   several `Contact` templates in the `Templates` tab; one for each
   version.  Your existing `Contact` contracts refer to the old
   version and therefore do not offer the `Send` choice.  You would
   have to explicitly upgrade the contracts; this process is explained
   in the Daml documentation at https://docs.daml.com/upgrade/index.html.
