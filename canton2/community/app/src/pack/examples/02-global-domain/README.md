# Connection to Canton.Global

***
WARNING: The global Canton domain is currently not running. This example does not work at the moment.
***
TODO(#7564) Make this example work again once the global domain is up
***


Participants require a domain to communicate with each other. Digital Asset is running a generally available
global Canton domain (Canton.Global). Any participant can decide to connect to the global domain and use it 
for bilateral communication.  

The global domain connectivity example demonstrates how to connect a participant node
to the global Canton domain. Currently, the global domain is operated as a test-net.
Longer term, the global domain will serve as a global fall-back committer which can be
used if no closer committer is available.

The global domain connectivity example contains two files, a configuration file and a
script which invokes the necessary registration call and subsequently tests the connection
by pinging the digital asset node.

```
       ../../bin/canton -c global-domain-participant.conf --bootstrap global-domain-participant.canton
```

After invoking above script, you will be prompted the terms of service for using the global
domain. You will have to accept it once in order to be able to use it.

Please note that right now, the global domain is a pure test-net and we are regularly resetting
the domain entirely, wiping all the content, as we are still developing the protocol. Therefore, 
just use it for demonstration purposes.

