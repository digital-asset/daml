# Canton Presentation & Demo

## Purpose

The following example is used to demonstrate the unique Canton capabilities:
  * Application Composability - Add new workflows at any time to a running system
  * Network Interoperability - Create workflows spanning across domains
  * Privacy - Canton uses data minimization and only shares data on a need to know basis.
  * Regulatory compliance - Canton can be used to even integrate personal sensitive information directly in workflows 
    without fear of failing to be GDPR compliant.

## Supported Java Versions

The demo application requires JavaFX. JavaFX comes as part of Oracle JRE/JDK 8 and above, however
it is not part of OpenJDK 8. There is an open source implementation (OpenJFX) however, which 
will be loaded automatically if you are using OpenJDK 11 and beyond.  

Therefore, if you should see an error like `UnsupportedClassVersionNumber`, then it is likely 
that you are using OpenJDK 8. 

Therefore, please use a JRE of version 11 or higher.

### Apple Silicon Users

Please note that Apple Silicon requires some tweaking due to poor Java FX for JDK 11 support on this platform.
The only way to run the demo is to run it with an JDK that packages JavaFX natively. You can download one from
https://www.azul.com/downloads/?version=java-11-lts&os=macos&architecture=arm-64-bit&package=jdk-fx

Unpack it, then restart the demo with:
```
    export JAVA_HOME=<path to the unpacked new jdk>"
    export PATH=$JAVA_HOME/bin:$PATH
    bash start-demo.command
```

## Running
     
The demo application can be started using  

```
       ./start-demo.command 
```
on Unix like systems and by clicking the ``start-demo-win.cmd`` on Windows.

Please note that you need to start the script from the directory where you unpacked the Canton distribution,
as otherwise the script won't find the resources.

The script will pull some dependencies (scalax) from Maven central the first time you run it. Therefore, you need 
a working internet connection.

Please navigate to the "Notes" tab for some instructions.

## The Daml Models

All models can be found in `demo/daml/[bank|health-insurance/medical-records/doctor]`. The models are 
simplistic in order to keep them small for educational purposes. 

## Analytics

If you run the demo, we will get a small notification that you've started it: We will generate 
an anonymous unique id of your system to capture the event. We don't send anything else (though
your internet address will be known to us.) Have a look at `Notify.send()` in  
`demo/runner/runner.sc`. You can turn it off by setting `NO_PHONE_HOME` as an
environment variable. 

