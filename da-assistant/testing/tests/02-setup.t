
Now that we have installed the tool in the previous step we can
configure it using the 'setup' command:

  $ da setup < $TESTROOT/setup-input
  Welcome to the SDK Assistant.
  
  You need to provide some information for me to work properly:
  
  Your email address: Your Digital Asset Bintray username: Your Digital Asset Bintray API key: Checking credentials...
  Your credentials are valid.
  Downloading latest SDK release...
  Installing packages:.* (re)
  Activating packages:.* (re)
  $ da config set cli.update-channel production








  $ da --version
  Installed SDK Assistant version: 0-aaa
  .{0} (re)
  Type `da update-info` to see information about update channels.

Here we test that the setup tells the user if the installed jdk version is too old.

  $ mkdir -p "$TESTROOT/bin"
  $ printf "#!/usr/bin/env sh\necho javac 1.7.0_50" > "$TESTROOT/bin/javac"
  $ printf "#!/usr/bin/env sh\necho Python 2.7.10" > "$TESTROOT/bin/python3"
  $ chmod +x $TESTROOT/bin/*
  $ PATH="$TESTROOT/bin:$PATH" da setup
  Checking credentials...
  Your credentials are valid.
  Downloading latest SDK release...
  Already installed:.* (re)
  Activating packages:.* (re)
  [Warn] The installed JDK version is not supported
  Installed version: JDK 7
  Min required version: JDK 8
  [Warn] The installed Python version is not supported.
  Installed version: Python 2.7.10
  Minimal required version: Python 3.6.2


  $ rm $TESTROOT/bin/*

