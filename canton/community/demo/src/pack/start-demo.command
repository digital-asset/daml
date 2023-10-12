#!/bin/bash

cd "$(dirname "$0")" || exit 1

java javafx.scene.control.TableCell 2>&1 | grep -q "Main method not found"
NATIVE_SUPPORT=$?

JAVA_VERSION=$(java -version 2>&1 | grep version | awk '{print $3}')

JAVA_MAJOR_VERSION=$(sed -E 's/"(1\.)?([0-9]+)\..*/\2/' <<< "$JAVA_VERSION")

CPU=$(uname -m)

echo "Detected Java version $JAVA_VERSION (major: $JAVA_MAJOR_VERSION)."
ARGS=""
if [ "$JAVA_MAJOR_VERSION" -lt 11 ]; then
  echo "Please use Java version 11 or higher."
  exit 1
else
  if [ $NATIVE_SUPPORT -ne 0 ]; then
    SCRIPT="demo.sc"
    if [[ "$CPU" == "arm64" ]]; then
      # TODO(#8460) remove this one once the dependencies can be upgraded (and work correctly)
      # Note: this is due to https://bugs.openjdk.java.net/browse/JDK-8275723 and https://github.com/sbt/sbt/issues/6564
      echo "On Apple Silicon, you need to run the demo with a JDK that natively includes JavaFX."
      echo "You can download it from here: https://www.azul.com/downloads/?version=java-11-lts&os=macos&architecture=arm-64-bit&package=jdk-fx"
      echo "Unpack it, then restart the demo with:"
      echo "export JAVA_HOME=<path to the unpacked new jdk>"
      echo "export PATH=\$JAVA_HOME/bin:\$PATH"
      echo "./$(basename $0)"
      exit 1
    else
      echo "Java FX is not installed. It will be fetched automatically."
    fi
  else
    echo "Looks like your JRE has native support for JavaFX"
    SCRIPT="demo-native.sc"
  fi
fi
# Run with --no-tty, as the demo otherwise tends to mess up terminal settings,
# e.g., input feedback and line breaks are gone after running the demo.
if [[ "$CPU" != "arm64" ]]; then
  ARGS="$ARGS --no-tty"
fi
echo $ARGS
bin/canton -v -c demo/demo.conf --bootstrap demo/$SCRIPT $ARGS
