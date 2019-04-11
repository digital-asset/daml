#!/bin/bash
# Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#
# This script creates a "capsule" release of da assistant, including
# the latest public da assistant and the latest public sdk release,
# all packaged together with an install script. It should be possible
# to install and use the capsule release without an internet connection,
# although there are no guarantees.
#

if [[ "$OSTYPE" == "darwin"* ]]; then
  OS="osx"
elif [[ "$OSTYPE" == "linux-gnu" ]]; then
  OS="linux"
else
  echo "Unsupported operating system."
  exit 1
fi


URL="https://bintray.com/api/v1/content/digitalassetsdk/DigitalAssetSDK/com/digitalasset/da-cli/\$latest/da-cli-\$latest-$OS.run?bt_package=da-cli"
FNAME="da-cli-\$latest-$OS.run?bt_package=da-cli"

rm -rf capsule-tmp da-capsule-$OS.tar.gz da-capsule-$OS # clean working directory
mkdir -p capsule-tmp

echo "Working inside capsule-tmp folder."
echo "Output goes to capsule-tmp/out.log file."
pushd capsule-tmp >> capsule-tmp/out.log

echo "Downloading SDK Assistant for $OS."
wget -o out.log $URL

echo "Deleting existing ~/.da folder."
rm -rf ~/.da >> out.log

echo "Running SDK Assistant installer."
sh $FNAME >> out.log

echo "Running da setup."
~/.da/bin/da setup >> out.log

echo "Setting script mode."
sed -e 's/script-mode: false/script-mode: true/g' -i .backup ~/.da/da.yaml

echo "Packaging everything up."
cp -r ~/.da da-data
tar cf da-data.tar da-data
mkdir da-capsule-$OS
mv da-data.tar da-capsule-$OS/da-data.tar

EXTENSION_VERSION=$(ls ~/.da/packages/daml-extension)

cat > da-capsule-$OS/install.sh <<"EOF"
#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd $DIR
tar xf da-data.tar da-data
rm -rf ~/.da
mv da-data ~/.da
if [[ $PATH == *"$HOME/.da/bin"* ]] ; then
  echo "DAML SDK Assistant da is installed."
else
  echo "Please add $HOME/.da/bin to your PATH."
fi
echo "Activating vscode extension."
mkdir -p ~/.vscode/extensions
rm -f ~/.vscode/extensions/da-vscode-daml-extension
rm -rf ~/.vscode/extensions/da-vscode-daml-extension
EOF

echo "ln -s ~/.da/packages/daml-extension/$EXTENSION_VERSION ~/.vscode/extensions/da-vscode-daml-extension" >> da-capsule-$OS/install.sh

chmod +x da-capsule-$OS/install.sh

cat > da-capsule-$OS/README.txt <<"EOF"
This is a capsule version of the SDK.

To install, open the terminal and run

    sh install.sh

in this folder.
EOF

tar czf da-capsule-$OS.tar.gz da-capsule-$OS
popd >> out.log
mv capsule-tmp/da-capsule-$OS.tar.gz .

echo "Created da-capsule-$OS.tar.gz"
