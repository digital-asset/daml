cd ..
ls -R
.\a\dev-env\windows\bin\dadew.ps1 install
Set-StrictMode -Version latest
.\a\dev-env\windows\bin\dadew.ps1 sync
.\a\dev-env\windows\bin\dadew.ps1 enable
curl -L https://github.com/bazelbuild/bazel/releases/download/2.0.1/bazel-2.0.1-windows-x86_64.exe -o bazel-bootstrap.exe
git clone https://github.com/cocreature/bazel.git -b more-debug-output
cd bazel
../\azel-bootstrap.exe build //scripts/packages:zip-bazel-exe -c opt --stamp --embed_label 2.0.
