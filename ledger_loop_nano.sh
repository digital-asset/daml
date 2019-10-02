# upload dars

DAR_LOCATION=/private/var/tmp/_bazel_brianhealey/191c77e1bcea6e1b949558cf8ebb3be9/execroot/com_github_digital_asset_daml/bazel-out/darwin-opt/bin/ledger/ledger-api-test-tool/ledger-api-test-tool.runfiles/com_github_digital_asset_daml
DAML_REPO_LOCATION=~/g/daml


cd $DAR_LOCATION
daml ledger upload-dar SemanticTests.dar --host localhost --port 6861
daml ledger upload-dar Test-stable.dar --host localhost --port 6861
daml ledger upload-dar Test-dev.dar --host localhost --port 6861

cd $DAML_REPO_LOCATION
for i in {1..2}
do
	echo "Running Iteration $i of Semantic Tests"
	bazel run -- //ledger/ledger-api-test-tool localhost:6861 --command-submission-ttl-scale-factor 3.5 --timeout-scale-factor 3.5 -v
done
