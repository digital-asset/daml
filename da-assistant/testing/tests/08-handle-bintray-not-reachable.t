
Test that the assistant ignores the auto-upgrade failures:

  $ da config set cli.upgrade-interval 0
  $ da config get cli.upgrade-interval
  0
  
  $ da config set bintray.download-url https://localhost:22334
  $ da config get bintray.download-url
  https://localhost:22334
  
  $ da config set bintray.api-url https://localhost:22334
  $ da config get bintray.api-url
  https://localhost:22334
  
  $ da path | grep '/packages/sandbox/'
  sandbox: /.*/home/\.da/packages/sandbox/.* (re)

Test that the assistant ignores the auto-upgrade failures in a project:

  $ da config set cli.upgrade-interval 0
  $ da config get cli.upgrade-interval
  0
  
  $ da new test-project
  Creating a project called test-project
  in /.*/08-.*/test-project. (re)
  $ cd test-project
  $ da config set bintray.download-url https://localhost:22334
  $ da config set --project bintray.download-url https://localhost:22334
  $ da config get bintray.download-url
  https://localhost:22334
  
  $ da config set bintray.api-url https://localhost:22334
  $ da config set --project bintray.api-url https://localhost:22334
  $ da config get bintray.api-url
  https://localhost:22334
  
  $ da path | grep '/packages/sandbox/'
  sandbox: /.*/home/\.da/packages/sandbox/.* (re)
