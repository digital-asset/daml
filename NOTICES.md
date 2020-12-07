## Generating NOTICES file

The notices file is generated after the completion of an automated Blackduck scan of the entire daml repo.

At present this needs to be updated by running the scan manually and checking in the updated NOTICES file on a PR. In future a PR will be automatically created when a change in the NOTICES file is detected as part of the Blackduck scan within the daily compat job on master.

To generate the file locally, you should run the Blackduck scan after performing a full Bazel build on the DAML repo

Full details on running a Blackduck scan can be found @ https://github.com/DACH-NY/security-blackduck/blob/master/README.md


1) Run full Bazel build
```bazel build //...```

2) Create personal Blackduck token and add to environment variable
Create a personal Blackduck token by authenticating to the Blackduck site with your DA Google account
https://digitalasset.blackducksoftware.com/api/current-user/tokens

Click Create New Token and give yourself read and write access, giving a memorable name (<username>-<machine> or similar)
Copy the contents of this token and define in a local environment variable called BLACKDUCK_HUBDETECT_TOKEN
```export BLACKDUCK_HUB_DETECT_TOKEN=<token_you_have_just_created>``` 

2) Run Haskell Blackduck scan
https://github.com/digital-asset/daml/blob/a17b340b47a711b53a1a5eb141c7835a9fb9bbbe/ci/cron/daily-compat.yml#L227-L234

3) Run Scan for all remaining languages, waiting for notices file to be generated
https://github.com/digital-asset/daml/blob/a17b340b47a711b53a1a5eb141c7835a9fb9bbbe/ci/cron/daily-compat.yml#L241-L257

4) Remove windows line endings and rename file to NOTICES
```tr -d '\015' <*_Black_Duck_Notices_Report.txt | grep -v dach-ny_daml-on-corda >NOTICES```

5) Create a new PR with the changes and submit for review for merge to master

