# Getting the data out of GCP

You can get the data out of GCP, provided you are a digital asset employee with sufficient permissions. First ask for permissions on "Logs Viewer" and "BigQuery User" on the `stackdriver-customer-telemetry` project from an administrator.

Then log into [Google Cloud console](https://console.cloud.google.com/). To view live logging click the hamburger icon and select Logging -> Log viewer from the side panel. In the far left dropdown menu below the filter select "Global" for the logs to view. This will show you all the logs from the last 30 days.

If you want to export the logs go to "BigQuery" under the hamburger icon then create a new query. This query exports everything for April 2019. 
```
SELECT severity, jsonPayload, timestamp
FROM `stackdriver-customer-telemetry.y.ide_201904*`
```
Then click 'Save Result' and download it in a format of your choosing. Trying to query datasets before 31st of March will result in an error due to the type of jsonPayload.machineId.
