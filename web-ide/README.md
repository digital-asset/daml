# DAML Web IDE

An online Web IDE for developing DAML, hosted at [webide.daml.com](https://webide.daml.com/). It is a VS Code server with the DAML SDK installed. The main components are:

* daml web ide docker image: this hosts a [web ide code-server](https://github.com/codercom/code-server) bundled with the sdk
* proxy server and image: this manages multiple daml web ide docker containers and proxies requests into them

Further details can be found in `proxy/README.md` and `ide-server/README.md`

## Google Analytics

User statistics are only available to Digital Asset employees. Once you have requested access, you can:

* Visit [Google Analytics](https://analytics.google.com/).
* On the left, select `Audience`, `Overview`.
* On the bottom right (you may need to scroll) select `view full report`.
* Select `Secondary Dimension` as `Page`.
* In the search area hit `advanced`, then include `Page` containing `/webide` and then `Apply`.

That gives you a graph of users per day. It is interesting to see session duration as well.

## GCP and Monitoring
This functionality is only available to Digital Asset employees.

For information on the gcp environment setup: https://digitalasset.atlassian.net/wiki/spaces/IS/pages/866779203/WebIDE+code+server

For healthcheck alerts and monitoring: https://app.google.stackdriver.com/?project=da-gcp-web-ide-project
* currently set up to email IT and slack #team-daml-language
* monitors the webide url and alerts if the server is unresponsive
* monitors the cpu usage of the webide instance group and alerts if over 79% utilization.
