# DAML WEB IDE docker
This is the dockerfile for creating and image with DAML SDK and the [code-server](https://github.com/codercom/code-server) IDE

### Building
Under the main daml directory `docker build --rm -t digitalasset/daml-webide:0.11.19-master web-ide/ide-server/`
the tagged version should match what is configured in `web-ide/proxy/config.json` if you want to run locally.

### Running
We haven't uploaded to github yet, so you must first create an image (mentioned above).

* To run the IDE server (with ports enabled)
`docker run --rm -i -t -P {IMAGE_ID}`

* To bash into the image without automatically running IDE server
`docker run --rm --user root -i -t -P {IMAGE_ID} /bin/bash`

To connect from local docker [simply open](http://localhost:8443)
