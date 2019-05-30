# DAML WEB IDE docker
This is the dockerfile for creating and image with DAML SDK and the [code-server](https://github.com/codercom/code-server) IDE

### Building
Under the main daml directory 
```
docker build --rm -t digitalasset/daml-webide:latest web-ide/ide-server/
```

### Running
Currently, images are in a private DA repo `digitalasset/daml-webide`. However, you can build the image as mentioned above

* To run the IDE server (with ports enabled)
`docker run --rm -i -t -p 8443:8443 {IMAGE_ID}`

* To bash into the image without automatically running IDE server
`docker run --rm --user root -i -t -P {IMAGE_ID} /bin/bash`

To connect from local docker [simply open](http://localhost:8443)
