# DAML quickstart-java in a docker container

To build this docker image, run
```
docker build --tag=quickstart .
```
This will download and install the newest version of the DAML SDK and set up
the `quickstart-java` project.

To execute the resulting image, run
```
docker run --interactive --tty --rm quickstart
```
This will run `da start` in the `quickstart-java` project. Point your browser
to http://localhost:7500 to use Navigator. Press `Ctrl-C` to stop the
execution.

To experiment with the `quickstart-java` project, run
```
docker run --interactive --tty --rm quickstart /bin/bash --login
```
This will drop you in a shell in the container. The project is located in the
`quickstart` directory.
