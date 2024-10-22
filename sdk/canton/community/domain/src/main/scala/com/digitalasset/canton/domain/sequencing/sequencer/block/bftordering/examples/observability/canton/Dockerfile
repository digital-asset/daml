FROM azul/zulu-openjdk-debian:17

# Clean up remaining python3 installs from the base image
RUN dpkg --purge python3 python3.9-minimal libpython3.9-minimal

# Install screen for running the console in a headless server, grpcurl and jq to perform gRPC healthchecks with Docker Compose
RUN export DEBIAN_FRONTEND=noninteractive \
 && apt-get update \
 && apt-get install --no-install-recommends -y screen curl jq \
 && curl -fsSLO https://github.com/fullstorydev/grpcurl/releases/download/v1.8.7/grpcurl_1.8.7_linux_x86_64.tar.gz \
 && curl -fsSLO https://github.com/fullstorydev/grpcurl/releases/download/v1.8.7/grpcurl_1.8.7_checksums.txt \
 && sha256sum --check --ignore-missing grpcurl_1.8.7_checksums.txt \
 && tar -xvf grpcurl_1.8.7_linux_x86_64.tar.gz -C /usr/local/bin --totals grpcurl \
 && rm grpcurl_1.8.7*

# Install grpc_health_probe (verifying checksums)
RUN GHP_VERSION="0.4.15" && \
    GHP_ARCH=$(uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/') && \
    GHP_EXE="/usr/local/bin/grpc_health_probe" && \
    curl -sSL "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v${GHP_VERSION}/grpc_health_probe-linux-${GHP_ARCH}" > ${GHP_EXE} && \
    sha256sum ${GHP_EXE} | grep -E "^(8194141f51f372d8f0668ebe7b3fff7bde519095f52e3ab326d12941035413da|ae81e4cdecd260953cb75c869635166176f86972e4204a5ed7933a7e7a2e4baf)" && \
    chmod +x ${GHP_EXE}

# Clean up
RUN apt-get clean && rm -rf /var/cache/apt/archives

# create and switch to a working directory
WORKDIR /canton

# only copy fat jar and simple topology configuration
COPY bin/canton bin/canton
COPY lib/canton-*.jar lib/
COPY examples/01-simple-topology/simple-topology.conf .

# pre-compile ammonite's dynamic scala by launching the REPL with no stdin
RUN echo "Precompiling canton console. Please ignore the following output 🙈" \
  && bin/canton --config simple-topology.conf --no-tty < /dev/null && rm -rf log

# point entrypoint to the canton executable
ENTRYPOINT [ "bin/canton" ]
