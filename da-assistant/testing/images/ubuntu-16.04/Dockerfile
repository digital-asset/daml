FROM ubuntu:16.04

RUN \
	apt-get update && \
	apt-get install -qy \
		bsdmainutils \
		python3 \
		python-cram \
		patch \
		ca-certificates \
		netbase \
		iputils-ping \
		curl \
		software-properties-common \
        openjdk-8-jdk-headless && \
	apt-get clean
RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -qy python3.6
RUN rm -rf /usr/bin/python3  && ln -s /usr/bin/python3.6 /usr/bin/python3
RUN echo "nameserver 8.8.8.8" > /etc/resolv.conf

# Create dummy xdg-open
RUN ln -s /bin/true /usr/bin/xdg-open

# Allow write access to /usr/local/bin
RUN chmod +w /usr/local/bin
