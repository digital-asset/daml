FROM ubuntu:18.04

RUN \
	apt-get update && \
	apt-get install -qy \
		bsdmainutils \
		python3 \
        python3-distutils \
        python-cram \
		patch \
		ca-certificates \
		netbase \
		iputils-ping \
		curl \
		openjdk-8-jdk-headless && \
	apt-get clean
RUN echo "nameserver 8.8.8.8" > /etc/resolv.conf

# Create dummy xdg-open
RUN ln -s /bin/true /usr/bin/xdg-open

# Allow write access to /usr/local/bin
RUN chmod +w /usr/local/bin
