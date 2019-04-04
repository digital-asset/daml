FROM centos:7

RUN \
	yum install -y \
		java-1.8.0-openjdk-devel \
		patch \
                libm.so.6

RUN yum install -y https://centos7.iuscommunity.org/ius-release.rpm
RUN yum update -y
RUN yum install -y python36u python36u-libs python36u-devel python36u-pip
RUN ln -s /usr/bin/python3.6 /usr/bin/python3

RUN curl -s https://bootstrap.pypa.io/get-pip.py | python -
RUN pip install cram

RUN echo "nameserver 8.8.8.8" > /etc/resolv.conf

# Create dummy xdg-open
RUN ln -s /bin/true /usr/bin/xdg-open

# Allow write access to /usr/local/bin
RUN chmod +w /usr/local/bin
