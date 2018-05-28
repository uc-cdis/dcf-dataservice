FROM ubuntu:16.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    gcc \
    git \
    openssh-client \
    python2.7 \
    python-dev \
    python-pip \
    python-setuptools \     
    vim \
    less \
    ssh \
    wget \
    boto3
    
 COPY . /dcf-dataservice
 WORKDIR /dcf-dataservice
