FROM quay.io/cdis/python-nginx:pybase3-1.0.0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y \
      apt-utils \
      apt-transport-https \
      lsb-release \
      curl \
      dnsutils \
      gcc \
      git \
      openssh-client \
      python-setuptools \
      vim \
      less \
      jq \
      ssh \
      ftp \
      wget

RUN  easy_install -U pip \
    && pip install --upgrade pip \
    && pip install --upgrade setuptools \
    && pip install -U crcmod \
    && pip install awscli --upgrade \
    && pip install yq --upgrade

RUN export CLOUD_SDK_REPO="cloud-sdk-$(lsb_release -c -s)" && \
    echo "deb https://packages.cloud.google.com/apt $CLOUD_SDK_REPO main" > /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk \
    gcloud config set core/disable_usage_reporting true && \
    gcloud --version 
 
 COPY . /dcf-dataservice
 WORKDIR /dcf-dataservice

 RUN  pip install -r requirements.txt

 CMD /bin/bash
