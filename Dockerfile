# To run: docker run -d -v /path/to/local_settings.py:/var/www/fence/local_settings.py --name=fence -p 80:80 fence
# To check running container: docker exec -it fence /bin/bash

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
    wget

COPY . /dcf-dataservice
WORKDIR /dcf-dataservice

RUN  easy_install -U pip \
    && pip install --upgrade pip \
    && pip install --upgrade setuptools \
    && pip install awscli --upgradei \
    && pip install -r requirments.txt

RUN 
    apt-get update && \
    apt-get install -y google-cloud-sdk \
    google-cloud-sdk-app-engine-python \ 
    gcloud --version 

CMD /bin/bash

