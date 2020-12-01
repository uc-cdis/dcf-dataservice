FROM quay.io/cdis/python:3.7-slim-buster

RUN apt update && apt install -y git jq curl bash snapd groff python3-pip zip

RUN curl -O https://bootstrap.pypa.io/get-pip.py

RUN python3 get-pip.py && pip install -I pip<20.3

RUN pip3 install awscli

# Installing gcloud package (includes gsutil)
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin


COPY . /dcf-dataservice
WORKDIR /dcf-dataservice

RUN  pip3 install -r requirements.txt

CMD /bin/bash
