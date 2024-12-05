FROM quay.io/cdis/python:python3.9-buster-2.0.0

RUN apt-get update && apt-get install -y git jq curl bash snapd groff python3-pip zip

RUN pip install --upgrade pip
RUN pip install awscli

# Installing gcloud package (includes gsutil)
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin


COPY . /dcf-dataservice
WORKDIR /dcf-dataservice

RUN pip install -r requirements.txt

CMD /bin/bash
