FROM quay.io/cdis/python-nginx:pybase3-1.0.0

RUN apk add --update && apk add git jq curl bash vim

RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install --upgrade setuptools

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
