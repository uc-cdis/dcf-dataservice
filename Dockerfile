FROM quay.io/cdis/python-nginx:pybase3-3.0.2

ENV appname=dcf-dataservice

RUN pip install --upgrade pip

RUN apk add --update \
  curl bash git jq groff zip \
  linux-headers musl-dev gcc g++ \
  build-base make cmake

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain=1.51

# Install Poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python

WORKDIR /$appname

# copy ONLY poetry artifact and install
# this will make sure than the dependencies is cached
COPY poetry.lock pyproject.toml /$appname/
RUN source $HOME/.poetry/env \
    && poetry config virtualenvs.create false \
    && poetry install -vv --no-root --no-dev --no-interaction \
    && poetry show -v

RUN pip install awscli

# Installing gcloud package (includes gsutil)
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin


COPY . /dcf-dataservice
WORKDIR /dcf-dataservice

# RUN  pip3 install -r requirements.txt

CMD /bin/bash
