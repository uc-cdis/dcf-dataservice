ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

ENV appname=dcfdataservice
WORKDIR /${appname}

FROM base AS builder
USER root

RUN pip install --upgrade pip setuptools wheel poetry

COPY poetry.lock pyproject.toml /${appname}/

RUN poetry config virtualenvs.create false \
    && poetry install -vv --no-root --without dev --no-interaction \
    && poetry show -v

COPY . /${appname}

FROM base
ENV PATH="/${appname}/.venv/bin:$PATH"
USER root

RUN yum install -y git jq bash groff python3 tar \
    && pip install awscli

# Installing gcloud package (includes gsutil)
RUN curl -fL https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz -o /tmp/google-cloud-sdk.tar.gz \
    && mkdir -p /usr/local/gcloud \
    && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
    && /usr/local/gcloud/google-cloud-sdk/install.sh \
    && rm /tmp/google-cloud-sdk.tar.gz

ENV PATH=$PATH:/usr/local/gcloud/google-cloud-sdk/bin

COPY --from=builder /$appname /$appname

WORKDIR /${appname}

RUN poetry config virtualenvs.create false \
    && poetry install -vv --no-root --no-dev --no-interaction \
    && poetry show -v

ENTRYPOINT ["/bin/bash"]
