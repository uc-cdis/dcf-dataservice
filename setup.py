from __future__ import absolute_import
from __future__ import print_function

import setuptools

# NOTE: apache-beam sdk requires some packages to have specific versions
# More details here: https://cloud.google.com/dataflow/docs/concepts/sdk-worker-dependencies#python-3.7.12
REQUIRED_PACKAGES = [
    "python_dateutil==2.8.0",
    "requests==2.27.1",
    "boto3>=1.9.111<2.0.0",
    "retry<=0.9.2",
    "google-auth>=1.24.0",
    "google-cloud==0.34.0",
    "google-resumable-media==2.3.2",
    "google-cloud-storage==2.*",
    "httplib2==0.20.*",
    "pyparsing>=2.4.2",
    "apache-beam[gcp]==2.*",
    "urllib3==2.5.0",
    "setuptools==41.4.0",
]

PACKAGE_NAME = "scripts"
PACKAGE_VERSION = "0.0.1"
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description="required dependencies",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
