from __future__ import absolute_import
from __future__ import print_function

import setuptools

REQUIRED_PACKAGES = [
    "python_dateutil>=2.8.0<3.0.0",
    "requests>=2.20.0<3.0.0",
    "boto3>=1.9.111<2.0.0",
    "retry<=0.9.2",
    "google-auth>=1.4.1<2.0.0",
    "google-cloud>=0.34.0<1.0.0",
    "google-resumable-media>=0.3.1<1.0.0",
    "google-cloud-storage>=1.6.0<2.0.0",
    "httplib2<1.0.0",
    "pyparsing>-2.4.7<3.0.0" "apache-beam[gcp]>=2.16.0<3.0.0",
    "setuptools>=40.3.0<41.0.0",
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
