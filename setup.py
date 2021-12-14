from __future__ import absolute_import
from __future__ import print_function

import setuptools

REQUIRED_PACKAGES = [
    "python_dateutil==2.8.2",
    "requests>=2.26.0<3.0.0",
    "boto3==1.20.24",
    "google-cloud==0.34.0",
    "google-resumable-media==2.1.0",
    "google-cloud-storage==1.43.0",
    "apache-beam[gcp]==2.34.0",
    "setuptools==59.6.0",
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
