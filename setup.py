from __future__ import absolute_import
from __future__ import print_function

import setuptools

REQUIRED_PACKAGES = [
    "python_dateutil==2.8.0",
    "requests>=2.18.0<3.0.0",
    "boto3>=1.9.111",
    "google-cloud==0.34.0",
    "google-resumable-media==0.3.1",
    "google-cloud-storage==1.6.0",
    "apache-beam[gcp]==2.16.0",
    "setuptools==40.3.0",
    "gen3>=2.3.1",
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
