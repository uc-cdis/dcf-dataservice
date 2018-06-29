from __future__ import absolute_import
from __future__ import print_function

import setuptools

REQUIRED_PACKAGES = [
        'python_dateutil==2.6.1',
        'requests>=2.18.0<3.0.0',
        'boto3==1.7.28',
        'google-cloud==0.33.1',
        'google-resumable-media==0.3.1',
        'google-cloud-storage==1.6.0',
        'setuptools==36.6.0'
        ]

PACKAGE_NAME = 'scripts'
PACKAGE_VERSION = '0.0.1'
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='required dependencies',
    install_requires=REQUIRED_PACKAGES,
    #dependency_links = [
    # "https://github.com/uc-cdis/indexclient@1.5.4#egg=indexclient",
    #],
    #include_package_data=True,
    packages=setuptools.find_packages()
)
