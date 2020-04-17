# Copyright (C) 2017 Beijing Didi Infinity Technology and Development Co.,Ltd.
# All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

from dlflow import VERSION

import setuptools
import os


NAME = "dlflow"
GIT_USER_NAME = "didi"
REPO_NAME = os.path.basename(os.getcwd())
URL = "https://github.com/{}/{}".format(GIT_USER_NAME, REPO_NAME)
DOWNLOAD_URL = ""

AUTHOR = "Profile"
AUTHOR_EMAIL = "profile@didiglobal.com"
MAINTAINER = "Qiwei Dou"
MAINTAINER_EMAIL = "douqiwei@didiglobal.com"

data_pkg = []
for _d, _, _f in os.walk(os.path.join(NAME, "resources")):
    if "msg.mo" in _f:
        _pkg = _d.split(os.sep)[1:]
        _pkg.append("msg.mo")
        data_pkg.append(os.sep.join(_pkg))
PACKAGE_DATA = {"dlflow": data_pkg}

PACKAGE = setuptools.find_packages()
LICENSE = "Apache Software License"

PLATFORMS = ["MacOS", "Unix"]
CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Natural Language :: English",
    "Operating System :: MacOS",
    "Operating System :: POSIX :: Linux",
    "Programming Language :: Python :: 3.6"
]

try:
    f = open("requirements.txt", "r")
    REQUIRES = [i.strip() for i in f.read().split("\n")]
except:
    print("'requirements.txt' not found!")
    REQUIRES = list()

SHORT_DESCRIPTION = ""
LONG_DESCRIPTION = ""


setuptools.setup(
    name=NAME,
    description=SHORT_DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    maintainer=MAINTAINER,
    maintainer_email=MAINTAINER_EMAIL,
    packages=PACKAGE,
    package_data=PACKAGE_DATA,
    include_package_data=True,
    entry_points={"console_scripts": ["dlflow = dlflow.main:run_app"]},
    url=URL,
    download_url=DOWNLOAD_URL,
    platforms=PLATFORMS,
    classifiers=CLASSIFIERS,
    license=LICENSE,
    python_requires=">=3.6",

    install_requires=REQUIRES,
)
