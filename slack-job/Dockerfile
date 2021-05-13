#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM python:3.8-slim-buster

ARG uid=1000
ARG gid=1000
ARG group=job-group
ARG username=job-user

USER root

RUN groupadd -g ${gid} ${group} \
    && useradd -u ${uid} -g ${gid} -m ${username}

USER ${username}

COPY requirements.txt logging.conf slack_job.py /home/${username}/

RUN pip install -r ~/requirements.txt

ENV LOGGER_CONFIG_FILE_PATH=/home/${username}/logging.conf

WORKDIR /home/${username}

# ENTRYPOINT ["python3", "./slack_job.py"]