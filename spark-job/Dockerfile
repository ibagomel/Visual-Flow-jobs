#
# Copyright (c) 2021 IBA Group, a.s. All rights reserved.
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
FROM ghcr.io/ibagomel/spark:spark-3.1.1-bin-hadoop3.2

ARG uid=1000
ARG gid=1000
ARG group=job-group
ARG username=job-user

USER root

RUN yum update -y \
    && yum install -y shadow-utils procps-ng \
    && /usr/sbin/groupadd -g ${gid} ${group} \
    && /usr/sbin/useradd -u ${uid} -g ${gid} -m ${username}

COPY target/spark-transformations-0.1-jar-with-dependencies.jar /opt/spark/work-dir/
COPY entrypoint.sh /opt/spark/work-dir/
RUN chmod -R 755 /opt/spark/work-dir

USER ${username}

CMD ["/usr/bin/bash", "/opt/spark/work-dir/entrypoint.sh"]
