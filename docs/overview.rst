.. ===============LICENSE_START=======================================================
.. Acumos CC-BY-4.0
.. ===================================================================================
.. Copyright (C) 2018 AT&T Intellectual Property. All rights reserved.
.. ===================================================================================
.. This Acumos documentation file is distributed by AT&T
.. under the Creative Commons Attribution 4.0 International License (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at
..
.. http://creativecommons.org/licenses/by/4.0
..
.. This file is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.
.. ===============LICENSE_END=========================================================

============================
Dataset Service Overview
============================

The Acumos Dataset Service provides ways to store the dataset metadata plus to store the connection detail about external datasource like hadoop/csv/database etc
The server component is a Spring-Boot application that provides REST service to callers.
The client component is a Java library that provides business objects (models) and
methods to simplify the use of the REST service.

The source is available from the Linux Foundation Gerrit server:

    https://gerrit.acumos.org/r/gitweb?p=databroker/dataset.git;a=summary

The CI/CD jobs are in the Linux Foundation Jenkins server:

    https://jenkins.acumos.org/view/databroker-dataset/

Issues are tracked in the Linux Foundation Jira server:

    https://jira.acumos.org/secure/Dashboard.jspa

Further information is available from the Linux Foundation Wiki:

    https://wiki.acumos.org/

