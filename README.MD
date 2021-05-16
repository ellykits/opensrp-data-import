
## Import Data Into OpenSRP

You can use this command line tool to import data into OpenSRP. The tool parses the provided data then makes a web
request to OpenSRP to the desired endpoint. The request interval and amount of data posted to opensrp can be configured
via the `application.properties` which you are required to pass as a command option.

This tool can read data from 2 sources:

1. OpenMRS (MySQL)
2. CSV File

### Supported data import

| Resource | Endpoint  |
|:------|:--- |
| Locations | /opensrp/rest/location/add?is_jurisdiction=true |
| Location Tags | /opensrp/rest/location-tag |
| Organizations | /opensrp/rest/organization/add  |
| Organization Locations  | /opensrp/rest/organization/assignLocationsAndPlans |
| Practitioners | /opensrp/rest/practitioner/add |
| Practitioner Roles | /opensrp/rest/practitionerRole/add |
| Keycloak Users | /auth/admin/realms/{realm}/users |


> NOTE: all opensrp endpoints MUST be set in the `application.properties` file. Conventionally the all start with `opensrp.rest` text

### Usage

1. Download or build the latest version of the `*-fat.jar` from the [Releases](https://github.com/ellykits/opensrp-data-import/releases) . When importing data (from OpenMRS, it is recommended to run the commands in the order described in the documentation.

2. Copy the content of the file `application_sample.properties` into your configs file. Replace the placeholders  with the correct values.

3. Execute the commands as desired.

> NOTE: Some actions are dependent on others for instance you cannot import teams from OpenMRS without adding the locations first.

### Importing data from OpenMRS

> Set  the value for `location.hierarchy` config with location tags from OpenMRS in the order they were added otherwise the locations will NOT be

#### Prerequisites
- Internet connection - This is required by the web client to communicate with OpenSRP
- Java 11 installed (code is compiled with Java 11)
- Access to OpenMRS database (Enable remote database access, You can create a user with remote database access and disable them after the process)
-  MySQL version 5.7+  (Currently the queries used on OpenMRS database use `json_object` and `json_array` data types that are only available in versions 5.7 and above

#### Command options

```text
--help or -h : List available command options

--configs-file or -c (required) : Name of the file with the configs

--import or -i (required) : The name of the resource to import (locations | organizations |organization_locations | keycloak_users | practitioners | practitioner_roles)

--skip-location-tags or -sT (optional) : Skip importing location tags

```

For example run the following command to list available options (`app.properties` is name of the file containing configurations for the app)

```shell script

java -jar opensrp-data-import-2.0.0-SNAPSHOT-fat.jar --configs-file app.properties  --help

```

### Import data from CSV file

#### Prerequisites
- Internet connection - This is required by the web client to communicate with OpenSRP
- Java 11 installed (code is compiled with Java 11)

#### Command options

```text
--help or -h : List available command options

--configs-file or -c (required) : Name of the file containing app configurations

--source-file or -s (required) : Name of file with locations data

--users-file or -u (required) : Name of file with user data

--assign-team (optional) : Indicate the location level for team assignment e.g "Health Facility"

--skip-location-tags or -sT (optional) : Skip importing location tags (just import locations)

--skip-locations or -sL (optional) : Skip importing locations (just import location tags)

--skip-user-group or -sG (optional) : Skip adding Keycloak assigning users to Provider group

```

***Example:**  Run the following command to import locations, migrate keycloak users, create team at the "Health Facility" and add the users to the created teams.*

```shell script

java -jar opensrp-data-import-2.0.0-SNAPSHOT-fat.jar --configs-file app.properties  --users-file users.csv --source-file locations.csv --assign-team "Health Facility"

```

### Configurations

| Config        | Explanation                                                                             |
| :----------------------|:--------------------------------------------------------------------------------- |
| openmrs.mysql.host | OpenMRS database host  |
| openmrs.mysql.port | OpenMRS database port |
| openmrs.mysql.user| OpenMRS database user |
| openmrs.mysql.password | OpenMRS database password|
| openmrs.mysql.database | OpenMRS database name|
| keycloak.client.id |  Keycloak client id|
| keycloak.client.secret | Keycloak client secret|
| keycloak.base.url | Keycloak host URL. MUST be in the format *{{keycloak-host}}/auth* |
| keycloak.realm | Keycloak Realm |
| keycloak.user.username | Username for Keycloak User|
| keycloak.user.password | Password for Keycloak User|
| keycloak.user.request.interval | Interval for making request to keycloak users |
| keycloak.rest.users.url | Get all Keycloak Users endpoint. Format *{{keycloak-host}}/auth/admin/realms/{{realm}}/users* |
| keycloak.rest.users.count.url | Endpoint for counting Keycloak Users. Format *{{keycloak-host}}/auth/admin/realms/{{realm}}/users/count* |
| keycloak.rest.groups.url | Endpoint for fetching Keycloak groups. Format *{{keycloak-host}}/auth/admin/realms/{{realm}}/groups* |
| opensrp.rest.location.url | OpenSRP endpoint for posting locations |
| opensrp.rest.location.tag.url |OpenSRP endpoint for posting location tags |
| opensrp.rest.organization.url | OpenSRP endpoint for posting organizations|
| opensrp.rest.organization.location.url | OpenSRP endpoint for mapping organizations to locations |
| opensrp.rest.practitioner.url | OpenSRP endpoint for posting practitioners |
| opensrp.rest.practitioner.role.url | OpenSRP endpoint for mapping practitioners to organizations|
| location.hierarchy | Comma separated string of location levels with their corresponding geographical level. Top level starts from 0. *e.g* *Country:0,Region:1,County:2, Sub-county:3, District:4, Health Facility: 5 * |
| data.limit | Number of records to send per request. Default `50`|
| request.interval | Set time interval in milliseconds between each request. Default `10000` |
| request.timeout | Sets the timeout in milliseconds. If an action is not completed before this timeout, the action is considered as a failure default `10000`|
| reset.timeout |  Sets the time in ms before it attempts to re-close the circuit (by going to the half-open state). If the circuit is closed when the timeout is reached, nothing happens. `-1` disables this feature. Default `10000` |

### Building

For local development, create a new file `conf/application.properties` and copy content from `application_sample.properties` file.

To launch your tests:
```
./gradlew clean test
```

To package your application (Will build a fat/uber jar):
```
./gradlew clean assembleShadowDist
```

To watch for files changes (Requires you to run the application in a separate terminal):
```
./gradlew -t installDist
```

To run your application:
```
./gradlew clean run
```

### Technologies
- The service implemented with [Vert.x](https://vertx.io/docs/)

- Command line tool built with [Clikt](https://ajalt.github.io/clikt/quickstart/) *Pronounced  "clicked"*

### Help
This application generated using http://start.vertx.io. Visit the following links for information.
* [Vert.x Documentation](https://vertx.io/docs/)
* [Vert.x Stack Overflow](https://stackoverflow.com/questions/tagged/vert.x?sort=newest&pageSize=15)
* [Vert.x User Group](https://groups.google.com/forum/?fromgroups#!forum/vertx)
* [Vert.x Gitter](https://gitter.im/eclipse-vertx/vertx-users)
