## Import OpenMRS Data Into OpenSRP

This service allows you to import data into OpenSRP from OpenMRS. The service can be configured to perform
the following operations:

- Import OpenMRS location tags into OpenSRP
- Import OpenMRS locations into OpenSRP
- Import OpenMRS Teams (aka organization in OpenSRP) into OpenSRP
- Import OpenMRS Users into Keycloak and add them to correct User Group
- Map Keycloak Users to OpenSRP Practitioners
- Map OpenSRP Locations to Organizations(Teams in OpenMRS)
- Map OpenSRP Practitioners (presented as Person in OpenMRS) to Organizations

### Prerequisites
- Java 11 installed
- Access to OpenMRS database (Enable remote database access, when using the tool from an external server. You can disable
 remote database access afterwards)
- Internet connection - This is required by the web client to communicate with OpenSRP
- MySQL version 5.7+
### Usage

1. Download or build the latest version of the `*-fat.jar`. When importing data to a new database it is recommended to run the commands in the order
described in the documentation.

2. Copy the content of the file `application_sample.properties` into your configs file. Replace the placeholders
with the correct values.

3. Execute the commands independently as desired. Just note that for new databases, some commands are dependent for
instance you cannot assign locations to nonexistent teams. You have to import the teams (organizations in OpenSRP) first.

Command options

```text
--configs-file - Name of the file with the configs
--import - The name of the resource to import (locations|organizations|organization_locations|keycloak_users|practitioners|practitioner_roles)
```

List available options (name of configs file `app.properties`)

```shell script
java -jar opensrp-data-import-1.0.4-SNAPSHOT-fat.jar --configs-file app.properties  --help
```

### Configurations

| Config        | Explanation                                                                             |
| ----------------------|:--------------------------------------------------------------------------------- |
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
| keycloak.rest.users.url | Get all Keycloak Users endpoint. Format *{{keycloak-host}}/auth/admin/realms/{{realm}}/users*  |
| keycloak.rest.users.count.url | Endpoint for counting Keycloak Users. Format *{{keycloak-host}}/auth/admin/realms/{{realm}}/users/count* |
| keycloak.rest.groups.url | Endpoint for fetching Keycloak groups. Format *{{keycloak-host}}/auth/admin/realms/{{realm}}/groups*  |
| opensrp.rest.location.url | OpenSRP endpoint for posting locations |
| opensrp.rest.location.tag.url |OpenSRP endpoint for posting location tags |
| opensrp.rest.organization.url | OpenSRP endpoint for posting organizations|
| opensrp.rest.organization.location.url | OpenSRP endpoint for mapping organizations to locations |
| opensrp.rest.practitioner.url | OpenSRP endpoint for posting practitioners |
| opensrp.rest.practitioner.role.url | OpenSRP endpoint for mapping practitioners to organizations|
| location.hierarchy | Comma separated ordered string of location level. *e.g* *Contry,Region,County* |
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
