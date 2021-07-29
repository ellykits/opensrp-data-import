
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
| Location Tags | /opensrp/rest/location-tag |
| Locations | /opensrp/rest/location/add?is_jurisdiction=true |
| Organizations | /opensrp/rest/organization/add  |
| Organization Locations  | /opensrp/rest/organization/assignLocationsAndPlans |
| Practitioners | /opensrp/rest/practitioner/add |
| Practitioner Roles | /opensrp/rest/practitionerRole/add |
| Keycloak Users | /auth/admin/realms/{realm}/users |


> NOTE: all opensrp endpoints MUST be set in the `application.properties` file. Conventionally the all start with `opensrp.rest` text


### Usage

1. Download the latest version of the `*-fat.jar` from [Releases](https://github.com/ellykits/opensrp-data-import/releases). Alternatively you can build the project by running the command `./gradlew shadowJar` then locate the uber jar inside `build/lib` directory.

2. Copy the content of the file `application_sample.properties` into your configs file. Replace the placeholders  with the correct values. It is okay to retain the defaults like `request.limit` config but you can alter the figures like required. (Optimal number of batch record that can be processed by OpenSRP in one request is 60 - this is of course dependent on the server specs)

3. Execute the commands as desired. When importing data from OpenMRS it is adviced to follow the order defined in the documentation.

> NOTE: Some actions are dependent on others for instance you cannot import teams from OpenMRS without adding the locations first.


### Importing data from OpenMRS

Set  the value for `location.hierarchy` config with location tags from OpenMRS in the order they were added otherwise the locations will NOT be tagged properly

#### Prerequisites
- Internet connection - The web client needs this to communicate to OpenSRP
- Java 11 installed (code is compiled with Java 11)
- Access to OpenMRS database (Enable remote database access, you can create a user with remote database access and disable them after the process)
-  MySQL version 5.7+  (Currently the queries used on OpenMRS database use `json_object` and `json_array` data types that are only available in versions 5.7 and above
-  Keycloak users MUST have roles to view/edit other keycloak users (This is a composite Role that can be added to other existing user roles)

#### Command options

```text
--help or -h : List available command options

--configs-file or -c (required) : Name of the file with the configs

--import or -i (required) : The name of the resource to import (locations | organizations |organization_locations | keycloak_users | practitioners | practitioner_roles)

--skip-location-tags or -sT (optional) : Skip importing location tags

--overwrite (optional) or -oW: Override the existing locations whose IDs are provided

```

For example run the following command to list available options (`app.properties` is name of the file containing configurations for the app)

```shell script

java -jar opensrp-data-import-2.0.4-SNAPSHOT-fat.jar --configs-file app.properties  --help

```

### Import data from CSV file

You can find sample google sheet that you can download as CSV here [Locations and Users](https://docs.google.com/spreadsheets/d/1QQwfOy-Tp3I9_eXEpTyVV1CLo4SrTAPl_p2cfSm0sDc/edit?usp=sharing)

>NOTE: Sample CSV is READ ONLY you can save a copy and edit as desired.

#### Prerequisites
- Internet connection - The web client needs this to communicate to OpenSRP
- Keycloak users MUST have roles to view/edit other keycloak users (This is a composite Role that can be added to other existing user roles)
- Java 11 installed (code is compiled with Java 11)

#### Command options

```text
--help or -h : List available command options

--configs-file or -c (required) : Name of the file containing app configurations

--import or -i (required) : The name of the resource to import (The only accepted value is 'locations')

--source-file or -s (required) : Name of file with locations data

--users-file or -u (required) : Name of file with user data

--assign-team (optional) : Indicate the location level for team assignment e.g "Health Facility"

--skip-location-tags or -sT (optional) : Skip importing location tags (just import locations)

--skip-locations or -sL (optional) : Skip importing locations (just import location tags)

--skip-user-group or -sG (optional) : Skip adding Keycloak assigning users to Provider group

--overwrite (optional) or -oW: Override the existing locations whose IDs are provided

```

***Example:**  Run the following command to import locations, migrate keycloak users, create team at the "Health Facility" and add the users to the created teams.*

```shell script

java -jar opensrp-data-import-2.0.4-SNAPSHOT-fat.jar --configs-file app.properties  --users-file users.csv --source-file locations.csv --assign-team "Health Facility" --import locations

```

#### Source file CSV file format
The locations source is expected to be in this format. Assuming your location hierarchy begins from the Country (level 0) down to Region (level 1), County (level 2) and finally Health Facility level (3), you need to provide a CSV file with the following format:

```csv
Country Id,Country,Region Id,Region,County Id,County,Health Facility Id,Health Facility
,Kenya,,Nairobi,,Nairob,,Nairobi Hospital
```

The ID columns are mandatory as they are used to map locations to their parents. EMPTY ID column means new location, so the system will generate a UUID for the specified location. Existing locations MUST have their IDs pre-populated to avoid duplications. Run the locations with EMPTY ID column once; you can retrieve the generated location ids from this file `/home/<your_username>/opensrp-data/locations.csv` for future use.

Also NOTE that the columns should be ordered according to the location hierarchy. It is logical to group all the locations belonging to one level.

The tool will validate the csv columns and fail if the correct format is not provided. The only thing that is assumed is you have ordered them corectly.

#### Users file CSV file format
The CSV tempalte for importing users to Keycloak and adding as practitioners in OpenSRP is as follows:

```csv
Parent Location,Location,First Name,Last Name,Username,Password
```

The names of the column headers is not important as the CSV reader uses the position of the column to read the file, the content however MUST be in that order.
The **Parent Location** is necessary because you can have locations sharing a name at different level. Also not that the names MUST be exactly what was provided in the source file.


#### Accessing Generated CSV files

This tool will generate CSV files with the content of the data that has been posted to OpenSRP. These file can be found inside a directory called **`opensrp-data`** in your home director. e.g /home/coder/opensrp-data. The directory is cleared everytime you run the service so Make sure you copy the content of that folder to a different place if you want to retain the data. The files will be named after the resource that has been posted, i.e. `location.csv`, `organizations.csv`,`organzation_locations.csv`, `practitioners.csv` and `practitioner_roles.csv`.

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
| single.request.interval | Interval between requests submitted one at a time |
| keycloak.request.delay | Delay in time in milliseconds before keycloak makes another batch request. Default is 1 minute |
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
| circuit.breaker.max.retries | Maximum of retries to make when connection is lost. Default `10`|
| request.interval | Set time interval in milliseconds between each request. Default `10000` |
| request.timeout | Sets the timeout in milliseconds. If an action is not completed before this timeout, the action is considered as a failure default `-1` (no timeout)|
| reset.timeout |  Sets the time in ms before it attempts to re-close the circuit (by going to the half-open state). If the circuit is closed when the timeout is reached, nothing happens. `-1` disables this feature. Default `10000` |


### Building Project

For local development, create a new file `conf/application.properties` and copy content from `application_sample.properties` file. The project as a file named `Debug.kt` that mimics the `Main.kt` file that you can use to run and debug the app, update the JSON object used to pass configurations to the app, which would be something similar to passing `args` to the command line application. Just be careful not to debug the app with production configurations.


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
