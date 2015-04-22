LetMeIn Sync Server
===================

Download and install the Go App Engine SDK:

* https://cloud.google.com/appengine/downloads

For the remaining steps, I will assume that you have it in your
path, and you have Go set up for normal development.

Download this package:

    goapp get github.com/russross/letmeinserver

Customize it. Create a new App Engine project and note the ID that
you use:

* https://appengine.google.com/

Copy that project ID into app.yaml on the line beginning with
application: (replace "letmein-app" with your ID).

Create a web client OAuth ID for your project according to the
Endpoints tutorail instructions. Create another one for your Android
client. Put these both in a file called `config.json` like this:

``` javascript
{
    "OAuthClientID": "your-client-id-here.apps.googleusercontent.com",
    "OAuthAndroidID": "your-android-id-here.apps.googleusercontent.com"
}
```

Deploy your app:

    goapp deploy

You may be prompted for your Google username and password.

Next, create the Android API stubbs. First generate the discovery
API document:

    curl -s https://your-app-id.appspot.com/_ah/api/discovery/v1/apis/letmein/v1/rest > letmein.rest.discovery

Inspect it to make sure it looks okay (it should have information
about the API calls). Then use it to generate the Android API
classes:

    endpointscfg.py gen_client_lib java -bs gradle letmein.rest.discovery

This will create a file called `letmein.rest.zip` with the Endpoints
API classes. Add this to your Android project according to the
Endpoints Android tutorial and use it to connect your app to your
server.
