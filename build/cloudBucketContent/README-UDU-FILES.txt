bucketAndBiqQueryServiceAcct.json: Our credentials that we have the user install to allow us access to their bucket and biqQuery dataset.
pubSubServiceAcct.json: Credentials that allow us to access Pub/Sub. Google bug requires us to provide this to use pubsub. Also need this service account to allow writing to logs so webserver logging can occur. Specific permissions: Logs Writer, Pub/Sub Editor.
sqlServiceAcct.json: Need to provide this to SQL proxy so we can get to SQL. Specific permissions: Cloud SQL Client.
flask-server.crt: UDU Flask server runs HTTPS; this supports that.
flask-server.key: UDU Flask server runs HTTPS; this supports that.
udu_secrets.txt: key-value pairs that hold secrets (e.g. username and password to be able to issue requests to UDU Flask server, or connect to database)
udu_config.txt: non-sensitive key-value pairs to configure UDU Flask server and UDU code
MySQLReleasePubKey.txt: PGP Public key for MySQL Release Engineering. Using local copy due to unreliable PGP server access.
startupConfig.sh: Environment variable definitions that are sourced by startup scripts. Currently holds name of SQL server instance for proxy to connect to.