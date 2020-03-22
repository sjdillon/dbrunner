RUNNER_CONFIG={
    "flyway_dir":"flyway-5.0.7",
    "working_dir":"/tmp/dbrunner/",
    "sql_migrations_dir":'/{working_dir}/{flyway_dir}/sql',
    "local_migration_tar_format":"{working_dir}/{migrations_filename}",
    "pw_key_format":"{}-{}-{}-mariadb-master-cred",
    "endpoint_key_format":"{}-{}-{}-{}-mariadb-endpoint",
    "port":"3306",
    "url_format" :"jdbc:mariadb://{endpoint}:{port}",
    "user":"culdee"
    }