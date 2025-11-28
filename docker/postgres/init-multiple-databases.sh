set -e

echo "Running init-multiple-databases.sh..."

create_user_if_not_exists() {
  local db_user="$1"
  local db_pass="$2"

  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    DO
    \$do$
    BEGIN
      IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles WHERE rolname = '${db_user}'
      ) THEN
        CREATE ROLE ${db_user} LOGIN PASSWORD '${db_pass}';
      END IF;
    END
    \$do$;
EOSQL
}

create_db_if_not_exists() {
  local db_name="$1"
  local db_owner="$2"

  # check if DB exists; if not, create it
  DB_EXISTS=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='${db_name}'" || echo "0")

  if [ "$DB_EXISTS" != "1" ]; then
    echo "Creating database ${db_name} with owner ${db_owner}..."
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -c "CREATE DATABASE ${db_name} OWNER ${db_owner} TEMPLATE template0 ENCODING 'UTF8';"
  else
    echo "Database ${db_name} already exists, skipping."
  fi
}

# 1) Metadata DB + user
create_user_if_not_exists "${METADATA_DATABASE_USERNAME}" "${METADATA_DATABASE_PASSWORD}"
create_db_if_not_exists "${METADATA_DATABASE_NAME}" "${METADATA_DATABASE_USERNAME}"

# 2) ELT DB + user
create_user_if_not_exists "${ELT_DATABASE_USERNAME}" "${ELT_DATABASE_PASSWORD}"
create_db_if_not_exists "${ELT_DATABASE_NAME}" "${ELT_DATABASE_USERNAME}"

# 3) Celery backend DB + user
create_user_if_not_exists "${CELERY_BACKEND_USERNAME}" "${CELERY_BACKEND_PASSWORD}"
create_db_if_not_exists "${CELERY_BACKEND_NAME}" "${CELERY_BACKEND_USERNAME}"

echo "init-multiple-databases.sh completed."
