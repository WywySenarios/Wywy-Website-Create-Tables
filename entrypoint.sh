#!/bin/sh
# Liquibase runner entrypoint.
#
#   migrate (default)  run liquibase update once per database
#   seed               upsert the admin user into info.users
#
# All connection args are passed on the CLI; there is intentionally no
# liquibase.properties file.

case "${1:-migrate}" in
migrate)
	for db in wywywebsite info; do
		url="jdbc:postgresql://${DATABASE_HOST}:${DATABASE_PORT}/${db}"
		# --search-path is required: Liquibase 4.29.x normalizes an absolute
		# --changeLogFile by stripping the leading '/' and resolving it against
		# the search path, so "/changelog/${db}.sql" can never be found. Pass
		# the directory as the search path and the file as a relative path.
		liquibase --url="$url" \
			--username="${DATABASE_MIGRATOR_USERNAME}" --password="${DATABASE_MIGRATOR_PASSWORD}" \
			--search-path=/changelog \
			--changeLogFile="${db}.sql" \
			--defaultSchemaName=public update
		rc=$?
		if [ $rc -ne 0 ]; then
			# releaseLocks must receive the SAME connection args, or it
			# targets the wrong DB and can't clear the lock.
			liquibase --url="$url" \
				--username="${DATABASE_MIGRATOR_USERNAME}" --password="${DATABASE_MIGRATOR_PASSWORD}" \
				--search-path=/changelog \
				--changeLogFile="${db}.sql" releaseLocks || true
			exit $rc
		fi
	done
	;;
seed)
	python3 /seed.py
	;;
*)
	echo "unknown mode: $1" >&2
	exit 2
	;;
esac
