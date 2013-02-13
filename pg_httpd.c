/*-------------------------------------------------------------------------
 *
 * pg_httpd.c
 *     FastCGI program that translate URI requests to SQL queries and returns
 *     the results as JSON.
 *
 * Portions Copyright (c) 2012, Mark Wong
 *
 * IDENTIFICATION
 *     pg_httpd.c
 *
 *-------------------------------------------------------------------------
 */

#include <fcgi_stdio.h>

#include <stdio.h>
#include <string.h>

#include <syslog.h>

#include <libpq-fe.h>

#include <json-c/json.h>

#define PGHOST_LEN 32
#define PGDATABASE_LEN 32
#define SCHEMANAME_LEN 66
#define TABLENAME_LEN 66

int
get_pgdatabase(char *script_name, char pgdatabase[])
{
	int length;
	char *tmp;

	/* Skip the leading / */
	script_name++;
	tmp = strstr(script_name, "/");
	if (tmp == NULL)
		return 1;
	length = tmp - script_name;
	length = length < PGDATABASE_LEN ? length : PGDATABASE_LEN;
	strncpy(pgdatabase, script_name, length);

	return 0;
}

int
get_schemaname(char *script_name, char schemaname[])
{
	int length;
	char *tmp;

	/* Skip the leading / */
	script_name++;
	script_name = strstr(script_name, "/") + 1;
	tmp = strstr(script_name, "/");
	if (tmp == NULL)
		return 1;
	length = tmp - script_name;
	length = length < SCHEMANAME_LEN ? length : SCHEMANAME_LEN;
	strncpy(schemaname, script_name, length);

	return 0;
}

int
get_tablename(char *script_name, char tablename[])
{
	int length;
	char *tmp;

	/* Skip the leading / */
	script_name++;
	script_name = strstr(script_name, "/") + 1;
	script_name = strstr(script_name, "/") + 1;
	tmp = strstr(script_name, "/");
	if (tmp == NULL)
		length = strlen(script_name);
	else
		length = tmp - script_name;
	length = length < TABLENAME_LEN ? length : TABLENAME_LEN;
	strncpy(tablename, script_name, length);

	return 0;
}

json_object *
process_get(char pgdatabase[], char schemaname[], char tablename[])
{
	int i, j;

	/* Postgres stuff */
	char conninfo[1024];
	PGconn *conn;
	PGresult *res;

	int nFields;
	char sql[128];

	/* JSON stuff */
	json_object *json_obj, *json_array;

	/* Connect to postgres. */
	snprintf(conninfo, 1024, "dbname=%s", pgdatabase);
	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		printf("%s", PQerrorMessage(conn));
		return NULL;
	}

	snprintf(sql, 128, "SELECT * FROM %s.%s;", schemaname, tablename);
	res = PQexec(conn, sql);
	if (!res || PQresultStatus(res) != PGRES_COMMAND_OK)
		printf("%s", PQerrorMessage(conn));

	/* Build the JSON result. */
	json_array = json_object_new_array();
	nFields = PQnfields(res);
	for (i = 0; i < PQntuples(res); i++)
	{
		json_obj = json_object_new_object();
		for (j = 0; j < nFields; j++)
		{
			/* Check if value is NULL before checking the column type. */
			if (PQgetisnull(res, i, j))
			{
				json_object_object_add(json_obj, PQfname(res, j), NULL);
				continue;
			}

			/*
			 * Postgres types are defined in src/include/catalog/pg_type.h
			 */
			switch (PQftype(res, j))
			{
			case 16:
			case 1000:
				json_object_object_add(json_obj, PQfname(res, j),
						json_object_new_boolean(PQgetvalue(res, i, j)[0] ==
								't' ? 1: 0));
				break;
			case 20:
			case 21:
			case 23:
			case 1005:
			case 1007:
			case 1016:
				json_object_object_add(json_obj, PQfname(res, j),
						json_object_new_int(atoi(PQgetvalue(res, i, j))));
				break;
			case 700:
			case 701:
			case 1021:
			case 1022:
			case 1231:
			case 1700:
				json_object_object_add(json_obj, PQfname(res, j),
						json_object_new_double(atof(PQgetvalue(res, i, j))));
				break;
			default:
				json_object_object_add(json_obj, PQfname(res, j),
						json_object_new_string(PQgetvalue(res, i, j)));
				break;
			}
		}
		json_object_array_add(json_array, json_obj);
	}

	PQclear(res);
	PQfinish(conn);

	return json_array;
}

int main()
{
	char *str;

	char pghost[PGHOST_LEN + 1] = "localhost";

	str = getenv("PGHOST");
	if (str != NULL)
		strncpy(pghost, str, PGHOST_LEN);

	openlog("pg_httpd", LOG_CONS | LOG_PID | LOG_NDELAY, LOG_LOCAL0);
	syslog(LOG_INFO, "pg_httpd starting");

	while (FCGI_Accept() >= 0) {
		char pgdatabase[PGDATABASE_LEN + 1];
		char schemaname[SCHEMANAME_LEN + 1];
		char tablename[TABLENAME_LEN + 1];

		char *request_method;
		char *script_name;

		const char *json_str;
		json_object *json_obj;

		int content_length;

		request_method = getenv("REQUEST_METHOD");
		if (request_method == NULL)
			continue;

		bzero(pgdatabase, PGDATABASE_LEN + 1);
		bzero(schemaname, SCHEMANAME_LEN + 1);
		bzero(tablename, TABLENAME_LEN + 1);

		if (strcmp(request_method, "GET") == 0) {
			script_name = getenv("SCRIPT_NAME");
			syslog(LOG_DEBUG, "GET SCRIPT_NAME %s", script_name);
			syslog(LOG_DEBUG, "GET QUERY_STRING %s", getenv("QUERY_STRING"));
			/* FIXME: handle when pgdatabase isn't identified. */
			get_pgdatabase(script_name, pgdatabase);
			syslog(LOG_DEBUG, "GET PGDATABASE %s", pgdatabase);
			/* FIXME: handle when schemaname isn't identified. */
			get_schemaname(script_name, schemaname);
			syslog(LOG_DEBUG, "GET SCHEMANAME %s", schemaname);
			/* FIXME: handle when tablename isn't identified. */
			get_tablename(script_name, tablename);
			syslog(LOG_DEBUG, "GET TABLENAME %s", tablename);

			json_obj = process_get(pgdatabase, schemaname, tablename);
		}

		/* Construct the HTTP response. */
		json_str = json_object_to_json_string(json_obj);
		syslog(LOG_DEBUG, "JSON: %s\n", json_str);
		content_length = strlen(json_str);

		printf("Content-type: text/plain\r\n"
				"Content-length: %d\r\n"
				"\r\n"
				"%s", content_length, json_str);

	}

	syslog(LOG_INFO, "pg_httpd stopping");
	closelog();

	return 0;
}
