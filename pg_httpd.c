/*-------------------------------------------------------------------------
 *
 * pg_httpd.c
 *     External HTTP listener process that translate URI requests to SQL
 *     queries and returns the results as JSON.
 *
 * Portions Copyright (c) 2012, Mark Wong
 *
 * IDENTIFICATION
 *     pg_httpd.c
 *
 *-------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#include <libpq-fe.h>

#include <json/json.h>

int http_port = 54321;

/*
 * main
 *
 * Very basic proof of concept.  Start listening to a port, loop indefinitely,
 * connect to the database based on the first part of the URI request, SELECT *
 * from the table in the next part of the URI request, create a JSON object
 * based on the results, and send it back.
 *
 * Example: http://localhost:54321/dbname/tablename
 *
 * This says connect to the database named "dbname" and SELECT * FROM
 * "tablename".
 *
 * The database host, port and user will default to the environment that starts
 * up this program.
 */
int main()
{
	struct sockaddr_in sa;
	int val;
	int socket_listener;

	/* Open a socket for incoming connections. */

	val= 1;

	memset(&sa, 0, sizeof(struct sockaddr_in));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = INADDR_ANY;
	sa.sin_port = htons((unsigned short) http_port);

	socket_listener = socket(PF_INET, SOCK_STREAM, 0);
	if (socket_listener < 0)
	{
		perror("socket");
		return -1;
	}

	setsockopt(socket_listener, SOL_SOCKET, SO_REUSEADDR, &val, sizeof (val));

	if (bind(socket_listener, (struct sockaddr *) &sa,
			sizeof(struct sockaddr_in)) < 0)
	{
		perror("bind");
		return -1;
	}

	if (listen(socket_listener, 1) < 0)
	{
		perror("listen");
		return -1;
	}

	/* Main HTTP listener loop. */
	for (;;)
	{
		int i, j;

		/* Socket stuff */
		int sockfd;
		socklen_t addrlen = sizeof(struct sockaddr_in);
		int received;
		int length = 2048;
		char data[length];
		int count;

		/* Postgres stuff */
		char conninfo[1024];
		char dbname[32];
		char tablename[64];
		char sql[128];
		PGconn *conn;
		PGresult *res;
		int nFields;

		/* HTTP stuff */
		char *p1, *p2;
		char method[8];
		char request_uri[1024];
		char *http_response;
		int content_length;

		/* JSON stuff */
		json_object *json_obj, *json_array;

		sockfd = accept(socket_listener, (struct sockaddr *) &sa, &addrlen);
		if (sockfd == -1)
		{
			perror("accept");
			continue;
		}

		memset(data, 0, sizeof(data));
		received = recv(sockfd, data, length, 0);
		printf("ohai %d %s\n", received, data);

		/* Get the HTTP method. */
		count = 0;
		p1 = data;
		p2 = p1 + 1;
		while (*p2 != ' ' && count++ < length)
			++p2;
		i = p2 - p1;
		i = i > 8 ? 8 : i;
		strncpy(method, p1, i);
		method[i] = '\0';
		printf("method: '%s'\n", method);

		/* Get the HTTP request URI. */
		count = 0;
		p1 = p2 + 1;
		p2 = p1 + 1;
		while (*p2 != ' ' && count++ < length)
			++p2;
		i = p2 - p1;
		i = i > 1024 ? 1024 : i;
		strncpy(request_uri, p1, i);
		request_uri[i] = '\0';
		printf("request_uri: '%s'\n", request_uri);

		/* Get the database name. */
		count = 0;
		p1 = request_uri + 1;
		p2 = p1 + 1;
		while (*p2 != '/' && count++ < 1024)
			++p2;
		i = p2 - p1;
		i = i > 32 ? 32 : i;
		strncpy(dbname, p1, i);
		dbname[i] = '\0';
		printf("dbname: '%s'\n", dbname);

		/* Get the table name. */
		p1 = p2 + 1;
		p2 = p1 + 1;
		while (*p2 != '\0' && count++ < 1024)
			++p2;
		i = p2 - p1;
		i = i > 64 ? 64 : i;
		strncpy(tablename, p1, i);
		tablename[i] = '\0';
		printf("tablename: '%s'\n", tablename);

		/* Connect to postgres. */
		snprintf(conninfo, 1024, "dbname=%s", dbname);
		conn = PQconnectdb(conninfo);
		if (PQstatus(conn) != CONNECTION_OK)
			printf("%s", PQerrorMessage(conn));

		snprintf(sql, 128, "SELECT * FROM %s;", tablename);
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
							json_object_new_double(atof(PQgetvalue(res, i,
									j))));
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

		/* Construct the HTTP response. */
		printf("json: %s\n", json_object_to_json_string(json_array));

		content_length = strlen(json_object_to_json_string(json_array));
		/*
		 * I don't think the HTTP response header will ever get more than 64
		 * bytes...
		 */
		http_response = (char *) malloc(content_length + 64);
		snprintf(http_response, content_length + 63,
				"HTTP/1.0 200 OK\n" \
				"Content-Length: %d\r\n\r\n" \
				"%s",
				content_length,
				json_object_to_json_string(json_array));
		send(sockfd, http_response, strlen(http_response), 0);
		close(sockfd);
		free(http_response);
	}

	return 0;
}
