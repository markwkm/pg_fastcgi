CFLAGS=-g -Wall -c
LDFLAGS=-lpq -ljson -lfcgi

all: pg_httpd

pg_httpd: pg_httpd.o
	$(CC) $(LDFLAGS) $< -o $@

pg_httpd.o: pg_httpd.c
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf pg_httpd pg_httpd.o
