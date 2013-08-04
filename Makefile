CFLAGS=-g -Wall -c
LDFLAGS=-lpq -ljson -lfcgi

all: pg_fastcgi

pg_fastcgi: pg_fastcgi.o
	$(CC) $(LDFLAGS) $< -o $@

pg_fastcgi.o: pg_fastcgi.c
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf pg_fastcgi pg_fastcgi.o
