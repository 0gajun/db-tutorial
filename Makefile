db: db.c
	gcc db.c -o db

run: db
	./db
