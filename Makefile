db: db.c
	gcc db.c -o db

run: db
	./db

test: db
	bundle exec rspec ./spec/*.rb
