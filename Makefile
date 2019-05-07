db: db.c
	gcc db.c -o db

run: db
	./db db-tutorial.db

clean:
	rm db db-tutorial.db

test: db
	bundle exec rspec ./spec/*.rb
