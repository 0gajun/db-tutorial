db: db.c
	gcc -g db.c -o db

run: db
	./db db-tutorial.db

debug: db
	lldb ./db

clean:
	rm db db-tutorial.db tags

tag:
	ctags db.c

test: db
	bundle exec rspec ./spec/*.rb
