DIALECT := duckdb

fix:
	sqlfluff fix --dialect $(DIALECT)

lint:
	sqlfluff lint --dialect $(DIALECT)

tests:
	@echo "Removing test.duckdb if it exists..."
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/tables/create.py --disable-warnings
	pytest ducksearch/tables/insert.py --disable-warnings
	pytest ducksearch/tables/select.py --disable-warnings
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/hf/insert.py --disable-warnings
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/evaluation/evaluation.py --disable-warnings
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/search/create.py --disable-warnings
	pytest ducksearch/search/select.py --disable-warnings
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/search/graphs.py --disable-warnings
	rm -rf test.duckdb
	rm -rf test.duckdb.wal

view:
	harlequin test.duckdb

livedoc:
	python docs/parse
	mkdocs build --clean
	mkdocs serve --dirtyreload

deploydoc:
	mkdocs gh-deploy --force