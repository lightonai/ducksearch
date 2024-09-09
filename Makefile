DIALECT := duckdb

fix:
	sqlfluff fix --dialect $(DIALECT)

lint:
	sqlfluff lint --dialect $(DIALECT)

tests:
	@echo "Removing test.duckdb if it exists..."
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/tables/create.py 
	pytest ducksearch/tables/insert.py 
	pytest ducksearch/tables/select.py
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/hf/insert.py
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/delete/documents.py
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/evaluation/evaluation.py
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/upload/upload.py
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/search/create.py
	pytest ducksearch/search/select.py
	rm -rf test.duckdb
	rm -rf test.duckdb.wal
	pytest ducksearch/search/graphs.py
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