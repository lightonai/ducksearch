# Variables
DIALECT := duckdb

# Rules
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

view:
	harlequin test.duckdb
