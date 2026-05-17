-- compact_table.sql
-- Trigger Iceberg rewrite_data_files for one table.
-- Parameter: {table} — fully qualified table name within the iceberg catalog
--                      e.g. 'normalized.book_ticker'
--
-- rewrite_data_files merges small files produced by streaming writers into
-- larger files sized for efficient batch reads (target ~128 MB).

CALL iceberg.system.rewrite_data_files(table => '{table}')
