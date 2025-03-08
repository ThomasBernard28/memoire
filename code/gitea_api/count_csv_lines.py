import duckdb

x = duckdb.sql("SELECT COUNT(*) FROM read_csv_auto('../data/gitea_repos_all_time.csv')").fetchone()[0]

print(x)