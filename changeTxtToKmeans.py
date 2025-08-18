import duckdb

input_file = 'twitter-2010.txt'
output_db = 'twitter_degrees.duckdb' # 可以持久化，也可以用:memory:

# 连接到数据库 (如果文件不存在会自动创建)
con = duckdb.connect(database=output_db, read_only=False)

print("Creating table from CSV... This will be fast.")
# DuckDB可以直接从CSV创建表，并自动推断类型
con.execute(f"""
    CREATE TABLE edges AS SELECT *
    FROM read_csv_auto('{input_file}', sep=' ', names=['followed_id', 'follower_id']);
""")

print("Calculating in-degrees...")
con.execute("""
    CREATE TABLE in_degrees AS
    SELECT followed_id AS user_id, COUNT(*) AS in_degree
    FROM edges
    GROUP BY followed_id;
""")

print("Calculating out-degrees...")
con.execute("""
    CREATE TABLE out_degrees AS
    SELECT follower_id AS user_id, COUNT(*) AS out_degree
    FROM edges
    GROUP BY follower_id;
""")

print("Joining degrees and calculating features...")
# 将入度、出度和派生特征合并到一个最终的表中
# 使用 COALESCE(degree, 0) 来处理只有入度或只有出度的用户
con.execute("""
    CREATE TABLE features AS
    SELECT
        t1.user_id,
        COALESCE(t1.in_degree, 0) AS in_degree,
        COALESCE(t2.out_degree, 0) AS out_degree,
        (COALESCE(t1.in_degree, 0) / (COALESCE(t2.out_degree, 0) + 1.0)) AS ratio
    FROM in_degrees t1
    FULL OUTER JOIN out_degrees t2 ON t1.user_id = t2.user_id;
""")

print("Fetching data into Pandas for scaling and saving...")
# 从DuckDB中将结果导出到Pandas DataFrame
features_df = con.execute("SELECT in_degree, out_degree, ratio FROM features ORDER BY user_id").fetch_df()

con.close()

# --- 后续处理和之前一样 ---
from sklearn.preprocessing import StandardScaler
import numpy as np

print("Scaling features...")
scaler = StandardScaler()
# 注意：直接在DataFrame上操作更方便
scaled_features = scaler.fit_transform(features_df)

print("Saving final data...")
np.savetxt('kmeans_features_duckdb.txt', scaled_features, fmt='%.8f', delimiter=' ')

print("All done!")