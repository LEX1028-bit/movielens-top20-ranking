from pathlib import Path
import sqlite3
import pandas as pd

BASE = Path(__file__).resolve().parent.parent

# 你现在的文件夹叫 Scripts，所以 schema.sql 在 Scripts/schema.sql
SCHEMA_PATH = BASE / "Scripts" / "schema.sql"

DATA_DIR = BASE / "data"
DB_PATH = DATA_DIR / "movielens.db"

RATINGS_PATH = DATA_DIR / "ratings.csv"
MOVIES_PATH = DATA_DIR / "movies.csv"

def clean_ratings(ratings: pd.DataFrame) -> pd.DataFrame:
    """
    清理评分数据，移除空行、重复项和无效评分

    Args:
        ratings: 包含用户评分的原始DataFrame

    Returns:
        清理后的评分DataFrame
    """
    ratings = ratings.dropna(how="all").drop_duplicates()
    ratings = ratings.dropna(subset=["userId", "movieId", "rating"])
    ratings = ratings[(ratings["rating"] >= 0.5) & (ratings["rating"] <= 5.0)]
    return ratings

def build_movie_stats(ratings: pd.DataFrame) -> pd.DataFrame:
    """
    根据评分数据构建电影统计信息

    Args:
        ratings: 清理后的评分DataFrame

    Returns:
        包含每部电影评分数量和平均评分的DataFrame
    """
    return (
        ratings.groupby("movieId")["rating"]
        .agg(rating_count="count", avg_rating="mean")
        .reset_index()
    )

def bayesian_weighted(movie_stats: pd.DataFrame, m: int = 1000) -> pd.DataFrame:
    """
    计算贝叶斯加权评分

    Args:
        movie_stats: 包含电影评分统计信息的DataFrame
        m: 贝叶斯权重参数，默认为1000

    Returns:
        添加了加权评分列的DataFrame
    """
    C = movie_stats["avg_rating"].mean()
    movie_stats["weighted_rating"] = (
        (movie_stats["rating_count"] / (movie_stats["rating_count"] + m)) * movie_stats["avg_rating"]
        + (m / (movie_stats["rating_count"] + m)) * C
    )
    return movie_stats

def main():
    # 创建数据目录
    DATA_DIR.mkdir(exist_ok=True)

    # 检查必需的输入文件是否存在
    if not RATINGS_PATH.exists():
        raise FileNotFoundError(f"找不到 {RATINGS_PATH} —— 请把 ratings.csv 放进 data/ 目录")
    if not MOVIES_PATH.exists():
        raise FileNotFoundError(f"找不到 {MOVIES_PATH} —— 请把 movies.csv 放进 data/ 目录")
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"找不到 {SCHEMA_PATH} —— 请确认 schema.sql 在 Scripts/ 目录下")

    # 读取CSV文件
    ratings = pd.read_csv(RATINGS_PATH)
    movies = pd.read_csv(MOVIES_PATH)

    # 数据处理流程：清理评分 -> 构建统计 -> 计算加权评分
    ratings = clean_ratings(ratings)
    stats = build_movie_stats(ratings)
    stats = bayesian_weighted(stats, m=1000)

    # 准备电影基础信息并合并数据
    movies = movies[["movieId", "title", "genres"]].drop_duplicates("movieId")
    merged = stats.merge(movies, on="movieId", how="left")

    # 连接数据库并执行数据导入
    conn = sqlite3.connect(DB_PATH)
    try:
        # 执行数据库表结构创建脚本
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

        # 将处理后的数据写入数据库表
        merged[["movieId", "title", "genres"]].to_sql("movies", conn, if_exists="replace", index=False)
        merged[["movieId", "rating_count", "avg_rating", "weighted_rating"]].to_sql(
            "movie_scores", conn, if_exists="replace", index=False
        )

        print(f"✅ OK: built sqlite db -> {DB_PATH}")

        # 查询并显示前5名高分电影作为验证
        top5 = pd.read_sql_query(
            """
            SELECT m.title, m.genres, s.weighted_rating
            FROM movie_scores s
            JOIN movies m ON m.movieId = s.movieId
            ORDER BY s.weighted_rating DESC
            LIMIT 5
            """,
            conn
        )
        print("Top 5 sample:\n", top5.to_string(index=False))
    finally:
        conn.close()

if __name__ == "__main__":
    main()

