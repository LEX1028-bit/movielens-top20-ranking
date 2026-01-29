from pathlib import Path
import pandas as pd

DATA_DIR = Path(__file__).parent / "data"
OUT_DIR = Path(__file__).parent / "output"
OUT_DIR.mkdir(exist_ok=True)

RATINGS_PATH = DATA_DIR / "ratings.csv"
MOVIES_PATH = DATA_DIR / "movies.csv"

def load():
    ratings = pd.read_csv(RATINGS_PATH)
    movies = pd.read_csv(MOVIES_PATH)
    return ratings, movies

def clean_ratings(ratings: pd.DataFrame) -> pd.DataFrame:
    before = len(ratings)

    # 去掉全空行 + 去重
    ratings = ratings.dropna(how="all").drop_duplicates()

    # 关键列不能为空
    ratings = ratings.dropna(subset=["userId", "movieId", "rating"])

    # rating 合法范围（MovieLens 通常是 0.5~5）
    ratings = ratings[(ratings["rating"] >= 0.5) & (ratings["rating"] <= 5.0)]

    after = len(ratings)
    print(f"[Clean] ratings rows: {before} -> {after}")
    return ratings

def main():
    ratings, movies = load()
    ratings = clean_ratings(ratings)

    # 合并：把 title/genres 拼到评分表
    df = ratings.merge(movies, on="movieId", how="left")

    # --- 统计1：最热门电影（评分次数最多，且次数>=50，避免冷门噪声） ---
    movie_stats = (
        df.groupby(["movieId", "title"], as_index=False)
          .agg(
              rating_count=("rating", "size"),
              avg_rating=("rating", "mean")
          )
    )
    popular_top20 = (
        movie_stats[movie_stats["rating_count"] >= 50]
        .sort_values(["rating_count", "avg_rating"], ascending=[False, False])
        .head(20)
    )

    # --- 统计2：最活跃用户（评分条数最多） ---
    user_top20 = (
        df.groupby("userId", as_index=False)
          .agg(
              rating_count=("rating", "size"),
              avg_rating=("rating", "mean")
          )
          .sort_values("rating_count", ascending=False)
          .head(20)
    )

    # 打印看看
    print("\n=== Top 20 Popular Movies (count>=50) ===")
    print(popular_top20.to_string(index=False))

    print("\n=== Top 20 Active Users ===")
    print(user_top20.to_string(index=False))

    # 导出结果（可当作品）
    df.to_csv(OUT_DIR / "ratings_merged_cleaned.csv", index=False)
    popular_top20.to_csv(OUT_DIR / "top20_popular_movies.csv", index=False)
    user_top20.to_csv(OUT_DIR / "top20_active_users.csv", index=False)

    print("\nSaved outputs to:", OUT_DIR)

if __name__ == "__main__":
    main()
