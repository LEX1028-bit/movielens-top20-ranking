from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

BASE = Path(__file__).parent
DATA_DIR = BASE / "data"
OUT_DIR = BASE / "output"
RATINGS_PATH = DATA_DIR / "ratings.csv"

def load_ratings():
    if not RATINGS_PATH.exists():
        raise FileNotFoundError(f"找不到 {RATINGS_PATH}，请把 ratings.csv 放进 data/ 目录")
    return pd.read_csv(RATINGS_PATH)

def build_movie_stats(ratings: pd.DataFrame) -> pd.DataFrame:
    # movieId + rating 是必须列
    need = {"movieId", "rating"}
    missing = need - set(ratings.columns)
    if missing:
        raise ValueError(f"ratings.csv 缺少列: {missing}")

    movie_stats = (
        ratings.groupby("movieId")["rating"]
        .agg(rating_count="count", avg_rating="mean")
        .reset_index()
    )
    return movie_stats

def bayesian_weighted(movie_stats: pd.DataFrame, m: int = 1000) -> pd.DataFrame:
    C = movie_stats["avg_rating"].mean()
    movie_stats["weighted_rating"] = (
        (movie_stats["rating_count"] / (movie_stats["rating_count"] + m)) * movie_stats["avg_rating"]
        + (m / (movie_stats["rating_count"] + m)) * C
    )
    return movie_stats

def save_outputs(top20: pd.DataFrame):
    OUT_DIR.mkdir(exist_ok=True)

    # 1) CSV 输出
    csv_path = OUT_DIR / "top20_weighted.csv"
    top20.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"Saved CSV -> {csv_path}")

    # 2) 可视化输出（柱状图）
    fig_path = OUT_DIR / "top20_weighted.png"
    plot_df = top20.sort_values("weighted_rating", ascending=True)

    plt.figure()
    plt.barh(plot_df["movieId"].astype(str), plot_df["weighted_rating"])
    plt.xlabel("weighted_rating")
    plt.ylabel("movieId")
    plt.tight_layout()
    plt.savefig(fig_path, dpi=200)
    plt.close()
    print(f"Saved Figure -> {fig_path}")

def main():
    ratings = load_ratings()
    movie_stats = build_movie_stats(ratings)
    movie_stats = bayesian_weighted(movie_stats, m=1000)

    top20 = movie_stats.sort_values("weighted_rating", ascending=False).head(20)
    print(top20.head())
    save_outputs(top20)

if __name__ == "__main__":
    main()
