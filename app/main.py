from pathlib import Path
import sqlite3
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

# 定义项目根目录路径
BASE = Path(__file__).resolve().parent.parent
# 定义数据库文件路径
DB_PATH = BASE / "data" / "movielens.db"

# 创建FastAPI应用实例
app = FastAPI(title="MovieLens Mood Recommendation MVP")

# 添加CORS中间件，允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 定义情绪到电影类型的映射关系
MOOD_TO_GENRES = {
    "calm": ["Drama", "Romance"],
    "fun": ["Comedy"],
    "intense": ["Action", "Thriller"],
    "sad": ["Drama"],
    "inspire": ["Adventure", "Animation"],
}

def get_conn():
    """
    获取数据库连接

    Returns:
        sqlite3.Connection: 数据库连接对象

    Raises:
        FileNotFoundError: 当数据库文件不存在时抛出异常
    """
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}. 先运行 Scripts/build_db.py 生成数据库")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/health")
def health():
    """
    健康检查接口

    Returns:
        dict: 包含状态和数据库路径的信息
    """
    return {"status": "ok", "db": str(DB_PATH)}

@app.get("/titles")
def titles(query: str = Query("", max_length=80), limit: int = 20):
    """
    根据查询字符串搜索电影标题

    Args:
        query (str): 搜索关键词，默认为空字符串，最大长度80
        limit (int): 返回结果数量限制，默认为20

    Returns:
        dict: 包含匹配电影信息的字典列表
    """
    q = f"%{query.strip()}%"
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT movieId, title, genres FROM movies WHERE title LIKE ? LIMIT ?",
            (q, limit),
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
    finally:
        conn.close()

@app.get("/recommendations")
def recommendations(mood: str = "calm", k: int = 20, min_count: int = 50):
    """
    根据情绪推荐电影

    Args:
        mood (str): 用户的情绪状态，默认为"calm"
        k (int): 返回推荐电影的数量，默认为20
        min_count (int): 最小评分次数阈值，默认为50

    Returns:
        dict: 包含推荐参数和电影列表的字典
    """
    mood = mood.lower().strip()
    genres = MOOD_TO_GENRES.get(mood, [])

    conn = get_conn()
    try:
        if not genres:
            # mood 不认识：直接按 weighted_rating 排序
            rows = conn.execute(
                """
                SELECT m.movieId, m.title, m.genres, s.rating_count, s.avg_rating, s.weighted_rating
                FROM movie_scores s
                JOIN movies m ON m.movieId = s.movieId
                WHERE s.rating_count >= ?
                ORDER BY s.weighted_rating DESC
                LIMIT ?
                """,
                (min_count, k),
            ).fetchall()
        else:
            # mood 认识：用 genres LIKE 过滤（最小可用版）
            like_clauses = " OR ".join(["m.genres LIKE ?"] * len(genres))
            params = [f"%{g}%" for g in genres]

            rows = conn.execute(
                f"""
                SELECT m.movieId, m.title, m.genres, s.rating_count, s.avg_rating, s.weighted_rating
                FROM movie_scores s
                JOIN movies m ON m.movieId = s.movieId
                WHERE s.rating_count >= ?
                  AND ({like_clauses})
                ORDER BY s.weighted_rating DESC
                LIMIT ?
                """,
                [min_count, *params, k],
            ).fetchall()

        return {
            "mood": mood,
            "genres_filter": genres,
            "k": k,
            "min_count": min_count,
            "items": [dict(r) for r in rows],
        }
    finally:
        conn.close()

