import os
import re
import time
import logging
import sqlite3
import asyncio
import email.utils
from datetime import datetime
from typing import Optional, List, Dict, Any
from xml.etree import ElementTree as ET
from xml.sax.saxutils import escape

# 尝试导入 python-dotenv（如果在本地测试安装了该库，会自动加载 .env 文件）
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, Request, Query, Response
from fastapi.responses import Response
import uvicorn
import httpx

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("EZTV-Torznab")

# Environment Variables
RSS_URL = os.getenv("EZTV_RSS_URL", "https://myrss.org/eztv")
FETCH_INTERVAL_MINUTES = int(os.getenv("FETCH_INTERVAL_MINUTES", "20"))
DB_PATH = os.getenv("DB_PATH", "eztv_torrents.db")
PORT = int(os.getenv("PORT", "8000"))


class DatabaseManager:
    """Manages SQLite database operations for torrent caching and search."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._ensure_db_dir()
        self._init_db()

    def _ensure_db_dir(self):
        """Ensure parent directories exist for the database file."""
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS torrents (
                    infohash TEXT PRIMARY KEY,
                    guid TEXT,
                    title TEXT NOT NULL,
                    clean_title TEXT,
                    season INTEGER,
                    episode INTEGER,
                    link TEXT,
                    pub_date TEXT,
                    pub_timestamp INTEGER,
                    size INTEGER DEFAULT 0,
                    magnet TEXT,
                    seeds INTEGER DEFAULT 0,
                    peers INTEGER DEFAULT 0,
                    download_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Create indexes for fast searching by Sonarr/Prowlarr
            conn.execute("CREATE INDEX IF NOT EXISTS idx_clean_title ON torrents(clean_title);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_season_ep ON torrents(season, episode);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pub_timestamp ON torrents(pub_timestamp DESC);")
            conn.commit()
            logger.info("Database initialized successfully.")

    def upsert_torrent(self, data: Dict[str, Any]) -> bool:
        """Insert or ignore existing torrent based on infohash."""
        sql = """
            INSERT OR REPLACE INTO torrents (
                infohash, guid, title, clean_title, season, episode, link, pub_date,
                pub_timestamp, size, magnet, seeds, peers, download_url
            ) VALUES (
                :infohash, :guid, :title, :clean_title, :season, :episode, :link, :pub_date,
                :pub_timestamp, :size, :magnet, :seeds, :peers, :download_url
            )
        """
        try:
            with self.get_connection() as conn:
                conn.execute(sql, data)
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"Failed to upsert torrent {data.get('infohash')}: {e}")
            return False


    def search_torrents(
        self,
        query: Optional[str] = None,
        season: Optional[int] = None,
        episode: Optional[int] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Search torrents based on title, season, and episode parameters."""
        conditions = []
        params = {}

        if query:
            # Clean and normalize search query
            clean_q = re.sub(r'[^a-zA-Z0-9\s]', '%', query).strip()
            conditions.append("clean_title LIKE :query")
            params['query'] = f"%{clean_q}%"

        if season is not None:
            conditions.append("season = :season")
            params['season'] = season

        if episode is not None:
            conditions.append("episode = :episode")
            params['episode'] = episode

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
            SELECT * FROM torrents
            {where_clause}
            ORDER BY pub_timestamp DESC
            LIMIT :limit OFFSET :offset
        """
        params['limit'] = limit
        params['offset'] = offset

        with self.get_connection() as conn:
            cursor = conn.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> Dict[str, Any]:
        """Returns total torrent count for health check."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) as count FROM torrents;")
            row = cursor.fetchone()
            return {"total_torrents": row["count"] if row else 0}


def parse_title_info(title: str):
    """
    Parses torrent title to extract cleaned show name, season, and episode.
    Example: 'The Westies 2026 S01E08 1080p HEVC x265-MeGusta'
    """
    # Standard SxxExx pattern
    pattern = r'(?i)^(.*?)[._\s]+S(\d{1,2})E(\d{1,2})\b'
    match = re.search(pattern, title)
    
    if match:
        raw_show = match.group(1)
        season = int(match.group(2))
        episode = int(match.group(3))
        # Replace dots/underscores with spaces
        clean_show = re.sub(r'[._]+', ' ', raw_show).strip()
        return clean_show, season, episode

    # Fallback: Just clean dots/underscores
    clean_show = re.sub(r'[._]+', ' ', title).strip()
    return clean_show, None, None


def parse_rfc2822_date(date_str: str) -> int:
    """Parses RFC 2822 date string into unix timestamp."""
    try:
        parsed_tuple = email.utils.parsedate_tz(date_str)
        if parsed_tuple:
            return int(email.utils.mktime_tz(parsed_tuple))
    except Exception:
        pass
    return int(time.time())


class EZTVFetcher:
    """Module responsible for fetching and parsing the EZTV RSS feed."""

    def __init__(self, rss_url: str, db: DatabaseManager):
        self.rss_url = rss_url
        self.db = db

    async def fetch_and_process(self):
        """Fetches RSS XML, parses items, and stores into database."""
        logger.info(f"Fetching EZTV RSS feed from {self.rss_url}...")
        try:
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                response = await client.get(self.rss_url)
                response.raise_for_status()
                xml_data = response.text

            count = self.parse_and_save_xml(xml_data)
            logger.info(f"RSS processing finished. Updated/inserted {count} items.")
        except Exception as e:
            logger.error(f"Error fetching/processing EZTV RSS: {e}")

    def parse_and_save_xml(self, xml_content: str) -> int:
        """Parses EZTV XML RSS and saves records."""
        ns = {'torrent': 'http://xmlns.ezrss.it/0.1/'}
        root = ET.fromstring(xml_content)
        channel = root.find('channel')
        if channel is None:
            return 0

        inserted_count = 0
        for item in channel.findall('item'):
            title = item.findtext('title', '').strip()
            guid = item.findtext('guid', '').strip()
            link = item.findtext('link', '').strip()
            pub_date = item.findtext('pubDate', '').strip()
            pub_ts = parse_rfc2822_date(pub_date)

            # Namespace specific elements
            content_length = item.findtext('torrent:contentLength', '0', namespaces=ns)
            info_hash = item.findtext('torrent:infoHash', '', namespaces=ns).strip().lower()
            magnet_uri = item.findtext('torrent:magnetURI', '', namespaces=ns)
            seeds = item.findtext('torrent:seeds', '0', namespaces=ns)
            peers = item.findtext('torrent:peers', '0', namespaces=ns)

            # Enclosure tag (Download torrent URL)
            enclosure = item.find('enclosure')
            download_url = enclosure.get('url', '') if enclosure is not None else magnet_uri
            length_attr = enclosure.get('length', '0') if enclosure is not None else '0'

            size = int(content_length) if content_length.isdigit() and int(content_length) > 0 else int(length_attr) if length_attr.isdigit() else 0

            # Parse title metadata
            clean_title, season, episode = parse_title_info(title)

            # Use infoHash as primary key; fallback to guid if infoHash is missing
            primary_hash = info_hash or guid or link

            torrent_data = {
                "infohash": primary_hash,
                "guid": guid or link or primary_hash,
                "title": title,
                "clean_title": clean_title,
                "season": season,
                "episode": episode,
                "link": link,
                "pub_date": pub_date,
                "pub_timestamp": pub_ts,
                "size": size,
                "magnet": magnet_uri,
                "seeds": int(seeds) if seeds.isdigit() else 322,
                "peers": int(peers) if peers.isdigit() else 322,
                "download_url": download_url or magnet_uri
            }

            if self.db.upsert_torrent(torrent_data):
                inserted_count += 1

        return inserted_count


class TorznabFormatter:
    """Generates standard Torznab XML responses for Prowlarr/Sonarr."""

    @staticmethod
    def get_capabilities_xml() -> str:
        """Returns standard Torznab caps XML."""
        return """<?xml version="1.0" encoding="UTF-8"?>
<caps>
    <server title="EZTV Torznab Proxy" version="1.0.0"/>
    <limits max="500" default="100"/>
    <searching>
        <search available="yes" supportedParams="q"/>
        <tv-search available="yes" supportedParams="q,season,ep"/>
    </searching>
    <categories>
        <category id="5000" name="TV">
            <subcat id="5030" name="TV/SD"/>
            <subcat id="5040" name="TV/HD"/>
            <subcat id="5045" name="TV/UHD"/>
        </category>
    </categories>
</caps>"""


    @staticmethod
    def get_feed_xml(items: List[Dict[str, Any]], host_url: str) -> str:
        """Formats list of torrent records into Torznab RSS XML."""
        xml_items = []
        for item in items:
            cat_id = "5040"  # Default HD
            title_lower = item['title'].lower()
            if '2160p' in title_lower or '4k' in title_lower:
                cat_id = "5045"
            elif '720p' in title_lower or '1080p' in title_lower:
                cat_id = "5040"
            else:
                cat_id = "5030"

            magnet_attr = f'<torznab:attr name="magneturl" value="{escape(item["magnet"])}"/>' if item["magnet"] else ""
            hash_attr = f'<torznab:attr name="infohash" value="{escape(item["infohash"])}"/>' if item["infohash"] else ""
            season_attr = f'<torznab:attr name="season" value="{item["season"]}"/>' if item["season"] is not None else ""
            ep_attr = f'<torznab:attr name="episode" value="{item["episode"]}"/>' if item["episode"] is not None else ""

            item_xml = f"""
        <item>
            <title>{escape(item['title'])}</title>
            <guid isPermaLink="false">{escape(item['guid'])}</guid>
            <link>{escape(item['download_url'] or item['magnet'])}</link>
            <comments>{escape(item['link'])}</comments>
            <pubDate>{escape(item['pub_date'])}</pubDate>
            <size>{item['size']}</size>
            <enclosure url="{escape(item['download_url'] or item['magnet'])}" length="{item['size']}" type="application/x-bittorrent"/>
            <torznab:attr name="category" value="5000"/>
            <torznab:attr name="category" value="{cat_id}"/>
            <torznab:attr name="size" value="{item['size']}"/>
            <torznab:attr name="seeders" value="{item['seeds']}"/>
            <torznab:attr name="peers" value="{item['peers']}"/>
            {hash_attr}
            {magnet_attr}
            {season_attr}
            {ep_attr}
        </item>"""
            xml_items.append(item_xml)

        items_str = "".join(xml_items)
        return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:torznab="http://torznab.com/schemas/2015/feed">
    <channel>
        <title>EZTV Torznab Indexer</title>
        <description>EZTV RSS Torznab Adapter for Prowlarr/Sonarr</description>
        <link>{escape(host_url)}</link>
        {"".join(xml_items)}
    </channel>
</rss>"""


db = DatabaseManager(DB_PATH)
fetcher = EZTVFetcher(RSS_URL, db)

async def background_scheduler():
    """Background task running RSS updates periodically."""
    while True:
        try:
            await fetcher.fetch_and_process()
        except Exception as e:
            logger.error(f"Scheduler exception: {e}")
        await asyncio.sleep(FETCH_INTERVAL_MINUTES * 60)

async def lifespan(app: FastAPI):
    """Lifecycle manager for background worker."""
    task = asyncio.create_task(background_scheduler())
    logger.info("Started background RSS fetch scheduler.")
    yield
    task.cancel()
    logger.info("Stopped background scheduler.")

app = FastAPI(title="EZTV Torznab Proxy", lifespan=lifespan)


@app.get("/api")
@app.get("/api/v1")
async def torznab_api(
    request: Request,
    t: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    season: Optional[int] = Query(None),
    ep: Optional[int] = Query(None),
    limit: int = Query(100, le=500),
    offset: int = Query(0)
):
    """Standard Torznab API endpoint for Prowlarr / Sonarr integration."""
    host_url = str(request.base_url)

    # 1. Capabilities query
    if t == "caps":
        return Response(
            content=TorznabFormatter.get_capabilities_xml(),
            media_type="application/xml; charset=utf-8"
        )

    # 2. Search & TV-Search query
    if t in ("search", "tvsearch"):
        results = db.search_torrents(
            query=q,
            season=season,
            episode=ep,
            limit=limit,
            offset=offset
        )
        xml_response = TorznabFormatter.get_feed_xml(results, host_url)
        return Response(content=xml_response, media_type="application/xml; charset=utf-8")

    # Default fallback: return latest cached releases
    results = db.search_torrents(limit=limit, offset=offset)
    xml_response = TorznabFormatter.get_feed_xml(results, host_url)
    return Response(content=xml_response, media_type="application/xml; charset=utf-8")


@app.get("/sync")
async def trigger_sync():
    """Manual sync trigger for testing."""
    await fetcher.fetch_and_process()
    return {"status": "ok", "message": "EZTV RSS fetch completed successfully."}

@app.get("/health")
async def health_check():
    """Health check route returning stats."""
    stats = db.get_stats()
    return {"status": "healthy", "stats": stats, "rss_url": RSS_URL}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=False)