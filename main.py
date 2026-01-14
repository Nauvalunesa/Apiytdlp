from fastapi import FastAPI, Request, Query, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import httpx
from selectolax.parser import HTMLParser
import yt_dlp
import os
import asyncio
from datetime import datetime, timedelta
from urllib.parse import quote
import io
import logging
import subprocess
import base64
from collections import defaultdict
import json
import psutil
import platform
import time
from typing import Union, Literal
from zipfile import ZipFile
import shutil
import tempfile
import uuid

SPOTIFY_CLIENT_ID = "isitokenkalian ambil di Spotify developer"
SPOTIFY_CLIENT_SECRET = "isitokenkalian ambil di Spotify developer"

SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_URL = "https://api.spotify.com/v1"

OUTPUT_DIR = "output"
SPOTIFY_OUTPUT_DIR = "spotify_output"
COOKIES_FILE = "yt.txt"
BANNED_IP_FILE = "bannedip.json"
MAX_REQUESTS_PER_MINUTE = 35

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(SPOTIFY_OUTPUT_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

server_start_time = time.time()
total_request_count = 0
request_counter = defaultdict(list)

_spotify_token: str | None = None
_spotify_token_expiry: float = 0.0
_spotify_lock = asyncio.Lock()

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient(timeout=30)
    try:
        yield
    finally:
        await app.state.http.aclose()

app = FastAPI(
    title="YouTube dan Spotify Downloader API",
    description="API untuk mengunduh video dan audio dari YouTube dan Spotify.",
    version="4.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def sanitize_filename(filename: str) -> str:
    return "".join(c if ord(c) < 128 else "_" for c in filename)

async def create_temp_cookies_file() -> str:
    """Create a temporary copy of cookies file to prevent yt-dlp from modifying the original"""
    temp_dir = tempfile.gettempdir()
    temp_cookies_path = os.path.join(temp_dir, f"yt_cookies_{uuid.uuid4().hex}.txt")

    def _copy():
        if os.path.exists(COOKIES_FILE):
            shutil.copy2(COOKIES_FILE, temp_cookies_path)
        else:
            # Create empty file if original doesn't exist
            open(temp_cookies_path, 'a').close()
        return temp_cookies_path

    return await asyncio.to_thread(_copy)

async def cleanup_temp_cookies(cookies_path: str):
    """Delete temporary cookies file"""
    def _delete():
        try:
            if os.path.exists(cookies_path):
                os.remove(cookies_path)
        except Exception as e:
            logger.warning(f"Failed to delete temp cookies: {e}")
    await asyncio.to_thread(_delete)

async def get_video_duration(file_path: str) -> int:
    """Get video/audio duration in seconds using ffprobe"""
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        duration = float(stdout.decode().strip())
        return int(duration)
    except Exception as e:
        logger.error(f"Failed to get duration for {file_path}: {e}")
        return 0

async def delete_file_after_delay(file_path: str, delay: int = 600):
    await asyncio.sleep(delay)
    try:
        os.remove(file_path)
        logger.info(f"File {file_path} dihapus setelah {delay} detik")
    except FileNotFoundError:
        logger.warning(f"File {file_path} tidak ditemukan saat ingin dihapus")
    except Exception as e:
        logger.error(f"Gagal menghapus file {file_path}: {e}")

async def load_banned_ips() -> set[str]:
    def _read():
        try:
            with open(BANNED_IP_FILE, "r") as f:
                data = json.load(f)
            return set(data)
        except (FileNotFoundError, json.JSONDecodeError):
            return set()
    return await asyncio.to_thread(_read)

async def save_banned_ips(banned_ips: set[str]):
    def _write():
        with open(BANNED_IP_FILE, "w") as f:
            json.dump(list(banned_ips), f)
    await asyncio.to_thread(_write)

async def block_ip_with_ufw(ip: str):
    try:
        proc = await asyncio.create_subprocess_exec(
            "sudo", "ufw", "deny", "from", ip,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        logger.warning(f"IP {ip} diblokir via ufw")
    except Exception as e:
        logger.error(f"Gagal memblokir IP {ip} dengan ufw: {e}")

@app.middleware("http")
async def log_requests(request: Request, call_next):
    global total_request_count
    total_request_count += 1

    start_time = datetime.now()

    forwarded_for = request.headers.get("X-Forwarded-For")
    client_ip = forwarded_for.split(",")[0].strip() if forwarded_for else request.client.host

    banned_ips = await load_banned_ips()
    if client_ip in banned_ips:
        return JSONResponse(
            status_code=403,
            content={"message": "Ups, maaf ya jangan spam request lagi. Jika ingin unblacklist, hubungi WA 6285336580720"},
        )

    current_time = datetime.now()
    request_counter[client_ip].append(current_time)
    request_counter[client_ip] = [t for t in request_counter[client_ip] if t > current_time - timedelta(minutes=1)]

    if len(request_counter[client_ip]) > MAX_REQUESTS_PER_MINUTE:
        await block_ip_with_ufw(client_ip)
        banned_ips.add(client_ip)
        await save_banned_ips(banned_ips)
        request_counter[client_ip] = []

    logger.info(f"Request from {client_ip} {request.method} {request.url}")

    response = await call_next(request)

    process_time_ms = (datetime.now() - start_time).total_seconds() * 1000
    logger.info(f"Response {response.status_code} in {process_time_ms:.2f} ms")
    return response

@app.get("/", summary="Root Endpoint", description="Menampilkan halaman index.html.")
async def root():
    html_path = "/root/Apiytdlp/index.html"
    if os.path.exists(html_path):
        return FileResponse(html_path)
    return JSONResponse(status_code=404, content={"error": "index.html file not found"})

async def douyin_scraper(url: str):
    payload = f"q={url}&lang=en"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "*/*",
        "X-Requested-With": "XMLHttpRequest",
    }
    client: httpx.AsyncClient = app.state.http
    resp = await client.post("https://savetik.co/api/ajaxSearch", headers=headers, content=payload)
    if resp.status_code != 200:
        return {"status": resp.status_code, "error": "Gagal menghubungi server"}

    data = resp.json()
    html = HTMLParser(data.get("data", ""))
    caption_element = html.css_first("h3")
    caption = caption_element.text(strip=True) if caption_element else "Tidak ada caption yang ditemukan."
    urls = [a.attributes.get("href") for a in html.css("a") if "Download Photo" in a.text()]
    slide = len(urls) > 0

    if slide:
        media = urls
    else:
        media = {
            "mp4_1": next((a.attributes.get("href") for a in html.css("a") if "Download MP4 [1]" in a.text()), None),
            "mp4_2": next((a.attributes.get("href") for a in html.css("a") if "Download MP4 [2]" in a.text()), None),
            "mp4_hd": next((a.attributes.get("href") for a in html.css("a") if "Download MP4 HD" in a.text()), None),
            "mp3": next((a.attributes.get("href") for a in html.css("a") if "Download MP3" in a.text()), None),
        }

    return {"status": 200, "creator": "nauval", "caption": caption, "slide": slide, "media": media}

@app.get("/douyin", summary="Download video atau foto dari Douyin (TikTok China)")
async def douyin_download(url: str = Query(..., description="URL video Douyin")):
    try:
        if not url:
            return JSONResponse(status_code=400, content={"error": "URL tidak boleh kosong."})
        result = await douyin_scraper(url)
        if result.get("status") != 200:
            return JSONResponse(status_code=500, content={"error": "Gagal mengambil data Douyin."})
        return result
    except Exception as e:
        logger.error(f"douyin_download | URL: {url} | Error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

async def ytdlp_extract(opts: dict, query: str, download: bool, use_temp_cookies: bool = True):
    """Extract info using yt-dlp with optional temporary cookies"""
    temp_cookies = None
    try:
        if use_temp_cookies and "cookiefile" in opts:
            temp_cookies = await create_temp_cookies_file()
            opts = opts.copy()
            opts["cookiefile"] = temp_cookies

        def _run():
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(query, download=download)

        return await asyncio.to_thread(_run)
    finally:
        if temp_cookies:
            await cleanup_temp_cookies(temp_cookies)

@app.get("/search/", summary="Pencarian Video/Musik/Playlist YouTube")
async def search_video(
    query: str = Query(..., description="Kata kunci pencarian untuk YouTube"),
    type: str = Query("video", description="Tipe pencarian: video, playlist, atau musik", enum=["video", "playlist", "music"]),
    limit: int = Query(5, ge=1, le=25),
):
    try:
        ydl_opts = {"quiet": True, "cookiefile": COOKIES_FILE}
        search_query = f"ytsearch{limit}:{query}"
        search_result = await ytdlp_extract(ydl_opts, search_query, download=False)

        entries = search_result.get("entries", [])
        if type == "playlist":
            results = [
                {"title": v.get("title"), "url": v.get("webpage_url"), "id": v.get("id")}
                for v in entries
                if v and v.get("webpage_url") and "/playlist" in v.get("webpage_url", "")
            ]
        elif type == "music":
            results = [
                {"title": v.get("title"), "url": v.get("webpage_url"), "id": v.get("id")}
                for v in entries
                if v and v.get("webpage_url")
            ]
        else:
            results = [
                {"title": v.get("title"), "url": v.get("webpage_url"), "id": v.get("id")}
                for v in entries
                if v and v.get("webpage_url")
            ]

        logger.info(f"search | Query: {query} | Type: {type} | Results: {len(results)}")
        return {"results": results}
    except Exception as e:
        logger.error(f"search | Query: {query} | Type: {type} | Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/info/", summary="Informasi Lengkap Video/Playlist YouTube")
async def get_info(url: str = Query(..., description="URL video atau playlist YouTube")):
    try:
        ydl_opts = {"quiet": True, "cookiefile": COOKIES_FILE}
        info = await ytdlp_extract(ydl_opts, url, download=False)

        is_playlist = "entries" in info
        if is_playlist:
            video_entries = info.get("entries", [])
            videos = []
            for idx, v in enumerate(video_entries, 1):
                if v:
                    videos.append(
                        {
                            "index": idx,
                            "title": v.get("title", "Unknown"),
                            "url": v.get("webpage_url", "Unknown"),
                            "duration": v.get("duration", 0),
                            "thumbnail": v.get("thumbnail"),
                        }
                    )
            return {
                "is_playlist": True,
                "playlist_id": info.get("id"),
                "playlist_title": info.get("title", "Unknown Playlist"),
                "uploader": info.get("uploader"),
                "uploader_url": info.get("uploader_url"),
                "webpage_url": info.get("webpage_url"),
                "total_videos": len(videos),
                "videos": videos,
            }

        total_duration = info.get("duration", 0)
        size_bytes = sum(
            (f.get("filesize") or f.get("filesize_approx") or 0)
            for f in info.get("formats", [])
            if f.get("vcodec") != "none" and f.get("ext") == "mp4"
        )
        size_mb = round(size_bytes / 1024 / 1024, 2) if size_bytes else "Unknown"

        def seconds_to_hms(seconds: int):
            h = seconds // 3600
            m = (seconds % 3600) // 60
            s = seconds % 60
            return f"{int(h):02}:{int(m):02}:{int(s):02}"

        resolutions = []
        for fmt in info.get("formats", []):
            if fmt.get("vcodec") != "none" and fmt.get("height"):
                size = fmt.get("filesize") or fmt.get("filesize_approx")
                resolutions.append(
                    {
                        "resolution": f"{fmt['height']}",
                        "ext": fmt.get("ext", "Unknown"),
                        "size": round(size / 1024 / 1024, 2) if size else "Unknown",
                    }
                )
        unique_resolutions = list({v["resolution"]: v for v in resolutions}.values())

        subtitles = info.get("subtitles", {})
        auto_captions = info.get("automatic_captions", {})
        subtitle_languages = list(set(subtitles.keys()) | set(auto_captions.keys()))

        return {
            "is_playlist": False,
            "title": info.get("title"),
            "channel": info.get("channel"),
            "channel_url": info.get("channel_url"),
            "duration": seconds_to_hms(total_duration),
            "size_mb": size_mb,
            "has_subtitle": bool(subtitle_languages),
            "subtitle_languages": subtitle_languages,
            "resolutions": unique_resolutions,
            "thumbnail": info.get("thumbnail"),
            "webpage_url": info.get("webpage_url"),
        }
    except Exception as e:
        logger.error(f"info | URL: {url} | Error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/download/", summary="Unduhan Video YouTube")
async def download_video(
    background_tasks: BackgroundTasks,
    url: str = Query(...),
    resolution: int = Query(720),
    mode: str = Query("url"),
):
    if mode not in ["url", "buffer"]:
        return JSONResponse(status_code=400, content={"error": "Mode unduhan tidak valid. Gunakan 'url' atau 'buffer'."})
    try:
        ydl_opts = {
            "format": f"bestvideo[height<={resolution}][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "outtmpl": os.path.join(OUTPUT_DIR, "%(title)s_%(height)sp.%(ext)s"),
            "cookiefile": COOKIES_FILE,
            "merge_output_format": "mp4",
            "quiet": True,
            "noplaylist": True,
            "remote_components": ["ejs:github"]
        }
        info = await ytdlp_extract(ydl_opts, url, download=True)

        def _prepare_name():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.prepare_filename(info)
        file_path = await asyncio.to_thread(_prepare_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File tidak ditemukan setelah unduhan: {file_path}")

        background_tasks.add_task(delete_file_after_delay, file_path)

        if mode == "url":
            return {
                "title": info["title"],
                "thumbnail": info.get("thumbnail"),
                "download_url": f"https://ytdlpyton.nvlgroup.my.id/download/file/{quote(os.path.basename(file_path))}",
            }

        with open(file_path, "rb") as f:
            video_buffer = io.BytesIO(f.read())
        return StreamingResponse(
            video_buffer,
            media_type="video/mp4",
            headers={"Content-Disposition": f"attachment; filename={os.path.basename(file_path)}"},
        )
    except yt_dlp.utils.DownloadError as e:
        logger.error(f"download | URL: {url} | yt_dlp Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
    except Exception as e:
        logger.error(f"download | URL: {url} | General Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/download/ytsub", summary="Unduh video dengan subtitle digabung")
async def download_with_subtitle(
    background_tasks: BackgroundTasks,
    url: str = Query(...),
    resolution: int = Query(720),
    lang: str = Query("id"),
    mode: str = Query("url"),
):
    if mode != "url":
        return JSONResponse(status_code=400, content={"error": "Hanya mode 'url' yang didukung untuk endpoint ini."})
    try:
        result = {}

        async def _do_download():
            ydl_info_opts = {"quiet": True, "cookiefile": COOKIES_FILE}
            info = await ytdlp_extract(ydl_info_opts, url, download=False)
            title = sanitize_filename(info.get("title", "video"))
            filename_base = f"ytsubbynvl-{title}-{resolution}p-{lang}"
            final_filename = f"{filename_base}.mp4"
            final_filepath = os.path.join(OUTPUT_DIR, final_filename)

            if os.path.exists(final_filepath):
                file_size_mb = round(os.path.getsize(final_filepath) / (1024 * 1024), 2)
                result.update(
                    {
                        "title": title,
                        "thumbnail": info.get("thumbnail"),
                        "size_mb": file_size_mb,
                        "download_url": f"https://ytdlpyton.nvlgroup.my.id/download/file/{quote(final_filename)}",
                    }
                )
                background_tasks.add_task(delete_file_after_delay, final_filepath)
                return

            ydl_opts = {
                "quiet": True,
                "cookiefile": COOKIES_FILE,
                "writesubtitles": True,
                "writeautomaticsub": True,
                "subtitleslangs": [lang],
                "skip_download": False,
                "outtmpl": os.path.join(OUTPUT_DIR, filename_base + ".%(ext)s"),
                "format": f"bestvideo[height<={resolution}][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]",
                "merge_output_format": "mp4",
                "postprocessors": [
                    {"key": "FFmpegSubtitlesConvertor", "format": "srt"},
                    {"key": "FFmpegVideoConvertor", "preferedformat": "mp4"},
                    "remote_components": ["ejs:github"]
                ],
            }
            await ytdlp_extract(ydl_opts, url, download=True)

            raw_filepath = os.path.join(OUTPUT_DIR, f"{filename_base}.mp4")
            subtitle_path = os.path.join(OUTPUT_DIR, f"{filename_base}.{lang}.srt")
            burned_filepath = os.path.join(OUTPUT_DIR, f"{filename_base}.burned.mp4")

            if os.path.exists(subtitle_path):
                proc = await asyncio.create_subprocess_exec(
                    "ffmpeg",
                    "-y",
                    "-i",
                    raw_filepath,
                    "-vf",
                    f"subtitles={subtitle_path}:force_style=FontName=Arial,FontSize=24,OutlineColour=&H80000000,BorderStyle=3,Outline=1,Shadow=0",
                    "-c:v",
                    "libx264",
                    "-preset",
                    "faster",
                    "-crf",
                    "27",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "96k",
                    burned_filepath,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                await proc.communicate()
                try:
                    os.remove(raw_filepath)
                except FileNotFoundError:
                    pass
                os.replace(burned_filepath, final_filepath)
            else:
                os.replace(raw_filepath, final_filepath)

            if os.path.exists(final_filepath):
                file_size_mb = round(os.path.getsize(final_filepath) / (1024 * 1024), 2)
                result.update(
                    {
                        "title": title,
                        "thumbnail": info.get("thumbnail"),
                        "size_mb": file_size_mb,
                        "download_url": f"https://ytdlpyton.nvlgroup.my.id/download/file/{quote(final_filename)}",
                    }
                )
                background_tasks.add_task(delete_file_after_delay, final_filepath)

        await _do_download()
        if not result:
            raise FileNotFoundError("Gagal mengunduh dan menggabungkan subtitle.")
        return result
    except Exception as e:
        logger.error(f"download/with-sub | URL: {url} | Error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/download/audio/", summary="Unduhan Audio YouTube (Max 45 menit, dikonversi ke MP3)")
async def download_audio(
    background_tasks: BackgroundTasks,
    url: str = Query(...),
    mode: str = Query("url"),
    bitrate: int = Query(128, ge=32, le=320, description="Bitrate MP3 kbps"),
):
    if mode not in ["url", "buffer"]:
        return JSONResponse(status_code=400, content={"error": "Mode unduhan tidak valid. Gunakan 'url' atau 'buffer'."})
    try:
        # Get info first to check duration
        info_opts = {
            "quiet": True,
            "cookiefile": COOKIES_FILE,
            "no_warnings": True,
        }
        info = await ytdlp_extract(info_opts, url, download=False)

        # Check duration (45 minutes = 2700 seconds)
        duration = info.get("duration", 0)
        MAX_DURATION = 45 * 60  # 45 minutes in seconds

        if duration and duration > MAX_DURATION:
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"Audio terlalu panjang. Maksimal 45 menit. Durasi video: {duration // 60} menit {duration % 60} detik"
                }
            )

        # Download audio
        out_template = os.path.join(OUTPUT_DIR, "%(title)s_audio_src.%(ext)s")
        ydl_opts = {
            "outtmpl": out_template,
            "format": "bestaudio/best",
            "cookiefile": COOKIES_FILE,
            "prefer_ffmpeg": True,
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "remote_components": ["ejs:github"]
        }
        info = await ytdlp_extract(ydl_opts, url, download=True)

        src_ext = info.get("ext", "webm")
        src_path = os.path.join(OUTPUT_DIR, f"{sanitize_filename(info['title'])}_audio_src.{src_ext}")
        mp3_filename = f"{sanitize_filename(info['title'])}_audio_downloadbynauval.mp3"
        mp3_path = os.path.join(OUTPUT_DIR, mp3_filename)

        # Find source file
        if not os.path.exists(src_path):
            candidates = [
                os.path.join(OUTPUT_DIR, f"{sanitize_filename(info['title'])}_audio_src.{src_ext}"),
                os.path.join(OUTPUT_DIR, f"{info['title']}_audio_src.{src_ext}"),
            ]
            src_path = next((p for p in candidates if os.path.exists(p)), None)

        if not src_path or not os.path.exists(src_path):
            return JSONResponse(status_code=500, content={"error": "File sumber audio tidak ditemukan setelah unduhan."})

        # Always convert to MP3
        logger.info(f"Converting audio to MP3: {src_path} -> {mp3_path}")

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-i", src_path,
            "-vn",
            "-ar", "44100",
            "-ac", "2",
            "-b:a", f"{bitrate}k",
            mp3_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()

        # Cleanup source file
        try:
            os.remove(src_path)
        except Exception:
            pass

        if not os.path.exists(mp3_path):
            return JSONResponse(status_code=500, content={"error": "Konversi MP3 gagal."})

        file_size = os.path.getsize(mp3_path)
        background_tasks.add_task(delete_file_after_delay, mp3_path)

        if mode == "url":
            return JSONResponse(
                content={
                    "title": info["title"],
                    "filesize": file_size,
                    "bitrate": bitrate,
                    "duration_seconds": duration,
                    "format": "MP3",
                    "download_url": f"https://ytdlpyton.nvlgroup.my.id/download/file/{quote(os.path.basename(mp3_path))}",
                    "status": "Success",
                }
            )

        with open(mp3_path, "rb") as f:
            audio_buffer = io.BytesIO(f.read())
        return StreamingResponse(
            audio_buffer,
            media_type="audio/mpeg",
            headers={"Content-Disposition": f"attachment; filename={os.path.basename(mp3_path)}"},
        )
    except yt_dlp.utils.DownloadError as e:
        logger.error(f"download/audio | URL: {url} | yt_dlp Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
    except Exception as e:
        logger.error(f"download/audio | URL: {url} | General Error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/download/playlist", summary="Unduhan Playlist YouTube")
async def download_playlist(
    background_tasks: BackgroundTasks,
    url: str = Query(...),
    limit: int = Query(5, ge=1),
    resolution: Union[Literal["audio"], int] = Query("720"),
    mode: str = Query("url"),
):
    if mode != "url":
        return JSONResponse(status_code=400, content={"error": "Mode tidak didukung. Gunakan mode 'url'."})
    try:
        is_audio_only = resolution == "audio"
        if is_audio_only:
            ydl_format = "bestaudio"
            merge_output = None
        else:
            video_resolution = int(resolution)
            ydl_format = f"bestvideo[height<={video_resolution}][ext=mp4]+bestaudio/best[ext=mp4]/best"
            merge_output = "mp4"

        ydl_opts = {
            "quiet": True,
            "cookiefile": COOKIES_FILE,
            "extract_flat": False,
            "playlistend": limit,
            "outtmpl": os.path.join(OUTPUT_DIR, "%(title)s.%(ext)s"),
            "format": ydl_format,
            "noplaylist": False,
            "remote_components": ["ejs:github"]
        }
        if merge_output:
            ydl_opts["merge_output_format"] = merge_output

        downloaded_files: list[dict] = []

        # Fully async download process
        async def _download():
            temp_cookies = await create_temp_cookies_file()
            try:
                ydl_opts_copy = ydl_opts.copy()
                ydl_opts_copy["cookiefile"] = temp_cookies

                def _run():
                    with yt_dlp.YoutubeDL(ydl_opts_copy) as ydl:
                        info = ydl.extract_info(url, download=True)
                        entries = info.get("entries", [])
                        result = []
                        for idx, entry in enumerate(entries, start=1):
                            filepath = ydl.prepare_filename(entry)
                            if os.path.exists(filepath):
                                result.append({
                                    "index": idx,
                                    "title": entry.get("title"),
                                    "filepath": filepath,
                                })
                        return result

                return await asyncio.to_thread(_run)
            finally:
                await cleanup_temp_cookies(temp_cookies)

        results = await _download()
        for item in results:
            filepath = item.pop("filepath")
            item["download_url"] = f"https://ytdlpyton.nvlgroup.my.id/download/file/{quote(os.path.basename(filepath))}"
            downloaded_files.append(item)
            background_tasks.add_task(delete_file_after_delay, filepath)

        return {"playlist_title": f"Download hasil playlist dari: {url}", "total_videos": len(downloaded_files), "videos": downloaded_files}
    except Exception as e:
        logger.error(f"playlist | URL: {url} | Error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

async def get_spotify_access_token():
    global _spotify_token, _spotify_token_expiry
    async with _spotify_lock:
        now = time.time()
        if _spotify_token and now < _spotify_token_expiry - 30:
            return _spotify_token

        auth_str = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
        b64_auth = base64.b64encode(auth_str.encode()).decode()
        headers = {"Authorization": f"Basic {b64_auth}"}
        data = {"grant_type": "client_credentials"}

        client: httpx.AsyncClient = app.state.http
        resp = await client.post(SPOTIFY_TOKEN_URL, headers=headers, data=data)
        if resp.status_code != 200:
            raise Exception(f"Gagal mendapatkan token: {resp.text}")
        payload = resp.json()
        _spotify_token = payload["access_token"]
        _spotify_token_expiry = time.time() + int(payload.get("expires_in", 3600))
        return _spotify_token

@app.get("/spotify/search", summary="Cari lagu di Spotify (dengan client ID)")
async def spotify_search(query: str = Query(..., description="Judul lagu atau artis")):
    try:
        token = await get_spotify_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        params = {"q": query, "type": "track", "limit": 5}
        client: httpx.AsyncClient = app.state.http
        resp = await client.get(f"{SPOTIFY_API_URL}/search", headers=headers, params=params)
        data = resp.json()
        tracks = data.get("tracks", {}).get("items", [])
        results = []
        for track in tracks:
            results.append(
                {
                    "spotify_url": track["external_urls"]["spotify"],
                    "title": track["name"],
                    "artist": ", ".join(artist["name"] for artist in track["artists"]),
                }
            )
        return {"query": query, "results": results}
    except Exception as e:
        logger.error(f"spotify_search | Query: {query} | Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/spotify/info", summary="Info lengkap Spotify URL (track/album/playlist/show/radio)")
async def spotify_info(url: str = Query(..., description="URL Spotify track, album, playlist, show, atau radio")):
    try:
        token = await get_spotify_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        if "track" in url:
            spotify_type = "track"
        elif "album" in url:
            spotify_type = "album"
        elif "playlist" in url:
            spotify_type = "playlist"
        elif "show" in url:
            spotify_type = "show"
        elif "radio" in url:
            spotify_type = "radio"
        else:
            return JSONResponse(status_code=400, content={"error": "URL tidak dikenali. Harus track, album, playlist, show, atau radio."})

        spotify_id = url.split("/")[-1].split("?")[0]
        endpoint = f"{SPOTIFY_API_URL}/{spotify_type}s/{spotify_id}"

        client: httpx.AsyncClient = app.state.http
        resp = await client.get(endpoint, headers=headers)
        if resp.status_code != 200:
            return JSONResponse(status_code=resp.status_code, content={"error": resp.text})
        data = resp.json()

        result = {
            "type": spotify_type,
            "title": data.get("name"),
            "url": data.get("external_urls", {}).get("spotify", url),
            "thumbnail": (data.get("images") or [{}])[0].get("url"),
            "is_album": spotify_type == "album",
            "is_playlist": spotify_type == "playlist",
            "is_show": spotify_type == "show",
            "is_radio": spotify_type == "radio",
        }

        if spotify_type == "track":
            result.update(
                {
                    "artist": ", ".join(artist["name"] for artist in data["artists"]),
                    "duration": round(data.get("duration_ms", 0) / 1000),
                    "album": data["album"]["name"] if data.get("album") else None,
                }
            )
        elif spotify_type == "album":
            result["artist"] = ", ".join(artist["name"] for artist in data["artists"])
            result["total_tracks"] = data.get("total_tracks", len(data.get("tracks", {}).get("items", [])))
            tracks = [
                {
                    "title": track["name"],
                    "artist": ", ".join(artist["name"] for artist in track["artists"]),
                    "spotify_url": track["external_urls"]["spotify"],
                }
                for track in data.get("tracks", {}).get("items", [])
            ]
            result["tracks"] = tracks
        elif spotify_type == "playlist":
            result["owner"] = data.get("owner", {}).get("display_name")
            tracks = []
            items = data.get("tracks", {}).get("items", [])
            next_url = data.get("tracks", {}).get("next")
            for item in items:
                track = item.get("track")
                if track:
                    tracks.append(
                        {
                            "title": track["name"],
                            "artist": ", ".join(artist["name"] for artist in track["artists"]),
                            "spotify_url": track["external_urls"]["spotify"],
                        }
                    )
            client = app.state.http
            while next_url:
                next_resp = await client.get(next_url, headers=headers)
                next_data = next_resp.json()
                for item in next_data.get("items", []):
                    track = item.get("track")
                    if track:
                        tracks.append(
                            {
                                "title": track["name"],
                                "artist": ", ".join(artist["name"] for artist in track["artists"]),
                                "spotify_url": track["external_urls"]["spotify"],
                            }
                        )
                next_url = next_data.get("next")
            result["total_tracks"] = len(tracks)
            result["tracks"] = tracks
        elif spotify_type == "show":
            result["publisher"] = data.get("publisher")
            result["description"] = data.get("description")
        elif spotify_type == "radio":
            result["owner"] = data.get("owner", {}).get("display_name")
            result["description"] = data.get("description")
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/spotify/download/audio", summary="Unduh audio dari Spotify track/show/episode (via YouTube)")
async def spotify_download_from_track_show_episode(
    background_tasks: BackgroundTasks,
    url: str = Query(..., description="URL Spotify track, show, atau episode"),
    mode: str = Query("url"),
    bitrate: int = Query(128, ge=32, le=320),
):
    if "track" not in url and "show" not in url and "episode" not in url:
        return JSONResponse(status_code=400, content={"error": "Hanya mendukung URL Spotify track, show, atau episode."})
    try:
        token = await get_spotify_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        spotify_id = url.split("/")[-1].split("?")[0]
        if "track" in url:
            endpoint = f"{SPOTIFY_API_URL}/tracks/{spotify_id}"
        elif "show" in url:
            endpoint = f"{SPOTIFY_API_URL}/shows/{spotify_id}"
        else:
            endpoint = f"{SPOTIFY_API_URL}/episodes/{spotify_id}"

        client: httpx.AsyncClient = app.state.http
        resp = await client.get(endpoint, headers=headers)
        if resp.status_code != 200:
            return JSONResponse(status_code=resp.status_code, content={"error": resp.text})
        data = resp.json()
        title = data["name"]
        artist = ", ".join(a["name"] for a in data.get("artists", [])) if "artists" in data else "Unknown Artist"
        search_query = f"{title} {artist} audio"

        ydl_opts = {
            "quiet": True,
            "cookiefile": COOKIES_FILE,
            "format": "bestaudio/best",
            "outtmpl": os.path.join(OUTPUT_DIR, "%(title)s_spotify_by_nauval.%(ext)s"),
            "prefer_ffmpeg": True,
            "noplaylist": True,
            "no_warnings": True,
            "remote_components": ["ejs:github"]
        }
        info = await ytdlp_extract(ydl_opts, f"ytsearch1:{search_query}", download=False)
        entry = info["entries"][0] if "entries" in info else info

        src_title = sanitize_filename(entry["title"])
        src_out = os.path.join(OUTPUT_DIR, f"{src_title}_spotify_src.%(ext)s")
        dl_opts = ydl_opts | {"outtmpl": src_out}
        await ytdlp_extract(dl_opts, entry["webpage_url"], download=True)

        src_candidates = [
            os.path.join(OUTPUT_DIR, f"{src_title}_spotify_src.webm"),
            os.path.join(OUTPUT_DIR, f"{src_title}_spotify_src.m4a"),
            os.path.join(OUTPUT_DIR, f"{src_title}_spotify_src.mp3"),
        ]
        src_path = next((p for p in src_candidates if os.path.exists(p)), None)
        if not src_path:
            return JSONResponse(status_code=500, content={"error": "Audio sumber tidak ditemukan."})

        output_filename = f"{src_title}_spotify_by_nauval.mp3"
        file_path = os.path.join(OUTPUT_DIR, output_filename)

        proc = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-i",
            src_path,
            "-vn",
            "-ar",
            "44100",
            "-ac",
            "2",
            "-b:a",
            f"{bitrate}k",
            file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.communicate()
        try:
            os.remove(src_path)
        except Exception:
            pass

        if not os.path.exists(file_path):
            raise FileNotFoundError("File hasil konversi tidak ditemukan.")

        background_tasks.add_task(delete_file_after_delay, file_path)
        if mode == "url":
            return {
                "title": title,
                "artist": artist,
                "bitrate": bitrate,
                "thumbnail": entry.get("thumbnail"),
                "download_url": f"https://ytdlpyton.nvlgroup.my.id/download/file/{quote(output_filename)}",
            }

        with open(file_path, "rb") as f:
            audio_buffer = io.BytesIO(f.read())
        return StreamingResponse(
            audio_buffer,
            media_type="audio/mpeg",
            headers={"Content-Disposition": f"attachment; filename={os.path.basename(file_path)}"},
        )
    except Exception as e:
        logger.error(f"spotify_download_audio | URL: {url} | Error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/spotify/download/playlist", summary="Unduh playlist Spotify jadi MP3 (via YouTube)")
async def spotify_download_playlist_audio(
    background_tasks: BackgroundTasks,
    url: str = Query(..., description="URL playlist Spotify"),
    limit: int = Query(10, ge=1, le=50, description="Jumlah maksimal lagu yang diunduh (1–50)"),
    mode: str = Query("url", description="Saat ini hanya mendukung mode 'url'"),
    bitrate: int = Query(128, ge=32, le=320),
):
    if "playlist" not in url:
        return JSONResponse(status_code=400, content={"error": "Hanya URL playlist Spotify yang didukung."})
    if mode != "url":
        return JSONResponse(status_code=400, content={"error": "Mode saat ini hanya mendukung 'url'."})
    try:
        token = await get_spotify_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        spotify_id = url.split("/")[-1].split("?")[0]
        endpoint = f"{SPOTIFY_API_URL}/playlists/{spotify_id}"
        client: httpx.AsyncClient = app.state.http
        resp = await client.get(endpoint, headers=headers)
        if resp.status_code != 200:
            return JSONResponse(status_code=resp.status_code, content={"error": resp.text})
        data = resp.json()

        playlist_title = data.get("name", "Spotify Playlist")
        tracks_data = data.get("tracks", {}).get("items", [])
        next_url = data.get("tracks", {}).get("next")

        all_tracks = []
        while len(all_tracks) < limit:
            for item in tracks_data:
                track = item.get("track")
                if track:
                    all_tracks.append({"title": track["name"], "artist": ", ".join(artist["name"] for artist in track["artists"])})
                if len(all_tracks) >= limit:
                    break
            if next_url and len(all_tracks) < limit:
                next_resp = await client.get(next_url, headers=headers)
                next_data = next_resp.json()
                tracks_data = next_data.get("items", [])
                next_url = next_data.get("next")
            else:
                break

        ydl_opts = {
            "quiet": True,
            "cookiefile": COOKIES_FILE,
            "format": "bestaudio/best",
            "outtmpl": os.path.join(OUTPUT_DIR, "%(title)s_spotify_playlist_src.%(ext)s"),
            "prefer_ffmpeg": True,
            "noplaylist": True,
            "no_warnings": True,
            "remote_components": ["ejs:github"]
        }

        downloaded: list[dict] = []

        async def _download_all_async():
            temp_cookies = await create_temp_cookies_file()
            try:
                ydl_opts_copy = ydl_opts.copy()
                ydl_opts_copy["cookiefile"] = temp_cookies

                def _extract_and_download():
                    with yt_dlp.YoutubeDL(ydl_opts_copy) as ydl:
                        results = []
                        for idx, track in enumerate(all_tracks, start=1):
                            query = f"{track['title']} {track['artist']} audio"
                            try:
                                info = ydl.extract_info(f"ytsearch1:{query}", download=False)
                                entry = info["entries"][0] if "entries" in info else info
                                base = sanitize_filename(entry["title"])
                                src_out = os.path.join(OUTPUT_DIR, f"{base}_spotify_playlist_src.%(ext)s")
                                ydl.params["outtmpl"] = src_out
                                ydl.download([entry["webpage_url"]])

                                candidates = [
                                    os.path.join(OUTPUT_DIR, f"{base}_spotify_playlist_src.webm"),
                                    os.path.join(OUTPUT_DIR, f"{base}_spotify_playlist_src.m4a"),
                                    os.path.join(OUTPUT_DIR, f"{base}_spotify_playlist_src.mp3"),
                                ]
                                src_path = next((p for p in candidates if os.path.exists(p)), None)
                                if src_path:
                                    results.append({
                                        "idx": idx,
                                        "track": track,
                                        "src_path": src_path,
                                        "base": base,
                                    })
                            except Exception as e:
                                logger.warning(f"Gagal unduh lagu: {query} | Error: {e}")
                        return results

                download_results = await asyncio.to_thread(_extract_and_download)

                # Convert to MP3 using async subprocess
                for result in download_results:
                    out_name = f"{result['base']}_spotify_playlist.mp3"
                    out_path = os.path.join(OUTPUT_DIR, out_name)

                    proc = await asyncio.create_subprocess_exec(
                        "ffmpeg", "-y", "-i", result['src_path'], "-vn", "-ar", "44100",
                        "-ac", "2", "-b:a", f"{bitrate}k", out_path,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await proc.communicate()

                    try:
                        os.remove(result['src_path'])
                    except Exception:
                        pass

                    if os.path.exists(out_path):
                        downloaded.append({
                            "index": result['idx'],
                            "title": result['track']["title"],
                            "artist": result['track']["artist"],
                            "download_url": f"https://ytdlpyton.nvlgroup.my.id/download/file/{quote(out_name)}",
                        })
            finally:
                await cleanup_temp_cookies(temp_cookies)

        await _download_all_async()
        for item in downloaded:
            filename = item["download_url"].split("/file/")[-1]
            background_tasks.add_task(delete_file_after_delay, os.path.join(OUTPUT_DIR, filename))

        return {"playlist": playlist_title, "total_downloaded": len(downloaded), "tracks": downloaded}
    except Exception as e:
        logger.error(f"spotify_download_playlist | URL: {url} | Error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/spotify/fullplaylist", summary="Unduh full playlist Spotify (MP3) dengan opsi ZIP")
async def spotify_full_playlist_download(
    background_tasks: BackgroundTasks,
    url: str = Query(..., description="URL Spotify playlist"),
    limit: int = Query(10, ge=1, le=50),
    mode: str = Query("zip", description="Mode: url, zip"),
    bitrate: int = Query(128, ge=32, le=320),
):
    if "playlist" not in url:
        return JSONResponse(status_code=400, content={"error": "Hanya URL playlist Spotify yang didukung."})
    try:
        token = await get_spotify_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        spotify_id = url.split("/")[-1].split("?")[0]
        endpoint = f"{SPOTIFY_API_URL}/playlists/{spotify_id}"
        client: httpx.AsyncClient = app.state.http
        resp = await client.get(endpoint, headers=headers)
        if resp.status_code != 200:
            return JSONResponse(status_code=resp.status_code, content={"error": resp.text})
        data = resp.json()

        playlist_title = data.get("name", "Spotify Playlist")
        tracks_data = data.get("tracks", {}).get("items", [])
        next_url = data.get("tracks", {}).get("next")
        all_tracks = []

        while len(all_tracks) < limit:
            for item in tracks_data:
                track = item.get("track")
                if track:
                    all_tracks.append({"title": track["name"], "artist": ", ".join(artist["name"] for artist in track["artists"])})
                if len(all_tracks) >= limit:
                    break
            if next_url and len(all_tracks) < limit:
                next_resp = await client.get(next_url, headers=headers)
                next_data = next_resp.json()
                tracks_data = next_data.get("items", [])
                next_url = next_data.get("next")
            else:
                break

        ydl_opts = {
            "quiet": True,
            "cookiefile": COOKIES_FILE,
            "format": "bestaudio/best",
            "outtmpl": os.path.join(OUTPUT_DIR, "%(title)s_spotifyfull_src.%(ext)s"),
            "prefer_ffmpeg": True,
            "noplaylist": True,
            "no_warnings": True,
            "remote_components": ["ejs:github"]
        }

        downloaded_files: list[str] = []

        async def _download_tracks_async():
            temp_cookies = await create_temp_cookies_file()
            try:
                ydl_opts_copy = ydl_opts.copy()
                ydl_opts_copy["cookiefile"] = temp_cookies

                def _extract_and_download():
                    with yt_dlp.YoutubeDL(ydl_opts_copy) as ydl:
                        results = []
                        for track in all_tracks:
                            query = f"{track['title']} {track['artist']} audio"
                            try:
                                info = ydl.extract_info(f"ytsearch1:{query}", download=False)
                                entry = info["entries"][0] if "entries" in info else info
                                base = sanitize_filename(entry["title"])
                                src_out = os.path.join(OUTPUT_DIR, f"{base}_spotifyfull_src.%(ext)s")
                                ydl.params["outtmpl"] = src_out
                                ydl.download([entry["webpage_url"]])

                                candidates = [
                                    os.path.join(OUTPUT_DIR, f"{base}_spotifyfull_src.webm"),
                                    os.path.join(OUTPUT_DIR, f"{base}_spotifyfull_src.m4a"),
                                    os.path.join(OUTPUT_DIR, f"{base}_spotifyfull_src.mp3"),
                                ]
                                src_path = next((p for p in candidates if os.path.exists(p)), None)
                                if src_path:
                                    results.append({
                                        "src_path": src_path,
                                        "base": base,
                                    })
                            except Exception as e:
                                logger.warning(f"Gagal unduh: {query} | Error: {e}")
                        return results

                download_results = await asyncio.to_thread(_extract_and_download)

                # Convert to MP3 using async subprocess
                for result in download_results:
                    out_path = os.path.join(OUTPUT_DIR, f"{result['base']}_spotifyfull.mp3")

                    proc = await asyncio.create_subprocess_exec(
                        "ffmpeg", "-y", "-i", result['src_path'], "-vn", "-ar", "44100",
                        "-ac", "2", "-b:a", f"{bitrate}k", out_path,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    await proc.communicate()

                    try:
                        os.remove(result['src_path'])
                    except Exception:
                        pass

                    if os.path.exists(out_path):
                        downloaded_files.append(out_path)
            finally:
                await cleanup_temp_cookies(temp_cookies)

        await _download_tracks_async()

        if mode == "url":
            for f in downloaded_files:
                background_tasks.add_task(delete_file_after_delay, f)
            return {
                "playlist": playlist_title,
                "mode": "url",
                "total_downloaded": len(downloaded_files),
                "files": [f"https://ytdlpyton.nvlgroup.my.id/download/file/{quote(os.path.basename(f))}" for f in downloaded_files],
            }

        if mode == "zip":
            zip_name = f"{sanitize_filename(playlist_title)}_spotify.zip"
            zip_path = os.path.join(OUTPUT_DIR, zip_name)

            def _zip():
                with ZipFile(zip_path, "w") as zipf:
                    for file in downloaded_files:
                        zipf.write(file, arcname=os.path.basename(file))
            await asyncio.to_thread(_zip)
            background_tasks.add_task(delete_file_after_delay, zip_path)
            for f in downloaded_files:
                background_tasks.add_task(delete_file_after_delay, f)

            return {"playlist": playlist_title, "mode": "zip", "download_zip": f"https://ytdlpyton.nvlgroup.my.id/download/file/{quote(zip_name)}"}

        return JSONResponse(status_code=400, content={"error": f"Mode tidak dikenali: {mode}"})
    except Exception as e:
        logger.error(f"spotify_fullplaylist | URL: {url} | Error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/stats", summary="Melihat statistik server")
async def server_stats():
    try:
        uptime_seconds = int(time.time() - server_start_time)
        uptime_string = str(timedelta(seconds=uptime_seconds))

        cpu_freq = psutil.cpu_freq()
        virtual_mem = psutil.virtual_memory()
        disk = psutil.disk_usage("/")
        load_avg = os.getloadavg() if hasattr(os, "getloadavg") else (0, 0, 0)

        return {
            "uptime": uptime_string,
            "uptime_seconds": uptime_seconds,
            "total_request": total_request_count,
            "cpu_usage_percent": psutil.cpu_percent(interval=None),
            "cpu_cores_logical": psutil.cpu_count(logical=True),
            "cpu_cores_physical": psutil.cpu_count(logical=False),
            "cpu_frequency_mhz": {
                "current": cpu_freq.current if cpu_freq else None,
                "min": cpu_freq.min if cpu_freq else None,
                "max": cpu_freq.max if cpu_freq else None,
            },
            "ram": {
                "total_mb": round(virtual_mem.total / 1024 / 1024, 2),
                "available_mb": round(virtual_mem.available / 1024 / 1024, 2),
                "used_percent": virtual_mem.percent,
            },
            "disk": {
                "total_gb": round(disk.total / 1024 / 1024 / 1024, 2),
                "used_gb": round(disk.used / 1024 / 1024 / 1024, 2),
                "free_gb": round(disk.free / 1024 / 1024 / 1024, 2),
                "used_percent": disk.percent,
            },
            "load_average": {"1_min": load_avg[0], "5_min": load_avg[1], "15_min": load_avg[2]},
            "platform": platform.platform(),
            "python_version": platform.python_version(),
        }
    except Exception as e:
        logger.error(f"server_stats | Error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/download/file/{filename}", summary="Mengunduh file hasil")
async def download_file(filename: str):
    file_path = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(file_path):
        return FileResponse(file_path, filename=filename)
    return JSONResponse(status_code=404, content={"error": "File tidak ditemukan"})
