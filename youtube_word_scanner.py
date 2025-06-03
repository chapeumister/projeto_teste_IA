#!/usr/bin/env python3
"""Search YouTube videos for a word and print timestamps.

This script queries the YouTube Data API for videos matching a given search
string and scans each video's transcript for a target word. When the word is
found, it prints a direct URL to the timestamp of each occurrence.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List, Sequence, Tuple

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from youtube_transcript_api import YouTubeTranscriptApi


def search_videos(query: str, api_key: str, *, max_results: int = 5) -> List[str]:
    """Return a list of video IDs from a YouTube search."""

    youtube = build("youtube", "v3", developerKey=api_key)
    request = youtube.search().list(
        q=query,
        part="id",
        type="video",
        maxResults=max_results,
    )
    response = request.execute()
    return [item["id"]["videoId"] for item in response.get("items", [])]


def find_word_in_video(video_id: str, word: str) -> List[Tuple[str, int]]:
    """Return list of tuples (url, timestamp) where the word appears."""

    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
    except Exception:
        # Some videos don't have transcripts available
        return []

    results: List[Tuple[str, int]] = []
    lowered = word.lower()
    for entry in transcript:
        if lowered in entry["text"].lower():
            timestamp = int(entry["start"])
            url = f"https://www.youtube.com/watch?v={video_id}&t={timestamp}s"
            results.append((url, timestamp))
    return results


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point used by the CLI."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("query", help="YouTube search query")
    parser.add_argument("word", help="Word to look for in transcripts")
    parser.add_argument(
        "--max-results",
        type=int,
        default=5,
        help="Maximum number of videos to search (default: 5)",
    )
    parser.add_argument(
        "--api-key",
        help="YouTube Data API key (defaults to YOUTUBE_API_KEY env var)",
    )
    args = parser.parse_args(argv)

    api_key = args.api_key or os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        parser.error("YOUTUBE_API_KEY environment variable not set and --api-key not provided")

    try:
        video_ids = search_videos(args.query, api_key, max_results=args.max_results)
    except HttpError as exc:
        print(f"YouTube API error: {exc}", file=sys.stderr)
        return 1

    found = False
    for vid in video_ids:
        matches = find_word_in_video(vid, args.word)
        for url, ts in matches:
            found = True
            print(f"{url} (at {ts}s)")

    if not found:
        print("No occurrences found.")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
