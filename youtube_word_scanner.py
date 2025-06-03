#!/usr/bin/env python3
import os
import sys
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi


def search_videos(query, api_key, max_results=5):
    youtube = build('youtube', 'v3', developerKey=api_key)
    request = youtube.search().list(
        q=query,
        part='id',
        type='video',
        maxResults=max_results
    )
    response = request.execute()
    return [item['id']['videoId'] for item in response.get('items', [])]


def find_word_in_video(video_id, word):
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id)
    except Exception:
        return []
    results = []
    for entry in transcript:
        if word.lower() in entry['text'].lower():
            timestamp = int(entry['start'])
            url = f"https://www.youtube.com/watch?v={video_id}&t={timestamp}s"
            results.append((url, timestamp))
    return results


def main():
    if len(sys.argv) != 3:
        print("Usage: python youtube_word_scanner.py \"search term\" \"word\"")
        return
    query, word = sys.argv[1], sys.argv[2]
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        print("YOUTUBE_API_KEY environment variable not set")
        return
    video_ids = search_videos(query, api_key)
    for vid in video_ids:
        matches = find_word_in_video(vid, word)
        for url, ts in matches:
            print(f"{url}")


if __name__ == '__main__':
    main()
