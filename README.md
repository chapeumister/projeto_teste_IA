# YouTube Word Scanner

This project provides a simple Python script that searches YouTube transcripts for a specified word.

Given a search query and a target word, the script finds videos that match the query, extracts their transcripts, and reports the timestamp of each occurrence of the word along with a direct link to the video.

## Requirements
- Python 3.11+
- [youtube_transcript_api](https://pypi.org/project/youtube_transcript_api/)
- [google-api-python-client](https://pypi.org/project/google-api-python-client/)

Install dependencies with:
```bash
pip install -r requirements.txt
```

## Usage
Create an environment variable `YOUTUBE_API_KEY` with your API key or pass it
via the `--api-key` option. Run:
```bash
python youtube_word_scanner.py "search term" "word" --max-results 10
```
Results are printed to the console, including the exact timestamp for each
occurrence.
