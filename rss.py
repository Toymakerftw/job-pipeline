import feedparser

# URL of the RSS feed
rss_url = "https://www.cyberparkkerala.org/?feed=job_feed"

# Parse the RSS feed
feed = feedparser.parse(rss_url)

# Print the feed title
print("Feed Title:", feed.feed.title)

# Iterate through the entries in the feed
for entry in feed.entries:
    print("Title:", entry.title)
    print("Link:", entry.link)
    print("Published:", entry.published)
    print("Summary:", entry.summary)
    print("-" * 80)
