# Google Places API Integration

This guide explains how to enable Google Places enrichment in the sourcerer scraper.

## Setup

### 1. Get a Google Places API Key

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. **Enable the Places API v1 only:**
   - Search for "Places API" in the APIs list
   - Click "Enable"
   - Do NOT enable the older "Places API (legacy)" or other Maps APIs initially
4. Create an API key:
   - Go to Credentials → Create Credentials → API Key
   - Copy the key
5. **Important:** Make sure the API key is unrestricted OR explicitly allows "Places API" in the API restrictions. Keys restricted to old API versions won't work.

### 2. Set Environment Variable

```bash
export GOOGLE_PLACES_API_KEY="your-api-key-here"
```

Or add to your `.env` file (ensure it's in `.gitignore`):

```
GOOGLE_PLACES_API_KEY=your-api-key-here
```

## Usage

The scraper will automatically use Google Places enrichment if the `GOOGLE_PLACES_API_KEY` environment variable is set. If not set, the scraper will warn and continue without Google Places enrichment.

```bash
./sourcerer -sources=rto,amtil,abr
```

## What Gets Enriched

For each business lead, the following fields are populated from Google Places:

- `google_places_id` - Unique identifier for future lookups
- `google_formatted_name` - Formatted business name
- `google_phone` - Phone number
- `google_email` - Email address (if available)
- `google_website` - Business website URL
- `google_formatted_addr` - Full formatted address
- `google_primary_type` - Primary business type classification

## How Matching Works

The enricher uses the business name to search Google Places, with address context (postcode/state) to improve accuracy:

- Searches: `"{Business Name} {Postcode} Australia"` (if postcode available)
- Falls back to: `"{Business Name} {State} Australia"` (if only state available)
- Falls back to: `"{Business Name} Australia"` (generic search)

The first (highest relevance) result is used.

## CSV Export

All Google Places fields are automatically included in the CSV export when you run the scraper.

## Costs

Google Places API has usage-based pricing. The scraper uses:
- **Text Search API**: $7 per 1000 requests
- **Place Details API**: $7 per 1000 requests

Typical cost per lead enriched: ~$0.014

Enable API quotas/budgets in Google Cloud Console to control spending.
