import requests
import csv
import time
import re
import os
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Comprehensive list of all countries with App Store presence
COUNTRIES = [
    ('Afghanistan', 'af'),
    ('Albania', 'al'),
    ('Algeria', 'dz'),
    ('Angola', 'ao'),
    ('Anguilla', 'ai'),
    ('Antigua and Barbuda', 'ag'),
    ('Argentina', 'ar'),
    ('Armenia', 'am'),
    ('Australia', 'au'),
    ('Austria', 'at'),
    ('Azerbaijan', 'az'),
    ('Bahamas', 'bs'),
    ('Bahrain', 'bh'),
    ('Barbados', 'bb'),
    ('Belarus', 'by'),
    ('Belgium', 'be'),
    ('Belize', 'bz'),
    ('Benin', 'bj'),
    ('Bermuda', 'bm'),
    ('Bhutan', 'bt'),
    ('Bolivia', 'bo'),
    ('Botswana', 'bw'),
    ('Brazil', 'br'),
    ('British Virgin Islands', 'vg'),
    ('Brunei', 'bn'),
    ('Bulgaria', 'bg'),
    ('Burkina Faso', 'bf'),
    ('Cambodia', 'kh'),
    ('Canada', 'ca'),
    ('Cape Verde', 'cv'),
    ('Cayman Islands', 'ky'),
    ('Chad', 'td'),
    ('Chile', 'cl'),
    ('China', 'cn'),
    ('Colombia', 'co'),
    ('Costa Rica', 'cr'),
    ('Croatia', 'hr'),
    ('Cyprus', 'cy'),
    ('Czech Republic', 'cz'),
    ('Denmark', 'dk'),
    ('Dominica', 'dm'),
    ('Dominican Republic', 'do'),
    ('Ecuador', 'ec'),
    ('Egypt', 'eg'),
    ('El Salvador', 'sv'),
    ('Estonia', 'ee'),
    ('Fiji', 'fj'),
    ('Finland', 'fi'),
    ('France', 'fr'),
    ('Gambia', 'gm'),
    ('Germany', 'de'),
    ('Ghana', 'gh'),
    ('Greece', 'gr'),
    ('Grenada', 'gd'),
    ('Guatemala', 'gt'),
    ('Guinea-Bissau', 'gw'),
    ('Guyana', 'gy'),
    ('Honduras', 'hn'),
    ('Hong Kong', 'hk'),
    ('Hungary', 'hu'),
    ('Iceland', 'is'),
    ('India', 'in'),
    ('Indonesia', 'id'),
    ('Ireland', 'ie'),
    ('Israel', 'il'),
    ('Italy', 'it'),
    ('Jamaica', 'jm'),
    ('Japan', 'jp'),
    ('Jordan', 'jo'),
    ('Kazakhstan', 'kz'),
    ('Kenya', 'ke'),
    ('Korea', 'kr'),
    ('Kuwait', 'kw'),
    ('Kyrgyzstan', 'kg'),
    ('Laos', 'la'),
    ('Latvia', 'lv'),
    ('Lebanon', 'lb'),
    ('Liberia', 'lr'),
    ('Lithuania', 'lt'),
    ('Luxembourg', 'lu'),
    ('Macau', 'mo'),
    ('Macedonia', 'mk'),
    ('Madagascar', 'mg'),
    ('Malawi', 'mw'),
    ('Malaysia', 'my'),
    ('Maldives', 'mv'),
    ('Mali', 'ml'),
    ('Malta', 'mt'),
    ('Mauritania', 'mr'),
    ('Mauritius', 'mu'),
    ('Mexico', 'mx'),
    ('Micronesia', 'fm'),
    ('Moldova', 'md'),
    ('Mongolia', 'mn'),
    ('Montenegro', 'me'),
    ('Montserrat', 'ms'),
    ('Morocco', 'ma'),
    ('Mozambique', 'mz'),
    ('Namibia', 'na'),
    ('Nauru', 'nr'),
    ('Nepal', 'np'),
    ('Netherlands', 'nl'),
    ('New Zealand', 'nz'),
    ('Nicaragua', 'ni'),
    ('Niger', 'ne'),
    ('Nigeria', 'ng'),
    ('Norway', 'no'),
    ('Oman', 'om'),
    ('Pakistan', 'pk'),
    ('Palau', 'pw'),
    ('Panama', 'pa'),
    ('Papua New Guinea', 'pg'),
    ('Paraguay', 'py'),
    ('Peru', 'pe'),
    ('Philippines', 'ph'),
    ('Poland', 'pl'),
    ('Portugal', 'pt'),
    ('Qatar', 'qa'),
    ('Romania', 'ro'),
    ('Russia', 'ru'),
    ('Saudi Arabia', 'sa'),
    ('Senegal', 'sn'),
    ('Serbia', 'rs'),
    ('Seychelles', 'sc'),
    ('Sierra Leone', 'sl'),
    ('Singapore', 'sg'),
    ('Slovakia', 'sk'),
    ('Slovenia', 'si'),
    ('Solomon Islands', 'sb'),
    ('South Africa', 'za'),
    ('Spain', 'es'),
    ('Sri Lanka', 'lk'),
    ('St. Kitts and Nevis', 'kn'),
    ('St. Lucia', 'lc'),
    ('St. Vincent and The Grenadines', 'vc'),
    ('Suriname', 'sr'),
    ('Swaziland', 'sz'),
    ('Sweden', 'se'),
    ('Switzerland', 'ch'),
    ('Taiwan', 'tw'),
    ('Tajikistan', 'tj'),
    ('Tanzania', 'tz'),
    ('Thailand', 'th'),
    ('Trinidad and Tobago', 'tt'),
    ('Tunisia', 'tn'),
    ('Turkey', 'tr'),
    ('Turkmenistan', 'tm'),
    ('Turks and Caicos', 'tc'),
    ('Uganda', 'ug'),
    ('Ukraine', 'ua'),
    ('United Arab Emirates', 'ae'),
    ('United Kingdom', 'gb'),
    ('United States', 'us'),
    ('Uruguay', 'uy'),
    ('Uzbekistan', 'uz'),
    ('Venezuela', 've'),
    ('Vietnam', 'vn'),
    ('Yemen', 'ye'),
    ('Zambia', 'zm'),
    ('Zimbabwe', 'zw'),
]

APPS = [
    {'id': '1010631459', 'display_id': 'id1010631459'},
    {'id': '1547222690', 'display_id': 'id1547222690'},
    {'id': '448142450', 'display_id': 'id448142450'},
    {'id': '1510944943', 'display_id': 'id1510944943'},
    {'id': '6695760755', 'display_id': 'id6695760755'},
]

ITUNES_LOOKUP_URL = 'https://itunes.apple.com/lookup'
RSS_TOP_FREE_TEMPLATE = 'https://itunes.apple.com/{country}/rss/topfreeapplications/limit=200/genre={genre_id}/json'

APP_METADATA_CACHE = {}
OUTPUT_FILENAME = 'AppRanks.csv'
CSV_FIELDNAMES = [
    'fetch_date',
    'id',
    'app_name',
    'country',
    'country_code',
    'rank',
    'category',
    'rating',
    'ratings_count',
    'price',
    'currency',
    'formatted_price',
    'url',
    'status'
]

def ensure_app_metadata(app_id):
    if app_id in APP_METADATA_CACHE:
        return APP_METADATA_CACHE[app_id]
    
    fallback = {
        'name': f'App {app_id}',
        'default_url': f'https://apps.apple.com/app/id{app_id}'
    }
    
    try:
        resp = requests.get(ITUNES_LOOKUP_URL, params={'id': app_id}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get('results'):
            info = data['results'][0]
            fallback = {
                'name': info.get('trackName', fallback['name']),
                'default_url': info.get('trackViewUrl', fallback['default_url'])
            }
    except (requests.exceptions.RequestException, ValueError):
        pass
    
    APP_METADATA_CACHE[app_id] = fallback
    return fallback

def get_app_ranking(app_entry, country_name, country_code, fetch_date):
    app_id = app_entry['id']
    display_id = app_entry['display_id']
    metadata = ensure_app_metadata(app_id)
    app_name = metadata['name']
    country_app_url = f"https://apps.apple.com/{country_code}/app/{display_id}"
    
    lookup_params = {
        'id': app_id,
        'country': country_code
    }
    
    try:
        lookup_resp = requests.get(ITUNES_LOOKUP_URL, params=lookup_params, timeout=15)
        lookup_resp.raise_for_status()
        try:
            lookup_data = lookup_resp.json()
        except ValueError:
            print(f"✗ {app_name} ({country_name}): Failed to parse lookup response")
            return {
                'fetch_date': fetch_date,
                'id': display_id,
                'app_name': app_name,
                'country': country_name,
                'country_code': country_code,
                'rank': 'Error',
                'category': 'N/A',
                'rating': 'N/A',
                'ratings_count': 'N/A',
                'price': 'N/A',
                'currency': 'N/A',
                'formatted_price': 'N/A',
                'url': country_app_url,
                'status': 'Lookup JSON parse error'
            }

        if lookup_data.get('resultCount', 0) == 0:
            print(f"⊘ {app_name} ({country_name}): App not available")
            return {
                'fetch_date': fetch_date,
                'id': display_id,
                'app_name': app_name,
                'country': country_name,
                'country_code': country_code,
                'rank': 'Not Available',
                'category': 'N/A',
                'rating': 'N/A',
                'ratings_count': 'N/A',
                'price': 'N/A',
                'currency': 'N/A',
                'formatted_price': 'N/A',
                'url': country_app_url,
                'status': 'App not available in this region'
            }

        app_info = lookup_data['results'][0]
        app_name = app_info.get('trackName', app_name)
        # Refresh cache with the new name/url if available
        APP_METADATA_CACHE[app_id] = {
            'name': app_name,
            'default_url': app_info.get('trackViewUrl', metadata['default_url'])
        }

        category = app_info.get('primaryGenreName', 'N/A')
        genre_id = app_info.get('primaryGenreId')
        if not genre_id:
            genre_ids = app_info.get('genreIds')
            if isinstance(genre_ids, (list, tuple)) and genre_ids:
                genre_id = genre_ids[0]
        rating_value = app_info.get('averageUserRating')
        rating_count = app_info.get('userRatingCount')
        track_url = app_info.get('trackViewUrl', country_app_url)
        price_value = app_info.get('price')
        currency_code = app_info.get('currency')
        formatted_price_value = app_info.get('formattedPrice')

        rating_str = f"{rating_value:.2f}" if isinstance(rating_value, (int, float)) else 'N/A'
        if isinstance(rating_count, int):
            ratings_count_str = f"{rating_count:,}"
        else:
            ratings_count_str = 'N/A'
        
        if isinstance(price_value, (int, float)):
            price_str = f"{price_value:.2f}"
        else:
            price_str = 'N/A'
        
        currency_str = currency_code if isinstance(currency_code, str) and currency_code else 'N/A'
        
        if isinstance(formatted_price_value, str) and formatted_price_value:
            formatted_price_str = formatted_price_value
        elif price_str != 'N/A' and currency_str != 'N/A':
            formatted_price_str = f"{price_str} {currency_str}"
        else:
            formatted_price_str = 'N/A'

        rank_text = 'Not ranked (Top 200)'
        status = 'Success'

        if genre_id:
            chart_url = RSS_TOP_FREE_TEMPLATE.format(country=country_code, genre_id=genre_id)
            try:
                chart_resp = requests.get(chart_url, timeout=15)
                chart_resp.raise_for_status()
                chart_data = chart_resp.json()
                entries = chart_data.get('feed', {}).get('entry', [])
                if isinstance(entries, dict):
                    entries = [entries]
                for index, entry in enumerate(entries, start=1):
                    entry_id = entry.get('id', {}).get('attributes', {}).get('im:id')
                    if entry_id == app_id:
                        rank_text = f"#{index} in {category}"
                        break
            except (requests.exceptions.RequestException, ValueError):
                status = 'Partial success (chart fetch failed)'
        else:
            status = 'Partial success (missing genre id)'

        result = {
            'fetch_date': fetch_date,
            'id': display_id,
            'app_name': app_name,
            'country': country_name,
            'country_code': country_code,
            'rank': rank_text,
            'category': category if category else 'N/A',
            'rating': rating_str,
            'ratings_count': ratings_count_str,
            'price': price_str,
            'currency': currency_str,
            'formatted_price': formatted_price_str,
            'url': track_url,
            'status': status
        }
        
        print(f"✓ {app_name} ({country_name}): {result['rank']}")
        return result
        
    except requests.exceptions.Timeout:
        print(f"⏱ {app_name} ({country_name}): Request timeout")
        return {
            'fetch_date': fetch_date,
            'id': display_id,
            'app_name': app_name,
            'country': country_name,
            'country_code': country_code,
            'rank': 'Timeout',
            'category': 'N/A',
            'rating': 'N/A',
            'ratings_count': 'N/A',
            'price': 'N/A',
            'currency': 'N/A',
            'formatted_price': 'N/A',
            'url': country_app_url,
            'status': 'Request timeout'
        }
    except requests.exceptions.ConnectionError:
        print(f"⚠ {app_name} ({country_name}): Connection error")
        return {
            'fetch_date': fetch_date,
            'id': display_id,
            'app_name': app_name,
            'country': country_name,
            'country_code': country_code,
            'rank': 'Connection Error',
            'category': 'N/A',
            'rating': 'N/A',
            'ratings_count': 'N/A',
            'price': 'N/A',
            'currency': 'N/A',
            'formatted_price': 'N/A',
            'url': country_app_url,
            'status': 'Connection error'
        }
    except requests.exceptions.RequestException as e:
        print(f"✗ {app_name} ({country_name}): Request error - {str(e)[:50]}")
        return {
            'fetch_date': fetch_date,
            'id': display_id,
            'app_name': app_name,
            'country': country_name,
            'country_code': country_code,
            'rank': 'Error',
            'category': 'N/A',
            'rating': 'N/A',
            'ratings_count': 'N/A',
            'price': 'N/A',
            'currency': 'N/A',
            'formatted_price': 'N/A',
            'url': country_app_url,
            'status': f'Error: {str(e)[:100]}'
        }
    except Exception as e:
        print(f"✗ {app_name} ({country_name}): Unexpected error - {str(e)[:50]}")
        return {
            'fetch_date': fetch_date,
            'id': display_id,
            'app_name': app_name,
            'country': country_name,
            'country_code': country_code,
            'rank': 'Error',
            'category': 'N/A',
            'rating': 'N/A',
            'ratings_count': 'N/A',
            'price': 'N/A',
            'currency': 'N/A',
            'formatted_price': 'N/A',
            'url': country_app_url,
            'status': f'Unexpected error: {str(e)[:100]}'
        }

def main():
    print("=" * 70)
    print(" " * 11 + "App Store Rankings Scraper (Multi-App)")
    print("=" * 70)
    
    total_countries = len(COUNTRIES)
    total_apps = len(APPS)
    total_checks = total_countries * total_apps
    fetch_date = datetime.utcnow().strftime('%Y-%m-%d')
    
    print(f"\n🌍 Checking {total_countries} countries for {total_apps} apps ({total_checks} combinations)...\n")
    print(f"🗓  Fetch date (UTC): {fetch_date}\n")
    
    print("🎯 Target apps:")
    for app in APPS:
        meta = ensure_app_metadata(app['id'])
        print(f"  • {meta['name']} ({app['display_id']})")
    print()
    
    results = []
    
    # Use ThreadPoolExecutor for parallel requests (faster)
    # Adjust max_workers based on your needs (lower = slower but more polite)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(get_app_ranking, app, country[0], country[1], fetch_date)
            for app in APPS
            for country in COUNTRIES
        ]
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            # Small delay to be polite to the server
            time.sleep(0.05)
    
    # Sort results by rank (extract number from rank string)
    def get_rank_number(result):
        rank_str = result['rank']
        match = re.search(r'#(\d+)', rank_str)
        if match:
            return int(match.group(1))
        return float('inf')  # Put non-ranked items at the end
    
    results.sort(key=lambda r: (r['id'], get_rank_number(r), r['country']))
    
    # Save to CSV
    csv_filename = OUTPUT_FILENAME
    fieldnames = CSV_FIELDNAMES
    file_exists = os.path.exists(csv_filename)
    append_mode = False
    existing_rows = []
    
    if file_exists:
        with open(csv_filename, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_header = reader.fieldnames or []
            if existing_header == fieldnames:
                append_mode = True
            else:
                for row in reader:
                    normalized_row = {field: row.get(field, 'N/A') for field in fieldnames}
                    if normalized_row['price'] in (None, ''):
                        normalized_row['price'] = 'N/A'
                    if normalized_row['currency'] in (None, ''):
                        normalized_row['currency'] = 'N/A'
                    if normalized_row['formatted_price'] in (None, ''):
                        normalized_row['formatted_price'] = 'N/A'
                    existing_rows.append(normalized_row)
    
    write_mode = 'a' if append_mode else 'w'
    with open(csv_filename, write_mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not append_mode:
            writer.writeheader()
            for row in existing_rows:
                writer.writerow(row)
        elif not file_exists:
            writer.writeheader()
        
        for result in results:
            writer.writerow(result)
    
    print("\n" + "=" * 70)
    if not file_exists:
        action = "created"
    elif append_mode:
        action = "appended to"
    else:
        action = "updated"
    print(f"✓ Data {action} '{csv_filename}'")
    print("=" * 70)
    
    # Print detailed summary
    print("\n📊 Summary Statistics:")
    print("-" * 70)
    
    print(f"  📱 Total checks completed: {len(results)}")
    
    results_by_app = defaultdict(list)
    for result in results:
        results_by_app[result['id']].append(result)
    
    for app in APPS:
        app_results = results_by_app.get(app['display_id'], [])
        meta = APP_METADATA_CACHE.get(app['id'], {'name': f'App {app["id"]}'})
        app_name = app_results[0]['app_name'] if app_results else meta['name']
        
        if app_results:
            ranked_count = sum(1 for r in app_results if '#' in r['rank'])
            not_available_count = sum(1 for r in app_results if 'Not Available' in r['rank'])
            error_count = sum(1 for r in app_results if r['rank'] in ['Error', 'Timeout', 'Connection Error', 'Access Forbidden'])
            not_ranked_count = sum(1 for r in app_results if 'Not ranked' in r['rank'])
        else:
            ranked_count = not_available_count = error_count = not_ranked_count = 0
        
        print(f"\n🔍 {app_name} ({app['display_id']}):")
        print("-" * 70)
        print(f"  📱 Countries checked: {len(app_results)}")
        print(f"  🏆 Countries with rankings: {ranked_count}")
        print(f"  ⊘  App not available: {not_available_count}")
        print(f"  📊 Available but not ranked: {not_ranked_count}")
        print(f"  ⚠️  Errors/Timeouts: {error_count}")
        
        ranked_entries = [r for r in app_results if '#' in r['rank']]
        ranked_entries.sort(key=lambda r: get_rank_number(r))
        
        if ranked_entries:
            print("\n  🏆 Top 20 Rankings:")
            print("  " + "-" * 66)
            for i, result in enumerate(ranked_entries[:20], 1):
                rating_info = f"({result['rating']}★, {result['ratings_count']} ratings)" if result['rating'] != 'N/A' else ""
                print(f"   {i:2d}. {result['country']:<25} {result['rank']:<25} {rating_info}")
        
        not_available_entries = [r for r in app_results if 'Not Available' in r['rank']]
        if not_available_entries:
            print(f"\n  ⊘ Countries where app is NOT available ({len(not_available_entries)}):")
            print("  " + "-" * 66)
            countries_list = [r['country'] for r in not_available_entries]
            for i in range(0, len(countries_list), 3):
                row = countries_list[i:i+3]
                print("    " + " | ".join(f"{c:<25}" for c in row))
    
    print("\n" + "=" * 70)
    print(f"✓ Done! Check '{csv_filename}' for the growing history.")
    print("=" * 70)

if __name__ == "__main__":
    main()
