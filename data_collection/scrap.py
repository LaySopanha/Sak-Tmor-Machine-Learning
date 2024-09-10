import requests
from bs4 import BeautifulSoup
import sqlite3

# URLs to scrape
urls = [
    'https://tourismcambodia.org/provinces/44/phnom-penh-capital-city',
    'https://tourismcambodia.org/provinces/45/rattanakiri',
    'https://tourismcambodia.org/provinces/46/mondulkiri',
    'https://tourismcambodia.org/provinces/47/siem-reap',
    'https://tourismcambodia.org/provinces/48/preah-sihanouk',
    'https://tourismcambodia.org/provinces/49/stung-treng',
    'https://tourismcambodia.org/provinces/50/kratie',
    'https://tourismcambodia.org/provinces/51/preah-vihear',
    'https://tourismcambodia.org/provinces/52/kampot',
    'https://tourismcambodia.org/provinces/53/kep',
    'https://tourismcambodia.org/provinces/54/koh-kong',
    'https://tourismcambodia.org/provinces/55/kampong-thom',
    'https://tourismcambodia.org/provinces/56/kandal',
    'https://tourismcambodia.org/provinces/57/takeo',
    'https://tourismcambodia.org/provinces/58/battambang',
    'https://tourismcambodia.org/provinces/59/kampong-cham',
    'https://tourismcambodia.org/provinces/60/kampong-chhnang',
    'https://tourismcambodia.org/provinces/61/kampong-speu',
    'https://tourismcambodia.org/provinces/62/pursat',
    'https://tourismcambodia.org/provinces/63/oddar-meanchey', 
    'https://tourismcambodia.org/provinces/64/pailin',
    'https://tourismcambodia.org/provinces/65/prey-veng',
    'https://tourismcambodia.org/provinces/66/svay-rieng',
    'https://tourismcambodia.org/provinces/67/banteay-meanchey',
    'https://tourismcambodia.org/provinces/245/tbong-khmum'
]

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('attractions.db')
cursor = conn.cursor()

# Create tables if they don't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS provinces (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS attractions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    province_id INTEGER,
    image_src TEXT,
    link TEXT,
    title TEXT,
    description TEXT,
    FOREIGN KEY (province_id) REFERENCES provinces(id)
)
''')

# Function to scrape data and insert into database
def scrape_and_store(url):
    province_id = url.split('/')[4]  # Extract province ID from URL
    province_name = url.split('/')[5].replace('-', ' ').title()  # Extract and format province name

    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Handle sections with dynamic content (hidden sections)
        hidden_sections = soup.find_all('section', style='display: none;')
        hidden_data = {}
        
        for section in hidden_sections:
            section_id = section.get('id')
            if section_id:
                hidden_data[section_id] = section

        # Find section with ID starting with 'section-'
        section = soup.find('section', id=lambda x: x and x.startswith('section-'))
        if section:
            # Insert or get province ID
            cursor.execute('''
            INSERT OR IGNORE INTO provinces (id, name)
            VALUES (?, ?)
            ''', (province_id, province_name))
            
            # Get the province ID
            cursor.execute('''
            SELECT id FROM provinces WHERE id = ?
            ''', (province_id,))
            province_id = cursor.fetchone()[0]

            for item in section.find_all('li', class_='media'):
                img_tag = item.find('img')
                link_tag = item.find('a', class_='view-more')
                title_tag = item.find('h4', class_='body-title')
                desc_tag = item.find('p')

                attraction = {
                    'image_src': img_tag['src'] if img_tag else None,
                    'link': link_tag['href'] if link_tag else None,
                    'title': title_tag.text.strip() if title_tag else None,
                    'description': desc_tag.text.strip() if desc_tag else None
                }
                
                # Handle description from hidden sections if available
                data_id = link_tag['data-id'] if link_tag else None
                if data_id:
                    hidden_section = hidden_data.get(f'section-{data_id}')
                    if hidden_section:
                        hidden_desc = hidden_section.find('p')
                        if hidden_desc:
                            attraction['description'] = hidden_desc.text.strip()
                
                # Insert data into database
                cursor.execute('''
                INSERT INTO attractions (province_id, image_src, link, title, description)
                VALUES (?, ?, ?, ?, ?)
                ''', (province_id, attraction['image_src'], attraction['link'], attraction['title'], attraction['description']))
                
            conn.commit()
        else:
            print(f"Attraction section not found in {url}.")
    else:
        print(f"Failed to retrieve the page {url}. Status code: {response.status_code}")

# Scrape all URLs
for url in urls:
    scrape_and_store(url)

# Close the database connection
conn.close()
