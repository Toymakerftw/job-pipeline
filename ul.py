import requests
from bs4 import BeautifulSoup

def scrape_jobs():
    base_url = "https://www.ulcyberpark.com/jobs/index"
    current_url = base_url
    all_jobs = []
    
    headers = {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/58.0.3029.110 Safari/537.3')
    }
    
    while current_url:
        try:
            response = requests.get(current_url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table_div = soup.find('div', class_='table-responsive-sm table-job')
        if not table_div:
            print("No job table found on the page.")
            break
        
        table = table_div.find('table', class_='table')
        if not table:
            print("No table found in the div.")
            break
        
        for row in table.find_all('tr'):
            tds = row.find_all('td')
            if len(tds) < 3:
                continue
            
            # Extract job title
            job_title_elem = tds[0].find('a', class_='btn-1')
            job_title = job_title_elem.text.strip() if job_title_elem else 'N/A'
            
            # Extract closing date
            closing_date_elem = tds[0].find('span')
            if closing_date_elem:
                closing_date = closing_date_elem.text.split('closing date: ')[-1].strip()
            else:
                closing_date = 'N/A'
            
            # Extract company name and apply link
            company_elem = tds[1].find('a', class_='btn-1')
            company_name = company_elem.text.strip() if company_elem else 'N/A'
            apply_link = company_elem.get('href', 'N/A') if company_elem else 'N/A'
            
            # Extract details URL
            details_elem = tds[2].find('a')
            details_url = details_elem.get('href', 'N/A') if details_elem else 'N/A'
            
            all_jobs.append({
                'job_title': job_title,
                'closing_date': closing_date,
                'company_name': company_name,
                'apply_link': apply_link,
                'details_url': details_url
            })
        
        # ----- Pagination Handling -----
        # Try to locate a pagination container (it could be a <ul> or <section>)
        pagination = soup.find(lambda tag: tag.name in ['ul', 'section'] and 
                                 tag.get('class') and any('pagination' in cls for cls in tag.get('class')))
        next_link = None
        
        if pagination:
            # First try to find an anchor with rel="next"
            next_a = pagination.find('a', rel='next')
            
            # If no "next" anchor, try to locate the current active page and get the next sibling link
            if not next_a:
                active_li = pagination.find('li', class_='active')
                if active_li:
                    next_li = active_li.find_next_sibling('li')
                    if next_li:
                        next_a = next_li.find('a')
            
            if next_a and next_a.get('href'):
                next_link = next_a['href']
                # If next_link is a relative URL, combine it with the base URL
                if not next_link.startswith('http'):
                    next_link = requests.compat.urljoin(base_url, next_link)
        
        current_url = next_link  # will be None if no next page is found
        
        # Optional: delay to avoid hammering the server (uncomment if needed)
        # import time
        # time.sleep(1)
    
    return all_jobs

if __name__ == "__main__":
    jobs = scrape_jobs()
    print(f"Scraped {len(jobs)} jobs:")
    for idx, job in enumerate(jobs, 1):
        print(f"\nJob {idx}:")
        print(f"Title: {job['job_title']}")
        print(f"Company: {job['company_name']}")
        print(f"Closing Date: {job['closing_date']}")
        print(f"Apply Link: {job['apply_link']}")
        print(f"Details URL: {job['details_url']}")
