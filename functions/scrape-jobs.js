const Parser = require('rss-parser');
const axios = require('axios');
const cheerio = require('cheerio');
const sqlite3 = require('sqlite3').verbose();
const { createClient } = require('@supabase/supabase-js');
const dotenv = require('dotenv');

// Load environment variables
dotenv.config();

// URLs
const CYBERPARK_RSS_URL = 'https://www.cyberparkkerala.org/?feed=job_feed';
const UL_CYBERPARK_URL = 'https://www.ulcyberpark.com/jobs/index';
const INFOPARK_URL = process.env.INFOPARK_URL || 'https://infopark.in/companies/job-search';
const TECHNOPARK_URL = process.env.TECHNOPARK_URL || 'https://technopark.org/api/paginated-jobs';

// Supabase
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;
const supabase = SUPABASE_URL && SUPABASE_KEY ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// Database
const DB_FILE = 'jobs.db';
function initDb() {
  const db = new sqlite3.Database(DB_FILE);
  db.serialize(() => {
    db.run(`
      CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company TEXT,
        role TEXT,
        deadline TEXT,
        link TEXT UNIQUE,
        tech_park TEXT,
        description TEXT,
        company_profile TEXT,
        email TEXT,
        status TEXT
      )
    `);
  });
  db.close();
  console.log('Database initialized.');
}

// Utilities
function extractEmails(text) {
  const emailPattern = /[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+/g;
  return text.match(emailPattern) || [];
}
function formatDescription(description) {
  return description.replace(/\s+/g, ' ').trim();
}
function uniformDate(deadline) {
  try {
    const date = new Date(deadline);
    return isNaN(date) ? deadline : date.toISOString().split('T')[0];
  } catch {
    return deadline;
  }
}
function getStatus(deadline) {
  try {
    const date = new Date(deadline);
    return isNaN(date) || date >= new Date() ? 'open' : 'closed';
  } catch {
    return 'open';
  }
}
async function fetch(url) {
  try {
    const response = await axios.get(url, { timeout: 30000 });
    return response.data;
  } catch (error) {
    console.error(`Error fetching ${url}:`, error.message);
    return null;
  }
}

// ----------- CYBERPARK & UL CYBERPARK -----------
const parser = new Parser();
async function parseRssFeed(url) {
  try {
    const feed = await parser.parseURL(url);
    return feed.items.map(entry => ({
      company: 'Cyberpark',
      role: entry.title,
      deadline: entry.pubDate,
      link: entry.link,
      tech_park: 'Cyberpark',
      description: entry.contentSnippet,
      company_profile: '',
      email: extractEmails(entry.contentSnippet)[0] || '',
      status: getStatus(entry.pubDate)
    }));
  } catch (error) {
    console.error('Error parsing RSS feed:', error);
    return [];
  }
}

async function scrapeUlJobs(baseUrl) {
  let currentUrl = baseUrl;
  const allJobs = [];
  const headers = {
    'User-Agent': 'Mozilla/5.0'
  };

  while (currentUrl) {
    try {
      const response = await axios.get(currentUrl, { headers });
      const $ = cheerio.load(response.data);
      const table = $('.table-responsive-sm.table-job table.table');
      if (!table.length) break;

      table.find('tr').each((_, row) => {
        const tds = $(row).find('td');
        if (tds.length < 3) return;

        const jobTitle = tds.eq(0).find('a.btn-1').text().trim() || 'N/A';
        const closingDate = tds.eq(0).find('span').text().split('closing date: ')[1]?.trim() || 'N/A';
        const companyName = tds.eq(1).find('a.btn-1').text().trim() || 'N/A';
        const applyLink = tds.eq(1).find('a.btn-1').attr('href') || 'N/A';
        const detailsUrl = tds.eq(2).find('a').attr('href') || 'N/A';

        allJobs.push({
          company: companyName,
          role: jobTitle,
          deadline: closingDate,
          link: detailsUrl,
          tech_park: 'UL Cyberpark',
          description: '',
          company_profile: '',
          email: '',
          status: getStatus(closingDate)
        });
      });

      const pagination = $('ul.pagination');
      let nextLink = pagination.find('a[rel="next"]').attr('href') || '';
      if (nextLink && !nextLink.startsWith('http')) {
        nextLink = new URL(nextLink, baseUrl).href;
      }
      currentUrl = nextLink || null;
    } catch (error) {
      console.error('Error scraping UL jobs:', error.message);
      break;
    }
  }

  return allJobs;
}

// ----------- INFOPARK & TECHNOPARK SCRAPING -----------
async function getInfoparkJobDetails(jobLink) {
  const html = await fetch(jobLink);
  if (!html) return { description: '', companyProfile: '' };

  const $ = cheerio.load(html);
  const description = $('.deatil-box').text().trim() || '';
  const companyId = jobLink.split('/').pop();
  const companyHtml = await fetch(`https://infopark.in/companies/profile/${companyId}`);
  let companyProfile = '';

  if (companyHtml) {
    const $company = cheerio.load(companyHtml);
    const carerBox = $company('.carer-box .con');
    if (carerBox.length) {
      const name = carerBox.find('h4').text().trim();
      const spans = carerBox.find('span');
      const address = spans.eq(0).text().trim();
      const phone = spans.eq(1).text().trim();
      const email = spans.eq(2).text().trim();
      const website = spans.eq(3).find('a').text().trim() || spans.eq(3).text().trim();
      companyProfile = `Company Name: ${name}\nAddress: ${address}\nPhone: ${phone}\nEmail: ${email}\nWebsite: ${website}`;
    }
  }

  return { description, companyProfile };
}

async function getTechnoparkJobDetails(jobLink) {
  const html = await fetch(jobLink);
  if (!html) return { description: '', companyProfile: '', email: '' };

  const $ = cheerio.load(html);
  const description = $('.mb-4.flex.w-full.flex-col.gap-8.pb-12.pt-10.lg\\:w-2\\/3').text().trim();
  const companySection = $('.w-full.border-b.px-8.pt-8.lg\\:w-1\\/3.lg\\:border-r.lg\\:border-b-0');

  const companyName = companySection.find('a.bodybold.text-theme_color_1').text().trim();
  const address = companySection.find('p.bodysmall').text().trim();
  const website = companySection.find('div.pt-4.pb-4 a').attr('href') || '';
  const companyProfile = `Company Name: ${companyName}\nAddress: ${address}\nWebsite: ${website}`;
  const emailTag = $('a[href^="mailto:"]');
  const email = emailTag.text().trim() || emailTag.attr('href')?.replace('mailto:', '').trim() || '';

  return { description, companyProfile, email };
}

async function scrapeJobs(baseUrl, techPark) {
  let page = 1;
  const allJobs = [];

  while (true) {
    const url = `${baseUrl}?page=${page}`;
    const data = await fetch(url);
    if (!data) break;

    let jobsInPage = [];

    if (techPark === 'Infopark') {
      const $ = cheerio.load(data);
      $('#job-list tbody tr').each((_, row) => {
        const role = $(row).find('td.head').text().trim();
        const company = $(row).find('td.date').text().trim();
        const deadline = $(row).find('td:nth-child(3)').text().trim();
        const link = $(row).find('td.btn-sec a').attr('href') || '';
        jobsInPage.push({ company, role, deadline, link });
      });
      if (!$('li.page-item a[rel="next"]').length) break;
    } else if (techPark === 'Technopark') {
      const json = JSON.parse(data);
      if (!json.data) break;
      jobsInPage = json.data.map(job => ({
        company: job.company.company,
        role: job.job_title,
        deadline: job.closing_date,
        link: `https://technopark.org/job-details/${job.id}`
      }));
      if (json.current_page >= json.last_page) break;
    }

    const jobDetails = await Promise.all(jobsInPage.map(async ({ company, role, deadline, link }) => {
      const details = techPark === 'Infopark'
        ? await getInfoparkJobDetails(link)
        : await getTechnoparkJobDetails(link);
      const email = techPark === 'Infopark' ? extractEmails(details.description)[0] || '' : details.email;
      return {
        company, role, deadline: uniformDate(deadline), link, tech_park: techPark,
        description: formatDescription(details.description),
        company_profile: details.companyProfile || '',
        email, status: getStatus(deadline)
      };
    }));

    allJobs.push(...jobDetails);
    page++;
  }

  return allJobs;
}

// Save to DB & Supabase
function saveJobsToDb(jobs) {
  const db = new sqlite3.Database(DB_FILE);
  db.serialize(() => {
    const stmt = db.prepare(`
      INSERT OR IGNORE INTO jobs (company, role, deadline, link, tech_park, description, company_profile, email, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    jobs.forEach(job => stmt.run([
      job.company, job.role, job.deadline, job.link,
      job.tech_park, job.description, job.company_profile,
      job.email, job.status
    ]));
    stmt.finalize();
  });
  db.close();
}

async function saveJobsToSupabase(jobs) {
  if (!supabase) return;
  const { error } = await supabase.from('jobs').insert(jobs);
  if (error) console.error('Supabase insert error:', error.message);
}

async function updateMissingEmails() {
  const db = new sqlite3.Database(DB_FILE);
  db.all(`SELECT * FROM jobs WHERE email IS NULL OR TRIM(email) = ''`, async (err, rows) => {
    if (err || !rows.length) return db.close();

    for (const job of rows) {
      const details = job.tech_park === 'Infopark'
        ? await getInfoparkJobDetails(job.link)
        : await getTechnoparkJobDetails(job.link);
      const email = job.tech_park === 'Infopark' ? extractEmails(details.description)[0] || '' : details.email;

      if (email) {
        db.run('UPDATE jobs SET email = ? WHERE link = ?', [email, job.link]);
        if (supabase) await supabase.from('jobs').update({ email }).eq('link', job.link);
      }
    }
    db.close();
  });
}

// MAIN
async function main() {
  initDb();
  const [rssJobs, ulJobs, infoparkJobs, technoparkJobs] = await Promise.all([
    parseRssFeed(CYBERPARK_RSS_URL),
    scrapeUlJobs(UL_CYBERPARK_URL),
    scrapeJobs(INFOPARK_URL, 'Infopark'),
    scrapeJobs(TECHNOPARK_URL, 'Technopark')
  ]);

  const allJobs = [...rssJobs, ...ulJobs, ...infoparkJobs, ...technoparkJobs];
  saveJobsToDb(allJobs);
  await saveJobsToSupabase(allJobs);
  await updateMissingEmails();
}

// Netlify Handler
exports.handler = async () => {
  try {
    await main();
    return { statusCode: 200, body: 'Job scraping completed successfully.' };
  } catch (error) {
    console.error('Error in scraping:', error);
    return { statusCode: 500, body: 'Job scraping failed.' };
  }
};
