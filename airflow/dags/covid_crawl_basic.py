# URL="https://www.worldometers.info/coronavirus/#main_table"

# # Initialize dummy first date for comparing latest data updated date.
# FIRST_DATE = '2022-04-28 00:00'

# # Make a GET request to fetch the raw HTML content
# response = requests.get(URL).text

# soup = BeautifulSoup(response, 'lxml')

# last_updated_date_label = soup.find_all(text=re.compile("Last updated"))[0]
# last_updated_date_fulltext = last_updated_date_label.split(': ')[1]
# last_updated_date_str, timezone = last_updated_date_fulltext.rsplit(maxsplit=1)
# last_updated_date = datetime.strptime(last_updated_date_str, '%B %d, %Y, %H:%M').replace(tzinfo=pytz.timezone(timezone)) 
# last_updated_date_format = last_updated_date.strftime('%Y-%m-%d_%H:%M:%S')

# # convert html to pandas dataframe, source: https://python.tutorialink.com/a-problem-with-web-scraping-using-python-beautifulsoup-and-pandas-read_html/
# covid_live_df = pd.read_html(response, attrs={'id': 'main_table_countries_today'}, displayed_only=False)[0]
# covid_live_df['TotalCases'] = covid_live_df['TotalCases'].astype('float')
# covid_live_df['last_updated_date'] = last_updated_date
# covid_live_df.rename(columns={'1 Caseevery X ppl': 'one_case_every_x_ppl'}, inplace=True)
# covid_live_df.rename(columns={'1 Deathevery X ppl': 'one_death_every_x_ppl'}, inplace=True)
# covid_live_df.rename(columns={'1 Testevery X ppl': 'one_test_every_x_ppl'}, inplace=True)
# covid_live_df.info()

# # convert to parquet
# covid_live_df.to_parquet(f'covid_crawl_{last_updated_date_format}.parquet')


# # upload to gcs