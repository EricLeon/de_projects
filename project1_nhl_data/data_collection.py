from data_scrapers import scrape_teams, scrape_players, scrape_stats


# Retrieve data from NHL API and store in PostgreSQL
scrape_teams()
scrape_players()
scrape_stats()
