# FBREF Football Stats Scraper

This repository contains a github action that scrapes data from [FBREF](www.fbref.com) and uploads it to Google BigQuery. The repository contains 2 workflows. The primary one scrapes 22/23 data for Europe's Top 5 leagues. Feel free to change the leagues or seasons scraped. Currently the actions is manually triggered but can be edited to run similar to a CRON job as well.

Note: `fbref.py` contains an edited version of the FBref scraper from [ScraperFC](https://github.com/oseymour/ScraperFC)

### How to use 
1. Create an account and a project with [Google BigQuery](https://cloud.google.com/bigquery)
2. Follow the steps listed [here](https://github.com/google-github-actions/auth) to set up authentication using Workload Identity Federation 
3. Clone this repository 
4. Create the following repository secrets
- GCP_WIP: Your workload identity provider from step 2
- GCP_SERVICE_ACCOUNT: Your service account from step 2 
- GCP_PROJECT_NAME: Your Bigquery project name from step 1

Now, you should have everything setup and the action should run succesfully. 

### Disclaimer 
Please do not abuse the services and data provided by the folks at FBRef. Only scrape when you need and only the data you require. 
