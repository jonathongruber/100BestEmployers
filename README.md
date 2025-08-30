--Employer Stock Analysis--

This Python script  scrapes Forbes Top 100 Employers 2025 and Great Place To Work 100 Best Companies 2025, collects stock data, and exports the results to Excel.

Features:

- Scrapes Forbes and Great Place To Work company lists.

- Finds stock tickers via Yahoo Finance.

- Retrieves stock info: Name, Ticker, Price, Sector, Industry, Market Cap, PE Ratio.

- Identifies companies on both lists.

- Exports results to Excel with three sheets: Forbes, GPTW, and Common Employers.

How to use:
pip install requests beautifulsoup4 yfinance pandas tqdm xlsxwriter
python employer_stock_analysis.py


Excel file is saved to:
- Update this to your local drive.

Notes:

- Includes error handling and retry logic for stock data.

- Random delays and user-agent rotation to avoid being blocked.

- Some companies may not have a ticker or stock data.
