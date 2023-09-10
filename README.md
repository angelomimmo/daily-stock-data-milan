# Yahoo! Finance daily stock data retrieve using AWS Lambda
I have created an automated flow to retrieve daily data for the Milan stock market (using Python on AWS Lambda), compute specific KPIs and publish them on a (Tableau Public) dashboard. The idea was to create a zero-cost tool to asses investment opportunities on the Milan stock market. Apart from Python, all the resources used (AWS Lambda, Amazon DynamoDB, Google Drive and Tableau Public) are free of cost.

The project is divided in two Python scripts running daily, after the Milan stock markets closes, on AWS Lambda. They can be summarised in the following step:
1. Using the [stocksymbol](https://pypi.org/project/stocksymbol/) library and the [Stock Symbol API](https://stock-symbol.herokuapp.com), I retrieve the symbols listed on Milan stock market. This way, it is possible to automatically search only for the symbols currently listed.
2. I use the [yfinance](https://pypi.org/project/yfinance/) library to retrieve the close price data for the symbols selected and I perform data cleaning.
3. I write the data on a [Amazon DynamoDB](https://aws.amazon.com/it/dynamodb/) table. The job of the first AWS Lambda function ends here.
4. The second AWS Lambda function starts its job by retrieving again the symbols listed on the Milan stock market through the [stocksymbol](https://pypi.org/project/stocksymbol/) library. This is necessary to daily query on the [Amazon DynamoDB](https://aws.amazon.com/it/dynamodb/) table metioned previously only the symbols currently listed.
5. I compute my KPIs. In this case, I simply computed the delta between today price and specific time points (yesterday, one week ago, one month ago, 3 months ago, 6 months ago, 1 year ago and 3 years ago). Morever, I checked whether the today price for each symbol reached the minimum or the maximum in the time spans mentioned above.
6. The resulting table with the KPIs is written as a CSV file on Google Drive. Thus this file is updated daily and it is used as a live data source for the dashboard published on [Tableau Public](https://www.tableau.com/it-it/community/public)

The dashboard is not publicly given that it is only for personal usage but a screenshot is available below.

## Tableau Public dashboard

![dashboard](https://github.com/angelomimmo/daily-stock-data-milan/assets/144590879/0ea6e095-bda9-49fe-b75d-55faee70c6ae)


### Project diagram

![Schema](https://github.com/angelomimmo/daily-stock-data-milan/assets/144590879/b4719880-2b61-4c1d-8a60-aad4660261f4)


### AWS Lambda functions

![lambda_fun_1](https://github.com/angelomimmo/daily-stock-data-milan/assets/144590879/57a28853-9aa5-4258-89ea-34e719286b27)

![lambda_fun_2](https://github.com/angelomimmo/daily-stock-data-milan/assets/144590879/f878879d-46fd-47ae-af0c-557c53ecfc9a)

