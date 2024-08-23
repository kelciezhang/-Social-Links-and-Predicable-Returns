### Social-Links-and-Predicable-Returns

It is always a hope of finance researchers and practitioners to understand the mystery of stock price dynamics and to forecast stock prices. This project attempts to use the board of directors data of listed companies to achieve this goal.

The theory underlying predictable stock prices is that the efficient market hypothesis (EMH) is not satisfied in practice. This means the stock prices do not reflect the information in the markets immediately, and hence some past information has predictive power for the future stock prices. These predictivity can be roughly characterized into two classes: the momentum effect and the reversal effect. The momentum effect represents a positive relationship between past and future stock prices, meaning that an upward price movement in the past suggests an upward movement in the future and vice versa, whereas the reversal effect defines the negative relationship. 

Among the numerous attempts, the data beyond traditional resources like company filings and financial statements, known as alternative data, demonstrates high potential in forecasting stock prices. As this kind of data is rarely available, the valuable information in it can slowly be incorporated into the stock prices. The board of directors data falls into this category. Luckily, a visionary company [Boardex](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4477466) sees the value in the data of executive directors and carefully collects it for years. We make use of their data. 

The project is motivated by the hypothesis that there is a cross-predictability momentum effect among companies that have an overlap of management team members in a certain time window. Specifically, if company A has a director who has worked in company B in the past few years, the stock prices of the two companies may establish some comovement and have predictive power over each other. The argument is twofold: first, there is a link between the fundamentals of the two companies that come through the shared directors. It could be expertise, private information, similar business, etc; second, the investors have limited attention to this link, and thus the similarity in the fundamentals along the social links reflects in one of the companies' stock price earlier than the other. In practice, we summarize this effect into a single quantity, the weighted sum of the stock prices of the related companies by the number of shared directors. We call it the social link momentum(SLM) factor.

I mainly contributed to writing the codes to construct such an SLM factor(this repository). If you are interested in the theories and experimental results, please refer to the [recent progress](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4477466) of the main authors for a more formal exposition and references. 

Next, I introduce the structure of the codes. 



Welcome comments, discussions, and corrections of any kind!
