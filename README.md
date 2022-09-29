# stonks
A Futile Attempt at Making Money.

Predicting the stock market based on price movement is hard, and it's not something I particularly want to learn. This project does not do stock market analysis in that way. Instead let's think of an easier way.

If someone says that the a certain company's stock price will go up, the likelihood of it going up increases. If someone says it will go down, the likelihood of it decreasing goes up. In other words, not only does the state of the market influence people's predictions, people's predictions have an influence on the stock market. I believe that most people underestimate the influence of the predicting party.

I am not the first to observe this. A perfect example is a "pump n' dump" scheme. In this scheme, someone makes claims all over the internet that a certain stock will skyrocket. They keep hidden the fact that they did no real analysis. They also conveniently just purchased a large quantity of said stock and plan on selling it as soon as their hype train gains a little momentum. This type of scheme has been successfully executed numerous times in the past.

It is not relevant whether or not there has been a real analysis or there exists information that indicates an increase in price. The point is that the bangwagon effect is real in a world with instantaneous communication, the internet, social media, news outlets, r/wallstreetbets, etc. We all saw what happened with GameStop right?

This code is designed to run daily, gathering ratings and recommendations from several different sources for hundreds of stocks. We then track how each stocks' price changes over the coming days. We push that data through a bunch of prediction models. Then when a new day bring new ratings, we can predict the price change!

EZPZ
