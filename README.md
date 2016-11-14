# Airbnb
[Kaggle - Where will a new guest book their first travel experience?] (https://www.kaggle.com/c/airbnb-recruiting-new-user-bookings)

## Introudction
The goal of this competition was to predict where a new Airbnb user will make their first booking, using demographic data, web session records and some further summary statistics. <br> 
There were 12 possible destinations and the evaluation metric for the competition was [Normalised Discounted Cumulative Gain @ 5](https://www.kaggle.com/c/airbnb-recruiting-new-user-bookings/details/evaluation).

## Performance
The solution obtained a rank of [31st out of 1462 competitors] (https://www.kaggle.com/c/airbnb-recruiting-new-user-bookings/leaderboard/private).

## Execution
1) Create a working directory for the project <br>
2) [Download the data from Kaggle] (https://www.kaggle.com/c/airbnb-recruiting-new-user-bookings/data) and place in the working directory. <br> The files required are: `train_users_2.csv`, `test_users.csv` and `sessions.csv`. <br>
3) Run `users_sessions.py` <br>
4) Run `xgb_bag.R`

## Requirements
* MySQL
* Python 3+
* R+


