#Airbnb
#Where will a new guest book their first travel experience?

#Author: Liam Culligan
#Date: February 2016

#Create a bagged ensemble of multiple gradient boosted decision trees with different seeds

#Load required packages
library(RMySQL)
library(data.table)
library(magrittr)
library(bit64)
library(car)
library(Matrix)
library(xgboost)

#SQL queries to create features in R
#Determine the count of each action and action_detail for each user

#Set up a connection to a MySQL database hosted locally
con =  dbConnect(RMySQL :: MySQL(),
                 dbname = "airbnb", 
                 host = "localhost", 
                 user = "root",
                 password = "")

#Execute the query to reutrn the unique values of action from the table sessions
unique_action_execute = dbSendQuery(con, "SELECT DISTINCT action FROM sessions;")

#Fetch the record of the above query
unique_action = fetch(unique_action_execute, n = -1)

#Convert the resulting data frame to a vector
unique_action = unique_action$action

#Remove NULL action level
unique_action = na.omit(unique_action)

#Replace '-' with '_' for column names - MySQL doesn't allow '-' in column name
unique_action_colname = gsub("-", "_", unique_action)

#Use the resulting string vector to create SQL Query
count_action_query = "SELECT user_id"
for (i in 1:length(unique_action)) {
  count_action_query = paste0(count_action_query, ", SUM(IF(action = '", unique_action[i], "', 1, 0)) AS num_", unique_action_colname[i])
}

#Complete the query
count_action_query = paste0(count_action_query, " FROM sessions WHERE user_id IS NOT NULL GROUP BY user_id;")

#Execute the query to reutrn the count of each action for each user from the table sessions
count_action_execute = dbSendQuery(con, count_action_query)

#Fetch the record of the above query and convert the resulting data.frame to a data.table
count_action = fetch(count_action_execute, n = -1) %>% as.data.table()

#Save count_action (so that it is not necessary to run the SQL queriy in future)
save(count_action, file = "airbnb_count_action.rda", compress = T)

#Load count_action if SQL has been run during a previous R session
# load("airbnb_count_action.rda")

#Repeat similar procedure for action_detail column in sessions

#Execute the query to reutrn the unique values of action_detail from the table sessions
unique_action_detail_execute = dbSendQuery(con, "SELECT DISTINCT action_detail FROM sessions;")

#Fetch the record of the above query
unique_action_detail = fetch(unique_action_detail_execute, n = -1)

#Convert the resulting data frame to a vector
unique_action_detail = unique_action_detail$action_detail

#Remove NULL action level
unique_action_detail = na.omit(unique_action_detail)

#Replace '-' with '_' for column names - MySQL doesn't allow '-' in column name
unique_action_detail_colname = gsub("-", "_", unique_action_detail)

#Use the resulting string vector to create SQL Query
count_action_detail_query = "SELECT user_id"
for (i in 1:length(unique_action_detail)) {
  count_action_detail_query = paste0(count_action_detail_query, ", SUM(IF(action_detail = '", unique_action_detail[i], "', 1, 0)) AS num_",
                                     unique_action_detail_colname[i])
}

#Complete the query
count_action_detail_query = paste0(count_action_detail_query, " FROM sessions WHERE user_id IS NOT NULL GROUP BY user_id;")

#Execute the query to reutrn the count of each action_detail for each user from the table sessions
count_action_detail_execute = dbSendQuery(con, count_action_detail_query)

#Fetch the record of the above query and convert the resulting data.frame to a data.table
count_action_detail = fetch(count_action_detail_execute, n = -1) %>% as.data.table()

#Save count_action_detail (so that it is not necessary to run the SQL queries in future)
save(count_action_detail, file = "airbnb_count_action_detail.rda", compress = T)

#Load count_action_detail if SQL has been run during a previous R session
# load("airbnb_count_action_detail.rda")

#Import train_users and test_users from database
data = dbReadTable(con, "users") %>% as.data.table()

#Save data (so that it is not necessary to connect to the database if not desired)
save(data, file = "airbnb_data.rda", compress = T)

#Load data if SQL has been run during a previous R session
# load("airbnb_data.rda")

#Disconnect from MySQL database
dbDisconnect(con)

#Only consider users first active on and after 2014-01-01 as session data only available from this date (all test data has session data)
data = data[timestamp_first_active >= 20140101000000] 

#Set the keys of the datatables for joining
setkey(data, id)
setkey(count_action, user_id)
setkey(count_action_detail, user_id)

#Join train and test with count_action and count_action_detail
data = count_action[data]
rm(count_action)
gc()

data = count_action_detail[data]
rm(count_action_detail)
gc()

#Remove date_first_booking - all missing in test set
data[, date_first_booking := NULL]

#Convert date_account_created to Date format
data[, date_account_created := as.Date(date_account_created)]

#Convert timestamps to Date format
data[, timestamp_first_active := as.Date(paste(substring(as.character(timestamp_first_active), 1, 4), 
                                               substring(as.character(timestamp_first_active), 5, 6), 
                                               substring(as.character(timestamp_first_active), 7, 8), sep = "-"))]

#Replace NAs from session information with 0. 3043 users signed up and never returned.
replace_missing = function(DT, replace_value) {
  for (j in seq_len(ncol(DT)))
    set(DT,which(is.na(DT[[j]])),j ,replace_value)
}

replace_missing(data, 0)

#Separate the elements of the date_account_created column
data[, Month := as.integer(format(data$date_account_created, "%m"))]
data[, Year := as.integer(format(data$date_account_created, "%y"))]
data[, Day := as.integer(format(data$date_account_created, "%d"))]

#Having separated the elements, no longer require date_account_created
data[, date_account_created := NULL]

#Separate the elements of the timestamp_first_active column
data[, tfa_year := as.integer(format(data$timestamp_first_active, "%y"))]
data[, tfa_month := as.integer(format(data$timestamp_first_active, "%m"))]
data[, tfa_day := as.integer(format(data$timestamp_first_active, "%d"))]

#Having separated the elements, no longer require timestamp_first_active
data[, timestamp_first_active := NULL]

#Unrelastic ages seem present in data - replace these unrealistic values with missing (-1)
data[, age := ifelse(age <14 | age > 100, -1, age)]

#Obtain train and test row indices - needed to split sparse matrix
train_rows = data[, .I[country_destination != 0]]
test_rows = data[, .I[country_destination == 0]]

#Extract ids
train_user_id = data[country_destination != 0, user_id]
test_user_id = data[country_destination == 0, user_id]

#Remove ids from data
data[, user_id := NULL]

#Extract target vector - only for train, not test
country_destination = data[country_destination != 0, country_destination]
data[, country_destination := NULL]

#One-hot-encode character variables (which represent factors)

#Return all column names containing character variables
character_cols = sapply(data, class) == "character"
character_names = names(data)[character_cols]

#One-hot-encode data.table by selecting all columns except for those in character_names and cbind the result with a matrix applied to
#the character columns
data = cbind(data[, !character_names, with = F], model.matrix(~ gender + signup_method  + language + affiliate_channel + affiliate_provider +
                                                                first_affiliate_tracked + signup_app + first_device_type + first_browser - 1,
                                                              data))
#Make the column names suitable for R
names(data) = make.names(names(data))

#Split data back into train and test
train = data[train_rows ,]
test = data[test_rows ,]

#No longer require data
rm(data)
gc()

#Use car package to convert country_destination to numeric values - necessary for xgboost
country_destination = recode(country_destination,"'NDF'=0; 'US'=1; 'other'=2; 'FR'=3;
                             'CA'=4; 'GB'=5; 'ES'=6; 'IT'=7; 'PT'=8; 'NL'=9; 'DE'=10; 'AU'=11")

#Convert the feature set and target variable to an xgb.DMatrix - required as an input to xgboost
dtrain = xgb.DMatrix(data = data.matrix(train), label = country_destination, missing = -1)

#Function to calculate normalised discounted cumulative gain @ 5
ndcg5_eval = function(preds, dtrain) {
  
  labels = getinfo(dtrain,"label")
  num.class = 12
  pred = matrix(preds, nrow = num.class)
  top = t(apply(pred, 2, function(y) order(y)[num.class:(num.class-4)]-1))
  
  x = ifelse(top==labels,1,0)
  dcg = function(y) sum((2^y - 1)/log(2:(length(y)+1), base = 2))
  ndcg = mean(apply(x,1,dcg))
  return(list(metric = "ndcg5", value = ndcg))
}

#Initialise the data frame Results - will be used to save the results of a grid search
Results = data.frame(eta = NA, max_depth = NA, subsample = NA, colsample_bytree = NA, ndcg5 = NA, st_dev = NA, n_rounds = NA,
                     iteration = NA)

#Initialise the list OOSPreds - will be used to save the out-of-fold model predictions - needed to evaluate the bagged model
OOSPreds = list()

#Initialise iteration
iteration = 0

#Fix the early stop round for boosting
early_stop_round = 10

#Set the parameters to be used within the grid search - only the best set of parameters found via tuning is retained here
eta_values = c(0.05)
max_depth_values = c(6)
subsample_values = c(0.9)
colsample_bytree_values = c(0.8)

#Loop through all combinations of the parameters to be used in the above grid
for (eta in eta_values) {
  for (max_depth in max_depth_values) {
    for (subsample in subsample_values) {
      for (colsample_bytree in colsample_bytree_values) {
        
        iteration = iteration + 1
        
        #Set the model parameters for the current iteration
        
        param = list(objective           = "multi:softprob",
                     num_class           = 12,
                     booster             = "gbtree",
                     eval_metric         = ndcg5_eval,
                     eta                 = eta,
                     max_depth           = max_depth,
                     subsample           = subsample,
                     colsample_bytree    = colsample_bytree
        )
        
        #Train the model using 5-fold cross validation with the given parameters
        
        set.seed(17)
        XGBcv = xgb.cv(params             = param, 
                       data               = dtrain,
                       nrounds            = 10000,
                       verbose            = T,
                       nfold              = 5,
                       early.stop.round   = early_stop_round,
                       maximize           = T,
                       prediction         = T
        )
        
        #Save the number of boosting rounds for this set of parameters
        n_rounds = length(XGBcv$dt$test.ndcg5.mean) - early_stop_round
        
        #Save the evaluation metric score obtained using 5-fold cross validation for this set of parameters
        ndcg5 = XGBcv$dt$test.ndcg5.mean[n_rounds]
        
        #Save the standard deviation of the evaluation metric for this set of parameters
        st_dev = XGBcv$dt$test.ndcg5.std[n_rounds]
        
        #Save the set of parameters and model tuning results in the data frame Results
        Results = rbind(Results, c(eta, max_depth, subsample, colsample_bytree, ndcg5, st_dev,
                                   n_rounds, iteration))
        
        #Save the out-of-fold predictions for this set of tuning parameters to a list
        OOSPreds[[iteration]] = XGBcv$pred
        
        rm(XGBcv)
        gc()
      }
    }
  }
}

#Order Results in descending order according to the scoring metric
Results = na.omit(Results)
Results = Results[order(-Results$ndcg5),]

#Save the evaulation metric score in a vector
EvalScore = Results$ndcg5[1]

#Extract the best out-of-fold training predictions from the above grid search
OOSPreds = list(OOSPreds[[Results$iteration[1]]])

#A separate grid search for each seed returns very similar results therefore:
#Use the best tuning paramters determined above for a single seed, train 5 xgboost models using different seeds to reduce variance

paramTuned = list(objective          = "multi:softprob",
                  num_class          = 12,
                  booster            = "gbtree",
                  eval_metric        = ndcg5_eval,
                  eta                = Results$eta[1],
                  max_depth          = Results$max_depth[1],
                  subsample          = Results$subsample[1],
                  colsample_bytree   = Results$colsample_bytree[1]
)

#Set number of rounds based on tuning
n_rounds = Results$n_rounds[1]

#Loop through other seeds to obtain out-of-fold training predictions - needed to evaluate the bagged model
seeds = c(12, 44, 77, 85)

iteration = 1

for (seed in seeds) {
  
  iteration = iteration + 1
  
  #Train the model using 5-fold cross validation with the given seed
  
  set.seed(seed)
  XGBcv = xgb.cv(params             = paramTuned, 
                 data               = dtrain,
                 nrounds            = n_rounds,
                 verbose            = T,
                 nfold              = 5,
                 maximize           = T,
                 prediction         = T
  )
  
  
  #Save the evaluation metric score obtained using 5-fold cross validation for this seed
  EvalScore = c(EvalScore, XGBcv$dt$test.ndcg5.mean[n_rounds])
  
  #Save the out-of-fold predictions for this seed to a list
  OOSPreds[[iteration]] = XGBcv$pred
  
  rm(XGBcv)
  gc()
}

#Initialise an empty list to store model predictions
TestPreds = list()

#Set all in seeds in a vector
seeds = c(17, seeds)

#Initialise iteration
iteration = 0

#Train each model using the full training set
for (seed in seeds) {
  
  iteration = iteration + 1
  
  set.seed(seed)
  XGB = xgboost(params              = paramTuned, 
                data                = dtrain,
                nrounds             = n_rounds,
                verbose             = 1,
                maximize            = T
  )
  
  #Save the model predictions for this seed to a list
  TestPreds[[iteration]] = predict(XGB, data.matrix(test), missing = -1)
}

#Bag the five model's out-of-fold training predictions
y_pred_train = Reduce("+", OOSPreds)/length(OOSPreds)

#Function to calculate normalised discounted cumulative gain @ 5 for the out-of-fold training predictions
ndcg5_eval_train = function(preds, actual) {
  
  preds = t(preds)
  
  num.class = 12
  pred = matrix(preds, nrow = num.class)
  top = t(apply(pred, 2, function(y) order(y)[num.class:(num.class-4)]-1))
  
  x = ifelse(top==actual,1,0)
  dcg = function(y) sum((2^y - 1)/log(2:(length(y)+1), base = 2))
  ndcg = mean(apply(x,1,dcg))
  return(ndcg)
}

#Obtain the evaluation score of the bagged model using out-of-fold predictions
#Slight improvement in the evaluation metric - potentially significant for a Kaggle competition
ndgc5_train_bag = ndcg5_eval_train(y_pred_train, country_destination)

#Using each of the five models, obtain test set predictions by bagging predictions
y_pred_test = Reduce("+", TestPreds)/length(TestPreds)

#Create a matrix, where each of the 12 rows represents the probability that a user will visit that destination
predictions = as.data.frame(matrix(y_pred_test, nrow=12))
rownames(predictions) = c('NDF','US','other','FR','CA','GB','ES','IT','PT','NL','DE','AU')

#For each user, extract the 5 most likely desitnations - necessary to calculate normalised discounted cumulative gain @ 5
predictions_top5 = as.vector(apply(predictions, 2, function(x) names(sort(x)[12:8])))

#Create submission file- contains the five most likely destinations (in descending order) for each user in the test set 
idx = test_user_id
id_mtx = matrix(idx, 1)[rep(1,5), ] 
ids = c(id_mtx)

submission = data.frame(id = ids, country = predictions_top5)

#Save submission as csv file for Kaggle submission
write.csv(submission, "airbnb_submission.csv", quote=FALSE, row.names = FALSE)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
