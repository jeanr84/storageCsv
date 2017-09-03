# storageCsv
Just a simple exercise manipulating CSV files with spark 

## Instructions

### Input
- *input.csv* : userId,itemId,rating,timestamp

### Output
- *agg_ratings.csv* : userIdAsInteger,itemIdAsInteger,ratingSum 
- *lookup_user.csv* : userId,userIdAsInteger 
- *lookup_product.csv* : itemId,itemIdAsInteger

### Fields description
- userId : Unique identifier of an user (String) 
- itemId : Unique identifier of a product (String) 
- rating : score (Float) 
- timestamp : timestamp unix, number of milliseconds since 1970-01-01 midnight GMT (Long/Int64) 
- userIdAsInteger : Unique identifier of an user (Int) 
- itemIdAsInteger : Unique identifier of a product (Int) 
- ratingSum : Sum  of the ratings for each couple user/product (Float)

### Constraints 
- userIdAsInteger and productIdAsInteger are consecutives integers starting from 0 index. 
- a multiplicative penalty of 0.95 is applied to the rating for every day of gap with the maximal timestamp in input.csv  
- in agg_ratings.csv we only for to keep the one with ratingSum > 0.01
