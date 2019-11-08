
# Exercise 1
## Problem 
- In the board meeting, Mr Jeff Atwood - co-founder of stackexchange want to know statistic number of  post by some industries of his website. These post was posted from start-up to currently. This was contained in data-badges.csv
- Data of post have format:

| id |date_time  |post_id|type_post|is_badge|user_id|
|--|--|--|--|--|--
| 3 | 2008-09-15T08:55:03.923 |82946|Teacher|false|3718
|3|2008-09-15T08:55:03.957|82947|Teacher|false|994 

- This is some posts and  industries that is he was interested. It was contained in data-industries.csv

 - Example:

| type_post | industry |
|--|--|
|Teacher|Education|
|Quorum|Math|

## Expected result example
```
 Literary	34870
 Sport	52186
 Math	4601
```
## Bonus exercise 1

- If you have successfully finished excercise 1 the earlier steps and still have time, feel free to continue with this optional bonus exercise.
- The other director of stackexchange.com want to know number of post by industry per year. He want to know trend of posts by year. Can you do it?

### Expected result 
```
Education	2008	35063
Education	2009	101026
```
