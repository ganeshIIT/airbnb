use airbnb
go

ALTER PROCEDURE stats_ReviewsAndPrice
(
@country varchar(150) = 'all',
@province varchar(150) = 'all',
@city varchar(150) = 'all'
)
AS
-- exec stats_ReviewsAndPrice @province ='tx'
-- declare @city varchar(150) = 'all'
-- declare @country varchar(150) = 'all'
-- declare @province varchar(150) = 'all'
-- set @city = 'austin'
-- set @country = 'united-states'
-- set @province = 'tx'
BEGIN

select l.listing_id, 
country, 
province,city, 
case when l.availability_365 > 0 then 1 else 0 end as isActive,
l.host_since,
l.review_scores_accuracy,
l.review_scores_checkin,
l.review_scores_cleanliness,
l.review_scores_communication,
l.review_scores_communication,
l.review_scores_location,
l.review_scores_rating,
l.review_scores_value,
l.number_of_reviews,
l.price,  
[c].[Price] as CurrentPrice 
from listings l 
left join (select listing_id, avg(price) as Price 
from calendar 
group by listing_id) c on c.listing_id = l.listing_id
where (l.city = @city or 'all' = @city)
and (l.country = @country or 'all' = @country)
and (l.province = @province or 'all' = @province)

END