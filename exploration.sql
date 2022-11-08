select top 6 * from listings(nolock)
select top 6 * from calendar(nolock)
select top 6 * from reviews(nolock) 


select count(*) from listings(nolock)
select count(*) from reviews(nolock)
select count(*) from calendar(nolock)


select top 100 c.*, l.host_name from listings l(nolock)
join calendar c (nolock) on c.listing_id = l.listing_id

select * from listings where len(listing_id) = 18



-- ALTER TABLE listings alter column listing_id varchar(24)

-- ALTER TABLE listings  
-- ADD CONSTRAINT unique_listing_id UNIQUE (listing_id);  


select c.*, l.city, l.host_name, l.number_of_reviews, l.review_scores_rating 
from listings l(nolock)
join calendar c (nolock) on c.listing_id = l.listing_id
where c.date = '2022-10-12'
and c.available = 1
and l.city like 'new%york%'
and number_of_reviews > 50 and review_scores_rating > 4.5
order by review_scores_rating desc, price asc


select distinct(province) from listings(nolock)


-- create proc test as 
-- select top 10 * from calendar

select top 6 * from listings(nolock)
select distinct(room_type) from listings

select 
sum(calculated_host_listings_count),
sum(calculated_host_listings_count_entire_homes)+
sum(calculated_host_listings_count_private_rooms)+
sum(calculated_host_listings_count_shared_rooms)
from listings