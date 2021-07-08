
## gen price family table to attach to priceFam table later
create temp table articlePriceFam as (
with dat as (
SELECT distinct
   ifnull(SalesOrg,'') as SalesOrg,
    --ifnull(Article,'') as Article,  
    ltrim(Article,'0') as Article,
    --ifnull((case when Sales_Unit in ('CA1','CA2','CA3') then 'CAR' else Sales_Unit end),'') as Sales_Unit,
    ifnull((case when Price_Family_Description is null or Price_Family_Description = '' then Article_Description else Price_Family_Description end), '') as Price_Family_Description,
    sum(ifnull(Sales_ExclTax,0)) as Sales_ExclTax
    FROM  `gcp-wow-ent-im-tbl-prod.gs_allgrp_fin_data.fin_group_profit_v`
    WHERE
    SalesOrg in ('1030', '1005', '2010', '1060', '2030') --and
    --Segment_Description = 'CANS - 24 PACK & OVER'
    group by 1,2,3
    ),
    dat2 as (
    select *,
    row_number() over(partition by SalesOrg, Article order by Sales_ExclTax desc) as rown
    from dat
    )
    select * from dat2 where rown = 1
);

## gen soh summary table
create temp table soh0 as (
select soh.soh_date,
DATE_ADD(soh.soh_date, INTERVAL -1 DAY) as soh_date_lag1d,
ifnull(soh.salesorg_id,'') as salesorg_id,
ifnull(soh.site,'') as site,
ifnull(soh.article,'') as article,
ifnull(art.ArticleDescription,'') as ArticleDescription,
ifnull(soh.article_uom,'') as article_uom,
--ifnull(c.Price_Family_Description, art.ArticleDescription) as Price_Family_Description,
ifnull(soh.stock_at_map,0) as stock_at_map,
ifnull(soh.stock_on_hand,0) as stock_on_hand,
(case when ifnull(soh.stock_on_hand,0) = 0 then null else ifnull(soh.stock_at_map,0)/ifnull(soh.stock_on_hand,0) end) as map

from
`gcp-wow-ent-im-tbl-prod.adp_inventory_curr.article_site_soh_snapshot_daily` soh
   inner join `gcp-wow-ent-im-tbl-prod.adp_dm_masterdata_view.dim_article_v` art on (soh.Article=art.Article)
   --left join articlePriceFam c on (ltrim(soh.article,'0') = ltrim(c.Article,'0')) and (soh.salesorg_id=c.SalesOrg)
where soh.soh_date between '2020-07-01' and  '2021-07-05'
and art.Article != "ZPRD"
 and --Site in ("1933", "3911", "4933", "5910", "1944", "2998", "3920", "1912", "1998", "1954") AND
 salesorg_id in ('1030', '1005', '2010', '1060', '2030') and
    soh.article in (select distinct Article from articlePriceFam)
order by soh.salesorg_id, soh.site, soh.article, soh.soh_date
);


## gen sohPriceFam summary table
create temp table soh0_priceFam as (
select a.*, b.Price_Family_Description
from soh0 a
--left join articlePriceFam b on (ltrim(a.article,'0') = ltrim(b.Article,'0')) and (a.salesorg_id=b.SalesOrg)
left join articlePriceFam b on (a.article=b.Article) and (a.salesorg_id = b.SalesOrg)
);

create temp table soh0_priceFamSummarySite as (
select salesorg_id, site, article_uom, Price_Family_Description, soh_date,
sum(stock_at_map) as stock_at_map,
sum(stock_on_hand) as stock_on_hand,
(case when sum(stock_on_hand) = 0 then null else sum(stock_at_map)/sum(stock_on_hand) end) as map
from soh0_priceFam
group by 1,2,3,4,5
);

create temp table soh0_priceFamSummarySalesOrg as (
select salesorg_id, article_uom, Price_Family_Description, soh_date,
sum(stock_at_map) as stock_at_map,
sum(stock_on_hand) as stock_on_hand,
(case when sum(stock_on_hand) = 0 then null else sum(stock_at_map)/sum(stock_on_hand) end) as map
from soh0_priceFam
group by 1,2,3,4
);

create temp table soh0_articleSalesOrg as (
select salesorg_id, article_uom, article, soh_date,
sum(stock_at_map) as stock_at_map,
sum(stock_on_hand) as stock_on_hand,
(case when sum(stock_on_hand) = 0 then null else sum(stock_at_map)/sum(stock_on_hand) end) as map
from soh0_priceFam
group by 1,2,3,4
);


create or replace table `gcp-wow-finance-de-lab-dev.017_map.00_mapData` as (
select a.*,
--b.Price_Family_Description, 
b.stock_at_map as stock_at_map_pf_Site, b.stock_on_hand as stock_on_hand_pf_Site, b.map as map_pf_Site,
c.stock_at_map as stock_at_map_pf_SalesOrg, c.stock_on_hand as stock_on_hand_pf_SalesOrg, c.map as map_pf_SalesOrg,
d.stock_at_map as stock_at_map_art_SalesOrg, d.stock_on_hand as stock_on_hand_art_SalesOrg, d.map as map_art_SalesOrg
from soh0_priceFam a
left join
soh0_priceFamSummarySite b
on (a.salesorg_id=b.salesorg_id) and (a.site=b.site) and (a.article_uom=b.article_uom) and (a.soh_date=b.soh_date) and (a.Price_Family_Description=b.Price_Family_Description)

left join
soh0_priceFamSummarySalesOrg c
on (a.salesorg_id=c.salesorg_id) and (a.article_uom=c.article_uom) and (a.soh_date=c.soh_date) and (a.Price_Family_Description=c.Price_Family_Description)

left join
soh0_articleSalesOrg d
on (a.salesorg_id=d.salesorg_id) and (a.article_uom=d.article_uom) and (a.soh_date=d.soh_date) and (a.Article=d.article)

-- where Article = '102341' and
-- a.map is not null
);


-- select *
-- from `gcp-wow-finance-de-lab-dev.017_map.00_mapData`
-- --where site  = '1004'
-- order by abs( (map-map_pf_Site) *stock_on_hand) desc
-- limit 1000


##
create or replace table `gcp-wow-finance-de-lab-dev.017_map.01_mapData` as (
with d0 as (
select *, 
spread_ma-spread_std*2 as spread_lower ,
spread_ma+spread_std*2 as spread_upper
from
(
select *,
map-map_art_SalesOrg as spread,
AVG(map-map_art_SalesOrg) OVER (PARTITION BY salesorg_id,	site,	article,	ArticleDescription,	article_uom ORDER BY UNIX_DATE(soh_date) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS spread_ma,
stddev(map-map_art_SalesOrg) OVER (PARTITION BY salesorg_id,	site,	article,	ArticleDescription,	article_uom ORDER BY UNIX_DATE(soh_date) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS spread_std
from `gcp-wow-finance-de-lab-dev.017_map.00_mapData`
-- where site = '1004' and 
-- ltrim(article, '0')='84552'
)
--order by salesorg_id,	site,	article,	ArticleDescription,	article_uom, soh_date
),
d1 as (
select *,
(case when spread_std = 0 then null else (spread-spread_ma)/spread_std end ) as spread_z
from d0
),
d2 as (
select *,
(spread-spread_ma)*stock_on_hand  as dollar_impact
--( case when spread_ma = 0 then null else (spread/spread_ma-1)*stock_at_map end ) as dollar_impact
from d1
),
d3 as (
select *,
lag(stock_at_map) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_stock_at_map,
lag(stock_on_hand) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_stock_on_hand,
lag(map) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_map,
lag(stock_at_map_pf_Site) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_stock_at_map_pf_Site,
lag(stock_on_hand_pf_Site) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_stock_on_hand_pf_Site,
lag(map_pf_Site) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_map_pf_Site,
lag(stock_at_map_pf_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_stock_at_map_pf_SalesOrg,
lag(stock_on_hand_pf_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_stock_on_hand_pf_SalesOrg,
lag(map_pf_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_map_pf_SalesOrg,
lag(stock_at_map_art_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_stock_at_map_art_SalesOrg,
lag(stock_on_hand_art_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_stock_on_hand_art_SalesOrg,
lag(map_art_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_map_art_SalesOrg,
lag(spread) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_spread,
lag(spread_ma) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_spread_ma,
lag(spread_std) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_spread_std,
lag(spread_lower) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_spread_lower,
lag(spread_upper) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_spread_upper,
lag(spread_z) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_spread_z,
lag(dollar_impact) over(partition by salesorg_id,site,article,ArticleDescription,article_uom,Price_Family_Description order by soh_date) as lag_dollar_impact
from d2
),
d4 as (
select *,
map-lag_map as map_diff,
(case when lag_map = 0 then null else map/lag_map-1 end) as map_perc_diff,
map_art_SalesOrg-lag_map_art_SalesOrg as map_art_SalesOrg_diff,
(case when lag_map_art_SalesOrg = 0 then null else map_art_SalesOrg/lag_map_art_SalesOrg-1 end) as map_art_SalesOrg_perc_diff
from d3
)
select *,
(case
when salesorg_id in ('1005', '1030') AND abs(map_diff)*stock_on_hand > 8000 then true
when salesorg_id in ('1060') AND abs(map_diff)*stock_on_hand > 200 then true
when salesorg_id in ('2010', '2030') AND abs(map_diff)*stock_on_hand > 800 then true
else false end) as detected_by_existing_method,
(case
when abs(spread_z)>2 and salesorg_id in ('1005', '1030') AND abs(map_diff)*stock_on_hand > 8000 then true
when abs(spread_z)>2 and salesorg_id in ('1060') AND abs(map_diff)*stock_on_hand > 200 then true
when abs(spread_z)>2 and salesorg_id in ('2010', '2030') AND abs(map_diff)*stock_on_hand > 800 then true
--when abs(spread_z)>2 then true
else false end) as detected_record_v_salesOrg_map_method
from d4
);

-- select *
-- from `gcp-wow-finance-de-lab-dev.017_map.01_mapData`
-- where site = '4933' and
-- ltrim(article, '0')='773142'
-- order by soh_date


