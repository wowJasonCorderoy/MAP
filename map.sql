declare so array<STRING>;
DECLARE dateFrom DATE;
DECLARE dateTo DATE;


set so = ['1030', '1005', '2010', '1060', '2030'];
set dateTo = DATE_ADD(CURRENT_DATE("Australia/Sydney"), INTERVAL -1 DAY);
set dateFrom = DATE_ADD(dateTo, INTERVAL -364 DAY);

############################################################
### START: create ASP table (regardless of whether there were any sales for that site article pair for that day) - excl UOM in this view

CREATE TEMP FUNCTION f_date_add(d date, inc int64) AS (DATE_ADD(d, INTERVAL inc DAY));

create or replace table
`gcp-wow-finance-de-lab-dev.017_map.daily_site_article_sellPrice` as (
select *
from
(
with 
dateTable as (
SELECT day
FROM UNNEST(
    #GENERATE_DATE_ARRAY(f_date_add(CURRENT_DATE(), -364), CURRENT_DATE(), INTERVAL 1 DAY)
    GENERATE_DATE_ARRAY(dateFrom, dateTo, INTERVAL 1 DAY)
) AS day
),
dat as (
SELECT 
    ltrim(Article,'0') as Article,
    ifnull(Site,'') as Site,
    cast(DateSellPriceStart as date) as DateSellPriceStart,
    cast(max(CurrentSellPrice) as float64) as CurrentSellPrice
--Article, Site, DateSellPriceStart, cast(max(CurrentSellPrice) as float64) as CurrentSellPrice
FROM `gcp-wow-ent-im-tbl-prod.adp_dm_masterdata_view.dim_article_site_uom_v_hist` 
where DateSellPriceStart is not null and DateSellPriceStart != ''
group by 1,2,3
),
dat2 as (
select *,

ifnull(
  f_date_add(
  cast(lead(DateSellPriceStart) over(partition by Article, Site order by DateSellPriceStart) as DATE)
  , -1)
  , f_date_add(CURRENT_DATE("Australia/Sydney"), 364)#,cast('2999-01-01' as DATE)
  )
  as to_DateSellPriceStart

from dat

--where DateSellPriceStart is not null and DateSellPriceStart != ''

)
select dt.*, d.*
--d.Article, d.Site, dt.day, max(d.CurrentSellPrice) as sell_price
from dateTable dt, 
dat2 d 
where 
d.DateSellPriceStart<=dt.day and
d.to_DateSellPriceStart>=dt.day
-- dt.day>=cast(d.DateSellPriceStart as date) and
-- dt.day <= cast(d.to_DateSellPriceStart as date)

--group by 1,2,3
order by d.Article, d.Site, dt.day
)

);

### END: create ASP table (regardless of whether there were any sales for that site article pair for that day) - excl UOM in this view
############################################################

## gen price family table to attach to priceFam table later - Get ASP from daily price table NOT from actual sales!!!
create temp table articleSales as (
SELECT
    ifnull(SalesOrg,'') as SalesOrg,
    ifnull(Site,'') as Site,
    ltrim(Article,'0') as Article,
    ifnull((case when Sales_Unit in ('CA1','CA2','CA3') then 'CAR' else Sales_Unit end),'') as Sales_Unit,
    Calendar_Day,
    sum(ifnull(Sales_ExclTax,0)) as Sales_ExclTax,
    sum(ifnull(Sales_Qty_SUoM,0)) as Sales_Qty_SUoM--,
    --(case when sum(Sales_Qty_SUoM) = 0 then NULL else sum(Sales_ExclTax)/sum(Sales_Qty_SUoM) end) as ASP # maybe  `gcp-wow-ent-im-tbl-prod.adp_dm_masterdata_view.dim_article_site_uom_v_hist` ? for ASP regardless of whether there are sales or not.
    FROM  `gcp-wow-ent-im-tbl-prod.gs_allgrp_fin_data.fin_group_profit_v`
    WHERE
    SalesOrg in unnest(so) and
    Calendar_Day between dateFrom and dateTo
    --Segment_Description = 'CANS - 24 PACK & OVER'
    group by 1,2,3,4,5
);


-- ## gen price family table to attach to priceFam table later
-- create temp table articleSales as (

-- select a.*, b.ASP -- Here ASP is actually standard price not the actual sell price!!!

-- from (

-- SELECT
--     ifnull(SalesOrg,'') as SalesOrg,
--     ifnull(Site,'') as Site,
--     ltrim(Article,'0') as Article,
--     ifnull((case when Sales_Unit in ('CA1','CA2','CA3') then 'CAR' else Sales_Unit end),'') as Sales_Unit,
--     Calendar_Day,
--     sum(ifnull(Sales_ExclTax,0)) as Sales_ExclTax,
--     sum(ifnull(Sales_Qty_SUoM,0)) as Sales_Qty_SUoM--,
--     --(case when sum(Sales_Qty_SUoM) = 0 then NULL else sum(Sales_ExclTax)/sum(Sales_Qty_SUoM) end) as ASP # maybe  `gcp-wow-ent-im-tbl-prod.adp_dm_masterdata_view.dim_article_site_uom_v_hist` ? for ASP regardless of whether there are sales or not.
--     FROM  `gcp-wow-ent-im-tbl-prod.gs_allgrp_fin_data.fin_group_profit_v`
--     WHERE
--     SalesOrg in unnest(so) and
--     Calendar_Day between dateFrom and dateTo
--     --Segment_Description = 'CANS - 24 PACK & OVER'
--     group by 1,2,3,4,5
    
--     ) a
    
--     left join 

--     (
--     SELECT day,Article,	Site, max(CurrentSellPrice) as ASP
--   FROM `gcp-wow-finance-de-lab-dev.017_map.daily_site_article_sellPrice`
--   group by 1,2,3
--     ) b on (a.Site=b.Site) and (a.Article=b.Article) and (a.Calendar_Day=b.day)
-- );

  
    
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
where soh.soh_date between dateFrom and  dateTo
and art.Article != "ZPRD"
 and --Site in ("1933", "3911", "4933", "5910", "1944", "2998", "3920", "1912", "1998", "1954") AND
 salesorg_id in unnest(so)
order by soh.salesorg_id, soh.site, soh.article, soh.soh_date
);


## gen sohPriceFam summary table
create temp table soh0_sales as (
select a.*, ifnull(b.Sales_ExclTax,0) as Sales_ExclTax, ifnull(b.Sales_Qty_SUoM,0) as Sales_Qty_SUoM, c.ASP
from soh0 a
--left join articlePriceFam b on (ltrim(a.article,'0') = ltrim(b.Article,'0')) and (a.salesorg_id=b.SalesOrg)
left join articleSales b on (a.article=b.Article) and (a.salesorg_id = b.SalesOrg) and (a.site=b.Site) and (a.article_uom=b.Sales_Unit) and (a.soh_date=b.Calendar_Day)

left join 
    (
    SELECT day,Article,	Site, max(CurrentSellPrice) as ASP
  FROM `gcp-wow-finance-de-lab-dev.017_map.daily_site_article_sellPrice`
  group by 1,2,3
    ) c on (a.site=c.Site) and (a.article=c.Article) and (a.soh_date=c.day)

);

create temp table soh0_articleSalesOrg as (
select salesorg_id, article_uom, article, soh_date,
sum(ifnull(Sales_ExclTax,0)) as Sales_ExclTax,
sum(ifnull(Sales_Qty_SUoM,0)) as Sales_Qty_SUoM,
--(case when sum(Sales_Qty_SUoM) = 0 then NULL else sum(Sales_ExclTax)/sum(Sales_Qty_SUoM) end) as ASP,
avg(ASP) as ASP,
sum(stock_at_map) as stock_at_map,
sum(stock_on_hand) as stock_on_hand,
(case when sum(stock_on_hand) = 0 then null else sum(stock_at_map)/sum(stock_on_hand) end) as map
from soh0_sales
group by 1,2,3,4
);


create or replace table `gcp-wow-finance-de-lab-dev.017_map.00_mapData` as (
select a.*,
ifnull(b.stock_at_map,0) as stock_at_map_art_SalesOrg, ifnull(b.stock_on_hand,0) as stock_on_hand_art_SalesOrg, b.map as map_art_SalesOrg, 
ifnull(b.Sales_ExclTax,0) as Sales_ExclTax_art_SalesOrg, ifnull(b.Sales_Qty_SUoM,0) as Sales_Qty_SUoM_art_SalesOrg, b.ASP as ASP_art_SalesOrg,

## create salesorg columns excluding this records data. In case a sites soh is >% of total salesorgs SOH. Better not letting that sites MAP influence the CONTROL group at all.
ifnull(b.stock_at_map,0)-ifnull(a.stock_at_map,0) as stock_at_map_art_SalesOrg_exclRec, ifnull(b.stock_on_hand,0)-ifnull(a.stock_on_hand,0) as stock_on_hand_art_SalesOrg_exclRec, 
(case
when (ifnull(b.stock_on_hand,0)-ifnull(a.stock_on_hand,0)) = 0 then null
else
(ifnull(b.stock_at_map,0)-ifnull(a.stock_at_map,0))/(ifnull(b.stock_on_hand,0)-ifnull(a.stock_on_hand,0)) end) as map_art_SalesOrg_exclRec,

ifnull(b.Sales_ExclTax,0)-ifnull(a.Sales_ExclTax,0) as Sales_ExclTax_art_SalesOrg_exclRec, ifnull(b.Sales_Qty_SUoM,0)-ifnull(a.Sales_Qty_SUoM,0) as Sales_Qty_SUoM_art_SalesOrg_exclRec, 
(case
when (ifnull(b.Sales_Qty_SUoM,0)-ifnull(a.Sales_Qty_SUoM,0)) = 0 then null
else
(ifnull(b.Sales_ExclTax,0)-ifnull(a.Sales_ExclTax,0))/(ifnull(b.Sales_Qty_SUoM,0)-ifnull(a.Sales_Qty_SUoM,0)) end) as ASP_art_SalesOrg_exclRec,

from soh0_sales a
left join
soh0_articleSalesOrg b
on (a.salesorg_id=b.salesorg_id) and (a.article_uom=b.article_uom) and (a.soh_date=b.soh_date) and (a.Article=b.article)

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
map_spread_ma-map_spread_std*2 as map_spread_lower ,
map_spread_ma+map_spread_std*2 as map_spread_upper ,

asp_map_spread_ma-asp_map_spread_std*2 as asp_map_spread_lower ,
asp_map_spread_ma+map_spread_std*2 as asp_map_spread_upper

from
(
select *,
--map-map_art_SalesOrg as map_spread,
map-map_art_SalesOrg_exclRec as map_spread,

ASP-map as asp_map_spread,

-- AVG(map-map_art_SalesOrg) OVER (PARTITION BY salesorg_id,	site,	article,	ArticleDescription,	article_uom ORDER BY UNIX_DATE(soh_date) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS map_spread_ma,
-- stddev(map-map_art_SalesOrg) OVER (PARTITION BY salesorg_id,	site,	article,	ArticleDescription,	article_uom ORDER BY UNIX_DATE(soh_date) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS map_spread_std

AVG(map-map_art_SalesOrg_exclRec) OVER (PARTITION BY salesorg_id,	site,	article,	ArticleDescription,	article_uom ORDER BY UNIX_DATE(soh_date) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS map_spread_ma,
stddev(map-map_art_SalesOrg_exclRec) OVER (PARTITION BY salesorg_id,	site,	article,	ArticleDescription,	article_uom ORDER BY UNIX_DATE(soh_date) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS map_spread_std,

AVG(ASP-map) OVER (PARTITION BY salesorg_id,	site,	article,	ArticleDescription,	article_uom ORDER BY UNIX_DATE(soh_date) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS asp_map_spread_ma,
stddev(ASP-map) OVER (PARTITION BY salesorg_id,	site,	article,	ArticleDescription,	article_uom ORDER BY UNIX_DATE(soh_date) RANGE BETWEEN 7 PRECEDING AND CURRENT ROW) AS asp_map_spread_std

from `gcp-wow-finance-de-lab-dev.017_map.00_mapData`
-- where site = '1004' and 
-- ltrim(article, '0')='84552'
)
--order by salesorg_id,	site,	article,	ArticleDescription,	article_uom, soh_date
),
d1 as (
select *,
(case when map_spread_std = 0 then 0 else (map_spread-map_spread_ma)/map_spread_std end ) as map_spread_z,
(case when asp_map_spread_std = 0 then 0 else (asp_map_spread-asp_map_spread_ma)/asp_map_spread_std end ) as asp_map_spread_z
from d0
),
d2 as (
select *,
(map_spread-map_spread_ma)*stock_on_hand  as map_dollar_impact
--( case when spread_ma = 0 then null else (spread/spread_ma-1)*stock_at_map end ) as dollar_impact
from d1
),
d3 as (
select *,
lag(stock_at_map) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_stock_at_map,
lag(stock_on_hand) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_stock_on_hand,
lag(map) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map,

lag(Sales_ExclTax) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_Sales_ExclTax,
lag(Sales_Qty_SUoM) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_Sales_Qty_SUoM,
lag(ASP) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_ASP,

lag(stock_at_map_art_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_stock_at_map_art_SalesOrg,
lag(stock_on_hand_art_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_stock_on_hand_art_SalesOrg,
lag(map_art_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map_art_SalesOrg,

lag(stock_at_map_art_SalesOrg_exclRec) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_stock_at_map_art_SalesOrg_exclRec,
lag(stock_on_hand_art_SalesOrg_exclRec) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_stock_on_hand_art_SalesOrg_exclRec,
lag(map_art_SalesOrg_exclRec) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map_art_SalesOrg_exclRec,

lag(Sales_ExclTax_art_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_Sales_ExclTax_art_SalesOrg,
lag(Sales_Qty_SUoM_art_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_Sales_Qty_SUoM_art_SalesOrg,
lag(ASP_art_SalesOrg) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_ASP_art_SalesOrg,

lag(Sales_ExclTax_art_SalesOrg_exclRec) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_Sales_ExclTax_art_SalesOrg_exclRec,
lag(Sales_Qty_SUoM_art_SalesOrg_exclRec) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_Sales_Qty_SUoM_art_SalesOrg_exclRec,
lag(ASP_art_SalesOrg_exclRec) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_ASP_art_SalesOrg_exclRec,

lag(map_spread) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map_spread,
lag(map_spread_ma) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map_spread_ma,
lag(map_spread_std) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map_spread_std,
lag(map_spread_lower) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map_spread_lower,
lag(map_spread_upper) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map_spread_upper,
lag(map_spread_z) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map_spread_z,
lag(map_dollar_impact) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_map_dollar_impact,

lag(asp_map_spread) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_asp_map_spread,
lag(asp_map_spread_ma) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_asp_map_spread_ma,
lag(asp_map_spread_std) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_asp_map_spread_std,
lag(asp_map_spread_lower) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_asp_map_spread_lower,
lag(asp_map_spread_upper) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_asp_map_spread_upper,
lag(asp_map_spread_z) over(partition by salesorg_id,site,article,ArticleDescription,article_uom order by soh_date) as lag_asp_map_spread_z

from d2
),
d4 as (
select *,
map-lag_map as map_diff,
stock_at_map-lag_stock_at_map as stock_at_map_diff,
ASP-lag_asp as ASP_diff,
(case when lag_map = 0 then null else map/lag_map-1 end) as map_perc_diff,
(case when lag_asp = 0 then null else asp/lag_asp-1 end) as ASP_perc_diff,

map_art_SalesOrg-lag_map_art_SalesOrg as map_art_SalesOrg_diff,
(case when lag_map_art_SalesOrg = 0 then null else map_art_SalesOrg/lag_map_art_SalesOrg-1 end) as map_art_SalesOrg_perc_diff,

asp_art_SalesOrg-lag_asp_art_SalesOrg as asp_art_SalesOrg_diff,
(case when lag_asp_art_SalesOrg = 0 then null else asp_art_SalesOrg/lag_asp_art_SalesOrg-1 end) as asp_art_SalesOrg_perc_diff

from d3
)
select *,
(case
when map_diff is null then 'is_null' 
when salesorg_id in ('1005', '1030') AND abs(map_diff)*stock_on_hand > 8000 then 'yes'
when salesorg_id in ('1060') AND abs(map_diff)*stock_on_hand > 200 then 'yes'
when salesorg_id in ('2010', '2030') AND abs(map_diff)*stock_on_hand > 800 then 'yes'
else 'no' end) as detected_by_dollar_impact,

(case
when map_spread_z is null then 'is_null'
when abs(map_spread_z)>2 then 'yes'
--when abs(spread_z)>2 then true
else 'no' end) as detected_record_v_salesOrg_map_method,

(case
when asp_map_spread_z is null then 'is_null'
when abs(asp_map_spread_z)>2 then 'yes'
--when abs(spread_z)>2 then true
else 'no' end) as detected_asp_map_spread_method,

(case
when (map_spread is null) or (lag_map_spread is null) then 'is_null'
when abs(map_spread)<abs(lag_map_spread) then 'yes'
else 'no'
end) as detect_map_spread_shrinking #basically if MAP for site is getting closer to map for salesOrg v yesterday then this isn't really odd at all.

from d4
);

-- select *
-- from `gcp-wow-finance-de-lab-dev.017_map.01_mapData`
-- where site = '4933' and
-- ltrim(article, '0')='773142'
-- order by soh_date

