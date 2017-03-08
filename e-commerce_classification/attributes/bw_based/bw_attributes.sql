--ecommerce solution
CREATE FUNCTION array_intersect(anyarray, anyarray)
  RETURNS anyarray
  language sql
as $FUNCTION$
    SELECT ARRAY(
        SELECT UNNEST($1)
        INTERSECT
        SELECT UNNEST($2)
    );
$FUNCTION$;

--queries in pipecandy server machine
\d+ ecommerce_companies
-----------------------------+-----------------------------+-----------+----------+--------------+-------------
 linkedin_url                | text                        |           | extended |              | 
 company_name                | text                        |           | extended |              | 
 company_size                | text                        |           | extended |              | 
 industry                    | text                        |           | extended |              | 
 company_type                | text                        |           | extended |              | 
 headquarters                | text                        |           | extended |              | 
 description                 | text                        |           | extended |              | 
 founded                     | text                        |           | extended |              | 
 specialties                 | text                        |           | extended |              | 
 website                     | text                        |           | extended |              | 
 timestamp                   | timestamp without time zone |           | plain    |              | 
 employee_details_array      | text[]                      |           | extended |              | 
 also_viewed_companies_array | text[]                      |           | extended |              | 
 domain                      | text                        |           | extended |              | 
 linkedin_name               | text                        |           | extended |              | 
 website_cleaned             | text                        |           | extended |              | 
 location                    | text                        |           | extended |              | 
 region                      | text                        |           | extended |              | 
 country                     | text                        |           | extended |              | 
 funding_rounds              | text                        |           | extended |              | 
 funding_total_usd           | text                        |           | extended |              | 
 first_funding_on            | text                        |           | extended |              | 
 last_funding_on             | text                        |           | extended |              | 
 bw_technologies             | text[]                      |           | extended |              | 
 channel_presence            | text                        |           | extended |              | 
 marketplace_type            | text                        |           | extended |              | 
 transaction_type            | text                        |           | extended |              | 
 product_categories          | text                        |           | extended |              | 
 products_quality            | text                        |           | extended |              | 
 browse_and_filter           | boolean                     |           | plain    |              | 
 faceted_search              | boolean                     |           | plain    |              | 
 recomm_collab_filter        | boolean                     |           | plain    |              | 
 visual_merchandising        | boolean                     |           | plain    |              | 
 unique_visitors             | double precision            |           | plain    |              | 
 unique_visitors_growth      | double precision            |           | plain    |              | 
 pageviews                   | double precision            |           | plain    |              | 
 time_on_site_seconds        | bigint                      |           | plain    |              | 
 alexa_global_rank           | bigint                      |           | plain    |              | 
 website_language            | text                        |           | extended |              | 
 alexa_category              | text                        |           | extended |              | 

\d+ ecommerce_classifcation_alexa 
------------+------+-----------+----------+--------------+-------------
 domain     | text |           | extended |              | 
 categories | text |           | extended |              | 

---queries
update ecommerce_companies set domain = crawler.get_domain_from_website(website);
update ecommerce_companies set domain = null where website='NULL';
update ecommerce_companies set domain=lower(domain);

select count(*) from ecommerce_companies a join ecommerce_classifcation_alexa_feb23 b using(domain);
--1661

-- add new data into the table
update ecommerce_companies a set alexa_category=b.category from ecommerce_classifcation_alexa_feb23 b where a.alexa_category is null and a.domain=lower(b.domain);
insert into ecommerce_companies (domain,alexa_category) select domain,category from ecommerce_classifcation_alexa_feb23 where domain not in 
	(select domain from ecommerce_companies);

-- add builtwith data
update ecommerce_companies a set bw_technologies=b.technologies from builtwith_company_technologies b where a.domain=lower(b.domain) and a.bw_technologies is null;

--funding
update ecommerce_companies a set funding_rounds=b.funding_rounds,funding_total_usd = b.funding_total_usd,
first_funding_on = b.first_funding_on ,last_funding_on  = b.last_funding_on
from crunchbase_data.organizations b where a.domain=lower(b.domain);

update ecommerce_companies a set unique_visitors=b.unique_visitors,unique_visitors_growth=b.unique_visitors_growth,pageviews=b.pageviews,
	time_on_site_seconds=b.time_on_site_seconds,alexa_global_rank=b.global_rank,website_language=b.language,alexa_category=b.category 
	from ecommerce_companies_alexa_data b where lower(a.domain)=lower(b.site) and a.domain is not null and b.site is not null;

--update linkedin details 
update ecommerce_companies a set 
linkedin_url=b.linkedin_url,company_name=b.company_name,company_size=b.company_size,industry=b.industry,company_type=b.company_type,
headquarters=b.headquarters,description=b.description,founded=b.founded,specialties=b.specialties,website=b.website,timestamp=b.timestamp,
employee_details_array=b.employee_details_array,also_viewed_companies_array=b.also_viewed_companies_array,
domain=c.domain,linkedin_name=c.linkedin_name,website_cleaned=c.website_cleaned,location=c.location,region=c.region,country=c.country
from linkedin_company_base b , linkedin_company_domains c 
where a.domain=c.domain and b.linkedin_url = c.linkedin_url and a.linkedin_url is null and a.domain is not null;

--alexa

update ecommerce_companies a set unique_visitors=b.unique_visitors,unique_visitors_growth=b.unique_visitors_growth,pageviews=b.pageviews,time_on_site_seconds=b.time_on_site_seconds,alexa_global_rank=b.global_rank,website_language=b.language,alexa_category=b.category from ecommerce_companies_alexa_data b where lower(a.domain)=lower(b.site) and a.domain is not null and b.site is not null;
update ecommerce_companies set alexa_category=regexp_replace(alexa_category, '^.*-', '');


--product recommendation
update ecommerce_companies set recomm_collab_filter = 't' where 
	array['Trustpilot', 'Nosto', 'ForeSee Results', 'ScarabResearch', 'Retail Rocket', 'SLI Systems', 'Nextopia', 'Econda', 'Certona', 'Rees46', 'Clerk', 'Commerce Sciences', 'chaordic', 'Baynote', 'Target2Sell', 'BrainSINS', 'RecoPick', 'Blueknow', 'BrainPad Rtoaster', 'Segmentify'] && bw_technologies;

--multi channel
alter table ecommerce_companies add column multi_channel_presence boolean;
update ecommerce_companies set multi_channel_presence = 't' where 
	array['BigCommerce', 'OpenCart', 'Hybris', 'NetSuite eCommerce', 'AspDotNetStorefront', 'Intershop', 'Visualsoft', 'Martjack', 'OneStop Internet', 'BT Fresca', 'Symphony Commerce', 'Sana Commerce', 'Unilog', 'eSellerPro', 'Vendio', 'Vend', 'ROC Commerce', 'Celerant', 'Xretail', 'TotalCode'] && bw_technologies;

--a/b testing present
alter table ecommerce_companies add column ab_testing_present boolean;
update ecommerce_companies set ab_testing_present = 't' where 
	array['Optimizely', 'Visual Website Optimizer', 'Mixpanel', 'Omniture Adobe Test and Target', 'Google Content Experiments', 'KISSmetrics', 'Adobe Target Standard', 'Maxymiser', 'Monetate', 'AB Tasty', 'Improvely', 'Google Website Optimizer', 'Google Optimize 360', 'Roistat', 'Dynamic Yield', 'Marketizator', 'Convert', 'Kameleoon', 'SiteSpect', 'Zarget'] && bw_technologies;

--cart abandment
alter table ecommerce_companies add column cart_abandment_present boolean;
update ecommerce_companies set cart_abandment_present = 't' where 
	array['TeaLeaf', 'Econda', 'Yieldify', 'PicReel', 'ShopBack', 'SaleCycle', 'Conversions on Demand', 'CartStack', 'UpSellit', 'Cloud IQ', 'Qubit Deliver', 'Barilliance', 'Triggered Messaging', 'Commerce Sciences', 'SeeWhy', 'Sub2 Technologies', 'BrainSINS', 'GhostMonitor', 'AbandonAid', 'Retention Science'] && bw_technologies;

--fraud prevention
alter table ecommerce_companies add column fraud_prevention boolean;
update ecommerce_companies set fraud_prevention = 't' where 
	array['Pixalate', 'Fraudlogix', 'Sift Science', 'Improvely', 'iovation', 'Clearsale', 'Riskified', 'ThreatMetrix', 'Signifyd', 'FruadLabs Pro', 'Inuvo', 'ClickProtector', 'Fireblade', 'Adwatcher', 'Oxford Biochronometrics', 'Vericlix','Who''s Clicking Who'] && bw_technologies;

--personalization
alter table ecommerce_companies add column personalization_software boolean;
update ecommerce_companies set personalization_software = 't' where 
	array['Optimizely', 'Shareaholic', 'Bazaarvoice', 'Nosto', 'CQuotient', 'Monetate', 'Dynamic Yield', 'Convert', 'Qubit Deliver', 'Triggered Messaging', 'Unbxd', 'Baynote', 'Target2Sell', 'BrainSINS', 'BrightInfo', 'Visilabs', 'CoolaData', 'LiftIgniter', 'Strands', 'Traqli'] && bw_technologies;

--shipping provider
alter table ecommerce_companies add column shipping_provider boolean;
update ecommerce_companies set shipping_provider = 't' where 
	array['FastSpring'] && bw_technologies;

alter table ecommerce_companies add column ecommerce_solution text[];
update ecommerce_companies set ecommerce_solution =
array_intersect(array['Interchange', 'Miva Merchant', 'GekoSale', 'Spree', 'Shopify', 'PlentyMarkets', 'Drupal Commerce', 'Spark Pay', 'Web Shop Manager', 'WooCommerce Add To Cart', 'Demandware', 'ShopGate', '3D Cart', 'K-eCommerce', 'Big Cartel', 'OXID Eshop Enterprise', 'Magento Enterprise', 'Hybris', 'WooCommerce 2.3', 'WP eCommerce', 'TomatoCart', 'GoDaddy Online Store', 'ProductCart', 'NetSuite eCommerce', 'Actinic', 'Easy Digital Downloads', 'IBM Websphere Commerce', 'MarketPress', 'Volusion', 'PrestaShop', 'Nexternal', 'Loaded Commerce', 'WooCommerce 2.2', 'E-junkie', 'Etsy', 'Visualsoft', 'nopCommerce', 'Magento 1.6', 'ePages', 'Shopify Conversions', 'Shopify British Pound', 'WellCommerce', 'Hextom', 'VTEX', 'OpenCart', 'Shopware', 'Weebly eCommerce', 'AbleCommerce', 'ECSHOP', 'CubeCart', 'Magento 2', 'CoreCommerce', 'Magento 1.9', 'Magento 1.8', 'WordPress eStore Plugin', 'UltraCart', 'Magento 1.7', 'Yahoo Store', 'Shopify Buy Button', 'InSales', 'Zen Cart', 'Wix Stores', 'VirtueMart', 'Cart66', 'Oracle Commerce', 'OXID EShop Community', 'GetShopped', 'SmartStore.NET', 'Magento', 'Ubercart', 'BigCommerce', 'Pinnacle Cart', 'GoDaddy Quick Shopping Cart', 'Shopify US Dollar', 'Shopify Plus', 'osCommerce'] ,bw_technologies);

--ecommerce plugin (eg: plugin to convert blog to ecommerce site etc)
alter table ecommerce_companies add column ecommerce_plugin text[];
update ecommerce_companies set ecommerce_plugin =
array_intersect(array['FotoMoto', 'Codeblackbelt', 'WooCommerce', 'Twigmo', 'Cashie Commerce', 'Hextom', 'ShopperPress', 'WordPress eStore Plugin', 'Ecwid', 'Sellfy', 'iJoomla ECommerce', 'AceShop OpenCart', 'Shoprocket', 'GetShopped', 'ShopGate', 'Shopp', 'E-junkie', 'Americart', 'WebAssist eCart', 'Jigoshop'] ,bw_technologies);

