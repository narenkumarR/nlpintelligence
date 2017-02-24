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
alter table ecommerce_companies add column cart_abandment_soft boolean;
update ecommerce_companies set cart_abandment_soft = 't' where 
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

--ecommerce enterprise technology
alter table ecommerce_companies add column ecommerce_enterprise_tech text;
update ecommerce_companies set ecommerce_enterprise_tech = 'Magento Enterprise' where 'Magento Enterprise' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Miva Merchant' where 'Miva Merchant' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Demandware' where 'Demandware' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Shopware' where 'Shopware' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Hybris' where 'Hybris' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Oracle Commerce' where 'Oracle Commerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Pinnacle Cart' where 'Pinnacle Cart' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'IBM Websphere Commerce' where 'IBM Websphere Commerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'NetSuite eCommerce' where 'NetSuite eCommerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'VTEX' where 'VTEX' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Shopify Plus' where 'Shopify Plus' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'OXID Eshop Enterprise' where 'OXID Eshop Enterprise' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'UltraCart' where 'UltraCart' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Actinic' where 'Actinic' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'PlentyMarkets' where 'PlentyMarkets' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Nexternal' where 'Nexternal' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Visualsoft' where 'Visualsoft' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'K-eCommerce' where 'K-eCommerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'Web Shop Manager' where 'Web Shop Manager' = any(bw_technologies);
update ecommerce_companies set ecommerce_enterprise_tech = 'CoreCommerce' where 'CoreCommerce' = any(bw_technologies);

--ecommerce hosted solution
alter table ecommerce_companies add column ecommerce_hosted_solution text;
update ecommerce_companies set ecommerce_hosted_solution = 'Shopify' where 'Shopify' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'BigCommerce' where 'BigCommerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Volusion' where 'Volusion' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Yahoo Store' where 'Yahoo Store' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Weebly eCommerce' where 'Weebly eCommerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Miva Merchant' where 'Miva Merchant' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = '3D Cart' where '3D Cart' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'GoDaddy Quick Shopping Cart' where 'GoDaddy Quick Shopping Cart' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Wix Stores' where 'Wix Stores' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Demandware' where 'Demandware' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Cart66' where 'Cart66' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Shopify Plus' where 'Shopify Plus' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Spark Pay' where 'Spark Pay' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'ShopGate' where 'ShopGate' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'AbleCommerce' where 'AbleCommerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'ePages' where 'ePages' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'GoDaddy Online Store' where 'GoDaddy Online Store' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'ProductCart' where 'ProductCart' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'InSales' where 'InSales' = any(bw_technologies);
update ecommerce_companies set ecommerce_hosted_solution = 'Big Cartel' where 'Big Cartel' = any(bw_technologies);

--ecommerce non platform solution
alter table ecommerce_companies add column ecommerce_non_platform text;
update ecommerce_companies set ecommerce_non_platform = 'Shopify Conversions' where 'Shopify Conversions' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Shopify US Dollar' where 'Shopify US Dollar' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Magento 1.9' where 'Magento 1.9' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Easy Digital Downloads' where 'Easy Digital Downloads' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Magento 1.7' where 'Magento 1.7' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'WooCommerce Add To Cart' where 'WooCommerce Add To Cart' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Magento 1.8' where 'Magento 1.8' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'WP eCommerce' where 'WP eCommerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'GetShopped' where 'GetShopped' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'E-junkie' where 'E-junkie' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'WordPress eStore Plugin' where 'WordPress eStore Plugin' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Hextom' where 'Hextom' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'WooCommerce 2.3' where 'WooCommerce 2.3' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Shopify Buy Button' where 'Shopify Buy Button' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Shopify British Pound' where 'Shopify British Pound' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Etsy' where 'Etsy' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Magento 2' where 'Magento 2' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'Magento 1.6' where 'Magento 1.6' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'MarketPress' where 'MarketPress' = any(bw_technologies);
update ecommerce_companies set ecommerce_non_platform = 'WooCommerce 2.2' where 'WooCommerce 2.2' = any(bw_technologies);

--opensource platform
alter table ecommerce_companies add column ecommerce_opensource_platform text;
update ecommerce_companies set ecommerce_opensource_platform = 'Magento' where 'Magento' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'PrestaShop' where 'PrestaShop' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'OpenCart' where 'OpenCart' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'Zen Cart' where 'Zen Cart' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'osCommerce' where 'osCommerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'Drupal Commerce' where 'Drupal Commerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'Ubercart' where 'Ubercart' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'VirtueMart' where 'VirtueMart' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'Shopware' where 'Shopware' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'nopCommerce' where 'nopCommerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'OXID EShop Community' where 'OXID EShop Community' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'Spree' where 'Spree' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'ECSHOP' where 'ECSHOP' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'CubeCart' where 'CubeCart' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'Interchange' where 'Interchange' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'Loaded Commerce' where 'Loaded Commerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'TomatoCart' where 'TomatoCart' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'SmartStore.NET' where 'SmartStore.NET' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'GekoSale' where 'GekoSale' = any(bw_technologies);
update ecommerce_companies set ecommerce_opensource_platform = 'WellCommerce' where 'WellCommerce' = any(bw_technologies);

--ecommerce plugin (eg: plugin to convert blog to ecommerce site etc)
alter table ecommerce_companies add column ecommerce_plugin text;
update ecommerce_companies set ecommerce_plugin = 'WooCommerce' where 'WooCommerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Ecwid' where 'Ecwid' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'GetShopped' where 'GetShopped' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'E-junkie' where 'E-junkie' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'WordPress eStore Plugin' where 'WordPress eStore Plugin' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Hextom' where 'Hextom' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'ShopGate' where 'ShopGate' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Shopp' where 'Shopp' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Codeblackbelt' where 'Codeblackbelt' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Americart' where 'Americart' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'FotoMoto' where 'FotoMoto' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Twigmo' where 'Twigmo' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'ShopperPress' where 'ShopperPress' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Jigoshop' where 'Jigoshop' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'WebAssist eCart' where 'WebAssist eCart' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Sellfy' where 'Sellfy' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'AceShop OpenCart' where 'AceShop OpenCart' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Shoprocket' where 'Shoprocket' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'Cashie Commerce' where 'Cashie Commerce' = any(bw_technologies);
update ecommerce_companies set ecommerce_plugin = 'iJoomla ECommerce' where 'iJoomla ECommerce' = any(bw_technologies);

