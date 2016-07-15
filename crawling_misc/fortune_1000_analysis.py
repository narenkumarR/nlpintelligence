
'''
lower(replace(company_name,' ','')) ilike '%walmart%' or lower(replace(company_name,' ','')) ilike '%exxonmobil%' or lower(replace(company_name,' ','')) ilike '%chevron%' or
lower(replace(company_name,' ','')) ilike '%berkshirehathaway%' or lower(replace(company_name,' ','')) ilike '%apple%' or
lower(replace(company_name,' ','')) ilike '%generalmotors%' or lower(replace(company_name,' ','')) ilike '%phillips66%' or
lower(replace(company_name,' ','')) ilike '%generalelectric%' or lower(replace(company_name,' ','')) ilike '%fordmotor%' or
lower(replace(company_name,' ','')) ilike '%cvshealth%' or lower(replace(company_name,' ','')) ilike '%mckesson%' or lower(replace(company_name,' ',''))
 ilike '%at&t%' or lower(replace(company_name,' ','')) ilike '%valeroenergy%' or lower(replace(company_name,' ','')) ilike
 '%unitedhealthgroup%' or lower(replace(company_name,' ','')) ilike '%verizon%' or lower(replace(company_name,' ','')) ilike
 '%amerisourcebergen%' or lower(replace(company_name,' ','')) ilike '%fanniemae%' or lower(replace(company_name,' ','')) ilike
 '%costco%' or lower(replace(company_name,' ','')) ilike '%hp%' or lower(replace(company_name,' ','')) ilike '%kroger%' or
 lower(replace(company_name,' ','')) ilike '%jpmorganchase%' or lower(replace(company_name,' ','')) ilike '%expressscriptsholding%'
 or lower(replace(company_name,' ','')) ilike '%bankofamericacorp.%' or lower(replace(company_name,' ','')) ilike '%ibm%' or
 lower(replace(company_name,' ','')) ilike '%marathonpetroleum%' or lower(replace(company_name,' ','')) ilike '%cardinalhealth%'
 or lower(replace(company_name,' ','')) ilike '%boeing%' or lower(replace(company_name,' ','')) ilike '%citigroup%' or lower(replace(company_name,' ',''))
 ilike '%amazon.com%' or lower(replace(company_name,' ','')) ilike '%wellsfargo%'


'%walmart%|%exxonmobil%|%chevron%|%berkshirehathaway%|%apple%|%generalmotors%|
%phillips66%|%generalelectric%|%fordmotor%|%cvshealth%|%mckesson%|%at&t%|
%valeroenergy%|%unitedhealthgroup%|%verizon%|%amerisourcebergen%|%fanniemae%|%costco%|hp|%kroger%'

select * from linkedin_company_base where
lower(replace(company_name,' ','')) similar to
'%walmart%|%exxonmobil%|%chevron%|%berkshirehathaway%|%apple%|%generalmotors%|
%phillips66%|%generalelectric%|%fordmotor%|%cvshealth%|%mckesson%|%at&t%|
%valeroenergy%|%unitedhealthgroup%|%verizon%|%amerisourcebergen%|%fanniemae%|%costco%|hp|%kroger%'
and company_size = '10,001+ employees'
 '''

import pandas as pd
import re
tmp = pd.read_excel('fortune1000_us_2015.xlsx')
names = list(tmp['name'])
names1 = ['%'+re.sub(' ','',i).lower()+'%' if len(re.sub(' ','',i))>3 else re.sub(' ','',i).lower() for i in names ]
names1 = [re.sub("'","''",i) for i in names1]

query_part = '|'.join(names1)

import psycopg2
con = psycopg2.connect(database='linkedin_data', user='postgres',password='$P$BptPVyArwpjzWXe1wz1cafxlpmVlGE',host='52.221.230.64')
query = "create table fortune_1000_companies as select * from linkedin_company_base where company_size = '10,001+ employees' and "\
        "lower(replace(company_name,' ','')) similar to '"+query_part+"' "
cursor = con.cursor()
cursor.execute(query)

'''
alter table fortune_1000_companies drop column timestamp
create table fortune_1000_companies_unique as select distinct * from fortune_1000_companies

create table fortune_1000_people as select b.* from
fortune_1000_companies_unique a join company_urls_mapper c on a.linkedin_url = c.alias_url
join people_company_mapper d on c.base_url=d.company_url join linkedin_people_base b on d.people_url = b.linkedin_url

create table fortune_1000_people_unique as
select distinct b.linkedin_url person_url,b.name, b.sub_text designation,location,a.linkedin_url as company_linkedin_url,
a.company_name, company_size, a.industry as company_industry,company_type,headquarters,website
 from
fortune_1000_companies_unique a join company_urls_mapper c on a.linkedin_url = c.alias_url
join people_company_mapper d on c.base_url=d.company_url join linkedin_people_base b on d.people_url = b.linkedin_url

select distinct person_url,name,company_name,designation,company_linkedin_url from fortune_1000_people_unique
where regexp_replace(lower(designation),' of | the | at | in |[^a-zA-Z]',' ','gi') similar to
'%chief learning%|%chief human resource%|%vp learning%|%vp training%|%vp talent management%|%director learning%|
%director training%|%director talent management%|%vice president learning%|%vice president training%|%vice president talent management%'
limit 100
'''


'''
#on 28 June
create table fortune_1000_initial_company_list as select * from linkedin_company_base where company_size = '10,001+ employees';

create table fortune_1000_people_2016_06_28 as
select distinct e.name,e.sub_text as designation,e.linkedin_url as linkedin_url_person,e.company_name as company_name,
e.industry as industry_person,a.linkedin_url as linkedin_url_company,a.company_name company_name_1, company_size,
a.industry as industry_company,website, 
replace(substring(website  from '.*://([^/]*)'),'www.','') as domain from
 fortune_1000_initial_company_list a join company_urls_mapper b on a.linkedin_url=b.alias_url join
 people_company_mapper c on b.base_url =c.company_url 
 join people_urls_mapper d on c.people_url = d.base_url 
 join linkedin_people_base e on d.alias_url=e.linkedin_url
 where 
a.company_name ~ 'Walmart|Exxon.Mobil|Chevron|Berkshire.Hathaway|Apple|General.Motors|Phillips.66|General.Electric|Ford.Motor|CVS.Health|McKesson|AT.T|Valero.Energy|UnitedHealth.Group|Verizon|AmerisourceBergen|Fannie.Mae|Costco|HP|Kroger|JP.Morgan.Chase|Express.Scripts.Holding|Bank.of.America.Corp.|IBM|Marathon.Petroleum|Cardinal.Health|Boeing|Citigroup|Amazon.com|Wells.Fargo|Microsoft|Procter...Gamble|Home.Depot|Archer.Daniels.Midland|Walgreens|Target|Johnson...Johnson|Anthem|MetLife|Google|State.Farm.Insurance.Cos.|Freddie.Mac|Comcast|PepsiCo|United.Technologies|AIG|UPS|Dow.Chemical|Aetna|Lowe.s|ConocoPhillips|Intel|Energy.Transfer.Equity|Caterpillar|Prudential.Financial|Pfizer|Disney|Humana|Enterprise.Products.Partners|Cisco.Systems|Sysco|Ingram.Micro|Coca.Cola|Lockheed.Martin|FedEx|Johnson.Controls|Plains.GP.Holdings|World.Fuel.Services|CHS|American.Airlines.Group|Merck|Best.Buy|Delta.Air.Lines|Honeywell.International|HCA.Holdings|Goldman.Sachs.Group|Tesoro|Liberty.Mutual.Insurance.Group|United.Continental.Holdings|New.York.Life.Insurance|Oracle|Morgan.Stanley|Tyson.Foods|Safeway|Nationwide|Deere|DuPont|American.Express|Allstate|Cigna|Mondelez.International|TIAA.CREF|INTL.FCStone|Massachusetts.Mutual.Life.Insur|DirecTV|Halliburton|Twenty.First.Century.Fox|3M|Sears.Holdings|General.Dynamics|Publix.Super.Markets|Philip.Morris.International|TJX|Time.Warner|Macy.s|Nike|Tech.Data|Avnet|Northwestern.Mutual|McDonald.s|Exelon|Travelers.Cos.|Qualcomm|International.Paper|Occidental.Petroleum|Duke.Energy|Rite.Aid|Gilead.Sciences|Baker.Hughes|Emerson.Electric|EMC|USAA|Union.Pacific|Northrop.Grumman|Alcoa|Capital.One.Financial|National.Oilwell.Varco|US.Foods|Raytheon|Time.Warner.Cable|Arrow.Electronics|Aflac|Staples|Abbott.Laboratories|Community.Health.Systems|Fluor|Freeport.McMoRan|U.S..Bancorp|Nucor|Kimberly.Clark|Hess|Chesapeake.Energy|Xerox|ManpowerGroup|Amgen|AbbVie|Danaher|Whirlpool|PBF.Energy|HollyFrontier|Eli.Lilly|Devon.Energy|Progressive|Cummins|Icahn.Enterprises|AutoNation|Kohl.s|Paccar|Dollar.General|Hartford.Financial.Services.Gro|Southwest.Airlines|Anadarko.Petroleum|Southern|Supervalu|Kraft.Foods.Group|Goodyear.Tire...Rubber|EOG.Resources|CenturyLink|Altria.Group|Tenet.Healthcare|General.Mills|eBay|ConAgra.Foods|Lear|TRW.Automotive.Holdings|United.States.Steel|Penske.Automotive.Group|AES|Colgate.Palmolive|Global.Partners|Thermo.Fisher.Scientific|PG.E.Corp.|NextEra.Energy|American.Electric.Power|Baxter.International|Centene|Starbucks|Gap|Bank.of.New.York.Mellon.Corp.|Micron.Technology|Jabil.Circuit|PNC.Financial.Services.Group|Kinder.Morgan|Office.Depot|Bristol.Myers.Squibb|NRG.Energy|Monsanto|PPG.Industries|Genuine.Parts|Omnicom.Group|Illinois.Tool.Works|Murphy.USA|Land.O.Lakes|Western.Refining|Western.Digital|FirstEnergy|Aramark|DISH.Network|Las.Vegas.Sands|Kellogg|Loews|CBS|Ecolab|Whole.Foods.Market|Chubb|Health.Net|Waste.Management|Apache|Textron|Synnex|Marriott.International|Viacom|Lincoln.National|Nordstrom|C.H..Robinson.Worldwide|Edison.International|Marathon.Oil|Yum.Brands|Computer.Sciences|Parker.Hannifin|DaVita.HealthCare.Partners|CarMax|Texas.Instruments|WellCare.Health.Plans|Marsh...McLennan|Consolidated.Edison|Oneok|Visa|Jacobs.Engineering.Group|CSX|Entergy|Facebook|Dominion.Resources|Leucadia.National|Toys.R..Us...|DTE.Energy|Ameriprise.Financial|VF|Praxair|J.C..Penney|Automatic.Data.Processing|L.3.Communications|CDW|Guardian.Life.Ins..Co..of.Ameri|Xcel.Energy|Norfolk.Southern|PPL|R.R..Donnelley...Sons|Huntsman|Bed.Bath...Beyond|Stanley.Black...Decker|L.Brands|Liberty.Interactive|Farmers.Insurance.Exchange|First.Data|Sherwin.Williams|BlackRock|Voya.Financial|Ross.Stores|Sempra.Energy|Estee.Lauder|H.J..Heinz|Reinsurance.Group.of.America|Public.Service.Enterprise.Group|Cameron.International|Navistar.International|CST.Brands|State.Street.Corp.|Unum.Group|Hilton.Worldwide.Holdings|Family.Dollar.Stores|Principal.Financial|Reliance.Steel...Aluminum|Air.Products...Chemicals|Assurant|Peter.Kiewit.Sons.|Henry.Schein|Cognizant.Technology.Solutions|MGM.Resorts.International|W.W..Grainger|Group.1.Automotive|BB.T.Corp.|Rock.Tenn|Advance.Auto.Parts|Ally.Financial|AGCO|Corning|Biogen|NGL.Energy.Partners|Stryker|Molina.Healthcare|Precision.Castparts|Discover.Financial.Services|Genworth.Financial|Eastman.Chemical|Dean.Foods|AutoZone|MasterCard|Owens...Minor|Hormel.Foods|GameStop|Autoliv|CenterPoint.Energy|Fidelity.National.Financial|Sonic.Automotive|HD.Supply.Holdings|Charter.Communications|Crown.Holdings|Applied.Materials|Mosaic|CBRE.Group|Avon.Products|Republic.Services|Universal.Health.Services|Darden.Restaurants|Steel.Dynamics|SunTrust.Banks|Caesars.Entertainment|Targa.Resources|Dollar.Tree|News.Corp.|Ball|Thrivent.Financial.for.Lutheran|Masco|Franklin.Resources|Avis.Budget.Group|Reynolds.American|Becton.Dickinson|Priceline.Group|Broadcom|Tenneco|Campbell.Soup|AECOM|Visteon|Delek.US.Holdings|Dover|BorgWarner|Jarden|UGI|Murphy.Oil|PVH|Core.Mark.Holding|Calpine|D.R..Horton|Weyerhaeuser|KKR|FMC.Technologies|American.Family.Insurance.Group|SpartanNash|WESCO.International|Quanta.Services|Mohawk.Industries|Motorola.Solutions|Lennar|TravelCenters.of.America|Sealed.Air|Eversource.Energy|Coca.Cola.Enterprises|Celgene|Williams|Ashland|Interpublic.Group|Blackstone.Group|Ralph.Lauren|Quest.Diagnostics|Hershey|Terex|Boston.Scientific|Newmont.Mining|Allergan|O.Reilly.Automotive|Casey.s.General.Stores|CMS.Energy|Foot.Locker|W.R..Berkley|PetSmart|Pacific.Life|Commercial.Metals|Agilent.Technologies|Huntington.Ingalls.Industries|Mutual.of.Omaha.Insurance|Live.Nation.Entertainment|Dick.s.Sporting.Goods|Oshkosh|Celanese|Spirit.AeroSystems.Holdings|United.Natural.Foods|Peabody.Energy|Owens.Illinois|Dillard.s|Level.3.Communications|Pantry|LKQ|Integrys.Energy.Group|Symantec|Buckeye.Partners|Ryder.System|SanDisk|Rockwell.Automation|Dana.Holding|Lansing.Trade.Group|NCR|Expeditors.International.of.Was|Omnicare|AK.Steel.Holding|Fifth.Third.Bancorp|Seaboard|NiSource|Cablevision.Systems|Anixter.International|EMCOR.Group|Fidelity.National.Information.S|Barnes...Noble|KBR|Auto.Owners.Insurance|Jones.Financial|Avery.Dennison|NetApp|iHeartMedia|Discovery.Communications|Harley.Davidson|Sanmina|Trinity.Industries|J.B..Hunt.Transport.Services|Charles.Schwab|Erie.Insurance.Group|Dr.Pepper.Snapple.Group|Ameren|Mattel|Laboratory.Corp..of.America|TEGNA|Starwood.Hotels...Resorts|General.Cable|A.Mark.Precious.Metals|Graybar.Electric|Energy.Future.Holdings|HRG.Group|MRC.Global|Spectra.Energy|Asbury.Automotive.Group|Packaging.Corp..of.America|Windstream.Holdings|PulteGroup|JetBlue.Airways|Newell.Rubbermaid|Con.way|Calumet.Specialty.Products.Part|Expedia|American.Financial.Group|Tractor.Supply|United.Rentals|Ingredion|Navient|MeadWestvaco|AGL.Resources|St..Jude.Medical|J.M..Smucker|Western.Union|Clorox|Domtar|Kelly.Services|Old.Republic.International|Advanced.Micro.Devices|Netflix|Booz.Allen.Hamilton.Holding|Quintiles.Transnational.Holding|Wynn.Resorts|Jones.Lang.LaSalle|Regions.Financial|CH2M.Hill|Western...Southern.Financial.Gr|Lithia.Motors|salesforce.com|Alaska.Air.Group|Host.Hotels...Resorts|Harman.International.Industries|Amphenol|Realogy.Holdings|Essendant|Hanesbrands|Kindred.Healthcare|ARRIS.Group|Insight.Enterprises|Alliance.Data.Systems|LifePoint.Health|Pioneer.Natural.Resources|Wyndham.Worldwide|Owens.Corning|Alleghany|McGraw.Hill.Financial|Big.Lots|Northern.Tier.Energy|Hexion|Markel|Noble.Energy|Leidos.Holdings|Rockwell.Collins|Airgas|Sprague.Resources|YRC.Worldwide|Hanover.Insurance.Group|Fiserv|Lorillard|American.Tire.Distributors.Hold|ABM.Industries|Sonoco.Products|Harris|Telephone...Data.Systems|WEC.Energy.Group|Linn.Energy|Raymond.James.Financial|Berry.Plastics.Group|Regency.Energy.Partners|SCANA|Cincinnati.Financial|Atmos.Energy|Pepco.Holdings|Flowserve|Simon.Property.Group|Constellation.Brands|Quad.Graphics|Burlington.Stores|Neiman.Marcus.Group|Bemis|Coach|Continental.Resources|Ascena.Retail.Group|Zoetis|Orbital.ATK|Frontier.Communications|Levi.Strauss|SPX|CF.Industries.Holdings|Michaels.Cos.|M.T.Bank.Corp.|Rush.Enterprises|Aleris|Nexeo.Solutions.Holdings|Keurig.Green.Mountain|Superior.Energy.Services|Williams.Sonoma|Robert.Half.International|Nvidia|First.American.Financial|Zimmer.Holdings|MDU.Resources.Group|Juniper.Networks|Arthur.J..Gallagher|Colfax|Cliffs.Natural.Resources|Yahoo|MasTec|Lam.Research|Axiall|Intercontinental.Exchange|Cintas|Coty|CA|Andersons|Valspar|Northern.Trust|Intuit|Tutor.Perini|Polaris.Industries|Hospira|FM.Global|NVR|Liberty.Media|Energizer.Holdings|Bloomin..Brands|Avaya|Westlake.Chemical|Hyatt.Hotels|Mead.Johnson.Nutrition|Activision.Blizzard|Protective.Life|Envision.Healthcare.Holdings|Fortune.Brands.Home...Security|RPM.International|VWR|LPL.Financial.Holdings|KeyCorp|Swift.Transportation|Alpha.Natural.Resources|Hasbro|Resolute.Forest.Products|Tiffany|McCormick|Graphic.Packaging.Holding|Greif|Allegheny.Technologies|Securian.Financial.Group|B.E.Aerospace|Exelis|Adobe.Systems|Molson.Coors.Brewing|Roundy.s|CNO.Financial.Group|Adams.Resources...Energy|Belk|Chipotle.Mexican.Grill|American.Tower|FMC|Hillshire.Brands|AmTrust.Financial.Services|Brunswick|Patterson|Southwestern.Energy|Ametek|T..Rowe.Price|Torchmark|Darling.Ingredients|Leggett...Platt|Watsco|Crestwood.Equity.Partners|Xylem|Silgan.Holdings|Toll.Brothers|Manitowoc|Science.Applications.Internatio|Carlyle.Group|Timken|Genesis.Energy|WPX.Energy|CareFusion|Pitney.Bowes|Ingles.Markets|PolyOne|Brookdale.Senior.Living|CommScope.Holding|Meritor|Joy.Global|Unified.Grocers|Triumph.Group|Magellan.Health|Sally.Beauty.Holdings|Flowers.Foods|Abercrombie...Fitch|New.Jersey.Resources|Fastenal|NII.Holdings|Consol.Energy|USG|Brink.s|Helmerich...Payne|Lexmark.International|American.Axle...Manufacturing|Crown.Castle.International|Targa.Energy|Oceaneering.International|Cabot|CIT.Group|Cabela.s|Forest.Laboratories|DCP.Midstream.Partners|Ryerson.Holding|QEP.Resources|Thor.Industries|HSN|Graham.Holdings|Electronic.Arts|Boise.Cascade|Hub.Group|CACI.International|Roper.Technologies|Towers.Watson|Smart...Final.Stores|Big.Heart.Pet.Brands|Fossil.Group|Nasdaq.OMX.Group|Country.Financial|Snap.on|Pinnacle.West.Capital|EchoStar|Systemax|WhiteWave.Foods|CUNA.Mutual.Group|Cooper.Tire...Rubber|ADT|Cerner|Clean.Harbors|First.Solar|Lennox.International|Enable.Midstream.Partners|Hubbell|Unisys|Alliant.Energy|Health.Care.REIT|Moody.s|C.R..Bard|Urban.Outfitters|Church...Dwight|American.Eagle.Outfitters|Oaktree.Capital.Group|Regal.Beloit|Men.s.Wearhouse|Cooper.Standard.Holdings|W.R..Grace|Ulta.Salon.Cosmetics...Fragranc|Hawaiian.Electric.Industries|SkyWest|Green.Plains|LVB.Acquisition|NBTY|Carlisle|United.Refining|Tesla.Motors|Groupon|Landstar.System|Patterson.UTI.Energy|EP.Energy|ON.Semiconductor|Rent.A.Center|SunGard.Data.Systems|Citrix.Systems|Amkor.Technology|TD.Ameritrade.Holding|Worthington.Industries|Valmont.Industries|Iron.Mountain|Puget.Energy|CME.Group|IAC.InterActiveCorp|Par.Petroleum|Taylor.Morrison.Home|Chiquita.Brands.International|International.Flavors...Fragran|Whiting.Petroleum|Under.Armour|Ventas|NuStar.Energy|Select.Medical.Holdings|Diebold|American.National.Insurance|Varian.Medical.Systems|Apollo.Education.Group|Westinghouse.Air.Brake.Technolo|SunPower|Warner.Music.Group|American.Water.Works|H.R.Block|Mercury.General|TECO.Energy|Service.Corp..International|Vulcan.Materials|Brown.Forman|Regal.Entertainment.Group|Tempur.Sealy.International|Steelcase|MWI.Veterinary.Supply|RadioShack|Sprouts.Farmers.Market|Sabre|Martin.Marietta.Materials|Huntington.Bancshares|Alere|TreeHouse.Foods|Arch.Coal|KLA.Tencor|Crane|Iasis.Healthcare|BWX.Technologies|Dentsply.International|Tribune.Media|ScanSource|Univision.Communications|Brinker.International|Exterran.Holdings|Carter.s|Analog.Devices|Genesco|Scotts.Miracle.Gro|Convergys|Exide.Technologies|WABCO.Holdings|Kennametal|Amerco|Bon.Ton.Stores|Team.Health.Holdings|Regeneron.Pharmaceuticals|Springleaf.Holdings|Lincoln.Electric.Holdings|Dresser.Rand.Group|West|Benchmark.Electronics|Pall|Old.Dominion.Freight.Line|MSC.Industrial.Direct|Sentry.Insurance.Group|Sigma.Aldrich|WGL.Holdings|Weis.Markets|Sanderson.Farms|StanCorp.Financial.Group|Hyster.Yale.Materials.Handling|Wolverine.World.Wide|DST.Systems|Legg.Mason|Teradata|Aaron.s|Antero.Resources|Metaldyne.Performance.Group|Range.Resources|Vornado.Realty.Trust|Boyd.Gaming|Covance|Armstrong.World.Industries|Cracker.Barrel.Old.Country.Stor|Chico.s.FAS|Scripps.Networks.Interactive|Universal.Forest.Products|Concho.Resources|ITT|HCC.Insurance.Holdings|Moog|IMS.Health.Holdings|Cinemark.Holdings|Comerica|Equity.Residential|Ryland.Group|GNC.Holdings|ArcBest|Vectren|Curtiss.Wright|Tupperware.Brands|Westar.Energy|Albemarle|AptarGroup|Pinnacle.Foods|Penn.National.Gaming|J.Crew.Group|Vantiv|Kansas.City.Southern|Caleres|Nu.Skin.Enterprises|Great.Plains.Energy|Kirby|General.Growth.Properties|Broadridge.Financial.Solutions|Stericycle|Global.Payments|Nortek|Schnitzer.Steel.Industries|Universal|ANN|Hologic|Panera.Bread|AOL|SM.Energy|Paychex|PriceSmart|Autodesk|Affiliated.Managers.Group|Tops.Holding|Dynegy|DSW|Vishay.Intertechnology|Mettler.Toledo.International|SunEdison|Tetra.Tech|Momentive.Performance.Materials|EnerSys|Donaldson|EQT|Monster.Beverage|PC.Connection|Total.System.Services|ServiceMaster.Global.Holdings|Medical.Mutual.of.Ohio|Applied.Industrial.Technologies|Maxim.Integrated.Products|OGE.Energy|A..Schulman|Equinix|Mednax|Equifax|Standard.Pacific|Denbury.Resources|Cimarex.Energy|Mutual.of.America.Life.Insuranc|Guess|Post.Holdings|HealthSouth|Ferrellgas.Partners|KB.Home|Boston.Properties|Trimble.Navigation|Teledyne.Technologies|Acuity.Brands|Skechers.U.S.A.|Xilinx|Plexus|Newfield.Exploration|TransDigm.Group|Kar.Auction.Services|Mueller.Industries|Zions.Bancorp.|Insperity|XPO.Logistics|Sears.Hometown...Outlet.Stores|A.O..Smith|Alliance.One.International|Take.Two.Interactive.Software|hhgregg|RPC|NewMarket|Beacon.Roofing.Supply|Edwards.Lifesciences|Triple.S.Management|Hawaiian.Holdings|Heartland.Payment.Systems|Belden|Magellan.Midstream.Partners|Outerwall|KapStone.Paper...Packaging|Alliance.Holdings|Skyworks.Solutions|Ciena|Granite.Construction|Education.Management|Party.City.Holdings|HCP|Parexel.International|Delta.Tucker.Holdings|Pinnacle.Entertainment|Stifel.Financial|Pool|Olin|Knights.of.Columbus|PerkinElmer|Alexion.Pharmaceuticals|IHS|Oil.States.International|HNI|LinkedIn|Diplomat.Pharmacy|Brocade.Communications.Systems|Greenbrier.Cos.|AMC.Networks|Kemper|Ocwen.Financial|Public.Storage|TriNet.Group|Chemtura|Symetra.Financial|Tower.International|Meritage.Homes|MarkWest.Energy.Partners|Bio.Rad.Laboratories|TrueBlue|Cabot.Oil...Gas|Carpenter.Technology|Toro|American.Equity.Investment.Life|Express|Eastman.Kodak|Hain.Celestial.Group|Nationstar.Mortgage.Holdings|IDEX|Popular|Werner.Enterprises|Esterline.Technologies|Intuitive.Surgical|Allison.Transmission.Holdings|SemGroup|Southwest.Gas|G.III.Apparel.Group|National.Fuel.Gas|H.B..Fuller|Penn.Mutual.Life.Insurance|RCS.Capital|Columbia.Sportswear|Amica.Mutual.Insurance|Primoris.Services|Energen|Rexnord|Seventy.Seven.Energy|Waste.Connections|Pep.Boys.Manny..Moe...Jack|Harsco|Hovnanian.Enterprises|Willbros.Group|Wendy.s|International.Game.Technology|Synopsys|Universal.American|AAR|Selective.Insurance.Group|Gartner|E.Trade.Financial'

create table fortune_1000_people_2016_06_28_unique as 
select distinct name,designation,company_name as company_name_person_page,industry_person,
company_name_1 as company_name_company_page,company_size,industry_company,website,domain
 from fortune_1000_people_2016_06_28 ;

select distinct * from (
select e.name,e.sub_text as designation,e.linkedin_url as linkedin_url_person,e.company_name as company_name,
e.industry as industry_person,a.linkedin_url as linkedin_url_company,a.company_name company_name_1, company_size,
a.industry as industry_company,website, 
replace(substring(website  from '.*://([^/]*)'),'www.','') as domain from
 fortune_1000_initial_company_list a join company_urls_mapper b on a.linkedin_url=b.alias_url join
 people_company_mapper c on b.base_url =c.company_url 
 join people_urls_mapper d on c.people_url = d.base_url 
 join linkedin_people_base e on d.alias_url=e.linkedin_url
 where 
 a.industry = 'Oil & Energy' and e.industry = 'Oil & Energy' and 
a.company_name ~ 'Walmart|Exxon.Mobil|Chevron|Berkshire.Hathaway|Apple|General.Motors|Phillips.66|General.Electric|Ford.Motor|CVS.Health|McKesson|AT.T|Valero.Energy|UnitedHealth.Group|Verizon|AmerisourceBergen|Fannie.Mae|Costco|HP|Kroger|JP.Morgan.Chase|Express.Scripts.Holding|Bank.of.America.Corp.|IBM|Marathon.Petroleum|Cardinal.Health|Boeing|Citigroup|Amazon.com|Wells.Fargo|Microsoft|Procter...Gamble|Home.Depot|Archer.Daniels.Midland|Walgreens|Target|Johnson...Johnson|Anthem|MetLife|Google|State.Farm.Insurance.Cos.|Freddie.Mac|Comcast|PepsiCo|United.Technologies|AIG|UPS|Dow.Chemical|Aetna|Lowe.s|ConocoPhillips|Intel|Energy.Transfer.Equity|Caterpillar|Prudential.Financial|Pfizer|Disney|Humana|Enterprise.Products.Partners|Cisco.Systems|Sysco|Ingram.Micro|Coca.Cola|Lockheed.Martin|FedEx|Johnson.Controls|Plains.GP.Holdings|World.Fuel.Services|CHS|American.Airlines.Group|Merck|Best.Buy|Delta.Air.Lines|Honeywell.International|HCA.Holdings|Goldman.Sachs.Group|Tesoro|Liberty.Mutual.Insurance.Group|United.Continental.Holdings|New.York.Life.Insurance|Oracle|Morgan.Stanley|Tyson.Foods|Safeway|Nationwide|Deere|DuPont|American.Express|Allstate|Cigna|Mondelez.International|TIAA.CREF|INTL.FCStone|Massachusetts.Mutual.Life.Insur|DirecTV|Halliburton|Twenty.First.Century.Fox|3M|Sears.Holdings|General.Dynamics|Publix.Super.Markets|Philip.Morris.International|TJX|Time.Warner|Macy.s|Nike|Tech.Data|Avnet|Northwestern.Mutual|McDonald.s|Exelon|Travelers.Cos.|Qualcomm|International.Paper|Occidental.Petroleum|Duke.Energy|Rite.Aid|Gilead.Sciences|Baker.Hughes|Emerson.Electric|EMC|USAA|Union.Pacific|Northrop.Grumman|Alcoa|Capital.One.Financial|National.Oilwell.Varco|US.Foods|Raytheon|Time.Warner.Cable|Arrow.Electronics|Aflac|Staples|Abbott.Laboratories|Community.Health.Systems|Fluor|Freeport.McMoRan|U.S..Bancorp|Nucor|Kimberly.Clark|Hess|Chesapeake.Energy|Xerox|ManpowerGroup|Amgen|AbbVie|Danaher|Whirlpool|PBF.Energy|HollyFrontier|Eli.Lilly|Devon.Energy|Progressive|Cummins|Icahn.Enterprises|AutoNation|Kohl.s|Paccar|Dollar.General|Hartford.Financial.Services.Gro|Southwest.Airlines|Anadarko.Petroleum|Southern|Supervalu|Kraft.Foods.Group|Goodyear.Tire...Rubber|EOG.Resources|CenturyLink|Altria.Group|Tenet.Healthcare|General.Mills|eBay|ConAgra.Foods|Lear|TRW.Automotive.Holdings|United.States.Steel|Penske.Automotive.Group|AES|Colgate.Palmolive|Global.Partners|Thermo.Fisher.Scientific|PG.E.Corp.|NextEra.Energy|American.Electric.Power|Baxter.International|Centene|Starbucks|Gap|Bank.of.New.York.Mellon.Corp.|Micron.Technology|Jabil.Circuit|PNC.Financial.Services.Group|Kinder.Morgan|Office.Depot|Bristol.Myers.Squibb|NRG.Energy|Monsanto|PPG.Industries|Genuine.Parts|Omnicom.Group|Illinois.Tool.Works|Murphy.USA|Land.O.Lakes|Western.Refining|Western.Digital|FirstEnergy|Aramark|DISH.Network|Las.Vegas.Sands|Kellogg|Loews|CBS|Ecolab|Whole.Foods.Market|Chubb|Health.Net|Waste.Management|Apache|Textron|Synnex|Marriott.International|Viacom|Lincoln.National|Nordstrom|C.H..Robinson.Worldwide|Edison.International|Marathon.Oil|Yum.Brands|Computer.Sciences|Parker.Hannifin|DaVita.HealthCare.Partners|CarMax|Texas.Instruments|WellCare.Health.Plans|Marsh...McLennan|Consolidated.Edison|Oneok|Visa|Jacobs.Engineering.Group|CSX|Entergy|Facebook|Dominion.Resources|Leucadia.National|Toys.R..Us...|DTE.Energy|Ameriprise.Financial|VF|Praxair|J.C..Penney|Automatic.Data.Processing|L.3.Communications|CDW|Guardian.Life.Ins..Co..of.Ameri|Xcel.Energy|Norfolk.Southern|PPL|R.R..Donnelley...Sons|Huntsman|Bed.Bath...Beyond|Stanley.Black...Decker|L.Brands|Liberty.Interactive|Farmers.Insurance.Exchange|First.Data|Sherwin.Williams|BlackRock|Voya.Financial|Ross.Stores|Sempra.Energy|Estee.Lauder|H.J..Heinz|Reinsurance.Group.of.America|Public.Service.Enterprise.Group|Cameron.International|Navistar.International|CST.Brands|State.Street.Corp.|Unum.Group|Hilton.Worldwide.Holdings|Family.Dollar.Stores|Principal.Financial|Reliance.Steel...Aluminum|Air.Products...Chemicals|Assurant|Peter.Kiewit.Sons.|Henry.Schein|Cognizant.Technology.Solutions|MGM.Resorts.International|W.W..Grainger|Group.1.Automotive|BB.T.Corp.|Rock.Tenn|Advance.Auto.Parts|Ally.Financial|AGCO|Corning|Biogen|NGL.Energy.Partners|Stryker|Molina.Healthcare|Precision.Castparts|Discover.Financial.Services|Genworth.Financial|Eastman.Chemical|Dean.Foods|AutoZone|MasterCard|Owens...Minor|Hormel.Foods|GameStop|Autoliv|CenterPoint.Energy|Fidelity.National.Financial|Sonic.Automotive|HD.Supply.Holdings|Charter.Communications|Crown.Holdings|Applied.Materials|Mosaic|CBRE.Group|Avon.Products|Republic.Services|Universal.Health.Services|Darden.Restaurants|Steel.Dynamics|SunTrust.Banks|Caesars.Entertainment|Targa.Resources|Dollar.Tree|News.Corp.|Ball|Thrivent.Financial.for.Lutheran|Masco|Franklin.Resources|Avis.Budget.Group|Reynolds.American|Becton.Dickinson|Priceline.Group|Broadcom|Tenneco|Campbell.Soup|AECOM|Visteon|Delek.US.Holdings|Dover|BorgWarner|Jarden|UGI|Murphy.Oil|PVH|Core.Mark.Holding|Calpine|D.R..Horton|Weyerhaeuser|KKR|FMC.Technologies|American.Family.Insurance.Group|SpartanNash|WESCO.International|Quanta.Services|Mohawk.Industries|Motorola.Solutions|Lennar|TravelCenters.of.America|Sealed.Air|Eversource.Energy|Coca.Cola.Enterprises|Celgene|Williams|Ashland|Interpublic.Group|Blackstone.Group|Ralph.Lauren|Quest.Diagnostics|Hershey|Terex|Boston.Scientific|Newmont.Mining|Allergan|O.Reilly.Automotive|Casey.s.General.Stores|CMS.Energy|Foot.Locker|W.R..Berkley|PetSmart|Pacific.Life|Commercial.Metals|Agilent.Technologies|Huntington.Ingalls.Industries|Mutual.of.Omaha.Insurance|Live.Nation.Entertainment|Dick.s.Sporting.Goods|Oshkosh|Celanese|Spirit.AeroSystems.Holdings|United.Natural.Foods|Peabody.Energy|Owens.Illinois|Dillard.s|Level.3.Communications|Pantry|LKQ|Integrys.Energy.Group|Symantec|Buckeye.Partners|Ryder.System|SanDisk|Rockwell.Automation|Dana.Holding|Lansing.Trade.Group|NCR|Expeditors.International.of.Was|Omnicare|AK.Steel.Holding|Fifth.Third.Bancorp|Seaboard|NiSource|Cablevision.Systems|Anixter.International|EMCOR.Group|Fidelity.National.Information.S|Barnes...Noble|KBR|Auto.Owners.Insurance|Jones.Financial|Avery.Dennison|NetApp|iHeartMedia|Discovery.Communications|Harley.Davidson|Sanmina|Trinity.Industries|J.B..Hunt.Transport.Services|Charles.Schwab|Erie.Insurance.Group|Dr.Pepper.Snapple.Group|Ameren|Mattel|Laboratory.Corp..of.America|TEGNA|Starwood.Hotels...Resorts|General.Cable|A.Mark.Precious.Metals|Graybar.Electric|Energy.Future.Holdings|HRG.Group|MRC.Global|Spectra.Energy|Asbury.Automotive.Group|Packaging.Corp..of.America|Windstream.Holdings|PulteGroup|JetBlue.Airways|Newell.Rubbermaid|Con.way|Calumet.Specialty.Products.Part|Expedia|American.Financial.Group|Tractor.Supply|United.Rentals|Ingredion|Navient|MeadWestvaco|AGL.Resources|St..Jude.Medical|J.M..Smucker|Western.Union|Clorox|Domtar|Kelly.Services|Old.Republic.International|Advanced.Micro.Devices|Netflix|Booz.Allen.Hamilton.Holding|Quintiles.Transnational.Holding|Wynn.Resorts|Jones.Lang.LaSalle|Regions.Financial|CH2M.Hill|Western...Southern.Financial.Gr|Lithia.Motors|salesforce.com|Alaska.Air.Group|Host.Hotels...Resorts|Harman.International.Industries|Amphenol|Realogy.Holdings|Essendant|Hanesbrands|Kindred.Healthcare|ARRIS.Group|Insight.Enterprises|Alliance.Data.Systems|LifePoint.Health|Pioneer.Natural.Resources|Wyndham.Worldwide|Owens.Corning|Alleghany|McGraw.Hill.Financial|Big.Lots|Northern.Tier.Energy|Hexion|Markel|Noble.Energy|Leidos.Holdings|Rockwell.Collins|Airgas|Sprague.Resources|YRC.Worldwide|Hanover.Insurance.Group|Fiserv|Lorillard|American.Tire.Distributors.Hold|ABM.Industries|Sonoco.Products|Harris|Telephone...Data.Systems|WEC.Energy.Group|Linn.Energy|Raymond.James.Financial|Berry.Plastics.Group|Regency.Energy.Partners|SCANA|Cincinnati.Financial|Atmos.Energy|Pepco.Holdings|Flowserve|Simon.Property.Group|Constellation.Brands|Quad.Graphics|Burlington.Stores|Neiman.Marcus.Group|Bemis|Coach|Continental.Resources|Ascena.Retail.Group|Zoetis|Orbital.ATK|Frontier.Communications|Levi.Strauss|SPX|CF.Industries.Holdings|Michaels.Cos.|M.T.Bank.Corp.|Rush.Enterprises|Aleris|Nexeo.Solutions.Holdings|Keurig.Green.Mountain|Superior.Energy.Services|Williams.Sonoma|Robert.Half.International|Nvidia|First.American.Financial|Zimmer.Holdings|MDU.Resources.Group|Juniper.Networks|Arthur.J..Gallagher|Colfax|Cliffs.Natural.Resources|Yahoo|MasTec|Lam.Research|Axiall|Intercontinental.Exchange|Cintas|Coty|CA|Andersons|Valspar|Northern.Trust|Intuit|Tutor.Perini|Polaris.Industries|Hospira|FM.Global|NVR|Liberty.Media|Energizer.Holdings|Bloomin..Brands|Avaya|Westlake.Chemical|Hyatt.Hotels|Mead.Johnson.Nutrition|Activision.Blizzard|Protective.Life|Envision.Healthcare.Holdings|Fortune.Brands.Home...Security|RPM.International|VWR|LPL.Financial.Holdings|KeyCorp|Swift.Transportation|Alpha.Natural.Resources|Hasbro|Resolute.Forest.Products|Tiffany|McCormick|Graphic.Packaging.Holding|Greif|Allegheny.Technologies|Securian.Financial.Group|B.E.Aerospace|Exelis|Adobe.Systems|Molson.Coors.Brewing|Roundy.s|CNO.Financial.Group|Adams.Resources...Energy|Belk|Chipotle.Mexican.Grill|American.Tower|FMC|Hillshire.Brands|AmTrust.Financial.Services|Brunswick|Patterson|Southwestern.Energy|Ametek|T..Rowe.Price|Torchmark|Darling.Ingredients|Leggett...Platt|Watsco|Crestwood.Equity.Partners|Xylem|Silgan.Holdings|Toll.Brothers|Manitowoc|Science.Applications.Internatio|Carlyle.Group|Timken|Genesis.Energy|WPX.Energy|CareFusion|Pitney.Bowes|Ingles.Markets|PolyOne|Brookdale.Senior.Living|CommScope.Holding|Meritor|Joy.Global|Unified.Grocers|Triumph.Group|Magellan.Health|Sally.Beauty.Holdings|Flowers.Foods|Abercrombie...Fitch|New.Jersey.Resources|Fastenal|NII.Holdings|Consol.Energy|USG|Brink.s|Helmerich...Payne|Lexmark.International|American.Axle...Manufacturing|Crown.Castle.International|Targa.Energy|Oceaneering.International|Cabot|CIT.Group|Cabela.s|Forest.Laboratories|DCP.Midstream.Partners|Ryerson.Holding|QEP.Resources|Thor.Industries|HSN|Graham.Holdings|Electronic.Arts|Boise.Cascade|Hub.Group|CACI.International|Roper.Technologies|Towers.Watson|Smart...Final.Stores|Big.Heart.Pet.Brands|Fossil.Group|Nasdaq.OMX.Group|Country.Financial|Snap.on|Pinnacle.West.Capital|EchoStar|Systemax|WhiteWave.Foods|CUNA.Mutual.Group|Cooper.Tire...Rubber|ADT|Cerner|Clean.Harbors|First.Solar|Lennox.International|Enable.Midstream.Partners|Hubbell|Unisys|Alliant.Energy|Health.Care.REIT|Moody.s|C.R..Bard|Urban.Outfitters|Church...Dwight|American.Eagle.Outfitters|Oaktree.Capital.Group|Regal.Beloit|Men.s.Wearhouse|Cooper.Standard.Holdings|W.R..Grace|Ulta.Salon.Cosmetics...Fragranc|Hawaiian.Electric.Industries|SkyWest|Green.Plains|LVB.Acquisition|NBTY|Carlisle|United.Refining|Tesla.Motors|Groupon|Landstar.System|Patterson.UTI.Energy|EP.Energy|ON.Semiconductor|Rent.A.Center|SunGard.Data.Systems|Citrix.Systems|Amkor.Technology|TD.Ameritrade.Holding|Worthington.Industries|Valmont.Industries|Iron.Mountain|Puget.Energy|CME.Group|IAC.InterActiveCorp|Par.Petroleum|Taylor.Morrison.Home|Chiquita.Brands.International|International.Flavors...Fragran|Whiting.Petroleum|Under.Armour|Ventas|NuStar.Energy|Select.Medical.Holdings|Diebold|American.National.Insurance|Varian.Medical.Systems|Apollo.Education.Group|Westinghouse.Air.Brake.Technolo|SunPower|Warner.Music.Group|American.Water.Works|H.R.Block|Mercury.General|TECO.Energy|Service.Corp..International|Vulcan.Materials|Brown.Forman|Regal.Entertainment.Group|Tempur.Sealy.International|Steelcase|MWI.Veterinary.Supply|RadioShack|Sprouts.Farmers.Market|Sabre|Martin.Marietta.Materials|Huntington.Bancshares|Alere|TreeHouse.Foods|Arch.Coal|KLA.Tencor|Crane|Iasis.Healthcare|BWX.Technologies|Dentsply.International|Tribune.Media|ScanSource|Univision.Communications|Brinker.International|Exterran.Holdings|Carter.s|Analog.Devices|Genesco|Scotts.Miracle.Gro|Convergys|Exide.Technologies|WABCO.Holdings|Kennametal|Amerco|Bon.Ton.Stores|Team.Health.Holdings|Regeneron.Pharmaceuticals|Springleaf.Holdings|Lincoln.Electric.Holdings|Dresser.Rand.Group|West|Benchmark.Electronics|Pall|Old.Dominion.Freight.Line|MSC.Industrial.Direct|Sentry.Insurance.Group|Sigma.Aldrich|WGL.Holdings|Weis.Markets|Sanderson.Farms|StanCorp.Financial.Group|Hyster.Yale.Materials.Handling|Wolverine.World.Wide|DST.Systems|Legg.Mason|Teradata|Aaron.s|Antero.Resources|Metaldyne.Performance.Group|Range.Resources|Vornado.Realty.Trust|Boyd.Gaming|Covance|Armstrong.World.Industries|Cracker.Barrel.Old.Country.Stor|Chico.s.FAS|Scripps.Networks.Interactive|Universal.Forest.Products|Concho.Resources|ITT|HCC.Insurance.Holdings|Moog|IMS.Health.Holdings|Cinemark.Holdings|Comerica|Equity.Residential|Ryland.Group|GNC.Holdings|ArcBest|Vectren|Curtiss.Wright|Tupperware.Brands|Westar.Energy|Albemarle|AptarGroup|Pinnacle.Foods|Penn.National.Gaming|J.Crew.Group|Vantiv|Kansas.City.Southern|Caleres|Nu.Skin.Enterprises|Great.Plains.Energy|Kirby|General.Growth.Properties|Broadridge.Financial.Solutions|Stericycle|Global.Payments|Nortek|Schnitzer.Steel.Industries|Universal|ANN|Hologic|Panera.Bread|AOL|SM.Energy|Paychex|PriceSmart|Autodesk|Affiliated.Managers.Group|Tops.Holding|Dynegy|DSW|Vishay.Intertechnology|Mettler.Toledo.International|SunEdison|Tetra.Tech|Momentive.Performance.Materials|EnerSys|Donaldson|EQT|Monster.Beverage|PC.Connection|Total.System.Services|ServiceMaster.Global.Holdings|Medical.Mutual.of.Ohio|Applied.Industrial.Technologies|Maxim.Integrated.Products|OGE.Energy|A..Schulman|Equinix|Mednax|Equifax|Standard.Pacific|Denbury.Resources|Cimarex.Energy|Mutual.of.America.Life.Insuranc|Guess|Post.Holdings|HealthSouth|Ferrellgas.Partners|KB.Home|Boston.Properties|Trimble.Navigation|Teledyne.Technologies|Acuity.Brands|Skechers.U.S.A.|Xilinx|Plexus|Newfield.Exploration|TransDigm.Group|Kar.Auction.Services|Mueller.Industries|Zions.Bancorp.|Insperity|XPO.Logistics|Sears.Hometown...Outlet.Stores|A.O..Smith|Alliance.One.International|Take.Two.Interactive.Software|hhgregg|RPC|NewMarket|Beacon.Roofing.Supply|Edwards.Lifesciences|Triple.S.Management|Hawaiian.Holdings|Heartland.Payment.Systems|Belden|Magellan.Midstream.Partners|Outerwall|KapStone.Paper...Packaging|Alliance.Holdings|Skyworks.Solutions|Ciena|Granite.Construction|Education.Management|Party.City.Holdings|HCP|Parexel.International|Delta.Tucker.Holdings|Pinnacle.Entertainment|Stifel.Financial|Pool|Olin|Knights.of.Columbus|PerkinElmer|Alexion.Pharmaceuticals|IHS|Oil.States.International|HNI|LinkedIn|Diplomat.Pharmacy|Brocade.Communications.Systems|Greenbrier.Cos.|AMC.Networks|Kemper|Ocwen.Financial|Public.Storage|TriNet.Group|Chemtura|Symetra.Financial|Tower.International|Meritage.Homes|MarkWest.Energy.Partners|Bio.Rad.Laboratories|TrueBlue|Cabot.Oil...Gas|Carpenter.Technology|Toro|American.Equity.Investment.Life|Express|Eastman.Kodak|Hain.Celestial.Group|Nationstar.Mortgage.Holdings|IDEX|Popular|Werner.Enterprises|Esterline.Technologies|Intuitive.Surgical|Allison.Transmission.Holdings|SemGroup|Southwest.Gas|G.III.Apparel.Group|National.Fuel.Gas|H.B..Fuller|Penn.Mutual.Life.Insurance|RCS.Capital|Columbia.Sportswear|Amica.Mutual.Insurance|Primoris.Services|Energen|Rexnord|Seventy.Seven.Energy|Waste.Connections|Pep.Boys.Manny..Moe...Jack|Harsco|Hovnanian.Enterprises|Willbros.Group|Wendy.s|International.Game.Technology|Synopsys|Universal.American|AAR|Selective.Insurance.Group|Gartner|E.Trade.Financial'
 ) a ;

--final query
select distinct * from fortune_1000_people_2016_06_28_unique 
where (regexp_replace(regexp_replace(designation,'\yin\y|\yof\y|\yat\y|[^a-zA-Z]',' '),' +',' ') ~* 
	'\yDirector\y|\ySenior Director\y|\ySenior VP\y|\yVice President\y|\yHead\y|\yGlobal\y|\yChief\y|\yPresident\y' or 
regexp_replace(regexp_replace(designation,'\yin\y|\yof\y|\yat\y|[^a-zA-Z]',' '),' +',' ') ~
	'\yVP\y|\yAVP\y|\yEVP\y') and 
(regexp_replace(regexp_replace(designation,'\yin\y|\yof\y|\yat\y|[^a-zA-Z]',' '),' +',' ') ~ 
	'\yHR\y' or
regexp_replace(regexp_replace(designation,'\yin\y|\yof\y|\yat\y|[^a-zA-Z]',' '),' +',' ') ~* 
	'\yHuman Resources\y|\yHuman Resource\y|\yLearning\y|\ySkill Development\y|\yTraining\y|\yTalent Management\y'
)

'''