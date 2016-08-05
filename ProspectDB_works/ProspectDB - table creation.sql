
CREATE TABLE Company
(
	Company_ID            serial NOT NULL,
	Website              varchar NULL,
	Founded              varchar NULL,
	Description          varchar NULL,
	Company_Size         varchar NULL,
	Pinterest            varchar NULL,
	Google               varchar NULL,
	Linkedin             varchar NULL,
	Facebook             varchar NULL,
	Twitter              varchar NULL,
	GIthub               varchar NULL,
	Instagram            varchar NULL,
	vk                   varchar NULL,
	Vimeo                varchar NULL,	
	Address              varchar NULL,	
	Location_ID           int NULL,
	Specialized_Group_ID   int NULL,
	Company_Type_ID        int NULL,
	Company_Group_ID       int NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

ALTER TABLE Company
ADD PRIMARY KEY (Company_ID);

CREATE TABLE Company_Funding_Rounds
(
	Is_Latest_Funding      BIT NULL,
	Funding_Amount        FLOAT NULL,
	Funding_Currency      varchar NULL,
	Funding_Date          timestamp default current_timestamp,
	Funding_Type_ID        int NULL,
	Investor_ID           int NULL,
	Pre_Money_Valuation_Amount float NULL,
	Pre_Money_Valuation_Code varchar NULL,
	Pre_Money_Valuation_Amount_USD varchar NULL,
	Post_Money_Valuation_Amount float NULL,
	Post_Money_Valuation_Code varchar NULL,
	Post_Money_Valuation_Amount_USD float NULL,
	Participants         int NULL,
	Public_At_IPO          date NULL,
	Stock_Symbol_IPO       varchar NULL,
	Company_ID            int NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

CREATE TABLE Company_Group
(
	Company_Group_ID       serial NOT NULL,
	Website              varchar NULL,
	Founded              varchar NULL,
	Description          varchar NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

ALTER TABLE Company_Group
ADD PRIMARY KEY (Company_Group_ID);

CREATE TABLE Company_Type
(
	Company_Type_ID        serial NOT NULL,
	Company_Type        varchar,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

ALTER TABLE Company_Type
ADD PRIMARY KEY (Company_Type_ID);

CREATE TABLE Industry
(
	Industry_ID           serial NOT NULL,
	Industry_Description  varchar NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

ALTER TABLE Industry
ADD PRIMARY KEY (Industry_ID);

CREATE TABLE Investor_CB_Funds
(
	Investor_ID           serial NOT NULL,
	Investor_Code         varchar NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

ALTER TABLE Investor_CB_Funds
ADD PRIMARY KEY (Investor_ID);

CREATE TABLE Investor_Funds
(
	Investor_ID           serial not NULL,
	Investor_Fund_Amount   varchar NULL,
	Fund_Date             timestamp default current_timestamp,
	Investor_Name         varchar NULL,
	Fund_Currency         varchar NULL,
	Source_URL            varchar NULL,
	Source_Description    varchar NULL,
	Investor_Fund_ID       int NOT NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

ALTER TABLE Investor_Funds
ADD PRIMARY KEY (Investor_Fund_ID);

CREATE TABLE Location_Hierarchy
(
	Location_ID           serial NOT NULL,
	Country              varchar NULL,
	State                varchar NULL,
	City                 varchar NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

ALTER TABLE Location_Hierarchy
ADD PRIMARY KEY (Location_ID);

CREATE TABLE People
(
	Person_ID	     serial not null,
	Name                 varchar NULL,
	Role_as_per_LinkedIn    varchar NULL,
	Location             int NULL,
	Current_Company       int NULL,
	Previous_Company_array varchar NULL,
	Summary              varchar NULL,
	Skills               varchar NULL,
	Contact_Number        int NULL,
	Linkedin_URL          varchar NULL,
	Experience           varchar NULL,
	Related_Person        varchar NULL,
	Derived_Role          varchar NULL,
	Location_ID           int NULL,
	Company_ID            int NULL,
	Email_Address         varchar NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp	
);

CREATE TABLE Specialized_Group
(
	Specialized_Group_ID   serial NOT NULL,
	Industry_ID           int NULL,
	Specialized_Group     varchar NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

ALTER TABLE Specialized_Group
ADD PRIMARY KEY (Specialized_Group_ID);

CREATE TABLE Technologies_Supported
(
	Array_of_Skills_Technologies varchar NULL,
	Company_ID            int NULL,
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

CREATE TABLE Type_Of_Funding_Angel_To_IPO
(
	Funding_Type_ID        serial NOT NULL,
	Funding_Type_CD        varchar NULL,
	Funding_Type          varchar NULL,
	Funding_Code          varchar NULL,	
	Record_Created_Date    timestamp default current_timestamp,
	Record_Updated_Date    timestamp default current_timestamp
);

ALTER TABLE Type_Of_Funding_Angel_To_IPO
ADD PRIMARY KEY (Funding_Type_ID);

ALTER TABLE Company
ADD CONSTRAINT R_17 FOREIGN KEY (Location_ID) REFERENCES Location_Hierarchy (Location_ID);

ALTER TABLE Company
ADD CONSTRAINT R_4 FOREIGN KEY (Specialized_Group_ID) REFERENCES Specialized_Group (Specialized_Group_ID);

ALTER TABLE Company
ADD CONSTRAINT R_5 FOREIGN KEY (Company_Type_ID) REFERENCES Company_Type (Company_Type_ID);

ALTER TABLE Company
ADD CONSTRAINT R_1 FOREIGN KEY (Company_Group_ID) REFERENCES Company_Group (Company_Group_ID);

ALTER TABLE Company_Funding_Rounds
ADD CONSTRAINT R_10 FOREIGN KEY (Funding_Type_ID) REFERENCES Type_Of_Funding_Angel_To_IPO (Funding_Type_ID);

ALTER TABLE Company_Funding_Rounds
ADD CONSTRAINT R_12 FOREIGN KEY (Investor_ID) REFERENCES Investor_CB_Funds (Investor_ID);

ALTER TABLE Company_Funding_Rounds
ADD CONSTRAINT R_18 FOREIGN KEY (Company_ID) REFERENCES Company (Company_ID);

ALTER TABLE Investor_Funds
ADD CONSTRAINT R_13 FOREIGN KEY (Investor_ID) REFERENCES Investor_CB_Funds (Investor_ID);

ALTER TABLE People
ADD CONSTRAINT R_6 FOREIGN KEY (Location_ID) REFERENCES Location_Hierarchy (Location_ID);

ALTER TABLE People
ADD CONSTRAINT R_7 FOREIGN KEY (Company_ID) REFERENCES Company (Company_ID);

ALTER TABLE Specialized_Group
ADD CONSTRAINT R_3 FOREIGN KEY (Industry_ID) REFERENCES Industry (Industry_ID);

ALTER TABLE Technologies_Supported
ADD CONSTRAINT R_9 FOREIGN KEY (Company_ID) REFERENCES Company (Company_ID);
