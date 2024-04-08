BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "sqm_total_property_listing_old" (
	"id"	INTEGER,
	"postcode"	TEXT,
	"month_year"	TEXT,
	"property_type"	TEXT,
	"property_num"	INTEGER,
	"property_total"	INTEGER,
	"create_datetime"	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "sqm_total_property_listing" (
	"id"	INTEGER,
	"postcode"	TEXT NOT NULL,
	"month_year"	TEXT,
	"unit_raw"	INTEGER,
	"unit_num"	INTEGER,
	"house_raw"	INTEGER,
	"house_num"	INTEGER,
	"property_total"	INTEGER,
	"hidden_seed"	INTEGER,
	"common_factors"	TEXT,
	"err_flag"	TEXT,
	"create_datetime"	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "sqm_established_properties" (
	"id"	INTEGER,
	"postcode"	TEXT NOT NULL,
	"year"	TEXT,
	"type"	TEXT,
	"established_prop"	INTEGER,
	"err_flag"	TEXT,
	"create_datetime"	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "sqm_vacancy_rate" (
	"id"	INTEGER,
	"postcode"	TEXT NOT NULL,
	"month_year"	TEXT,
	"vacancies_raw"	INTEGER,
	"vacancies_num"	INTEGER,
	"vacancy_rate_raw"	REAL,
	"vacancy_rate_percentage"	REAL,
	"hidden_seed"	INTEGER,
	"err_flag"	TEXT,
	"create_datetime"	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "sqm_occupant_type" (
	"id"	INTEGER,
	"postcode"	TEXT NOT NULL,
	"year"	TEXT,
	"owner_outright"	REAL,
	"mortgage_holders"	REAL,
	"rented"	REAL,
	"others"	REAL,
	"not_stated"	REAL,
	"err_flag"	TEXT,
	"create_datetime"	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "pv_suburb_url" (
	"id"	INTEGER,
	"postcode"	TEXT NOT NULL,
	"suburb"	TEXT,
	"suburb_url"	TEXT,
	"original_url"	TEXT,
	"err_flag"	TEXT,
	"create_datetime"	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("id" AUTOINCREMENT)
);
CREATE TABLE IF NOT EXISTS "pv_market_trends" (
	"id"	INTEGER,
	"postcode"	TEXT NOT NULL,
	"suburb"	TEXT,
	"median_value"	TEXT,
	"properties_sold"	TEXT,
	"median_rent"	TEXT,
	"median_gross_yield"	TEXT,
	"average_days_on_market"	TEXT,
	"average_vendor_discount"	TEXT,
	"median_price_change_1yr"	TEXT,
	"data_time"	TEXT,
	"suburb_url"	TEXT,
	"err_flag"	TEXT,
	"create_datetime"	TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY("id" AUTOINCREMENT)
);
COMMIT;
