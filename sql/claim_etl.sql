-- PostgreSQL and PL/pgSQL
-- This is an example of a simple ETL (Extract, Transform, Load) process 
-- for loading raw claim data into staging tables, performing validation, 
-- normalizing the data, and loading it into a final table for easier querying and analysis.
-- 
-- 
-- Create three faux tables with insurance claim type data. Each table represents a different
-- insurance company and has differing columns, column names, and data.

create table staging_ins_a (
	claimdate date,
	patientid varchar,
	memberid varchar,
	claimid varchar,
	servicecode varchar,
	servicedescs varchar,
	amount numeric,
	status varchar
);



create table staging_ins_b (
	claim_date date,
	claim_id varchar,
	member_id varchar,
	patient_id varchar,
	patient_fname varchar,
	patient_lname varchar,
	service_code varchar,
	amount numeric,
	status varchar
);



create table staging_ins_c (
	claimdate date,
	mrn varchar,
	mem_id varchar,
	patient_name varchar,
	claim_num varchar,
	serv_type varchar,
	serv varchar,
	amount_chrg numeric,
	claim_status varchar
);


-- Create a table that will contain the data from the three different data sources.

create table normalized_claims (
	id integer generated always as identity primary key,
	claim_date date,
	claim_id varchar,
	claim_status varchar,
	payer_id varchar,
	payer_name varchar,
	patient_id varchar,
	patient_memb_id varchar,
	cpt_code varchar,
	cpt_desc varchar,
	amount numeric,
  date_rec_added timestamp,
);


-- Use COPY to load the .csv file from each data source into the corresponding tables
-- created above.

copy staging_ins_a(patientid, memberid, claimid, servicecode, servicedescs, amount, claimdate, status)
from 'X:/projects/ins_claims/company_a/ins_src_a.csv'
delimiter ','
csv header;

copy staging_ins_b(claim_date, claim_id, member_id, patient_id, patient_fname, patient_lname, service_code, amount, status)
from 'X:/projects/ins_claims/company_b/ins_src_b.csv'
delimiter ','
csv header;

copy staging_ins_c(mrn, patient_name, mem_id, claim_num, claim_date, serv_type, serv, amount_chrg, claim_status)
from 'X:/projects/ins_claims/company_c/ins_src_c.csv'
delimiter ','
csv header;

-- Query each staging table and view the data.

select * from staging_ins_a;

select * from staging_ins_b;

select * from staging_ins_c;

select * from normalized_claims;


-- Get statistics for Null values, Distinct values, and Duplicate values from each staging 
-- table for data validation.

-- For staging_ins_a
do $$
declare
	ins_table text := 'staging_ins_a';
	rec RECORD;
	col_name text;
	null_count INTEGER;
	unique_count INTEGER;
	duplicate_count INTEGER;
	row_count INTEGER;
begin
	select count(*) into row_count from staging_ins_a;

	for rec in 
		select column_name
		from information_schema.columns 
		where table_name = 'staging_ins_a'
	loop 
		col_name := rec.column_name;
		-- count null values
		execute 'select count(*) from staging_ins_a WHERE ' || col_name || ' IS NULL'
		into null_count;
		-- count distinct values
		execute 'select count(distinct ' || col_name || ') FROM staging_ins_a WHERE ' || col_name || ' is not null' into unique_count;
		-- count duplicate values
		execute 'select count(' || col_name || ') - count(distinct ' || col_name || ') from staging_ins_a WHERE ' || col_name || ' is not null' into duplicate_count;
		-- show results
		raise notice  'Table: %, Column: %, Null Values: %, Unique Values: %, Duplicate Values: %', 
					ins_table, col_name, null_count, unique_count, duplicate_count;
	end loop;
end $$;

-- For staging_ins_b
do $$
declare
	ins_table text := 'staging_ins_b';
	rec RECORD;
	col_name text;
	null_count INTEGER;
	unique_count INTEGER;
	duplicate_count INTEGER;
	row_count INTEGER;
begin
	select count(*) into row_count from staging_ins_b;

	for rec in 
		select column_name
		from information_schema.columns 
		where table_name = 'staging_ins_b'
	loop 
		-- column names
		col_name := rec.column_name;
		-- count null values
		execute 'select count(*) from staging_ins_b WHERE ' || col_name || ' IS NULL'
		into null_count;
		-- count distinct values
		execute 'select count(distinct ' || col_name || ') FROM staging_ins_b WHERE ' || col_name || ' is not null' into unique_count;
		-- count duplicate values
		execute 'select count(' || col_name || ') - count(distinct ' || col_name || ') from staging_ins_b WHERE ' || col_name || ' is not null' into duplicate_count;
		-- show results
		raise notice  'Table: %, Column: %, Null Values: %, Unique Values: %, Duplicate Values: %', 
					ins_table, col_name, null_count, unique_count, duplicate_count;
	end loop;
end $$;

-- For staging_ins_c
do $$
declare
	ins_table text := 'staging_ins_c';
	rec RECORD;
	col_name text;
	null_count INTEGER;
	unique_count INTEGER;
	duplicate_count INTEGER;
	row_count INTEGER;
begin
	select count(*) into row_count from staging_ins_c;

	for rec in 
		select column_name
		from information_schema.columns 
		where table_name = 'staging_ins_c'
	loop 
		col_name := rec.column_name;
		-- count null values
		execute 'select count(*) from staging_ins_c WHERE ' || col_name || ' IS NULL'
		into null_count;
		-- count distinct values
		execute 'select count(distinct ' || col_name || ') FROM staging_ins_c WHERE ' || col_name || ' is not null' into unique_count;
		-- count duplicate values
		execute 'select count(' || col_name || ') - count(distinct ' || col_name || ') from staging_ins_c WHERE ' || col_name || ' is not null' into duplicate_count;
		-- show results
		raise notice  'Table: %, Column: %, Null Values: %, Unique Values: %, Duplicate Values: %', 
					ins_table, col_name, null_count, unique_count, duplicate_count;
	end loop;
end $$;

-- Insert staging_ins_a into the final unified table
insert into normalized_claims (
	claim_date, 
	claim_id, 
	claim_status, 
	payer_id, 
	payer_name, 
	patient_id, 
	patient_memb_id, 
	cpt_code, 
	cpt_desc, 
	amount,
	date_rec_added)
select 
	claimdate as claim_date,
	claimid as claim_id,
	status as claim_status,
	'A1234' as payer_id,
	'ins_A' as payer_name,
	patientid as patient_id,
	memberid as patient_memb_id,
	servicecode as cpt_code,
	servicedescs as cpt_desc,
	amount as amount,
	NOW() as date_rec_added 
from
	staging_ins_a;

select * from normalized_claims;

-- Insert staging_ins_b into the final unified table
insert into normalized_claims (
	claim_date, 
	claim_id, 
	claim_status, 
	payer_id, 
	payer_name, 
	patient_id, 
	patient_memb_id, 
	cpt_code, 
	cpt_desc, 
	amount,
	date_rec_added)
select 
	claim_date as claim_date,
	claim_id as claim_id,
	status as claim_status,
	'B4321' as payer_id,
	'ins_B' as payer_name,
	patient_id as patient_id,
	member_id as patient_memb_id,
	service_code as cpt_code,
	NULL as cpt_desc,
	amount as amount,
	NOW() as date_rec_added 
from
	staging_ins_b;

select * from normalized_claims;


-- Insert staging_ins_c into the final unified table
insert into normalized_claims (
	claim_date, 
	claim_id, 
	claim_status, 
	payer_id, 
	payer_name, 
	patient_id, 
	patient_memb_id, 
	cpt_code, 
	cpt_desc, 
	amount,
	date_rec_added)
select 
	claim_date as claim_date,
	claim_num as claim_id,
	claim_status as claim_status,
	'C1324' as payer_id,
	'ins_C' as payer_name,
	mrn as patient_id,
	mem_id as patient_memb_id,
	serv_type as cpt_code,
	serv as cpt_desc,
	amount_chrg as amount,
	NOW() as date_rec_added 
from
	staging_ins_c;

select * from normalized_claims;


-- These are possible useful indexes depending on frequent queries.
create index idx_claim_date on normalized_claims (claim_date);

create index idx_claim_id on normalized_claims (claim_id);

create index idx_payer_id on normalized_claims (payer_id);

create index idx_cpt_code on normalized_claims (cpt_code);

create index idx_patient_id on normalized_claims (patient_id);

create index idx_claim_date_payer_id on normalized_claims (claim_date, payer_id);

