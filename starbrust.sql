create schema if not exists hive.hol_hive WITH (location = 's3a://lab02/hive/');
create schema IF not exists iceberg.hol_iceberg WITH (location = 's3a://lab02/iceberg/');
create schema if not exists postgresql.hol;

create table postgresql.hol.customer
(
Index int,
Customer_Id varchar(15),
First_Name varchar(100),
Last_Name varchar(100),
Company varchar(100),
City varchar(100),
Country varchar(100),
Phone_1 varchar(100),
Phone_2 varchar(100),
Email varchar(100),
Subscription Date,
Website varchar(100)


) ;

insert into postgresql.hol.customer values(1,'EB54EF1154C3A78','Heather','Callahan','Mosley-David','Lake Jeffborough','Norway','043-797-5229','915.112.1727','urangel@espinoza-francis.net',date('2020-08-26'),'http://www.escobar.org/');
insert into postgresql.hol.customer values(2,'10dAcafEBbA5FcA','Kristina','Ferrell','Horn ,Shepard and Watson' ,'Aaronville','Andorra','932-062-1802','(209)172-7124x3651','xreese@hall-donovan.com',date('2020-04-27'),'https://tyler-pugh.info/');
insert into postgresql.hol.customer values(3,'67DAB15Ebe4BE4a','Briana','Andersen','Irwin-Oneal','East Jordan','Nepal','8352752061','(567)135-1918','haleybraun@blevins-sexton.com',date('2022-03-22'),'https://www.mack-bell.net/');
insert into postgresql.hol.customer values(4,'6d350C5E5eDB4EE','Patty','Ponce','Richardson Group','East Kristintown','Northern Mariana Islands','302.398.3833','196-189-7767x770','hohailey@anthony.com',date('2020-07-02'),'https://delacruz-freeman.org/');
insert into postgresql.hol.customer values(5,'5820deAdCF23EFe','Kathleen','Mccormick','Carson-Burch','Andresmouth','Macao','001-184-153-9683x1497','552.051.2979x342','alvaradojesse@rangel-shields.com',date('2021-01-17'),'https://welch.info/');
insert into postgresql.hol.customer values(6,'E1CDEaC63fDd5aA','Trevor','Lee','Maddox Group','Lake Madelineburgh','Senegal','+1-134-348-0265x9132','+1-313-882-4167','briangriffin@chang.org',date('2021-08-13'),'https://www.roberts.com/');
insert into postgresql.hol.customer values(7,'3e1187fCcebC8d2','Mathew','Hoffman','Bender, Pittman and Kidd','West Ralph','Uzbekistan','842.380.1168','(178)178-5447x32603','bauergerald@morrison.com',date('2020-04-09'),'http://www.holt.com/');
insert into postgresql.hol.customer values(8,'47C5cEE243c9A7b','Glenn','Wiggins','Glenn-Harvey','Ambershire','Falkland Islands (Malvinas)','245-207-5608x563','8806867785','changkellie@howell.com',date('2021-04-02'),'http://carlson.com/');
insert into postgresql.hol.customer values(9,'cacaD68a5e4BF4b','Bruce','Payne','Arroyo, Cain and Hudson','Barrettview','Zimbabwe','391.313.4649x42910','166.227.5055','mayerjerome@hurst-graham.net',date('2020-11-26'),'https://www.glenn-snow.com/');
insert into postgresql.hol.customer values(10,'436b9c41cfb1fa3','Brendan','Franco','Schaefer , Blair and Shaffer','New Rickey','Ukraine','001-315-224-3556','254-621-7128x7573','kentryan@stone-oneill.com',date('2021-06-29'),'http://ruiz.com/');


-- delete from  postgresql.hol.customer;


create table if not exists iceberg.hol_iceberg.customer_landing
(
Index int,
Customer_Id varchar(15),
First_Name varchar(100),
Last_Name varchar(100),
Company varchar(100),
City varchar(100),
Country varchar(100),
Phone_1 varchar(100),
Phone_2 varchar(100),
Email varchar(100),
Subscription Date,
Website varchar(100)
) ;


insert into iceberg.hol_iceberg.customer_landing
(select * from postgresql.hol.customer);


create table if not exists iceberg.hol_iceberg.customer_full
(
Index int,
Customer_Id varchar(15),
First_Name varchar(100),
Last_Name varchar(100),
Company varchar(100),
City varchar(100),
Country varchar(100),
Phone_1 varchar(100),
Phone_2 varchar(100),
Email varchar(100),
Subscription Date,
Website varchar(100),
Eff_dt_from date,
Eff_dt_to date
) ;

----------------------------CDC----------------------------------------------------

drop table if exists iceberg.hol_iceberg.temp_customer;

create table if not exists iceberg.hol_iceberg.temp_customer
(
Index int,
Customer_Id varchar(15),
First_Name varchar(100),
Last_Name varchar(100),
Company varchar(100),
City varchar(100),
Country varchar(100),
Phone_1 varchar(100),
Phone_2 varchar(100),
Email varchar(100),
Subscription Date,
Website varchar(100),
Eff_dt_from date,
Eff_dt_to date,
Flag varchar(20)
) ;

insert into  iceberg.hol_iceberg.temp_customer  
select * from 
(
	Select * from (
		Select hist.Index ,
		hist.Customer_Id,
		hist.First_Name,
		hist.Last_Name ,
		hist.Company ,
		hist.City ,
		hist.Country ,
		hist.Phone_1 ,
		hist.Phone_2 ,
		hist.Email ,
		hist.Subscription ,
		hist.Website ,
		Eff_dt_from,
		CURRENT_DATE,
		'Update' 
		from iceberg.hol_iceberg.customer_full hist
		left outer join iceberg.hol_iceberg.customer_landing existing
		on existing.Customer_Id = hist.Customer_Id
        and hist.Eff_dt_to = date('9999-12-31')
		where existing.Customer_Id is null

	)
	union all
	Select *  from
	(
		select existing.*,hist_2.Eff_dt_from,CURRENT_DATE,'Update' from iceberg.hol_iceberg.customer_landing existing
		left outer join iceberg.hol_iceberg.customer_full hist
		on existing.Customer_Id = hist.Customer_Id
		and existing.First_Name = hist.First_Name
		and existing.Last_Name = hist.Last_Name 
		and existing.Company = hist.Company 
		and existing.City = hist.City 
		and existing.Country = hist.Country 
		and existing.Phone_1 = hist.Phone_1 
		and existing.Phone_2 = hist.Phone_2 
		and existing.Email = hist.Email
		and existing.Subscription = hist.Subscription 
		and  existing.Website = hist.Website      
		and hist.Eff_dt_to =date('9999-12-31') 
		left outer join iceberg.hol_iceberg.customer_full hist_2
		on existing.Customer_Id = hist_2.Customer_Id
		and hist_2.Eff_dt_to =date('9999-12-31') 
		where hist.Customer_Id is null and hist_2.Customer_Id is not null
	
	)
)
union all 
(
    select existing.*,date('1900-01-01'),date('9999-12-31'),'Insert' from iceberg.hol_iceberg.customer_landing existing
    left outer join iceberg.hol_iceberg.customer_full hist
    on existing.Customer_Id = hist.Customer_Id
	and hist.Eff_dt_to=date('9999-12-31')
    where hist.Customer_Id is null

)
union all
(
	select existing.*,CURRENT_DATE,date('9999-12-31'),'Insert' from iceberg.hol_iceberg.customer_landing existing
    left outer join iceberg.hol_iceberg.customer_full hist
    on existing.Customer_Id = hist.Customer_Id
	and existing.First_Name = hist.First_Name
	and existing.Last_Name = hist.Last_Name 
	and existing.Company = hist.Company 
	and existing.City = hist.City 
	and existing.Country = hist.Country 
	and existing.Phone_1 = hist.Phone_1 
	and existing.Phone_2 = hist.Phone_2 
	and existing.Email = hist.Email
	and existing.Subscription = hist.Subscription 
	and  existing.Website = hist.Website 
	and hist.Eff_dt_to =date('9999-12-31') 
		left outer join iceberg.hol_iceberg.customer_full hist_2
		on existing.Customer_Id = hist_2.Customer_Id
		and hist_2.Eff_dt_to =date('9999-12-31') 
		where hist.Customer_Id is null and hist_2.Customer_Id is not null
	
);

Select * from  iceberg.hol_iceberg.customer_full ;

update iceberg.hol_iceberg.customer_full 
set eff_dt_to = CURRENT_DATE 
where exists (select 1 from iceberg.hol_iceberg.temp_customer temp
where temp.Customer_Id = iceberg.hol_iceberg.customer_full.Customer_Id
and flag = 'Update');

insert into iceberg.hol_iceberg.customer_full 
(
    Select  
    temp.Index ,
temp.Customer_Id,
temp.First_Name,
temp.Last_Name ,
temp.Company ,
temp.City ,
temp.Country ,
temp.Phone_1 ,
temp.Phone_2 ,
temp.Email ,
temp.Subscription ,
temp.Website,
temp.eff_dt_from,
temp.eff_dt_to
    from iceberg.hol_iceberg.temp_customer temp
    where flag='Insert'

);

-- delete from iceberg.hol_iceberg.customer_full;

Select * from  iceberg.hol_iceberg.customer_full;



-------------------------Test CDC------------------------------------------

update postgresql.hol.customer 
set first_name ='abc'
where customer_id ='EB54EF1154C3A78'
;

delete from postgresql.hol.customer 
where customer_id = '10dAcafEBbA5FcA';

insert into postgresql.hol.customer 
values(11,'436b9feoihfioa3','def','haha','Schaefer , Blair and Shaffer','New Rickey','Ukraine','001-315-224-3556','254-621-7128x7573','kentryan@stone-oneill.com',date('2021-06-29'),'http://ruiz.com/');


delete from iceberg.hol_iceberg.customer_landing;

insert into iceberg.hol_iceberg.customer_landing
(select * from postgresql.hol.customer);

---------------------rerun CDC script----------------------------------------------
