-- REQUIREMENT
-- Save the folder in disk F
-- Otherwise, the path in bulk insert will not work.


Create database Sql_cleaning
go 
USE SQL_CLEANING
GO



-- Create table to hold data.
go
CREATE TABLE REAL_ESTATE
( 
	UniqueID 		varchar(20)	,
	ParcelID		varchar(20)	,
	LandUse			varchar(50)	,
	PropertyAddress	varchar(200),
	SaleDate		varchar(200),
	SalePrice		varchar(200),
	LegalReference	varchar(50)	,
	SoldAsVacant	varchar(20)	,
	OwnerName		varchar(200),
	OwnerAddress	varchar(200),
	Acreage			float,
	TaxDistrict		varchar(200),
	LandValue		varchar(200),
	BuildingValue	varchar(200),
	TotalValue		varchar(200),
	YearBuilt		float,
	Bedrooms		float,
	FullBath		float,
	HalfBath		float,

)
GO




-- Insert data from the tab separated file.

BULK INSERT Real_estate
FROM 'F:\Nashville_Housing_Data.txt'
WITH(
    --CODEPAGE = '65001', 
    --DATAFILETYPE = 'Char',
	FIRSTROW = 2,
    FIELDTERMINATOR = '\t',
    ROWTERMINATOR = '\n'
)
GO


--- CHANGE DATATYPES OF THE COLUMNS ----


go
alter table Real_Estate alter Column SalePrice money 
alter table Real_Estate alter Column LandValue money
alter table Real_Estate alter Column BuildingValue money
alter table Real_Estate alter Column TotalValue money


go
alter table Real_Estate alter Column YearBuilt int
alter table Real_Estate alter Column Bedrooms int
alter table Real_Estate alter Column FullBath int
alter table Real_Estate alter Column HalfBath int
go


--- Change SoldAsVacant datatypes to Bit

go
select distinct SoldAsVacant, count(SoldAsVacant)
from Real_Estate
group by SoldAsVacant

--- There are for values in SoldAsVacant
--- Y,N,Yes,No. Change them to 1 and 0
go
update re
set SoldAsVacant = Case 
		When SoldAsVacant = 'Y' then 1
		when SoldAsVacant = 'Yes' then 1
		else 0
	end
from Real_Estate re

alter table Real_Estate Alter Column SoldAsVacant BIT

go
alter table Real_Estate alter Column SaleDate Date


--- EXPLORATION ----
--- USE SOME QUERIES TO LOOK AT THE DATA
--- HAVE A BASIC UNDERSTANDING OF THE DATA



go
select * from real_estate
--- Have an overall view of the data
-- There are some missing values in the data 



go
select * 
from real_estate 
where parcelid is null
--- So now, i know that parcelID have no missing value
--- By the definition, ParcelID is different by any property.
--- So let's see if there are records of the same properties?



go 
select ParcelID, count(*) as count_num
from real_estate
group by ParcelID
order by count_num desc
--- There are some properties with multi records
--- By any chance in one of the records having missing values 
--- while other records of the same property have that values?



go
select re1.uniqueID, re1.ParcelID, re1.PropertyAddress , re2.PropertyAddress
from real_estate re1, real_estate re2
where re1.PropertyAddress is null  
	and re2.PropertyAddress is not null
	and re1.ParcelId = re2.ParcelId
order by re1.parcelID
--- I chose PropertyAddress to demonstrate the problem.
--- Now, I see that I can fill PropertyAddress by another.


	--- In above code, I filter the values first to reduce number of rows
	--- It makes the join process run much faster
	--- Especially the number of missing values is normally small.






--- DATA CLEANSING



--- Create a procedure to fill blank

go
create proc fill_blanks(@table varchar(100), @attribute varchar(100))
as 
begin 
	declare @col_name table( rownumber int unique, col varchar(100))

	insert into @col_name (rownumber , col) 
	select *
	from (
		SELECT Row_number() over (order by name) as rowNumber, name
		FROM sys.columns 
		WHERE object_id = OBJECT_ID(@table) ) as temp

	declare @i int = 0
	declare @query varchar(5000), @col varchar(100)
	declare @len_col int = (select count(*) from @col_name)

	while @i <= @len_col
	begin
		set @i = @i + 1
		set @col = (select col from @col_name where rownumber = @i)

		set @query = 
			'update re1
			set re1.' + @col +' = re2.'+ @col + '
			from '+ @table +' re1, '+ @table + ' as re2 
			where re1.' + @col + ' is null
				and re2.' + @col + ' is not null
				and re1.' + @attribute +' = re2.' + @attribute
		--print('++++++++++++++++++   ' +@col+'      ++++++++++++++++++++++++++++++++')
		exec(@query)
	end
end
go

exec fill_blanks 'real_estate', 'parcelID'


--- It seems like the only attribute that benefit from this proc is PropertyAddress.
--- But this proc would come in handy with other datasets.


select * from Real_Estate



--- Split Property Address (Street Number, Street, City)


--- Update delimeter for PropertyAddress

--- Replace comma to dot

update  re
set PropertyAddress = Replace(PropertyAddress, ',','.')
from Real_Estate re



--- Replace the first space to dot

update re
set PropertyAddress = Stuff(PropertyAddress, charindex(' ', PropertyAddress), 1, '.')
from Real_Estate re
go

select * from Real_Estate
go

alter table Real_Estate
add [StreetNumber] varchar(100),
	Street varchar(100),
	City varchar(100)
go

update re
set StreetNumber = parsename(PropertyAddress,3),
	Street = parsename(PropertyAddress, 2),
	City = parsename(PropertyAddress, 1)
from Real_Estate re


go
select * from Real_Estate
go


--- Delete unuse columns

alter table Real_Estate
drop column PropertyAddress, OwnerName, OwnerAddress, TaxDistrict
go


--- Drop duplicated rows

go

delete re
from Real_Estate re, (
	select uniqueId, rank() over (PARTITION  by
								ParcelID, SaleDate, SalePrice 
							order by uniqueId) as rank2
	from Real_Estate) temp 
where rank2 > 1 and temp.uniqueID = re.uniqueID 

go
select * from Real_Estate