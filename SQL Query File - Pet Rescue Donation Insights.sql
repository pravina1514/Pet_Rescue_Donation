--============== Drop query for donor_data table =====================
Drop table IF EXISTS Donor_Data;

--================== Create Donor_data table =========================
--Create Donor_Data table to store donors information from various files.
CREATE TABLE Donor_data (
	--ID INT IDENTITY(1,1) PRIMARY KEY,
    Address VARCHAR(255),
    DonorName VARCHAR(255),
    Date_of_Donation varchar(50),
    Time VARCHAR(255),
    Amount VARCHAR(255),
    Type VARCHAR(50),
    VolunteerID VARCHAR(255)
);

--==================== Drop query for Procedure ==============================
DROP PROCEDURE IF EXISTS ImportDonorsCSVFiles;

--====================  Create New Procedure ==================================================
--Create new procedure to store data into donor_data table from 6 different CSV files

CREATE PROCEDURE ImportDonorsCSVFiles
AS
BEGIN
    -- Disable triggers if it exists
    DISABLE TRIGGER ALL ON Donor_Data;
    
    -- Bulk insert for each csv file
    BULK INSERT Donor_Data
    FROM 'D:\Sem 4\BIA\case 2\East York.csv' -- Local file path where all the files are stored
    WITH (
        FIELDTERMINATOR = '|',
        ROWTERMINATOR = '\n',
        FIRSTROW = 2,
		TABLOCK
	 );

    BULK INSERT Donor_Data
    FROM 'D:\Sem 4\BIA\case 2\Etobicoke.csv' -- Local file path where all the files are stored
    WITH (
        FIELDTERMINATOR = '|',
        ROWTERMINATOR = '\n',
        FIRSTROW = 2,
		TABLOCK
    );

	BULK INSERT Donor_Data
    FROM 'D:\Sem 4\BIA\case 2\North York.csv' -- Local file path where all the files are stored
    WITH (
        FIELDTERMINATOR = '|',
        ROWTERMINATOR = '\n',
        FIRSTROW = 2,
		TABLOCK
    );

	BULK INSERT Donor_Data
    FROM 'D:\Sem 4\BIA\case 2\Scarborough.csv' -- Local file path where all the files are stored
    WITH (
        FIELDTERMINATOR = '|',
        ROWTERMINATOR = '\n',
        FIRSTROW = 2,
		TABLOCK
    );

	BULK INSERT Donor_Data
    FROM 'D:\Sem 4\BIA\case 2\Toronto.csv' -- Local file path where all the files are stored
    WITH (
        FIELDTERMINATOR = '|',
        ROWTERMINATOR = '\n',
        FIRSTROW = 2,
		TABLOCK
    );

	BULK INSERT Donor_Data
    FROM 'D:\Sem 4\BIA\case 2\York.csv' -- Local file path where all the files are stored
    WITH (
        FIELDTERMINATOR = '|',
        ROWTERMINATOR = '\n',
        FIRSTROW = 2,
		TABLOCK
    );
	
    -- Enable triggers if it exists
    ENABLE TRIGGER ALL ON Donor_Data;
END

--===================  Execute the procedure ===============================
--Execute the procedure to populate the donor_data table
EXEC ImportDonorsCSVFiles;

--==================== Query for ID column ==================================
-- Query for adding ID (Auto incremented) column to donor_data table.
ALTER TABLE Donor_data ADD ID INT IDENTITY(1,1) PRIMARY KEY;

--================ For output ==================================
--To see the records in donor_data table
select * from Donor_data;

--=========================== Query For Address Table ==========================================================
--Query for, IF Address Table is already exists then Drop table
Drop table IF EXISTS Address;

-- Create new Address table
CREATE TABLE Address (
	addressID int Primary key,
    unit_num VARCHAR(50),
    street_number VARCHAR(50),
    street_name VARCHAR(100),
    street_type VARCHAR(50),
    street_direction VARCHAR(50),
    postal_code VARCHAR(10),
    city VARCHAR(100),
    province VARCHAR(50)
);

--====================== For Insert data into Address Table ==========================================
--Query for Populate Address table from Donor_data table

WITH SplitAddress AS (
    SELECT
		Address,
        value,
		ID,
        ROW_NUMBER() OVER (PARTITION BY address ORDER BY (SELECT NULL)) AS rn
    FROM Donor_Data
    CROSS APPLY STRING_SPLIT(Address, ',')
),
Result_1 AS (

SELECT
    
    MAX(CASE WHEN rn = 1 THEN value END) AS unit_number,
    MAX(CASE WHEN rn = 2 THEN value END) AS street_number,
    MAX(CASE WHEN rn = 3 THEN value END) AS street_name,
    MAX(CASE WHEN rn = 4 THEN value END) AS street_direction,
    MAX(CASE WHEN rn = 5 THEN value END) AS postalcode,
	MAX(CASE WHEN rn = 6 THEN value END) AS city,
	MAX(CASE WHEN rn = 7 THEN value END) AS province,
	ID

FROM SplitAddress
GROUP BY Address, ID
)
INSERT INTO Address(addressID, unit_num, street_number, street_name, street_type, street_direction, postal_code, city,province)

SELECT
	ID AS addressID,
   unit_number,
   street_number,
   street_name,
   REVERSE(SUBSTRING(REVERSE(street_name), 1, CHARINDEX(' ', REVERSE(street_name) + ' ') - 1)) AS street_type,
   street_direction,
   postalcode,
   TRIM(city),
   province
FROM Result_1;

--======================== Query For output of Address table ============================
select * from Address;

--=========================== For Volunteer table ===============================================
--If Volunteer table is exist then delete and re-create it.
Drop table IF EXISTS Volunteer;

--Create volunteer table
CREATE TABLE Volunteer (
	volunteerID int Primary Key identity(1,1),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    Group_Leader_ID int
);

--Inster records into Volunteer table
INSERT INTO Volunteer (First_Name, Last_Name, Group_Leader_ID)
VALUES ('Vraj', 'Patel', 1),
       ('Param', 'Panchal', 1),
       ('Dhairya', 'Dangi', 1),
       ('Pravina', 'Prajapati', 1),
       ('Prappan', 'Bhatra', 1),
       ('Harsh', 'Agrawal', 1);

-- To see the records in Volunteer table
select * from Volunteer;

--========================== For Insert data into payment_method Table ==============================================
--Drop table if it already present in database
DROP table IF EXISTS payment_method;

--Create new table payment_method
CREATE TABLE payment_method (
    Payment_Method_ID INT Primary key Identity(1,1),
    Payment_Type VARCHAR(50)
);

--Insert values in Payment_Method table
INSERT INTO payment_method (Payment_Type) VALUES
('Cash'),
('Credit'),
('Check');

-- to see the output in payment_method table
select * from payment_method

--======================== For Donation Table =====================================================================
--Drop donation table if it already present in database
Drop table IF EXISTS Donation;

--Create table for Donation
CREATE TABLE Donation (
    Donation_ID INT Primary key,
    donor_first_name VARCHAR(50),
    donor_last_name VARCHAR(50),
    donation_date DATE,
    donation_amount int,
    payment_method_ID int
);

--Insert values into Donation table from Donor_data table
INSERT INTO Donation(Donation_ID, donor_first_name, donor_last_name,donation_date,donation_amount,payment_method_ID)
SELECT 
	ID AS Donation_ID,
    SUBSTRING(DonorName, 1, CHARINDEX(' ', DonorName) - 1) AS donor_first_name,
    SUBSTRING(DonorName, CHARINDEX(' ', DonorName) + 1, LEN(DonorName) - CHARINDEX(' ', DonorName)) AS donor_last_name,
    TRY_CONVERT(DATE, Date_of_Donation, 120) AS donation_date,
	Amount,
	pm.Payment_Method_ID AS payment_method_id
FROM Donor_data d
JOIN payment_method pm ON d.Type = pm.Payment_Type
WHERE TRY_CONVERT(DATE, Date_of_Donation, 120) IS NOT NULL;

-- Query To check the data in Donation table
select * from Donation

--======================For Fact_Donations table ====================================================
-- Drop fact_donations table if it already present in database
Drop table IF EXISTS Fact_Donations;

-- Query to Create new Fact_Donations table
CREATE TABLE Fact_Donations (
    Fact_DonationID INT IDENTITY(1,1) PRIMARY KEY,
    Donation_ID INT,
    Payment_Method_ID INT, --Foreign key to Payment_Method table
    Donation_Amount INT,
    AddressID INT, -- Foreign key to Address
    VolunteerID INT, -- Foreign key to Volunteer
    FOREIGN KEY (Donation_ID) REFERENCES Donation(Donation_ID),
    FOREIGN KEY (Payment_Method_ID) REFERENCES payment_method(Payment_Method_ID),
    FOREIGN KEY (AddressID) REFERENCES Address(AddressID),
    FOREIGN KEY (VolunteerID) REFERENCES Volunteer(volunteerID)
);

--======================= To populate the Fact_Donations table ==================
-- Insert Query for populate the Fact_Donations table from all other dimention tables like Donation, Address,Payment_Method, Volunteer and Donor_data etc.
INSERT INTO Fact_Donations (AddressID, Donation_ID, Payment_Method_ID, VolunteerID, Donation_Amount)
SELECT 
    a.AddressID,
    d.Donation_ID,
    pm.Payment_Method_ID,
    v.VolunteerID,
    d.donation_amount
FROM 
    Donor_Data dd
JOIN 
    Donation d ON dd.ID = d.Donation_ID
JOIN 
    Address a ON d.Donation_ID = a.addressID
JOIN 
    Payment_Method pm ON dd.Type = pm.Payment_Type
JOIN 
    Volunteer v ON dd.VolunteerID = v.volunteerID

--============================ To see the records in Fact_donations table=======================
select * from Fact_Donations

--========================= Query 1 ======================
--Query 1: The average and sum of the donation by day, month, and year
--By day
SELECT 
    Day(donation_date) AS day,
    SUM(donation_amount) AS sum_of_donation,
    AVG(donation_amount) AS average_of_donation
FROM 
    Donation
GROUP BY 
    Day(donation_date)
ORDER BY 
    day
--=================== By Month ===========================
--By Month
SELECT 
    Month(donation_date) AS month,
    SUM(donation_amount) AS sum_of_donation,
    AVG(donation_amount) AS average_of_donation
FROM 
    Donation
GROUP BY 
    month(donation_date)
ORDER BY 
    month
--==================== By Year ==========================
--By Year
SELECT 
    Year(donation_date) AS year,
    SUM(donation_amount) AS sum_of_donation,
    AVG(donation_amount) AS average_of_donation
FROM 
    Donation
GROUP BY 
    Year(donation_date)
ORDER BY 
    year

--======================= Query 2 ================
--Query 2: The average and sum of the donations by postal code and City in a specific month. define
--the city and month as variables to allow flexibility.

DECLARE @city VARCHAR(100)
DECLARE @month INT

SET @city = 'Toronto' -- change city name as per requirement
SET @month = 05 -- change month as per requirement

SELECT SUM(fd.donation_amount) as Sum_Of_Donation_Amount,
       AVG(fd.donation_amount)as Average_Of_Donation_Amount,
       a.postal_code,
       a.city,
       MONTH(d.donation_date) AS d_month

FROM Fact_donations fd
JOIN Donation d ON fd.donation_id = d.donation_ID
JOIN Address a ON d.Donation_ID = a.addressID
WHERE a.city = @city
  AND MONTH(d.donation_date) = @month
GROUP BY a.postal_code, a.city, MONTH(d.donation_date)

--==================== Query 3 =====================================
--Query 3: The amount collected per payment method from the city with highest $ value of
--donations. Define the payment method as variable to allow flexibility.
DECLARE @PaymentMethod VARCHAR(50) = 'Check'; -- Specify the payment method

SELECT 
    MAX(fd.donation_amount) AS max_donation_amount,
    a.city,
    @PaymentMethod AS payment_method
FROM 
    Fact_Donations fd
JOIN 
    Address a ON fd.AddressID = a.addressID
JOIN 
    Payment_Method pm ON fd.Payment_Method_ID = pm.Payment_Method_ID
WHERE 
    pm.Payment_Type = @PaymentMethod
GROUP BY 
    a.city,
    pm.Payment_Type;




