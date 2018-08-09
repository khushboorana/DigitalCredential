Create database if not exists DigitalCredX;

Use DigitalCredX;

Drop table if exists temporarydata;

create table temporarydata(
Institution_Name varchar(500)Unique,
Mobile_Number_1 bigint(20)Unique,
Mobile_Number_2 bigint(20)Unique,
Email_Id_1 varchar(50)Unique,
Email_Id_2 varchar(50)Unique,
Address_1 varchar(500),
Address_2 varchar(500),
Address_3 varchar(500),
City varchar(50),
State varchar(50),
Country varchar(20),
Postal_Code bigint(11));

Drop table if exists `user`;

create table if not exists `user`(
User_Id bigint(10)  primary key auto_increment,
User_Name varchar(50)  Unique,
`Password` varchar(20) ,
Email_Id varchar(50)  Unique,
Created_Date Date,
Social_Id varchar(50) Unique,
Status_Id varchar(20)
);

Alter table `user` auto_increment= 100001;

Drop table if exists `role`;

create table if not exists `role`(
role_Id bigint(10)  primary key,
role_name varchar(20),
role_desc varchar(30)
);

Drop table if exists user_role;

create table if not exists user_role(
User_Id bigint(10) ,
role_Id bigint(10) ,
foreign key(User_Id) references User(User_Id),
foreign key(role_Id) references Role(role_Id)
);


Drop table if exists Contact;

create table if not exists Contact(
Contact_Id bigint(10)  primary key auto_increment,
Full_Name varchar(500) ,
First_Name varchar(20) ,
Last_Name varchar(20) ,
DOB date,
Mobile_Number_1 bigint(20),
Mobile_Number_2 bigint(20),
Email_Id_1 varchar(50),
Email_Id_2 varchar(50));

Alter table Contact auto_increment= 200001;

Drop table if exists  Address_Type;

create table if not exists Address_Type(
Address_Type varchar(2)  primary key,
Address_Type_Desc varchar(15)
);

 Drop table if exists Contact_Address;

create table if not exists Contact_Address(
Address_Id bigint(10)  primary key auto_increment,
Contact_Id bigint(10) ,
Address_Type varchar(2),
Address_1 varchar(500),
Address_2 varchar(500),
Address_3 varchar(500),
City varchar(50),
State varchar(50),
Country varchar(20),
Postal_Code bigint(11) ,
 foreign key(Contact_Id) references Contact(Contact_Id),
foreign key(Address_Type) references Address_Type(Address_Type)
);

Alter table Contact_Address auto_increment= 300001;

Drop table if exists learner;

create table if not exists learner(
Learner_Id bigint(10)  primary key auto_increment,
User_Id bigint(10) ,
Contact_Id bigint(10) ,
foreign key(User_Id) references User(User_Id),
foreign key(Contact_Id) references Contact(Contact_Id));

Alter table learner auto_increment= 400001;

Drop table if exists institution;

create table if not exists institution(
Institution_Id bigint(10)  primary key auto_increment,
Contact_Id bigint(10) ,
Parent_Institution_Id bigint(10),
Institution_Name varchar(500),
foreign key(Contact_Id) references Contact(Contact_Id),
foreign key(Parent_Institution_Id) references Institution(Institution_Id));

Alter table institution auto_increment= 500001;

Drop table if exists institution_user;

create table if not exists institution_user(
Institution_User_Id bigint(10) ,
Institution_Id bigint(10) ,
User_Id bigint(10) ,
Contact_Id bigint(10),
primary key(Institution_Id,Institution_User_Id),
foreign key(User_Id) references `User`(User_Id),
foreign key(Contact_Id) references Contact(Contact_Id));

Alter table institution_user auto_increment=600001;

Drop table if exists requester_user;

create table if not exists requester_user(
requester_Id bigint(10)  primary key auto_increment,
User_Id bigint(10) ,
Contact_Id bigint(10) ,
Foreign key(User_Id) references `User`(User_Id),
foreign key(Contact_Id) references Contact(Contact_Id));

Alter table requester_user auto_increment= 700001;

Drop table if exists request;

create table if not exists request(
Request_Id bigint(10)  primary key auto_increment,
requester_Id bigint(10) ,
Learner_Id bigint(10) ,
Status_Id varchar(20),
foreign key(requester_Id) references requester_User(requester_Id),
foreign key(Learner_Id) references Learner(Learner_Id)
);

Alter table request auto_increment= 800001;

Drop table if exists requester;

create table if not exists requester(
Request_Id bigint(10) ,
Contact_ID bigint(10) ,
foreign key(Request_Id) references Request(Request_Id),
foreign key(Contact_Id) references Contact(Contact_Id));

Drop table if exists course;

create table if not exists course(
Course_Id bigint(10)  primary key auto_increment,
Course_Name varchar(200) ,
Short_Name varchar(10),
Description varchar(100),
Institution_Id bigint(10) ,
Course_Period varchar(4) ,
foreign key(Institution_Id) references Institution(Institution_Id));

Alter table course auto_increment= 900001;

Drop table if exists `subject`;

Create table if not exists `subject`(
Subject_Id bigint(10)   primary key auto_increment,
Subject_Name varchar(30) ,
Course_Id bigint(10) ,
foreign key(Course_Id) references Course(Course_Id)
);

Alter table `subject` auto_increment= 1000001;

Drop table if exists Grade;

create table if not exists Grade(
Grade_Id bigint(10)  primary key,
Grade_Name varchar(20),
Description varchar(20));


Drop table if exists  subject_marks;

CREATE TABLE if not exists subject_marks
(  `Learner_Id` bigint(10)  , 
 `Subject_Id` bigint(10)  ,
Course_Id bigint(10)  ,
`Year` Year(4),
Grade_Id bigint(10),  
`Marks` bigint(4) ,
foreign key(Learner_Id) references Learner(Learner_Id),
foreign key(Subject_Id) references `Subject`(Subject_Id),
foreign key(Course_Id) references Course(Course_Id),
foreign key(Grade_Id) references Grade(Grade_Id)
);

Drop table if exists Course_Grade;

create table if not exists Course_Grade(
Course_Id bigint(10) ,
Grade_Id bigint(10),
foreign key(Course_Id) references Course(Course_Id),
foreign key(Grade_Id) references Grade(Grade_Id));

Drop table if exists Credential;

create table if not exists Credential(
Credential_Id bigint(11)  primary key auto_increment,
Credential_Name varchar(20),
Credential_Year Date,
Course_Id bigint(10),
Institution_Id bigint(10),
foreign key(Course_Id) references Course(Course_Id),
foreign key(Institution_Id) references Institution(Institution_Id));

Alter table Credential auto_increment= 1100001;

Drop table if exists  Request_Credential_List;

create table if not exists Request_Credential_List(
Request_Id bigint(10),
Credential_Id bigint(10),
foreign key(Request_Id) references Request(Request_Id),
foreign key(Credential_Id) references Credential(Credential_Id));

Drop table if exists learner_credential;

create table if not exists learner_credential(
Learner_Credential_Id bigint(10) primary key auto_increment,
Learner_Id bigint(10),
Credential_Id bigint(10),
Course_Id bigint(10),
Grade_Id bigint(10),
Marks varchar(4),
Issued_Date Date,
foreign key(Learner_Id) references Learner(Learner_Id),
foreign key(Credential_Id) references Credential(Credential_Id),
foreign key(Course_Id) references Course(Course_Id),
foreign key(Grade_Id) references Grade(Grade_Id));

Alter table learner_credential auto_increment= 1200001;

Drop table if exists Learner_Credential_Resourse;

create table Learner_Credential_Resourse(
Learner_Credential_Id bigint(10),
Resourse_Id bigint(10)  primary key,
Resourse varchar(20) Unique,
foreign key(Learner_Credential_Id) references Learner_Credential(Learner_Credential_Id));

Alter table learner_credential_Resourse auto_increment= 1300001;

insert into address_type values('P','Permanent');
insert into address_type values('T','Temporary');
insert into address_type values('O','Office');
insert into address_type values('H','HeadOffice');
insert into address_type values('B','BranchOffice');

Insert into grade  values(1,'A','Outstanding');
Insert into grade  values(2,'B','Excellent');
Insert into grade  values(3,'C','Good');
Insert into grade  values(4,'D','Average');
Insert into grade  values(5,'E','Accepted');
Insert into grade  values(6,'F','Satisfactory');

Insert into `role` values (1,'Learner','Student');
Insert into `role` values (2,'Institute Admin','Credential Issuing Authority');
Insert into `role` values (3,'Requester','Requesting for Credential');

CREATE VIEW `institution_contact` AS
    SELECT 
        `it`.`Institution_Id` AS `Institution_id`,
        `it`.`Institution_Name` AS `Institution_name`,
        `co`.`Contact_Id` AS `Contact_Id`,
        `co`.`Mobile_Number_1` AS `Mobile_Number`,
        `co`.`Email_Id_1` AS `Email_id`,
        `ca`.`Address_Id` AS `Address_Id`,
        `ca`.`Address_Type` AS `Address_Type`,
        `ca`.`Address_1` AS `Address`,
        `ca`.`City` AS `City`,
        `ca`.`State` AS `State`,
        `ca`.`Country` AS `Country`,
        `ca`.`Postal_Code` AS `Postal_Code`
    FROM
        ((`institution` `it`
        JOIN `contact` `co` ON ((`it`.`Contact_Id` = `co`.`Contact_Id`)))
        JOIN `contact_address` `ca` ON ((`ca`.`Contact_Id` = `co`.`Contact_Id`)));
        
       
	CREATE VIEW `institution_course_details` AS
    SELECT 
        `iu`.`User_Id` AS `Institution_user_id`,
        `iu`.`Institution_Id` AS `institution_id`,
        `it`.`Institution_Name` AS `Institution_name`,
        `co`.`Course_Id` AS `course_id`,
        `co`.`Course_Name` AS `course_name`,
        `co`.`Course_Period` AS `course_period`,
        `sb`.`Subject_Id` AS `subject_id`,
        `sb`.`Subject_Name` AS `subject_name`
    FROM
        (((`institution_user` `iu`
        JOIN `contact` `cn` ON ((`iu`.`Contact_Id` = `cn`.`Contact_Id`)))
        JOIN `institution` `it` ON ((`it`.`Contact_Id` = `cn`.`Contact_Id`)))
        JOIN `course` `co` ON ((`it`.`Institution_Id` = `co`.`Institution_Id`)))
        JOIN `subject` `sb` ON ((`sb`.`Course_Id` = `co`.`Course_Id`));
        
        CREATE VIEW `institution_user_details` AS
    SELECT 
        `iu`.`User_Id` AS `User_Id`,
        `co`.`Contact_Id` AS `Contact_Id`,
        `iu`.`Institution_Id` AS `Institution_Id`,
        `it`.`Institution_Name` AS `Institution_Name`,
        `iu`.`Institution_User_Id` AS `Institution_User_Id`,
        `co`.`Full_Name` AS `Institution_UserName`,
        `co`.`Mobile_Number_2` AS `Institution_User_Mobile_Number`,
        `co`.`Email_Id_2` AS `Institution_User_Email_Id`,
        `ca`.`Address_Id` AS `Institution_Address_Id`,
        `ca`.`Address_Type` AS `Address_Type`,
        `ca`.`Address_1` AS `Institution_Address`,
        `ca`.`City` AS `city`,
        `ca`.`State` AS `State`,
        `ca`.`Country` AS `Country`,
        `ca`.`Postal_Code` AS `Postal_Code`
    FROM
        (((`institution_user` `iu`
        JOIN `contact` `co` ON ((`iu`.`Contact_Id` = `co`.`Contact_Id`)))
        JOIN `institution` `it` ON ((`it`.`Contact_Id` = `co`.`Contact_Id`)))
        JOIN `contact_address` `ca` ON ((`co`.`Contact_Id` = `ca`.`Contact_Id`)));
        
        
        CREATE VIEW `learner_marks` AS
    SELECT 
        `ln`.`Learner_Id` AS `learner_id`,
        `cn`.`Full_Name` AS `Learner_Name`,
        `lc`.`Learner_Credential_Id` AS `learner_credential_id`,
        `lc`.`Course_Id` AS `course_id`,
        `co`.`Course_Name` AS `course_name`,
        `co`.`Course_Period` AS `course_period`,
        `sb`.`Subject_Id` AS `subject_id`,
        `sb`.`Subject_Name` AS `subject_name`,
        `lc`.`Grade_Id` AS `grade_id`,
        `lc`.`Marks` AS `marks`,
        `sm`.`Year` AS `year`
    FROM
        (((((`learner` `ln`
        JOIN `contact` `cn` ON ((`ln`.`Contact_Id` = `cn`.`Contact_Id`)))
        JOIN `learner_credential` `lc` ON ((`lc`.`Learner_Id` = `ln`.`Learner_Id`)))
        JOIN `course` `co` ON ((`lc`.`Course_Id` = `co`.`Course_Id`)))
        JOIN `subject` `sb` ON ((`co`.`Course_Id` = `sb`.`Course_Id`)))
        JOIN `subject_marks` `sm` ON ((`sb`.`Subject_Id` = `sm`.`Subject_Id`)));
        
        CREATE VIEW `learner_user_details` AS
    SELECT 
        `us`.`User_Id` AS `User_Id`,
        `co`.`Contact_Id` AS `Contact_Id`,
        `lr`.`Learner_Id` AS `Learner_Id`,
        `co`.`Full_Name` AS `Learner_Name`,
        `co`.`DOB` AS `DOB`,
        `co`.`Mobile_Number_1` AS `Mobile_Number_1`,
        `co`.`Email_Id_1` AS `Email_Id_1`,
        `ca`.`Address_Id` AS `Address_Id`,
        `ca`.`Address_Type` AS `Address_Type`,
        `ca`.`Address_1` AS `Address_1`,
        `ca`.`City` AS `city`,
        `ca`.`State` AS `State`,
        `ca`.`Country` AS `Country`,
        `ca`.`Postal_Code` AS `Postal_Code`
    FROM
        (((`user` `us`
        JOIN `learner` `lr` ON ((`us`.`User_Id` = `lr`.`User_Id`)))
        JOIN `contact` `co` ON ((`lr`.`Contact_Id` = `co`.`Contact_Id`)))
        JOIN `contact_address` `ca` ON ((`co`.`Contact_Id` = `ca`.`Contact_Id`)));
        
        CREATE VIEW `requester_credential` AS
    SELECT 
        `rt`.`Requester_Id` AS `requester_id`,
        `co`.`Full_Name` AS `Requester_name`,
        `ru`.`Request_Id` AS `request_id`,
        `ru`.`Learner_Id` AS `Learner_Id`,
        `cr`.`Institution_Id` AS `Institution_Id`,
        `it`.`Institution_Name` AS `institution_name`,
        `cr`.`Credential_Id` AS `credential_id`,
        `cr`.`Credential_Name` AS `credential_name`,
        `cr`.`Credential_Year` AS `credential_year`,
        `cr`.`Course_Id` AS `course_id`,
        `lc`.`Grade_Id` AS `grade_Id`,
        `lc`.`Marks` AS `Total_Marks`,
        `lc`.`Issued_date` AS `Issued_date`
    FROM
        (((((`requester_user` `rt`
        JOIN `contact` `co` ON ((`rt`.`Contact_Id` = `co`.`Contact_Id`)))
        JOIN `request` `ru` ON ((`ru`.`Requester_Id` = `rt`.`Requester_Id`)))
        JOIN `learner_credential` `lc` ON ((`lc`.`Learner_Id` = `ru`.`Learner_Id`)))
        JOIN `credential` `cr` ON ((`cr`.`Course_Id` = `lc`.`Course_Id`)))
        JOIN `institution` `it` ON ((`it`.`Institution_Id` = `cr`.`Institution_Id`)));
        
        CREATE VIEW `requester_user_details` AS
    SELECT 
        `us`.`User_Id` AS `User_Id`,
        `ru`.`Contact_Id` AS `Contact_Id`,
        `ru`.`Requester_Id` AS `requester_Id`,
        `co`.`Full_Name` AS `Requester_Name`,
        `co`.`Mobile_Number_1` AS `Mobile_Number_1`,
        `co`.`Email_Id_1` AS `Email_Id_1`,
        `ca`.`Address_Id` AS `Address_Id`,
        `ca`.`Address_Type` AS `Address_Type`,
        `ca`.`Address_1` AS `Address_1`,
        `ca`.`City` AS `city`,
        `ca`.`State` AS `State`,
        `ca`.`Country` AS `Country`,
        `ca`.`Postal_Code` AS `Postal_Code`
    FROM
        (((`requester_user` `ru`
        JOIN `user` `us` ON ((`ru`.`User_Id` = `us`.`User_Id`)))
        JOIN `contact` `co` ON ((`ru`.`Contact_Id` = `co`.`Contact_Id`)))
        JOIN `contact_address` `ca` ON ((`co`.`Contact_Id` = `ca`.`Contact_Id`)));
        
        
        
        
        CREATE VIEW `institutecontactaddress` AS
    SELECT 
        `it`.`Institution_Id` AS `Institution_Id`,
        `it`.`Institution_Name` AS `Institution_Name`,
        `co`.`Contact_Id` AS `Contact_Id`,
        `co`.`Mobile_Number_1` AS `Mobile_Number_1`,
        `co`.`Mobile_Number_2` AS `Mobile_Number_2`,
        `co`.`Email_Id_1` AS `Email_Id_1`,
        `co`.`Email_Id_2` AS `Email_Id_2`,
        `ca`.`Address_1` AS `Address_1`,
        `ca`.`Address_2` AS `Address_2`,
        `ca`.`Address_3` AS `Address_3`,
        `ca`.`City` AS `City`,
        `ca`.`State` AS `State`,
        `ca`.`Country` AS `Country`,
        `ca`.`Postal_Code` AS `Postal_Code`
    FROM
        ((`institution` `it`
        JOIN `contact` `co` ON ((`it`.`Contact_Id` = `co`.`Contact_Id`)))
        JOIN `contact_address` `ca` ON ((`ca`.`Contact_Id` = `co`.`Contact_Id`)))
        
        
        