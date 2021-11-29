--CREATE PORTS TABLE
create table PORTS(
    Port_key int not null,
    Port_name varchar(100),
    primary key (Port_key)
)

--CREATE VESSEL TYPES TABLE
create table VESSEL_TYPES(
    Vessel_type_ID INT IDENTITY(1000,1) not null,
    VesselTypeName varchar(100),
    primary key (Vessel_type_ID)
)

--CREATE VESSEL STATUS TABLE
create table VESSEL_STATUS(
    Vessel_status_ID INT IDENTITY(2000,1) not null,
    VesselStatusName varchar(100),
    primary key (Vessel_status_ID)
)

--CREATE MANAGEMENT COMPANY TABLE
create table MANAGEMENT_COMPANY(
    ManagementcompanyCode INT not null,
    ManagementCompany varchar(500),
    primary key (ManagementcompanyCode)
)

--CREATE VESSEL INFO TABLE
create table VESSEL_INFO(
    VesselIMO INT not null,
    VesselName varchar(500),
    ManagementcompanyCode INT not null,
    VesselTypeName varchar(500),
    VesselStatusName varchar(500),
    DeadWeightTonnage int,
    Speed int,
    primary key (VesselIMO)
)


------------STORED PROCEDURES

CREATE PROCEDURE ADD_COLUMNS_VESSELINFO AS
   alter table [dbo].[VESSEL_INFO]
    add 
    Vessel_status_ID int,
    Vessel_type_ID int 
GO;



CREATE PROCEDURE TRANSFORM_VESSELINFO AS

    --update vessel info with vessel status IDs
    update  a
    set vessel_status_ID  = b.vessel_status_id
    from [dbo].[VESSEL_INFO] a
    join [dbo].[VESSEL_STATUS] b on a.vesselstatusname = b.vesselstatusname

    --update vessel info with vessel type IDs
    update  a
    set vessel_type_ID  = b.vessel_type_id
    from [dbo].[VESSEL_INFO] a
    join [dbo].[VESSEL_TYPES] b on a.vesseltypename = b.vesseltypename

    --drop unwanted wcolumns 
    alter table  [dbo].[VESSEL_INFO] drop column VesselTypeName,VesselStatusName

    ---add foreign key constraints
    ALTER TABLE [dbo].[VESSEL_INFO]
    ADD FOREIGN KEY (vessel_type_id) REFERENCES VESSEL_TYPES(vessel_type_id);

    ALTER TABLE [dbo].[VESSEL_INFO]
    ADD FOREIGN KEY (vessel_status_id) REFERENCES VESSEL_STATUS(vessel_status_id);


GO;