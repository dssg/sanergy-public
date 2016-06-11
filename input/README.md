
### Input data files and instructions
##### "IPA/IPA_data_incomplete.sav" (SPSS)
###### Setup
###### Description
##### "Waste Data January 22 2015 - January 22 2016.xlsx" (Excel)
##### "Sanergy - Fresh Life Toilet Waste DataBase.bak" (MS SQL Server)
###### Setup
See **Joe Walsh** about spinning up a MS SQL Server, installing the backup file, restoring it, and uploading it to the postgres server.
###### Description
The .bak file includes 10 data tables, the following describes their shape (rows, columns) and what we perceive as unique identifiers.
* Collection_Data__c (522,263 rows):
* FLT_Collection_Schedule__c (54,836 rows):
* _IPA_tbl_system_user (110 rows):
* _IPA_tbl_toilet (73 rows):
* _IPA_tbl_transactions (53,818 rows):
* _IPA_tbl_user (4,335 rows):
* _IPA_tbl_user_card (4,227 rows):
* spatial_ref_sys (3,911):
* stdin (0 rows):
* tblToilet (893 rows):

##### "Map Kibera/Shapefiles" (Shapefiles)

Some of the data came in SPSS format. We used R to convert it to CSV:

1. We ran an RStudio server on our server.
2. Create an ssh tunnel: ```ssh -i ~/.ssh/[your private key] -fNL [your port number]:localhost:8787 [your username]@[server name]```
3. Open your web browser and go to ```localhost:[your port number]```

Replace the information in the brackets with your info.


