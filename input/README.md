
# Input data files and instructions

## Data provided by Sanergy

Data transfer was accomplished via Box.com, and contained a number of SPSS,
and Excel files, as well as a MS SQL Server backup.

Drake can automate most of these steps, except for the MS SQL Server conversion.

### "Sanergy - Fresh Life Toilet Waste DataBase.bak" (MS SQL Server)

Spin up a MS SQL Server, and open the SQL Server Management Studio

1. Right click on Server -> Databases, select "Restore Database…", and use all default settings
2. Right click on Server -> Security -> Logins, select "New Login…"
	* Use SQL Server authentication
	* Unselect all password management options (unsure if this step is required)
	* Map the user to the DSSG table, grant appropriate privileges
3. Right click on Server, select "Properties"
	* Under Security, select "Allow SQL Server authentication"
4. Open the SQL Server Configuration Manager
	* Restart the SQL Server in the SQL Server Services tab

Now create a `mspass` file at the repository root in the format of `pgpass`,
with the format: `host:port:user:db:pass`.

#### Description

The .bak file includes 10 data tables, the following describes their shape (rows, columns).
* `Collection_Data__c` (522,263 rows):
* `FLT_Collection_Schedule__c` (54,836 rows):
* `_IPA_tbl_system_user` (110 rows):
* `_IPA_tbl_toilet` (73 rows):
* `_IPA_tbl_transactions` (53,818 rows):
* `_IPA_tbl_user` (4,335 rows):
* `_IPA_tbl_user_card` (4,227 rows):
* `spatial_ref_sys` (3,911):
* `stdin` (0 rows):
* `tblToilet` (893 rows):

### "Map Kibera/Shapefiles" (Shapefiles)

Some of the data came in SPSS format. We used R to convert it to CSV. Drake
will automatically run the R scripts, but you can setup R for further
development work as follows:

1. We ran an RStudio server on our server.
2. Create an ssh tunnel: ```ssh -i ~/.ssh/[your private key] -fNL [your port number]:localhost:8787 [your username]@[server name]```
3. Open your web browser and go to ```localhost:[your port number]```

Replace the information in the brackets with your info.
