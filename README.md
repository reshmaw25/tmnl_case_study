# tmnl_case_study

The transformations file is scheduler for transforming data from source ( landing area) to Data warehouse.

Overview:
The architecture shows that the data from various sources is loaded into S3 files each day. This gives the complete tracking of what data came each day.
After the arrival of data and some initial form of cleansing the data is loaded into the s3 again. This acts like the source for the transformations.

Input :
When the data has arrived from different banks , the transformation proceeds. The transformation is divided into different stages for ease of applying business logic , modularity and readability. 

Functions : This is the phase where all the functions are defined and replaced for any calculations needed in the transformations

Staging : The data from different sources is read and business rules are applied. For example , consider data comes from different banks with different field names mentioned for "IBAN number" and "transactions". E.g. Same field transactions can have different field name in different banks , like transactions , txns , trnscs and so on. Hence , in this stage we get all the data loaded into  unified fields , define data , define source systems like which bank it comes from etc

Dimensions : Here we define all the different types of attributes that are needed for the downstream analyses. Example we can define Customer , generate surrogate keys for each customer , so as to have traceleibility but also be able to have data security and privacy in place. 

Facts : Here we generate all the measures needed , like fact_transactions , i.e. number of transactions which could be analysed based on customerid, locations and many more dimensions. This is based on the business needs/rules provided to make sure we identify which could be the fraudulent tasks. 

Marts : This is the stage where we create a unified view w.r.t to a particular business need. This also improves performance incase of certain calculations that are done more than once downstream by having it stored in the form of tables. 
