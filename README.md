# Growth-Leads-DEA

## Setup
1. Clone project to local machine
2. Create DB named 'GrowthLeads' in postgres
3. Execute sql file 'growthleads_db'
4. Open db_connect.py and change user and password in 'connect_to_postgres()' function depending on the credentials used in local machine
5. Open process.py file and indicate the files to be loaded in 'main()' function. Please use the absolute directory path of the file. (This is where you will choose which date to be loaded by the script)
6. Run process. (python3 process.py)



## Assumptions
1. Based on this statement , *'your script should load and later process data for each day separately'*, dates should be loaded one at a time. So the script is configured to load the files separately.
2. In order to merge routy and voluum files. I used the voluum_mapper.csv file as a basis to map the voluum_brand to its respective marketing_source
3. I created 3 tables to save the results of the script. (marketing_commission, operator_commission, operator_monthly_commissions)
    1. marketing_commissions - will save here the daily marketing source commission as needed in instructio #2
    2. operator_monthly_commissions - will save here the monthly operator commission as needed in instruction #5. Will reference data in operator_commissions to get the calculations of the previous days.
         1. 
    4. operator_commissions - will save here the daily operator commission. This will be used to calculate monthly commissions as this will save the daily operator commissions.
  
4. Made inserting of data in database as an upsert logic in order to address any recalculation needed in Part 1B
   
