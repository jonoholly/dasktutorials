import pandas
import dask.dataframe

person_IDs = [1,2,3,4,5,6,7,8,9,10]
person_last_names = ['Smith', 'Williams', 'Williams','Jackson','Johnson','Smith','Anderson','Christiansen','Carter','Davidson']
person_first_names = ['John', 'Bill', 'Jane','Cathy','Stuart','James','Felicity','Liam','Nancy','Christina']
person_dobs = ['1982-10-06', '1990-07-04', '1989-05-06', '1974-01-24', '1995-06-05', '1984-04-16', '1976-09-15', '1992-10-02', '1986-02-05', '1993-08-11']

people_df = pandas.DataFrame({'Person ID':person_IDs, 
              'Last Name': person_last_names, 
              'First Name': person_first_names,
             'Date of Birth': person_dobs},
            columns=['Person ID', 'Last Name', 'First Name', 'Date of Birth'])

people_ddf = dask.dataframe.from_pandas(people_df, npartitions=2)
people_ddf.npartitions
people_ddf.divisions
people_ddf.map_partitions(len).compute() # shows the split, 5-5
# filter and repartition. now 3/5
people_filtered = people_ddf[people_ddf['Last Name'] != 'Williams']
print(people_filtered.map_partitions(len).compute())
# now split is just 1 partition of 8
people_filtered_reduced = people_filtered.repartition(npartitions=1)
print(people_filtered_reduced.map_partitions(len).compute())
