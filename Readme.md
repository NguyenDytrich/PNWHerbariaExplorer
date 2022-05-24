# Getting Started
### 1. Getting the Corpus
Download the **Native format** from any of the contributers from [PNW Herbaria](https://www.pnwherbaria.org/data/datasets.php). Extract the `CPNWH_Native.zip` file and put the `.txt.` files into `./corpus`. 

### 2. Creating the tables
The corpus data will be copied into the tables `corpus_occurrences`, `corpus_annotations`, `corpus_media`, and `corpus_types`. 
To start, create the docker containers by running:
``` shell
$ docker-compose up
```

This will allow you to run:
``` shell
$ pqsl -U postgres -h localhost -f ./create_tables.sql
``` 
which will generate the required tables for you.

**N.B.** If you have a local PostgreSQL server you may need to configure your ports in `docker-compose.yml`.

### 3. Importing the TSV data
**N.B.** because the dataset is so large, these commands will take some time to run.

There is an entry in `corpus/media.txt` with the date `2011-00-00 00:00:00`. Replace this with `2011-01-01 00:00:00`. Go ahead and change this with your preferred text editor.

After downloading and extracting the corpus data from PNW Herbaria, go ahead and run
```shell
$ python import_corpus.py
```

*N.B.* you can also run `python import_corpus.py -h` to see additional tools and options available for migration

this should ignore any entries that would validate any SQL constraints and import the data. Records will be inserted in batches of 10,000 by default with the exception of `corpus_types`, which is small enough to insert one by one. If `corpus_types` doesn't import successfully, go ahead and run `python import_corpus -t types -n` which will skip foreign key validation and insert the rows.

