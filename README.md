dist-semantic-geospatial-synthetic-generator
--------------------------------------------
Distributed Semantic Geospatial Synthetic Generator 

Can create an ontology-based Synthetic geospatial dataset along with a GeoSPARQL queryset with different thematic and spatial selectivities.

Ontology comprises:
- land ownerships (small hexagons)
- states (large hexagons)
- roads (linestrings)
- points of interest (points)

Compiling, packing
--------------------

There are two profiles (hdfs, hops) which target a different variant of a spark hadoop cluster

	$ mvn clean package -DskipTests [-Phdfs]
	$ mvn clean package -DskipTests -Phops

Create a synthetic dataset
--------------------------
The 'DistDataSyntheticGenerator' main class has the following syntax:

	DistDataSyntheticGenerator <DstDir> <N> <P>
	<DstDir> : destination folder in HDFS
	<N> : dataset scale factor, a value (preferably 2^k) which scales the size of the dataset
	<P> : number of partitions, to be used for the generation of the 5 data files (0=automatic)
	
The following command uses the 'hdfs' jar to create a N=1024 scaled dataset with 5 files each one in 1 partition

	$ $SPARK_HOME/bin/spark-submit --class generator.DistSyntheticGenerator --master spark://localhost:7077 target/SyntheticGenerator-1.0-SNAPSHOT_hdfs.jar hdfs://localhost:9000/user/tioannid/tmp/data/ 1024 1

	$ hdfs dfs -ls tmp/data/*
	Found 2 items
	-rw-r--r--   1 tioannid supergroup          0 2021-03-31 14:48 tmp/data/HEXAGON_LARGE/_SUCCESS
	-rw-r--r--   1 tioannid supergroup  157818453 2021-03-31 14:48 tmp/data/HEXAGON_LARGE/part-00000
	Found 2 items
	-rw-r--r--   1 tioannid supergroup          0 2021-03-31 14:48 tmp/data/HEXAGON_LARGE_CENTER/_SUCCESS
	-rw-r--r--   1 tioannid supergroup  143199414 2021-03-31 14:48 tmp/data/HEXAGON_LARGE_CENTER/part-00000
	Found 2 items
	-rw-r--r--   1 tioannid supergroup          0 2021-03-31 14:48 tmp/data/HEXAGON_SMALL/_SUCCESS
	-rw-r--r--   1 tioannid supergroup 1574413982 2021-03-31 14:48 tmp/data/HEXAGON_SMALL/part-00000
	Found 2 items
	-rw-r--r--   1 tioannid supergroup          0 2021-03-31 14:49 tmp/data/LINESTRING/_SUCCESS
	-rw-r--r--   1 tioannid supergroup   25577349 2021-03-31 14:49 tmp/data/LINESTRING/part-00000
	Found 2 items
	-rw-r--r--   1 tioannid supergroup          0 2021-03-31 14:49 tmp/data/POINT/_SUCCESS
	-rw-r--r--   1 tioannid supergroup 1371138151 2021-03-31 14:49 tmp/data/POINT/part-00000

Change the N=1024 to a preferebly 2^k value of your choice. For production clusters and jobs with sufficient resources allocated, the number of partitions P should be set either to 0 or to a relatively high value eg. 8 or 16.

Create a synthetic queryset
--------------------------
The 'DistDataSyntheticGenerator' main class has the following syntax:

	DistQuerySyntheticGenerator <DstDir> <N> <S>
	<DstDir> : destination folder in HDFS
	<N> : dataset scale factor, the queries will be used for the corresponding scaled dataset
	<S> : selectivities list, eg. "1, 0.5, 0.1, 0.01" for 100%, 50%, 10%, 1%, 0.1% selectivities
	
The following command uses the 'hdfs' jar to create a N=1024 queryset to be used with the corresponding N=1024 scaled dataset

	$ $SPARK_HOME/bin/spark-submit --class generator.DistQuerySyntheticGenerator --master spark://localhost:7077 target/SyntheticGenerator-1.0-SNAPSHOT_hdfs.jar hdfs://localhost:9000/user/tioannid/tmp/queries/ 1024 "1, 0.25, 0.1, 0.001"

	$ hdfs dfs -ls tmp/queries
	Found 28 items
	-rw-r--r--   1 tioannid supergroup        930 2021-03-31 14:45 tmp/queries/Q00_Synthetic_Selection_Intersects_Landownerships_1_1.0.qry
	-rw-r--r--   1 tioannid supergroup        933 2021-03-31 14:45 tmp/queries/Q01_Synthetic_Selection_Intersects_Landownerships_1024_1.0.qry
	-rw-r--r--   1 tioannid supergroup        956 2021-03-31 14:45 tmp/queries/Q02_Synthetic_Selection_Intersects_Landownerships_1_0.25.qry
	-rw-r--r--   1 tioannid supergroup        959 2021-03-31 14:45 tmp/queries/Q03_Synthetic_Selection_Intersects_Landownerships_1024_0.25.qry
	-rw-r--r--   1 tioannid supergroup        960 2021-03-31 14:45 tmp/queries/Q04_Synthetic_Selection_Intersects_Landownerships_1_0.1.qry
	-rw-r--r--   1 tioannid supergroup        963 2021-03-31 14:45 tmp/queries/Q05_Synthetic_Selection_Intersects_Landownerships_1024_0.1.qry
	-rw-r--r--   1 tioannid supergroup        960 2021-03-31 14:45 tmp/queries/Q06_Synthetic_Selection_Intersects_Landownerships_1_0.001.qry
	-rw-r--r--   1 tioannid supergroup        963 2021-03-31 14:45 tmp/queries/Q07_Synthetic_Selection_Intersects_Landownerships_1024_0.001.qry
	-rw-r--r--   1 tioannid supergroup       1106 2021-03-31 14:45 tmp/queries/Q08_Synthetic_Join_Intersects_Landownerships_States_1_1.qry
	-rw-r--r--   1 tioannid supergroup       1109 2021-03-31 14:45 tmp/queries/Q09_Synthetic_Join_Intersects_Landownerships_States_1_1024.qry
	-rw-r--r--   1 tioannid supergroup       1109 2021-03-31 14:45 tmp/queries/Q10_Synthetic_Join_Intersects_States_Landownerships_1_1024.qry
	-rw-r--r--   1 tioannid supergroup       1112 2021-03-31 14:45 tmp/queries/Q11_Synthetic_Join_Intersects_Landownerships_States_1024_1024.qry
	-rw-r--r--   1 tioannid supergroup       1071 2021-03-31 14:45 tmp/queries/Q12_Synthetic_Join_Touches_States_States_1_1.qry
	-rw-r--r--   1 tioannid supergroup       1074 2021-03-31 14:45 tmp/queries/Q13_Synthetic_Join_Touches_States_States_1_1024.qry
	-rw-r--r--   1 tioannid supergroup       1074 2021-03-31 14:45 tmp/queries/Q14_Synthetic_Join_Touches_States_States_1_1024.qry
	-rw-r--r--   1 tioannid supergroup       1077 2021-03-31 14:45 tmp/queries/Q15_Synthetic_Join_Touches_States_States_1024_1024.qry
	-rw-r--r--   1 tioannid supergroup        908 2021-03-31 14:45 tmp/queries/Q16_Synthetic_Selection_Within_Pois_1_1.0.qry
	-rw-r--r--   1 tioannid supergroup        911 2021-03-31 14:45 tmp/queries/Q17_Synthetic_Selection_Within_Pois_1024_1.0.qry
	-rw-r--r--   1 tioannid supergroup        936 2021-03-31 14:45 tmp/queries/Q18_Synthetic_Selection_Within_Pois_1_0.25.qry
	-rw-r--r--   1 tioannid supergroup        939 2021-03-31 14:45 tmp/queries/Q19_Synthetic_Selection_Within_Pois_1024_0.25.qry
	-rw-r--r--   1 tioannid supergroup        934 2021-03-31 14:45 tmp/queries/Q20_Synthetic_Selection_Within_Pois_1_0.1.qry
	-rw-r--r--   1 tioannid supergroup        937 2021-03-31 14:45 tmp/queries/Q21_Synthetic_Selection_Within_Pois_1024_0.1.qry
	-rw-r--r--   1 tioannid supergroup        938 2021-03-31 14:45 tmp/queries/Q22_Synthetic_Selection_Within_Pois_1_0.001.qry
	-rw-r--r--   1 tioannid supergroup        941 2021-03-31 14:45 tmp/queries/Q23_Synthetic_Selection_Within_Pois_1024_0.001.qry
	-rw-r--r--   1 tioannid supergroup       1110 2021-03-31 14:45 tmp/queries/Q24_Synthetic_Join_Within_Pois_States_1_1.qry
	-rw-r--r--   1 tioannid supergroup       1113 2021-03-31 14:45 tmp/queries/Q25_Synthetic_Join_Within_Pois_States_1_1024.qry
	-rw-r--r--   1 tioannid supergroup       1113 2021-03-31 14:45 tmp/queries/Q26_Synthetic_Join_Within_States_Pois_1_1024.qry
	-rw-r--r--   1 tioannid supergroup       1116 2021-03-31 14:45 tmp/queries/Q27_Synthetic_Join_Within_Pois_States_1024_1024.qry
