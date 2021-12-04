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

	DistDataSyntheticGenerator <FileFormat> <DstDir> <N> <P> {<ALL_THEMA>}
	<FileFormat> : spark output file format {text | parquet}
	<DstDir> : destination folder in HDFS
	<N> : dataset scale factor, a value (preferably 2^k) which scales the size of the dataset
	<P> : number of partitions, to be used for the generation of the 5 data files (0=automatic)
    {<ALL_THEMA>} : default value=false, produce all thematic tag values
	
The following command uses the 'hdfs' jar to create a N=256 scaled dataset comprising 5 parquet snappy-compressed files each one in 1 partition and use all thematic tag values

	$ $SPARK_HOME/bin/spark-submit --class generator.DistDataSyntheticGenerator --master spark://localhost:7077 --conf spark.sql.parquet.compression.codec=snappy target/SyntheticGenerator-2.4.4-SNAPSHOT_hdfs.jar parquet hdfs://localhost:9000/user/tioannid/Resources/Synthetic/256/data/ 256 1 ALL_THEMA

	$ hdfs dfs -ls Resources/Synthetic/256/data/*
        Found 2 items
        -rw-r--r--   1 tioannid supergroup          0 2021-04-21 09:57 Resources/Synthetic/256/data/HEXAGON_LARGE/_SUCCESS
        -rw-r--r--   1 tioannid supergroup    1150557 2021-04-21 09:57 Resources/Synthetic/256/data/HEXAGON_LARGE/part-00000-7bd028db-93ad-4982-ace0-c8b95b02ac6f-c000.snappy.parquet
        Found 2 items
        -rw-r--r--   1 tioannid supergroup          0 2021-04-21 09:57 Resources/Synthetic/256/data/HEXAGON_LARGE_CENTER/_SUCCESS
        -rw-r--r--   1 tioannid supergroup     862458 2021-04-21 09:57 Resources/Synthetic/256/data/HEXAGON_LARGE_CENTER/part-00000-14a7306c-c243-46ae-8ff3-e89a86f1f084-c000.snappy.parquet
        Found 2 items
        -rw-r--r--   1 tioannid supergroup          0 2021-04-21 09:57 Resources/Synthetic/256/data/HEXAGON_SMALL/_SUCCESS
        -rw-r--r--   1 tioannid supergroup   11826896 2021-04-21 09:57 Resources/Synthetic/256/data/HEXAGON_SMALL/part-00000-be2d7fde-b7b2-4327-bb1e-689f93627082-c000.snappy.parquet
        Found 2 items
        -rw-r--r--   1 tioannid supergroup          0 2021-04-21 09:57 Resources/Synthetic/256/data/LINESTRING/_SUCCESS
        -rw-r--r--   1 tioannid supergroup     616819 2021-04-21 09:57 Resources/Synthetic/256/data/LINESTRING/part-00000-aee735c3-3327-4bf3-93a0-5ffc12df1068-c000.snappy.parquet
        Found 2 items
        -rw-r--r--   1 tioannid supergroup          0 2021-04-21 09:57 Resources/Synthetic/256/data/POINT/_SUCCESS
        -rw-r--r--   1 tioannid supergroup    9444757 2021-04-21 09:57 Resources/Synthetic/256/data/POINT/part-00000-4825de83-c3cf-46dd-8eab-d89fe8552b0a-c000.snappy.parquet

Change the N=256 to a (preferably) 2^k value of your choice. For production clusters and jobs with sufficient resources allocated, the number of partitions P should be set either to 0 or to a relatively high value eg. 8 or 16.

Create a synthetic queryset
--------------------------
The 'DistQuerySyntheticGenerator' main class has the following syntax:

	DistQuerySyntheticGenerator <DstDir> <N> <S> <T>
	<DstDir> : destination folder in HDFS
	<N> : dataset scale factor, the queries will be used for the corresponding scaled dataset
	<S> : selectivities list, eg. "1,0.5,0.1,0.01" for 100%, 50%, 10%, 1%, 0.1% selectivities
        <T> : thematic tag list (comma separated within double-quotes of 2^i values <= N)
	
The following command uses the 'hdfs' jar to create a N=512 queryset to be used with the corresponding N=512 scaled dataset, using spatial selectivities (100%, 10%, 1%) and thematic tag list "1,2,512" (1-> 100%, 2-> 50%, 512-> 1/512*100 = 0,19%)

	$ $SPARK_HOME/bin/spark-submit --class generator.DistQuerySyntheticGenerator --master spark://localhost:7077 target/SyntheticGenerator-2.4.5-SNAPSHOT_hdfs.jar hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/ 512 "1,0.1,0.01" "1,2,512"

	$ hdfs dfs -ls hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest
    Found 72 items
    -rw-r--r--   1 tioannid supergroup        928 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q00_Synthetic_Selection_Intersects_Landownerships_1_1.0.qry
    -rw-r--r--   1 tioannid supergroup        928 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q01_Synthetic_Selection_Intersects_Landownerships_2_1.0.qry
    -rw-r--r--   1 tioannid supergroup        930 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q02_Synthetic_Selection_Intersects_Landownerships_512_1.0.qry
    -rw-r--r--   1 tioannid supergroup        960 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q03_Synthetic_Selection_Intersects_Landownerships_1_0.1.qry
    -rw-r--r--   1 tioannid supergroup        960 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q04_Synthetic_Selection_Intersects_Landownerships_2_0.1.qry
    -rw-r--r--   1 tioannid supergroup        962 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q05_Synthetic_Selection_Intersects_Landownerships_512_0.1.qry
    -rw-r--r--   1 tioannid supergroup        958 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q06_Synthetic_Selection_Intersects_Landownerships_1_0.01.qry
    -rw-r--r--   1 tioannid supergroup        958 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q07_Synthetic_Selection_Intersects_Landownerships_2_0.01.qry
    -rw-r--r--   1 tioannid supergroup        960 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q08_Synthetic_Selection_Intersects_Landownerships_512_0.01.qry
    -rw-r--r--   1 tioannid supergroup       1106 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q09_Synthetic_Join_Intersects_Landownerships_States_1_1.qry
    -rw-r--r--   1 tioannid supergroup       1106 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q10_Synthetic_Join_Intersects_Landownerships_States_1_2.qry
    -rw-r--r--   1 tioannid supergroup       1108 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q11_Synthetic_Join_Intersects_Landownerships_States_1_512.qry
    -rw-r--r--   1 tioannid supergroup       1106 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q12_Synthetic_Join_Intersects_States_Landownerships_1_1.qry
    -rw-r--r--   1 tioannid supergroup       1106 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q13_Synthetic_Join_Intersects_States_Landownerships_1_2.qry
    -rw-r--r--   1 tioannid supergroup       1108 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q14_Synthetic_Join_Intersects_States_Landownerships_1_512.qry
    -rw-r--r--   1 tioannid supergroup       1106 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q15_Synthetic_Join_Intersects_Landownerships_States_2_1.qry
    -rw-r--r--   1 tioannid supergroup       1106 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q16_Synthetic_Join_Intersects_Landownerships_States_2_2.qry
    -rw-r--r--   1 tioannid supergroup       1108 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q17_Synthetic_Join_Intersects_Landownerships_States_2_512.qry
    -rw-r--r--   1 tioannid supergroup       1106 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q18_Synthetic_Join_Intersects_States_Landownerships_2_1.qry
    -rw-r--r--   1 tioannid supergroup       1106 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q19_Synthetic_Join_Intersects_States_Landownerships_2_2.qry
    -rw-r--r--   1 tioannid supergroup       1108 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q20_Synthetic_Join_Intersects_States_Landownerships_2_512.qry
    -rw-r--r--   1 tioannid supergroup       1108 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q21_Synthetic_Join_Intersects_Landownerships_States_512_1.qry
    -rw-r--r--   1 tioannid supergroup       1108 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q22_Synthetic_Join_Intersects_Landownerships_States_512_2.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q23_Synthetic_Join_Intersects_Landownerships_States_512_512.qry
    -rw-r--r--   1 tioannid supergroup       1108 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q24_Synthetic_Join_Intersects_States_Landownerships_512_1.qry
    -rw-r--r--   1 tioannid supergroup       1108 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q25_Synthetic_Join_Intersects_States_Landownerships_512_2.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q26_Synthetic_Join_Intersects_States_Landownerships_512_512.qry
    -rw-r--r--   1 tioannid supergroup       1071 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q27_Synthetic_Join_Touches_States_States_1_1.qry
    -rw-r--r--   1 tioannid supergroup       1071 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q28_Synthetic_Join_Touches_States_States_1_2.qry
    -rw-r--r--   1 tioannid supergroup       1073 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q29_Synthetic_Join_Touches_States_States_1_512.qry
    -rw-r--r--   1 tioannid supergroup       1071 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q30_Synthetic_Join_Touches_States_States_1_1.qry
    -rw-r--r--   1 tioannid supergroup       1071 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q31_Synthetic_Join_Touches_States_States_1_2.qry
    -rw-r--r--   1 tioannid supergroup       1073 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q32_Synthetic_Join_Touches_States_States_1_512.qry
    -rw-r--r--   1 tioannid supergroup       1071 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q33_Synthetic_Join_Touches_States_States_2_1.qry
    -rw-r--r--   1 tioannid supergroup       1071 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q34_Synthetic_Join_Touches_States_States_2_2.qry
    -rw-r--r--   1 tioannid supergroup       1073 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q35_Synthetic_Join_Touches_States_States_2_512.qry
    -rw-r--r--   1 tioannid supergroup       1071 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q36_Synthetic_Join_Touches_States_States_2_1.qry
    -rw-r--r--   1 tioannid supergroup       1071 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q37_Synthetic_Join_Touches_States_States_2_2.qry
    -rw-r--r--   1 tioannid supergroup       1073 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q38_Synthetic_Join_Touches_States_States_2_512.qry
    -rw-r--r--   1 tioannid supergroup       1073 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q39_Synthetic_Join_Touches_States_States_512_1.qry
    -rw-r--r--   1 tioannid supergroup       1073 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q40_Synthetic_Join_Touches_States_States_512_2.qry
    -rw-r--r--   1 tioannid supergroup       1075 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q41_Synthetic_Join_Touches_States_States_512_512.qry
    -rw-r--r--   1 tioannid supergroup       1073 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q42_Synthetic_Join_Touches_States_States_512_1.qry
    -rw-r--r--   1 tioannid supergroup       1073 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q43_Synthetic_Join_Touches_States_States_512_2.qry
    -rw-r--r--   1 tioannid supergroup       1075 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q44_Synthetic_Join_Touches_States_States_512_512.qry
    -rw-r--r--   1 tioannid supergroup        908 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q45_Synthetic_Selection_Within_Pois_1_1.0.qry
    -rw-r--r--   1 tioannid supergroup        908 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q46_Synthetic_Selection_Within_Pois_2_1.0.qry
    -rw-r--r--   1 tioannid supergroup        910 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q47_Synthetic_Selection_Within_Pois_512_1.0.qry
    -rw-r--r--   1 tioannid supergroup        934 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q48_Synthetic_Selection_Within_Pois_1_0.1.qry
    -rw-r--r--   1 tioannid supergroup        934 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q49_Synthetic_Selection_Within_Pois_2_0.1.qry
    -rw-r--r--   1 tioannid supergroup        936 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q50_Synthetic_Selection_Within_Pois_512_0.1.qry
    -rw-r--r--   1 tioannid supergroup        936 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q51_Synthetic_Selection_Within_Pois_1_0.01.qry
    -rw-r--r--   1 tioannid supergroup        936 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q52_Synthetic_Selection_Within_Pois_2_0.01.qry
    -rw-r--r--   1 tioannid supergroup        938 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q53_Synthetic_Selection_Within_Pois_512_0.01.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q54_Synthetic_Join_Within_Pois_States_1_1.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q55_Synthetic_Join_Within_Pois_States_1_2.qry
    -rw-r--r--   1 tioannid supergroup       1112 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q56_Synthetic_Join_Within_Pois_States_1_512.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q57_Synthetic_Join_Within_States_Pois_1_1.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q58_Synthetic_Join_Within_States_Pois_1_2.qry
    -rw-r--r--   1 tioannid supergroup       1112 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q59_Synthetic_Join_Within_States_Pois_1_512.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q60_Synthetic_Join_Within_Pois_States_2_1.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q61_Synthetic_Join_Within_Pois_States_2_2.qry
    -rw-r--r--   1 tioannid supergroup       1112 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q62_Synthetic_Join_Within_Pois_States_2_512.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q63_Synthetic_Join_Within_States_Pois_2_1.qry
    -rw-r--r--   1 tioannid supergroup       1110 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q64_Synthetic_Join_Within_States_Pois_2_2.qry
    -rw-r--r--   1 tioannid supergroup       1112 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q65_Synthetic_Join_Within_States_Pois_2_512.qry
    -rw-r--r--   1 tioannid supergroup       1112 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q66_Synthetic_Join_Within_Pois_States_512_1.qry
    -rw-r--r--   1 tioannid supergroup       1112 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q67_Synthetic_Join_Within_Pois_States_512_2.qry
    -rw-r--r--   1 tioannid supergroup       1114 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q68_Synthetic_Join_Within_Pois_States_512_512.qry
    -rw-r--r--   1 tioannid supergroup       1112 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q69_Synthetic_Join_Within_States_Pois_512_1.qry
    -rw-r--r--   1 tioannid supergroup       1112 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q70_Synthetic_Join_Within_States_Pois_512_2.qry
    -rw-r--r--   1 tioannid supergroup       1114 2021-12-03 22:37 hdfs://localhost:9000/user/tioannid/Resources/Synthetic/512/qrytest/Q71_Synthetic_Join_Within_States_Pois_512_512.qry