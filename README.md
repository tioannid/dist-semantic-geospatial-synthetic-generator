# dist-semantic-geospatial-synthetic-generator
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
