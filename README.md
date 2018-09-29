# K-Nearest Neighbors Count
Stored Procedure to find the k-neighborhood count of a node 

Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/knn-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/knn-1.0-SNAPSHOT.jar neo4j-enterprise-3.4.7/plugins/.
    


Edit your Neo4j/conf/neo4j.conf file by adding this line:

    dbms.security.procedures.unrestricted=com.maxdemarzi.*    

Restart your Neo4j Server. A new Stored Procedure is available:


    MATCH (n) WHERE id(n) = x WITH n 
    CALL com.maxdemarzi.knn(n) YIELD number
    RETURN number
    
    MATCH (n) WHERE id(n) = x WITH n 
    CALL com.maxdemarzi.knnx(n, 7) YIELD number
    RETURN number
    
    MATCH (n) WHERE id(n) = x WITH n 
    CALL com.maxdemarzi.knnx2(n, 7) YIELD number
    RETURN number