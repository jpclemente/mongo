./neo4j-admin import \
--database practica.db \
--nodes:author "../import/author_nodes.csv" \
--nodes:publication "../import/publication_nodes.csv" \
--relationships:publicated "../import/relationships.csv"