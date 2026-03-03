//  Find the top 3 products most frequently purchased together with “headphones”.
MATCH (p1:Product)<-[:OF_PRODUCT]-(v1:Variant)<-[:PURCHASED]-(:Customer)-[:PURCHASED]->(v2:Variant)-[:OF_PRODUCT]->(p2:Product)
WHERE (lower(p1.name) CONTAINS "headphones") AND NOT (lower(p2.name) CONTAINS "headphones")
RETURN p2.name, p1.name, COUNT(p1) AS cnt
ORDER BY cnt DESC
LIMIT 5
;