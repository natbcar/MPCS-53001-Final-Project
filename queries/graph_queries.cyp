//  Find the top 3 products most frequently purchased together with “headphones”.
MATCH (p1:Product)<-[:OF_PRODUCT]-(v1:Variant)<-[o1:PURCHASED]-(:Customer)-[o2:PURCHASED]->(v2:Variant)-[:OF_PRODUCT]->(p2:Product)
WHERE (LOWER(p1.name) CONTAINS "headphones" OR LOWER(p1.name) CONTAINS "earbuds") 
  AND NOT (LOWER(p2.name) CONTAINS "headphones" OR LOWER(p2.name) CONTAINS "earbuds")
  AND o1.order_id = o2.order_id
RETURN p2.name, COUNT(DISTINCT o1.order_id) AS cnt
ORDER BY cnt DESC
LIMIT 3
;
