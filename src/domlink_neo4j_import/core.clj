(ns domlink-neo4j-import.core
  (:require [clojure.string :as s])
  (:import [org.neo4j.unsafe.batchinsert BatchInserters]
           [org.neo4j.graphdb DynamicRelationshipType])
  (:gen-class))

(defn process-data
  "Reads data from inf and creates neo4j db in outdir"
  [inf outdir]
  (let [inserter (BatchInserters/inserter outdir)
        LINKS_TO (DynamicRelationshipType/withName "LINKS_TO")]

    (letfn [(insert-node! [{:as props}]
              (.createNode inserter props))

            (insert-rel! [from to type {:as props}]
              (.createRelationship inserter from to type props))

            (maybe-insert-node! [name name-map]
              (let [id (name-map name)]
                (if id
                  [id name-map]
                  (let [new-id (insert-node! {"name" name})
                        new-map (assoc name-map name new-id)]
                    [new-id new-map]))))]

      (->> (line-seq inf)
           (reduce
            (fn [names line]
              (let [[from to link-count] (s/split line #"\s+")
                    [from-id names] (maybe-insert-node! from names)
                    [to-id names] (maybe-insert-node! to names)]
                (insert-rel! from-id to-id LINKS_TO {"count" link-count})
                names))
            {})))
    (.shutdown inserter)))

(defn -main
  ([inname outname]
     (with-open [inf (clojure.java.io/reader inname)]
       (process-data inf outname))))
