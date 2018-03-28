(ns datasplash.bq-test
  (:require [clojure.test :refer :all]
            [datasplash.bq :refer :all]
            [datasplash.bq.schema :as bqs])
  (:import (java.time ZoneId)
           (java.time.format DateTimeFormatter)
           (java.util Date)
           (org.apache.beam.sdk.io.gcp.bigquery DynamicDestinations)))

(deftest ->time-partitioning-test
  (are [opts expected]
    (let [tp (-> opts ->time-partitioning bean (select-keys [:type :expirationMs]))]
      (= expected tp))
    {} {:type "DAY" :expirationMs nil}
    {:type :day} {:type "DAY" :expirationMs nil}
    {:type :day :expiration-ms 1000} {:type "DAY" :expirationMs 1000}
    {:expiration-ms "bad format"} {:type "DAY" :expirationMs nil}))

(defn day [^Date inst]
  (let [utc      (ZoneId/of "UTC")
        iso-date DateTimeFormatter/BASIC_ISO_DATE]
    (-> inst .toInstant (.atZone utc) .toLocalDate (.format iso-date))))

(deftest custom-destination-test

  (testing "It should support time partitioning"

    (let [schema (bqs/->TableSchema {:fields [{:name "timestamp"
                                               :mode :nullable
                                               :type :timestamp}]})
          dd     (custom-destinations {:destination :timestamp
                                       :table       (fn [x]
                                                      {:qname             (str "project:dataset.table$" (day x))
                                                       :time-partitioning {:type :day}})
                                       :schema      (constantly schema)})]
      (is (instance? DynamicDestinations dd))
      (let [{:keys [jsonTimePartitioning tableSpec]} (->> #inst"2017-07-04T09:14:28" (.getTable dd) bean)]
        (is (= "{\"type\":\"DAY\"}" jsonTimePartitioning))
        (is (= "project:dataset.table$20170704" tableSpec)))
      (is (= {:name "timestamp" :mode "NULLABLE" :type "TIMESTAMP"}
             (-> (.getSchema dd (Date.))
                 .getFields
                 first
                 bean
                 (select-keys [:name :mode :type])))))))
