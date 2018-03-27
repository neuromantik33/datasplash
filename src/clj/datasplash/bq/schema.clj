(ns datasplash.bq.schema
  (:require [clojure.spec.alpha :as s]
            [cheshire.core :as json])
  (:import (com.google.common.base Converter CaseFormat)
           (com.google.api.client.json.jackson2 JacksonFactory)
           (com.google.api.client.json JsonFactory)
           (clojure.lang IPersistentMap)
           (com.google.api.services.bigquery.model TableSchema)))

(def ^:private ^Converter lh->lc
  (.converterTo CaseFormat/LOWER_HYPHEN CaseFormat/LOWER_CAMEL))

(def ^:private ^Converter lh->uu
  (.converterTo CaseFormat/LOWER_HYPHEN CaseFormat/UPPER_UNDERSCORE))

(defn ^String kw->field-name
  "Converts a keyword into a field name"
  [kw]
  (->> kw name (.convert lh->lc)))

(defn ^String kw->enum-str
  "Converts a keyword into a enum string"
  [kw]
  (->> kw name (.convert lh->uu)))

(defn dsl->google-json-map
  "Recursively transforms a DSL map to conform to the Google JSON format:
  - All keyword keys become lower camel strings
  - All keyword values become upper underscore strings"
  [m]
  (let [xform-key (fn [[k v]] (if (keyword? k) [(kw->field-name k) v] [k v]))
        xform-val (fn [[k v]] (if (keyword? v) [k (kw->enum-str v)] [k v]))]
    ;; only apply to maps
    (clojure.walk/postwalk
      (fn [x] (if (map? x) (into {} (map (comp xform-key xform-val) x)) x))
      m)))

(def ^JsonFactory json-factory (JacksonFactory/getDefaultInstance))

;; Helper methods for creating TableSchema objects

(s/def ::mode #{:nullable :required :repeated})
(s/def ::description (s/and string? #(<= (count %) 512)))
(s/def ::type #{:string :bytes :integer :float :boolean :timestamp :date :time :datetime :record})
(s/def ::name (s/and string? #(<= (count %) 128)))
(s/def ::table-field-schema (s/keys :req-un [::name ::type]
                                    :opt-un [::description ::fields ::mode]))
(s/def ::fields (s/coll-of ::table-field-schema))
(s/def ::table-schema (s/keys :req-un [::fields]))

(defprotocol BigQuerySchema
  (->TableSchema ^TableSchema [obj]))

(extend TableSchema
  BigQuerySchema
  {:->TableSchema identity})

(extend IPersistentMap
  BigQuerySchema
  {:->TableSchema
   (fn [spec]
     (if (s/valid? ::table-schema spec)
       (->> spec
            dsl->google-json-map
            json/encode
            ->TableSchema)
       (throw
         (ex-info (with-out-str (s/explain ::table-schema spec)) spec))))})

(extend String
  BigQuerySchema
  {:->TableSchema
   (fn [^String s]
     (.fromString json-factory s TableSchema))})
