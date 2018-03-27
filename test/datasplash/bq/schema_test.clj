(ns datasplash.bq.schema-test
  (:require [clojure.test :refer :all]
            [datasplash.bq.schema :refer :all])
  (:import (com.google.api.services.bigquery.model TableSchema)))

(deftest kw->field-name-test
  (is (= "tableId" (kw->field-name :table-id))))

(deftest kw->enum-str-test
  (is (= "CREATE_IF_NEEDED" (kw->enum-str :create-if-needed))))

(deftest dsl->google-json-map-test
  (is (= {"key"
          {"aKey"       "string"
           "bLongerKey" 1
           "type"       "THIS_IS_AN_ENUM"
           "anotherKey" {"nested" "VALUE"}}}
         (dsl->google-json-map {:key
                                {:a-key        "string"
                                 :b-longer-key 1
                                 :type         :this-is-an-enum
                                 :another-key  {:nested :value}}}))))

(deftest ->TableSchema-test

  (testing "A table schema is a table schema"
    (let [schema (TableSchema.)]
      (is (identical? schema (->TableSchema schema)))))

  (testing "A JSON string schema is coerced into a table schema"
    (let [json-schema "{\"fields\":[{\"name\":\"ts\", \"mode\":\"NULLABLE\", \"type\":\"TIMESTAMP\"}]}"
          schema      (-> json-schema ->TableSchema .getFields first bean)]
      (is (= {:name "ts" :mode "NULLABLE" :type "TIMESTAMP"}
             (select-keys schema [:name :mode :type])))))

  (testing "A edn DSL can also be coerced into a table schema (as long as it's valid)"
    (let [dsl    {:fields
                  [{:name "ts" :type :timestamp :mode :required}
                   {:name "str" :type :string :mode :required}
                   {:name "intAry" :type :integer :mode :repeated}
                   {:name "bool" :type :boolean :mode :nullable}
                   {:name "bytes" :type :bytes :mode :nullable}
                   {:name "record" :type :record :mode :nullable :fields
                          [{:name "ts" :type :timestamp}
                           {:name "str" :type :string :mode :nullable}
                           {:name "intAry" :type :integer :mode :repeated}
                           {:name "bool" :type :boolean}
                           {:name "bytes" :type :bytes}]}
                   {:name "int" :type :integer}
                   {:name "float" :type :float}]}
          schema (->TableSchema dsl)]
      (is (= "{
  \"fields\" : [ {
    \"mode\" : \"REQUIRED\",
    \"name\" : \"ts\",
    \"type\" : \"TIMESTAMP\"
  }, {
    \"mode\" : \"REQUIRED\",
    \"name\" : \"str\",
    \"type\" : \"STRING\"
  }, {
    \"mode\" : \"REPEATED\",
    \"name\" : \"intAry\",
    \"type\" : \"INTEGER\"
  }, {
    \"mode\" : \"NULLABLE\",
    \"name\" : \"bool\",
    \"type\" : \"BOOLEAN\"
  }, {
    \"mode\" : \"NULLABLE\",
    \"name\" : \"bytes\",
    \"type\" : \"BYTES\"
  }, {
    \"fields\" : [ {
      \"name\" : \"ts\",
      \"type\" : \"TIMESTAMP\"
    }, {
      \"mode\" : \"NULLABLE\",
      \"name\" : \"str\",
      \"type\" : \"STRING\"
    }, {
      \"mode\" : \"REPEATED\",
      \"name\" : \"intAry\",
      \"type\" : \"INTEGER\"
    }, {
      \"name\" : \"bool\",
      \"type\" : \"BOOLEAN\"
    }, {
      \"name\" : \"bytes\",
      \"type\" : \"BYTES\"
    } ],
    \"mode\" : \"NULLABLE\",
    \"name\" : \"record\",
    \"type\" : \"RECORD\"
  }, {
    \"name\" : \"int\",
    \"type\" : \"INTEGER\"
  }, {
    \"name\" : \"float\",
    \"type\" : \"FLOAT\"
  } ]
}" (.toPrettyString json-factory schema))))))
