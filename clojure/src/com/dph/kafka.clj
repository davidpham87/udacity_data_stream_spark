(ns com.dph.kafka
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.edn :as edn]
   [jsonista.core :as j]
   [clojure.pprint :as pprint]
   [integrant.core :as ig]
   [integrant.repl :as ir]
   [integrant.repl.state :as irs]
   [troy-west.arche.integrant]
   [troy-west.thimble.kafka :as ttk]
   [troy-west.thimble.zookeeper])
  (:import (org.apache.kafka.clients.consumer KafkaConsumer)))

(def object-mapper
  (j/object-mapper {:decode-key-fn csk/->kebab-case-keyword}))

(defn kafka-config []
  {:thimble/zookeeper.server {}

   [:thimble/kafka.broker :broker/id-0]
   {:zookeeper (ig/ref :thimble/zookeeper.server)
    :topics    ["org.sf.crime"]
    :config    {"brocker.id" "1"}}

   ;; [:thimble/kafka.broker :broker/id-1]
   ;; {:zookeeper (ig/ref :thimble/zookeeper.server)
   ;;  :config    {"brocker.id" "2"
   ;;              "port"       "9093"}}

   :thimble/kafka.producer
   {:broker (ig/ref [:thimble/kafka.broker :broker/id-0])
    :config {"bootstrap.servers" "localhost:9092"}}

   :thimble/kafka.consumer
   {:broker (ig/ref [:thimble/kafka.broker :broker/id-0])
    :config {"group.id"           "test"
             "enable.auto.commit" "true"
             "bootstrap.servers"  "localhost:9092"}}})

(defn consume [topic]
  (when (:consumer (:thimble/kafka.consumer integrant.repl.state/system))
    (let [consumer ^KafkaConsumer (:consumer (:thimble/kafka.consumer integrant.repl.state/system))
          _ (doto consumer (.subscribe [topic]))
          records (.poll consumer 100)
          msg (mapv #(-> (.value %)) records)]
      (when (seq msg) (pprint/pprint {:count (count msg)}))
      msg)))

(defn send-message [topic key value]
  (when-let [producer (:thimble/kafka.producer integrant.repl.state/system)]
    (ttk/send-message producer topic key value)))

(defn start
  ([]
   (start (kafka-config)))
  ([config]
   (ig/init config)))

(defn stop
  "Stop the server"
  [state]
  (ig/halt! state))

(defn -main []
  (start))

(comment
  (ir/set-prep! kafka-config)
  (ir/prep)
  (ir/go)
  (ir/halt)
  (ir/reset)
  (ir/clear)
  irs/system
  (ttk/list-topics (irs/system [:thimble/kafka.broker :broker/id-0]))
  irs/config

  (doseq [i (repeatedly 1000 #(rand-int 1000))]
    (send-message "org.sf.crime" "test" (str {:msg "Hello-world" :idx i})))

  (loop []
    (let [msg (consume "org.sf.crime")]
      (when (seq msg)
        (recur))))

  (mapv #(j/read-value % object-mapper) (consume "sf_crime"))
  (mapv edn/read-string (consume "org.sf.crime"))

  (def status (atom {:consumer {:stop? false}}))
  (swap! status assoc-in [:consumer :stop?] true)
  (swap! status assoc-in [:consumer :stop?] false)

  (tap> integrant.repl.state/system)


  )


;; (remove-hook flycheck-clj-kondo)
