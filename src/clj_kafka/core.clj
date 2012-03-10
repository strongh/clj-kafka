(ns clj-kafka.core
  (:import [java.util Properties]
           [kafka.message MessageAndOffset Message]))

(defn as-properties
  [m]
  (let [props (Properties. )]
    (doseq [[n v] m] (.setProperty props n v))
    props))

(defmacro with-resource
  [binding close-fn & body]
  `(let ~binding
     (try
       (do ~@body)
       (finally
        (~close-fn ~(binding 0))))))

(defn get-message
  "Based on the function in kafka.examples.ExampleUtils."
  [m]
  (let [buffer (.payload m)
        bytes  (byte-array (.remaining buffer))]
    (.get buffer bytes)
    bytes))

(defprotocol ToClojure
  (to-clojure [x] "Converts type to Clojure structure"))

(extend-protocol ToClojure
  MessageAndOffset
  (to-clojure [x] {:message (to-clojure (.message x))
                   :offset (.offset x)})
  Message
  (to-clojure [x] {:crc (.checksum x)
                   :payload (get-message x)
                   :size (.size x)}))
