(ns task-c6)

(def transact-cnt (atom 0))

(def empty-map
  {:forward {},
   :backward {}})

(defn route
  [route-map from to price tickets-num]
  (let [tickets (ref tickets-num :validator (fn [state] (>= state 0))),
        orig-source-desc (or (get-in route-map [:forward from]) {}),
        orig-reverse-dest-desc (or (get-in route-map [:backward to]) {}),
        route-desc {:price price, :tickets tickets},
        source-desc (assoc orig-source-desc to route-desc),
        reverse-dest-desc (assoc orig-reverse-dest-desc from route-desc)]
    (-> route-map
        (assoc-in [:forward from] source-desc)
        (assoc-in [:backward to] reverse-dest-desc))))

(defn- get-path [parents end]
  (loop [curr end, path '()]
    (if (nil? curr)
      path
      (recur (get parents curr) (conj path curr)))))

(defn- find-path [route-map from to]
  (loop [costs {from 0}, parents {}, queue (set [from])]
    (if (empty? queue)
      (if (get costs to) {:price (get costs to) :path (get-path parents to)} nil)
      (let [curr (apply min-key #(get costs % Long/MAX_VALUE) queue)
            curr-cost (get costs curr)
            neighbors (get-in route-map [:forward curr])]
        (if (= curr to)
          {:price curr-cost :path (get-path parents to)}
          (let [updates (for [[next-node desc] neighbors
                              :let [price (:price desc)
                                    tickets @(:tickets desc)
                                    new-cost (+ curr-cost price)]
                              :when (and (> tickets 0)
                                         (< new-cost (get costs next-node Long/MAX_VALUE)))]
                          {:node next-node :cost new-cost :parent curr})
                new-costs (reduce #(assoc %1 (:node %2) (:cost %2)) costs updates)
                new-parents (reduce #(assoc %1 (:node %2) (:parent %2)) parents updates)
                new-queue (reduce #(conj %1 (:node %2)) (disj queue curr) updates)]
            (recur new-costs new-parents new-queue)))))))

(defn book-tickets
  [route-map from to]
  (if (= from to)
    {:path '(), :price 0}
    (dosync
     (swap! transact-cnt inc) ; Счетчик попыток транзакции
     (if-let [res (find-path route-map from to)]
         (let [path-nodes (:path res)
               segments (partition 2 1 path-nodes)]
           (doseq [[u v] segments]
             (let [tickets-ref (:tickets (get-in route-map [:forward u v]))]
               (alter tickets-ref dec))) ; Списание билета
           res)
         {:error :no-tickets-or-path}))))

(def spec1 (-> empty-map
               (route "City1" "Capital"    200 5)
               (route "Capital" "City1"    250 5)
               (route "City2" "Capital"    200 5)
               (route "Capital" "City2"    250 5)
               (route "City3" "Capital"    300 3)
               (route "Capital" "City3"    400 3)
               (route "City1" "Town1_X"    50 2)
               (route "Town1_X" "City1"    150 2)
               (route "Town1_X" "TownX_2"  50 2)
               (route "TownX_2" "Town1_X"  150 2)
               (route "TownX_2" "City2"    50 3)
               (route "City2" "TownX_2"    150 3)
               (route "City2" "Town2_3"    50 2)
               (route "Town2_3" "City2"    150 2)
               (route "Town2_3" "City3"    50 3)
               (route "City3" "Town2_3"    150 2)))

(defn booking-future [route-map from to init-delay loop-delay]
  (future
    (Thread/sleep init-delay)
    (loop [bookings []]
      (Thread/sleep loop-delay)
      (let [booking (book-tickets route-map from to)]
        (if (booking :error)
          bookings
          (recur (conj bookings booking)))))))

(defn print-bookings [name ft]
  (println (str name ":") (count ft) "bookings")
  (doseq [booking ft]
    (println "price:" (booking :price) "path:" (booking :path))))

(defn run []
  (reset! transact-cnt 0)
  ;;try to tune timeouts in order to all the customers gain at least one booking 
  (let [f1 (booking-future spec1 "City1" "City3" 0 300),
        f2 (booking-future spec1 "City1" "City2" 100 200),
        f3 (booking-future spec1 "City2" "City3" 200 200)]
    (print-bookings "City1->City3:" @f1)
    (print-bookings "City1->City2:" @f2)
    (print-bookings "City2->City3:" @f3)
    ;;replace with you mechanism to monitor a number of transaction restarts
    (println "Total (re-)starts:" @transact-cnt)))

(run)
(shutdown-agents) ;; "авто-выход", удобней, чем ждать автовыключения 60 секунд (проверено о.о) или жать Ctrl-C
