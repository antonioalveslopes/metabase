(ns metabase.sync.field-values-test
  "Tests around the way Metabase syncs FieldValues, and sets the values of `field.has_field_values`."
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [metabase.models :refer [Field FieldValues Table]]
            [metabase.models.field-values :as field-values]
            [metabase.sync :as sync]
            [metabase.sync.util-test :as sync.util-test]
            [metabase.test :as mt]
            [metabase.test.data :as data]
            [metabase.test.data.one-off-dbs :as one-off-dbs]
            [toucan.db :as db]))

(defn- venues-price-field-values []
  (db/select-one-field :values FieldValues, :field_id (mt/id :venues :price), :type :full))

(defn- sync-database!' [step database]
  (let [{:keys [step-info task-history]} (sync.util-test/sync-database! step database)]
    [(sync.util-test/only-step-keys step-info)
     (:task_details task-history)]))

(deftest sync-recreate-field-values-test
  (testing "Test that when we delete FieldValues syncing the Table again will cause them to be re-created"
    (testing "Check that we have expected field values to start with"
      (is (= [1 2 3 4]
             (venues-price-field-values))))
    (testing "Delete the Field values, make sure they're gone"
      (db/delete! FieldValues :field_id (mt/id :venues :price))
      (is (= nil
             (venues-price-field-values))))
    (testing "After the delete, a field values should be created, the rest updated"
      (is (= (repeat 2 {:errors 0, :created 1, :updated 0, :deleted 0})
             (sync-database!' "update-field-values" (data/db)))))
    (testing "Now re-sync the table and make sure they're back"
      (sync/sync-table! (Table (mt/id :venues)))
      (is (= [1 2 3 4]
             (venues-price-field-values))))))

(deftest sync-should-update-test
  (testing "Test that syncing will cause FieldValues to be updated"
    (testing "Check that we have expected field values to start with"
      (is (= [1 2 3 4]
             (venues-price-field-values))))
    (testing "Update the FieldValues, remove one of the values that should be there"
      (db/update! FieldValues (db/select-one-id FieldValues :field_id (mt/id :venues :price) :type :full) :values [1 2 3])
      (is (= [1 2 3]
             (venues-price-field-values))))
    (testing "Now re-sync the table and validate the field values updated"
      (is (= (repeat 2 {:errors 0, :created 0, :updated 1, :deleted 0})
             (sync-database!' "update-field-values" (data/db)))))
    (testing "Make sure the value is back"
      (is (= [1 2 3 4]
             (venues-price-field-values))))))

(deftest sync-should-delete-expired-advanced-field-values-test
  (testing "Test that the expired Advanced FieldValues should be removed"
    (let [field-id           (mt/id :venues :price)
          expired-created-at (t/minus (t/offset-date-time) (t/days (inc field-values/advanced-field-values-max-age)))
          now                (t/offset-date-time)
          [expired-sandbox-id
           expired-linked-filter-id
           valid-sandbox-id
           valid-linked-filter-id
           old-full-id
           new-full-id]
          (db/simple-insert-many!
           FieldValues
           [;; expired sandbox fieldvalues
            {:field_id   field-id
             :type       "sandbox"
             :hash_key   "random-hash"
             :created_at expired-created-at
             :updated_at expired-created-at}
            ;; expired linked-filter fieldvalues
            {:field_id   field-id
             :type       "linked-filter"
             :hash_key   "random-hash"
             :created_at expired-created-at
             :updated_at expired-created-at}
            ;; valid sandbox fieldvalues
            {:field_id   field-id
             :type       "sandbox"
             :hash_key   "random-hash"
             :created_at now
             :updated_at now}
            ;; valid linked-filter fieldvalues
            {:field_id   field-id
             :type       "linked-filter"
             :hash_key   "random-hash"
             :created_at now
             :updated_at now}
            ;; old full fieldvalues
            {:field_id   field-id
             :type       "full"
             :hash_key   "random-hash"
             :created_at expired-created-at
             :updated_at expired-created-at}
            ;; new full fieldvalues
            {:field_id   field-id
             :type       "full"
             :created_at now
             :updated_at now}])]
      (is (= (repeat 2 {:deleted 2})
             (sync-database!' "delete-expired-advanced-field-values" (data/db))))
      (is (not (db/exists? FieldValues :id [:in [expired-sandbox-id expired-linked-filter-id]])))
      (is (db/exists? FieldValues :id [:in [valid-sandbox-id valid-linked-filter-id new-full-id old-full-id]])))))

(deftest auto-list-test
  ;; A Field with 50 values should get marked as `auto-list` on initial sync, because it should be 'list', but was
  ;; marked automatically, as opposed to explicitly (`list`)
  (one-off-dbs/with-blueberries-db
    ;; insert 50 rows & sync
    (one-off-dbs/insert-rows-and-sync! (range 50))
    (testing "has_field_values should be auto-list"
      (is (= :auto-list
             (db/select-one-field :has_field_values Field :id (mt/id :blueberries_consumed :num)))))

    (testing "... and it should also have some FieldValues"
      (is (= {:values                (range 50)
              :human_readable_values []}
             (into {} (db/select-one [FieldValues :values :human_readable_values]
                        :field_id (mt/id :blueberries_consumed :num))))))

    ;; Manually add an advanced field values to test whether or not it got deleted later
    (db/insert! FieldValues {:field_id (mt/id :blueberries_consumed :num)
                             :type :sandbox
                             :hash_key "random-key"})

    (testing (str "if the number grows past the threshold & we sync again it should get unmarked as auto-list and set "
                  "back to `nil` (#3215)\n")
      ;; now insert enough bloobs to put us over the limit and re-sync.
      (one-off-dbs/insert-rows-and-sync! (range 50 (+ 100 field-values/auto-list-cardinality-threshold)))
      (testing "has_field_values should have been set to nil."
        (is (= nil
               (db/select-one-field :has_field_values Field :id (mt/id :blueberries_consumed :num)))))

      (testing "All of its FieldValues should also get deleted."
        (is (= nil
               (db/select-one FieldValues
                 :field_id (mt/id :blueberries_consumed :num))))))))

(deftest list-test
  (testing (str "If we had explicitly marked the Field as `list` (instead of `auto-list`), adding extra values "
                "shouldn't change anything!")
    (one-off-dbs/with-blueberries-db
      ;; insert 50 bloobs & sync
      (one-off-dbs/insert-rows-and-sync! (range 50))
      ;; change has_field_values to list
      (db/update! Field (mt/id :blueberries_consumed :num) :has_field_values "list")
      ;; Manually add an advanced field values to test whether or not it got deleted later
      (db/insert! FieldValues {:field_id (mt/id :blueberries_consumed :num)
                               :type :sandbox
                               :hash_key "random-key"})
       ;; insert more bloobs & re-sync
     (one-off-dbs/insert-rows-and-sync! (range 50 (+ 100 field-values/auto-list-cardinality-threshold)))
     (testing "has_field_values shouldn't change"
       (is (= :list
              (db/select-one-field :has_field_values Field :id (mt/id :blueberries_consumed :num)))))
     (testing (str "it should still have FieldValues, and have new ones for the new Values. It should have 200 values "
                   "even though this is past the normal limit of 100 values!")
       (is (= {:values                (range 200)
               :human_readable_values []}
              (into {} (db/select-one [FieldValues :values :human_readable_values]
                         :field_id (mt/id :blueberries_consumed :num)
                         :type :full)))))
     (testing "The advanced field values of this field should be deleted"
       (is (= 0 (db/count FieldValues :field_id (mt/id :blueberries_consumed :num)
                          :type [:not= :full])))))))
